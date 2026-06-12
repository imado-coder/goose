"""
Binance USD-M Futures adapter — Sprint 0.
Streams: aggTrade + depth@100ms + forceOrder.
REST: OI every 30s, funding every 30s.
Book integrity: pu==last_u validation; DEGRADED + in-band resync on gap.
"""
from __future__ import annotations
import asyncio, json, logging, time
from collections.abc import Awaitable, Callable
from typing import AsyncIterator
import httpx, websockets
from websockets.exceptions import ConnectionClosed
from adapters.base import EventSource
from core.config import Settings
from core.events import (
    BookDeltaPayload, BookLevel, BookSnapshotPayload, EventKind,
    FundingPayload, LiquidationPayload, MarketEvent, OIPayload, TradePayload,
)

log = logging.getLogger(__name__)
VENUE = "binance-perp"
_BACKOFF = [1, 2, 4, 8, 16, 30]
GapCallback = Callable[[str, int, int], Awaitable[None]]

def _backoff(n: int) -> float: return _BACKOFF[min(n, len(_BACKOFF)-1)]

class BinancePerpAdapter(EventSource):
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=10_000)
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._book_last_u:   dict[str, int]  = {}
        self._book_degraded: dict[str, bool] = {}
        self._gap_start_ns:  dict[str, int]  = {}
        self._gap_cb: GapCallback | None = None

    def set_gap_callback(self, cb: GapCallback) -> None: self._gap_cb = cb
    @property
    def name(self) -> str: return VENUE
    def _canonical(self, s: str) -> str: return self._s.canonical_map.get(s.upper(), s.upper())

    async def connect(self) -> None:
        self._running = True
        for sym in self._s.symbols:
            self._tasks += [
                asyncio.create_task(self._ws_worker(sym),      name=f"ws:{sym}"),
                asyncio.create_task(self._oi_poller(sym),      name=f"oi:{sym}"),
                asyncio.create_task(self._funding_poller(sym), name=f"funding:{sym}"),
            ]
        log.info("BinancePerpAdapter started: %s", self._s.symbols)

    async def stream(self) -> AsyncIterator[MarketEvent]:
        while self._running:
            try: yield await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError: continue

    async def close(self) -> None:
        self._running = False
        for t in self._tasks: t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _ws_worker(self, symbol: str) -> None:
        sym_lower = symbol.lower()
        url = (f"{self._s.binance_ws_base}/stream?streams="
               f"{sym_lower}@aggTrade/{sym_lower}@depth@100ms/{sym_lower}@forceOrder")
        attempt = 0
        while self._running:
            await self._initial_snapshot(symbol)
            try:
                log.info("WS connecting %s (attempt %d)", symbol, attempt)
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, max_size=2**22) as ws:
                    attempt = 0
                    async for raw in ws:
                        if not self._running: return
                        try: await self._dispatch(json.loads(raw), symbol)
                        except Exception as exc: log.warning("dispatch error: %s", exc)
            except ConnectionClosed as exc: log.warning("WS closed (%s): %s", symbol, exc)
            except Exception as exc:        log.error("WS error (%s): %s", symbol, exc)
            if not self._running: return
            delay = _backoff(attempt); attempt += 1
            log.info("WS reconnect in %.0fs", delay)
            await asyncio.sleep(delay)

    async def _dispatch(self, msg: dict, symbol: str) -> None:
        stream = msg.get("stream", ""); data = msg.get("data", msg); ts = time.time_ns()
        if   "@aggTrade"  in stream: self._enqueue(_parse_trade(data, self._canonical(symbol), ts))
        elif "@depth"     in stream: await self._handle_depth(data, symbol, ts)
        elif "@forceOrder" in stream: self._enqueue(_parse_liq(data, self._canonical(symbol), ts))

    async def _handle_depth(self, data: dict, symbol: str, ts_recv: int) -> None:
        u = data.get("u", 0); pu = data.get("pu"); last_u = self._book_last_u.get(symbol)
        if last_u is not None and pu != last_u:
            log.error("DOM SEQ GAP %s: pu=%s != last_u=%d → DEGRADED", symbol, pu, last_u)
            if not self._book_degraded.get(symbol):
                self._book_degraded[symbol] = True; self._gap_start_ns[symbol] = ts_recv
                asyncio.create_task(self._resync_book(symbol), name=f"resync:{symbol}")
            return
        self._book_last_u[symbol] = u
        if self._book_degraded.get(symbol): return
        ts_ex = data.get("T", 0) * 1_000_000 or ts_recv
        self._enqueue(MarketEvent(
            ts_exchange=ts_ex, ts_received=ts_recv, venue=VENUE,
            symbol=self._canonical(symbol), kind=EventKind.BOOK_DELTA,
            payload=BookDeltaPayload(
                bids=[BookLevel(price=float(p),qty=float(q)) for p,q in data.get("b",[])[:10]],
                asks=[BookLevel(price=float(p),qty=float(q)) for p,q in data.get("a",[])[:10]],
                first_update_id=data.get("U",0), last_update_id=u,
            ), seq=u,
        ))

    async def _resync_book(self, symbol: str) -> None:
        try:
            snap = await self.fetch_book_snapshot(symbol); self._enqueue(snap)
            end = time.time_ns(); start = self._gap_start_ns.pop(symbol, end)
            self._book_degraded[symbol] = False
            log.info("Book resynced %s (gap=%.2fs)", symbol, (end-start)/1e9)
            if self._gap_cb: await self._gap_cb(symbol, start, end)
        except Exception as exc:
            log.error("Resync failed %s: %s", symbol, exc)
            self._book_degraded[symbol] = False

    async def _initial_snapshot(self, symbol: str) -> None:
        for i in range(5):
            try:
                snap = await self.fetch_book_snapshot(symbol); self._enqueue(snap)
                self._book_degraded[symbol] = False
                log.info("Snapshot anchored %s (last_u=%d)", symbol, self._book_last_u.get(symbol,0))
                return
            except Exception as exc:
                log.warning("Snapshot attempt %d failed %s: %s", i+1, symbol, exc)
                await asyncio.sleep(2**i)
        log.error("Could not snapshot %s after 5 attempts", symbol)

    async def _oi_poller(self, symbol: str) -> None:
        url = f"{self._s.binance_rest_base}/fapi/v1/openInterest"
        async with httpx.AsyncClient(timeout=10) as c:
            while self._running:
                try:
                    r = await c.get(url, params={"symbol": symbol}); r.raise_for_status(); d = r.json()
                    self._enqueue(MarketEvent(
                        ts_exchange=int(d.get("time",0))*1_000_000, ts_received=time.time_ns(),
                        venue=VENUE, symbol=self._canonical(symbol), kind=EventKind.OI,
                        payload=OIPayload(open_interest=float(d["openInterest"]), open_interest_value=0.0),
                    ))
                except Exception as exc: log.warning("OI poll error (%s): %s", symbol, exc)
                await asyncio.sleep(30)

    async def _funding_poller(self, symbol: str) -> None:
        url = f"{self._s.binance_rest_base}/fapi/v1/premiumIndex"
        async with httpx.AsyncClient(timeout=10) as c:
            while self._running:
                try:
                    r = await c.get(url, params={"symbol": symbol}); r.raise_for_status(); d = r.json()
                    self._enqueue(MarketEvent(
                        ts_exchange=int(d.get("time",0))*1_000_000, ts_received=time.time_ns(),
                        venue=VENUE, symbol=self._canonical(symbol), kind=EventKind.FUNDING,
                        payload=FundingPayload(
                            funding_rate=float(d.get("lastFundingRate",0)),
                            next_funding_time_ms=int(d.get("nextFundingTime",0)),
                        ),
                    ))
                except Exception as exc: log.warning("Funding poll error (%s): %s", symbol, exc)
                await asyncio.sleep(30)

    async def fetch_book_snapshot(self, symbol: str) -> MarketEvent:
        url = f"{self._s.binance_rest_base}/fapi/v1/depth"
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(url, params={"symbol": symbol, "limit": 20}); r.raise_for_status(); d = r.json()
        ts = time.time_ns(); last_u = int(d["lastUpdateId"])
        self._book_last_u[symbol] = last_u
        return MarketEvent(
            ts_exchange=ts, ts_received=ts, venue=VENUE, symbol=self._canonical(symbol),
            kind=EventKind.BOOK_SNAPSHOT,
            payload=BookSnapshotPayload(
                bids=[BookLevel(price=float(p),qty=float(q)) for p,q in d["bids"][:10]],
                asks=[BookLevel(price=float(p),qty=float(q)) for p,q in d["asks"][:10]],
                last_update_id=last_u,
            ), seq=last_u,
        )

    def _enqueue(self, event: MarketEvent) -> None:
        try: self._queue.put_nowait(event)
        except asyncio.QueueFull: log.warning("Queue full — dropping %s", event.kind)

def _parse_trade(data: dict, canonical: str, ts: int) -> MarketEvent:
    return MarketEvent(
        ts_exchange=data["T"]*1_000_000, ts_received=ts, venue=VENUE, symbol=canonical,
        kind=EventKind.TRADE,
        payload=TradePayload(price=float(data["p"]), qty=float(data["q"]),
                             aggressor_side="sell" if data["m"] else "buy",
                             trade_id=int(data["a"])),
        seq=int(data["a"]),
    )

def _parse_liq(data: dict, canonical: str, ts: int) -> MarketEvent:
    o = data.get("o", data)
    return MarketEvent(
        ts_exchange=int(o.get("T",0))*1_000_000 or ts, ts_received=ts,
        venue=VENUE, symbol=canonical, kind=EventKind.LIQUIDATION,
        payload=LiquidationPayload(
            side="buy" if o.get("S")=="BUY" else "sell",
            price=float(o.get("p",0)), qty=float(o.get("q",0)), order_type=o.get("o","MARKET"),
        ),
    )
