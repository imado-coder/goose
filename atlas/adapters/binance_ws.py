"""
Binance USD-M Futures adapter.
Streams per symbol: trade, depth@100ms, forceOrder.
REST polling: openInterest (30s), premiumIndex/fundingRate (30s).

NOTE: We subscribe to @trade (individual trades), NOT @aggTrade. Binance
silently withholds the @aggTrade stream from many datacenter IP ranges
(returns zero messages while @depth still flows). @trade is delivered
normally and carries the same fields plus a per-trade id ('t').

Book integrity: validates pu==last_u on every depth update (Binance futures spec).
On sequence gap → mark DEGRADED, resnapshot via REST, log gap, resume.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import AsyncIterator

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from adapters.base import EventSource
from core.config import Settings
from core.events import (
    BookDeltaPayload,
    BookLevel,
    BookSnapshotPayload,
    EventKind,
    FundingPayload,
    LiquidationPayload,
    MarketEvent,
    OIPayload,
    TradePayload,
)

log = logging.getLogger(__name__)

VENUE = "binance-perp"
_BACKOFF = [1, 2, 4, 8, 16, 30]


def _backoff(attempt: int) -> float:
    return _BACKOFF[min(attempt, len(_BACKOFF) - 1)]


# Type alias for the gap callback injected by main.py
GapCallback = Callable[[str, int, int], Awaitable[None]]  # (symbol, start_ns, end_ns)


class BinancePerpAdapter(EventSource):

    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=10_000)
        self._tasks: list[asyncio.Task] = []
        self._running = False

        # ── Book sequence state (per symbol) ──────────────────────────────────
        # Binance USD-M depth stream carries:
        #   U  = first update id in this event
        #   u  = last  update id in this event   ← we track this
        #   pu = last  update id in PREVIOUS event ← must equal our saved last_u
        # Invariant: pu_this == last_u_prev; any violation = DOM gap → resnapshot.
        self._book_last_u: dict[str, int] = {}       # symbol → last 'u' seen
        self._book_anchored: dict[str, bool] = {}    # symbol → first delta after snapshot consumed
        self._book_degraded: dict[str, bool] = {}    # symbol → is book DEGRADED
        self._gap_start_ns: dict[str, int] = {}      # symbol → when gap started

        # Optional async callback (symbol, gap_start_ns, gap_end_ns) → DB write
        self._gap_cb: GapCallback | None = None

    def set_gap_callback(self, cb: GapCallback) -> None:
        self._gap_cb = cb

    @property
    def name(self) -> str:
        return VENUE

    def _canonical(self, symbol: str) -> str:
        return self._s.canonical_map.get(symbol.upper(), symbol.upper())

    # ── Public interface ───────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._running = True
        for sym in self._s.symbols:
            self._tasks += [
                asyncio.create_task(self._ws_worker(sym),      name=f"ws:{sym}"),
                asyncio.create_task(self._oi_poller(sym),      name=f"oi:{sym}"),
                asyncio.create_task(self._funding_poller(sym), name=f"funding:{sym}"),
            ]
        log.info("BinancePerpAdapter started for symbols: %s", self._s.symbols)

    async def stream(self) -> AsyncIterator[MarketEvent]:
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                yield event
            except asyncio.TimeoutError:
                continue

    async def close(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("BinancePerpAdapter stopped")

    # ── WebSocket worker ───────────────────────────────────────────────────────

    async def _ws_worker(self, symbol: str) -> None:
        sym_lower = symbol.lower()
        streams = [
            f"{sym_lower}@trade",
            f"{sym_lower}@depth@100ms",
            f"{sym_lower}@forceOrder",
        ]
        url = f"{self._s.binance_ws_base}/stream?streams=" + "/".join(streams)
        attempt = 0

        while self._running:
            # FIX-2: Always resnapshot before streaming deltas — on first connect
            # AND after any reconnect (guarantees _book_last_u is anchored).
            await self._initial_snapshot(symbol)

            try:
                log.info("WS connecting: %s (attempt %d)", symbol, attempt)
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=2**22,
                ) as ws:
                    attempt = 0
                    async for raw in ws:
                        if not self._running:
                            return
                        try:
                            await self._dispatch(json.loads(raw), symbol)
                        except Exception as exc:
                            log.warning("WS dispatch error (%s): %s", symbol, exc)

            except ConnectionClosed as exc:
                log.warning("WS closed (%s): %s — reconnecting", symbol, exc)
            except Exception as exc:
                log.error("WS error (%s): %s — reconnecting", symbol, exc)

            if not self._running:
                return
            delay = _backoff(attempt)
            attempt += 1
            log.info("WS reconnect in %.0fs (attempt %d)", delay, attempt)
            await asyncio.sleep(delay)

    async def _dispatch(self, msg: dict, symbol: str) -> None:
        # Route on the event-type field 'e' inside the payload rather than the
        # combined-stream name: Binance lowercases the 'stream' field, and a
        # single-stream endpoint omits it entirely. 'e' is always present.
        data  = msg.get("data", msg)
        etype = data.get("e", "")
        ts_recv = time.time_ns()

        if etype in ("trade", "aggTrade"):
            self._enqueue(_parse_trade(data, self._canonical(symbol), ts_recv))

        elif etype == "depthUpdate":
            await self._handle_depth(data, symbol, ts_recv)

        elif etype == "forceOrder":
            self._enqueue(_parse_liquidation(data, self._canonical(symbol), ts_recv))

    # ── FIX-1: Strict pu/u sequence validation ─────────────────────────────────

    async def _handle_depth(self, data: dict, symbol: str, ts_recv: int) -> None:
        U  = data.get("U", 0)    # first update id in THIS event
        u  = data.get("u", 0)    # last  update id in THIS event
        pu = data.get("pu")      # last  update id in PREVIOUS event (Binance futures)

        last_u = self._book_last_u.get(symbol)

        # ── Anchor phase ──────────────────────────────────────────────────────
        # The first delta after a REST snapshot cannot satisfy pu==last_u (pu is
        # the prior *stream* event's id, unrelated to the snapshot). Per Binance
        # spec we anchor on the event whose range covers the snapshot id:
        #   drop while u < last_u, then accept the event with U <= last_u+1 <= u.
        if not self._book_anchored.get(symbol):
            if last_u is not None and u < last_u:
                return                              # stale: predates snapshot
            if last_u is not None and U > last_u + 1:
                # We missed the covering event → snapshot is already behind.
                self._book_last_u[symbol] = u
                self._book_anchored[symbol] = True
                if not self._book_degraded.get(symbol):
                    self._book_degraded[symbol] = True
                    self._gap_start_ns[symbol] = ts_recv
                    asyncio.create_task(self._resync_book(symbol), name=f"resync:{symbol}")
                return
            self._book_last_u[symbol] = u
            self._book_anchored[symbol] = True
            self._book_degraded[symbol] = False
            self._emit_book_delta(data, symbol, ts_recv, U, u)
            return

        # ── Steady state: strict pu==last_u contiguity ────────────────────────
        if pu != last_u:
            log.error(
                "DOM SEQ GAP on %s: pu=%s != last_u=%s → DEGRADED",
                symbol, pu, last_u,
            )
            if not self._book_degraded.get(symbol):
                self._book_degraded[symbol] = True
                self._gap_start_ns[symbol] = ts_recv
                # In-band resync: trades continue flowing; only depth is paused.
                asyncio.create_task(self._resync_book(symbol), name=f"resync:{symbol}")
            # Drop this delta — book is corrupt until resync completes.
            return

        self._book_last_u[symbol] = u

        # Skip deltas while DEGRADED (they arrive before resync completes).
        if self._book_degraded.get(symbol):
            return

        self._emit_book_delta(data, symbol, ts_recv, U, u)

    def _emit_book_delta(self, data: dict, symbol: str, ts_recv: int, U: int, u: int) -> None:
        ts_ex_ns = data.get("T", 0) * 1_000_000 or ts_recv
        self._enqueue(MarketEvent(
            ts_exchange=ts_ex_ns,
            ts_received=ts_recv,
            venue=VENUE,
            symbol=self._canonical(symbol),
            kind=EventKind.BOOK_DELTA,
            payload=BookDeltaPayload(
                bids=[BookLevel(price=float(p), qty=float(q)) for p, q in data.get("b", [])[:10]],
                asks=[BookLevel(price=float(p), qty=float(q)) for p, q in data.get("a", [])[:10]],
                first_update_id=U,
                last_update_id=u,
            ),
            seq=u,
        ))

    async def _resync_book(self, symbol: str) -> None:
        """Fetch REST snapshot, re-anchor sequence, clear DEGRADED flag."""
        log.info("Resyncing book for %s …", symbol)
        try:
            snap = await self.fetch_book_snapshot(symbol)
            self._enqueue(snap)
            gap_end_ns = time.time_ns()
            gap_start_ns = self._gap_start_ns.pop(symbol, gap_end_ns)
            gap_sec = (gap_end_ns - gap_start_ns) / 1e9
            self._book_degraded[symbol] = False
            log.info("Book resynced for %s (gap=%.2fs)", symbol, gap_sec)
            if self._gap_cb:
                await self._gap_cb(symbol, gap_start_ns, gap_end_ns)
        except Exception as exc:
            log.error("Book resync failed for %s: %s — will retry on next gap", symbol, exc)
            self._book_degraded[symbol] = False   # allow next violation to trigger again

    async def _initial_snapshot(self, symbol: str) -> None:
        """Take REST snapshot before connecting WS (or after reconnect)."""
        for _attempt in range(5):
            try:
                snap = await self.fetch_book_snapshot(symbol)
                self._enqueue(snap)
                self._book_degraded[symbol] = False
                log.info("Book snapshot anchored for %s (last_u=%d)",
                         symbol, self._book_last_u.get(symbol, 0))
                return
            except Exception as exc:
                log.warning("Snapshot attempt %d failed for %s: %s", _attempt + 1, symbol, exc)
                await asyncio.sleep(2 ** _attempt)
        log.error("Could not snapshot %s after 5 attempts — starting degraded", symbol)

    # ── REST pollers ───────────────────────────────────────────────────────────

    async def _oi_poller(self, symbol: str) -> None:
        # FIX-4: 30s (was 3s — no practical value in polling OI faster)
        url = f"{self._s.binance_rest_base}/fapi/v1/openInterest"
        async with httpx.AsyncClient(timeout=10) as client:
            while self._running:
                try:
                    r = await client.get(url, params={"symbol": symbol})
                    r.raise_for_status()
                    d = r.json()
                    ts_recv = time.time_ns()
                    self._enqueue(MarketEvent(
                        ts_exchange=int(d.get("time", 0)) * 1_000_000,
                        ts_received=ts_recv,
                        venue=VENUE,
                        symbol=self._canonical(symbol),
                        kind=EventKind.OI,
                        payload=OIPayload(
                            open_interest=float(d["openInterest"]),
                            open_interest_value=0.0,
                        ),
                    ))
                except Exception as exc:
                    log.warning("OI poll error (%s): %s", symbol, exc)
                await asyncio.sleep(30)   # ← FIX-4

    async def _funding_poller(self, symbol: str) -> None:
        url = f"{self._s.binance_rest_base}/fapi/v1/premiumIndex"
        async with httpx.AsyncClient(timeout=10) as client:
            while self._running:
                try:
                    r = await client.get(url, params={"symbol": symbol})
                    r.raise_for_status()
                    d = r.json()
                    ts_recv = time.time_ns()
                    self._enqueue(MarketEvent(
                        ts_exchange=int(d.get("time", 0)) * 1_000_000,
                        ts_received=ts_recv,
                        venue=VENUE,
                        symbol=self._canonical(symbol),
                        kind=EventKind.FUNDING,
                        payload=FundingPayload(
                            funding_rate=float(d.get("lastFundingRate", 0)),
                            next_funding_time_ms=int(d.get("nextFundingTime", 0)),
                        ),
                    ))
                except Exception as exc:
                    log.warning("Funding poll error (%s): %s", symbol, exc)
                await asyncio.sleep(30)

    # ── REST book snapshot ─────────────────────────────────────────────────────

    async def fetch_book_snapshot(self, symbol: str) -> MarketEvent:
        url = f"{self._s.binance_rest_base}/fapi/v1/depth"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, params={"symbol": symbol, "limit": 20})
            r.raise_for_status()
            d = r.json()
        ts_recv = time.time_ns()
        last_u = int(d["lastUpdateId"])
        self._book_last_u[symbol] = last_u         # anchor sequence tracker
        self._book_anchored[symbol] = False        # next delta re-anchors the book
        return MarketEvent(
            ts_exchange=ts_recv,
            ts_received=ts_recv,
            venue=VENUE,
            symbol=self._canonical(symbol),
            kind=EventKind.BOOK_SNAPSHOT,
            payload=BookSnapshotPayload(
                bids=[BookLevel(price=float(p), qty=float(q)) for p, q in d["bids"][:10]],
                asks=[BookLevel(price=float(p), qty=float(q)) for p, q in d["asks"][:10]],
                last_update_id=last_u,
            ),
            seq=last_u,
        )

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _enqueue(self, event: MarketEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            log.warning("Event queue full — dropping %s", event.kind)


# ── Pure parse helpers ──────────────────────────────────────────────────────────

def _parse_trade(data: dict, canonical: str, ts_recv: int) -> MarketEvent:
    # @trade carries 't' (trade id); @aggTrade carries 'a' (agg id). Accept both.
    trade_id = int(data.get("a", data.get("t", 0)))
    return MarketEvent(
        ts_exchange=data["T"] * 1_000_000,
        ts_received=ts_recv,
        venue=VENUE,
        symbol=canonical,
        kind=EventKind.TRADE,
        payload=TradePayload(
            price=float(data["p"]),
            qty=float(data["q"]),
            aggressor_side="sell" if data["m"] else "buy",
            trade_id=trade_id,
        ),
        seq=trade_id,
    )


def _parse_liquidation(data: dict, canonical: str, ts_recv: int) -> MarketEvent:
    order = data.get("o", data)
    ts_ex_ns = int(order.get("T", 0)) * 1_000_000 or ts_recv
    return MarketEvent(
        ts_exchange=ts_ex_ns,
        ts_received=ts_recv,
        venue=VENUE,
        symbol=canonical,
        kind=EventKind.LIQUIDATION,
        payload=LiquidationPayload(
            side="buy" if order.get("S") == "BUY" else "sell",
            price=float(order.get("p", 0)),
            qty=float(order.get("q", 0)),
            order_type=order.get("o", "MARKET"),
        ),
    )
