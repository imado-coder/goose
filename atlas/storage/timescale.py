from __future__ import annotations
import json, logging
from datetime import datetime, timezone
import asyncpg
from core.events import (
    BookDeltaPayload, EventKind, FundingPayload,
    LiquidationPayload, MarketEvent, OIPayload, TradePayload,
)
log = logging.getLogger(__name__)

def _dt(ns: int) -> datetime: return datetime.fromtimestamp(ns/1e9, tz=timezone.utc)

class TimescaleWriter:
    def __init__(self, dsn: str, min_size: int=2, max_size: int=10) -> None:
        self._dsn=dsn; self._min=min_size; self._max=max_size; self._pool=None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=self._min, max_size=self._max)
        log.info("TimescaleDB pool connected")

    async def close(self) -> None:
        if self._pool: await self._pool.close()

    async def write(self, event: MarketEvent) -> None:
        assert self._pool
        try:
            match event.kind:
                case EventKind.TRADE:                              await self._trade(event)
                case EventKind.BOOK_DELTA|EventKind.BOOK_SNAPSHOT: await self._book(event)
                case EventKind.OI:                                 await self._oi(event)
                case EventKind.FUNDING:                            await self._funding(event)
                case EventKind.LIQUIDATION:                        await self._liq(event)
        except Exception as exc: log.error("DB write error (kind=%s): %s", event.kind, exc)

    async def _trade(self, ev: MarketEvent) -> None:
        p: TradePayload = ev.payload  # type: ignore
        await self._pool.execute(
            "INSERT INTO trades (ts_exchange,ts_received,venue,symbol,price,qty,side,trade_id,seq) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",
            _dt(ev.ts_exchange),_dt(ev.ts_received),ev.venue,ev.symbol,p.price,p.qty,p.aggressor_side,p.trade_id,ev.seq)

    async def _book(self, ev: MarketEvent) -> None:
        p = ev.payload
        kind_str = "delta" if isinstance(p, BookDeltaPayload) else "snapshot"
        bids = json.dumps([{"price":l.price,"qty":l.qty} for l in p.bids])
        asks = json.dumps([{"price":l.price,"qty":l.qty} for l in p.asks])
        await self._pool.execute(
            "INSERT INTO book_top (ts_exchange,ts_received,venue,symbol,kind,bids,asks,last_update_id) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8)",
            _dt(ev.ts_exchange),_dt(ev.ts_received),ev.venue,ev.symbol,kind_str,bids,asks,p.last_update_id)

    async def _oi(self, ev: MarketEvent) -> None:
        p: OIPayload = ev.payload  # type: ignore
        await self._pool.execute(
            "INSERT INTO open_interest (ts_exchange,ts_received,venue,symbol,open_interest,open_interest_value) VALUES ($1,$2,$3,$4,$5,$6)",
            _dt(ev.ts_exchange),_dt(ev.ts_received),ev.venue,ev.symbol,p.open_interest,p.open_interest_value)

    async def _funding(self, ev: MarketEvent) -> None:
        p: FundingPayload = ev.payload  # type: ignore
        next_dt = datetime.fromtimestamp(p.next_funding_time_ms/1000, tz=timezone.utc)
        await self._pool.execute(
            "INSERT INTO funding (ts_exchange,ts_received,venue,symbol,funding_rate,next_funding_time) VALUES ($1,$2,$3,$4,$5,$6)",
            _dt(ev.ts_exchange),_dt(ev.ts_received),ev.venue,ev.symbol,p.funding_rate,next_dt)

    async def _liq(self, ev: MarketEvent) -> None:
        p: LiquidationPayload = ev.payload  # type: ignore
        await self._pool.execute(
            "INSERT INTO liquidations (ts_exchange,ts_received,venue,symbol,side,price,qty,order_type) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _dt(ev.ts_exchange),_dt(ev.ts_received),ev.venue,ev.symbol,p.side,p.price,p.qty,p.order_type)

    async def record_heartbeat(self, source: str, event_count: int, note: str="") -> None:
        await self._pool.execute("INSERT INTO heartbeats (source,event_count,note) VALUES ($1,$2,$3)", source,event_count,note)

    async def record_gap(self, source: str, symbol: str, gap_start: datetime, gap_sec: float) -> None:
        await self._pool.execute("INSERT INTO data_gaps (source,symbol,gap_start,gap_sec) VALUES ($1,$2,$3,$4)", source,symbol,gap_start,gap_sec)

    async def record_book_gap(self, symbol: str, start_ns: int, end_ns: int) -> None:
        s = datetime.fromtimestamp(start_ns/1e9, tz=timezone.utc)
        e = datetime.fromtimestamp(end_ns/1e9,   tz=timezone.utc)
        await self._pool.execute(
            "INSERT INTO data_gaps (source,symbol,gap_start,gap_end,gap_sec,resolved) VALUES ($1,$2,$3,$4,$5,TRUE)",
            "binance-perp:book",symbol,s,e,(end_ns-start_ns)/1e9)
