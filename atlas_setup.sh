#!/bin/bash
set -e
echo "═══ ATLAS Setup ═══"
mkdir -p barbaros-trader/{core,adapters,storage/migrations,ops/grafana/provisioning/{dashboards/atlas,datasources},tests}
cd barbaros-trader

cat > docker-compose.yml << 'ATLASEOF'
version: "3.9"

services:
  db:
    image: timescale/timescaledb:latest-pg16
    restart: unless-stopped
    environment:
      POSTGRES_DB: atlas
      POSTGRES_USER: atlas
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-atlasdev}
    volumes:
      - timescale_data:/var/lib/postgresql/data
      - ./storage/migrations:/docker-entrypoint-initdb.d:ro
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U atlas -d atlas"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 120s

  redis:
    image: redis:7-alpine
    restart: unless-stopped
    command: redis-server --save 60 1 --loglevel warning
    volumes:
      - redis_data:/data
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  app:
    build: .
    restart: unless-stopped
    env_file: .env
    environment:
      DATABASE_URL: postgresql://atlas:${POSTGRES_PASSWORD:-atlasdev}@db:5432/atlas
      REDIS_URL: redis://redis:6379
      PARQUET_DIR: /data/parquet
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_healthy
    ports:
      - "8000:8000"
    volumes:
      - parquet_data:/data/parquet
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8000/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 30s

  grafana:
    image: grafana/grafana:10.4.0
    restart: unless-stopped
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:-admin}
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_INSTALL_PLUGINS: grafana-clock-panel
    volumes:
      - grafana_data:/var/lib/grafana
      - ./ops/grafana/provisioning:/etc/grafana/provisioning:ro
    ports:
      - "3001:3000"
    depends_on:
      - db

volumes:
  timescale_data:
  redis_data:
  grafana_data:
  parquet_data:
ATLASEOF

cat > .env << 'ATLASEOF'
BINANCE_API_KEY=
BINANCE_API_SECRET=
POSTGRES_PASSWORD=atlasdev
DATABASE_URL=postgresql://atlas:atlasdev@db:5432/atlas
REDIS_URL=redis://redis:6379
GRAFANA_PASSWORD=admin
LOG_LEVEL=INFO
HEALTH_PORT=8000
GAP_THRESHOLD_SEC=30
HEARTBEAT_INTERVAL_SEC=5
ARCHIVE_HOUR_UTC=0
PARQUET_DIR=/data/parquet
SYMBOLS=["BTCUSDT"]
ATLASEOF

cat > Dockerfile << 'ATLASEOF'
FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[runtime]"
COPY . .
CMD ["python", "main.py"]
ATLASEOF

cat > pyproject.toml << 'ATLASEOF'
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "atlas"
version = "0.1.0"
description = "ATLAS v3 - Personal Institutional Trading OS"
requires-python = ">=3.11"

[project.optional-dependencies]
runtime = [
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "asyncpg>=0.29",
    "redis[hiredis]>=5.0",
    "websockets>=12.0",
    "httpx>=0.27",
    "fastapi>=0.111",
    "uvicorn[standard]>=0.30",
    "pandas>=2.2",
    "pyarrow>=16.0",
    "loguru>=0.7",
]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.23",
    "ruff>=0.4",
    "mypy>=1.10",
]

[tool.setuptools.packages.find]
where = ["."]
include = ["core*", "adapters*", "storage*", "ops*"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
ATLASEOF

cat > .gitignore << 'ATLASEOF'
.env
__pycache__/
*.pyc
.pytest_cache/
.mypy_cache/
dist/
*.egg-info/
/data/
*.parquet
ATLASEOF

cat > main.py << 'ATLASEOF'
from __future__ import annotations
import asyncio
import logging
import signal
import sys
import uvicorn
from adapters.binance_ws import BinancePerpAdapter
from core.config import get_settings
from ops.health import HealthMonitor, create_health_app
from storage.archiver import NightlyArchiver
from storage.timescale import TimescaleWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("atlas.main")

async def ingest_loop(adapter, writer, monitor):
    log.info("Ingest loop starting")
    async for event in adapter.stream():
        try:
            await writer.write(event)
            monitor.record(adapter.name, event.symbol, event.kind.value)
        except Exception as exc:
            log.error("Ingest loop error: %s", exc)

async def main():
    cfg = get_settings()
    adapter = BinancePerpAdapter(cfg)
    writer = TimescaleWriter(cfg.database_url, cfg.db_pool_min, cfg.db_pool_max)
    monitor = HealthMonitor(gap_threshold_sec=cfg.gap_threshold_sec, heartbeat_interval_sec=cfg.heartbeat_interval_sec)
    monitor.attach_writer(writer)
    archiver = NightlyArchiver(cfg.database_url, cfg.parquet_dir, cfg.archive_hour_utc)
    health_app = create_health_app(monitor)
    await writer.connect()
    await archiver.start()
    await adapter.connect()

    async def _on_book_gap(symbol, start_ns, end_ns):
        try:
            await writer.record_book_gap(symbol, start_ns, end_ns)
        except Exception as exc:
            log.warning("Gap record failed: %s", exc)

    adapter.set_gap_callback(_on_book_gap)
    uv_config = uvicorn.Config(health_app, host="0.0.0.0", port=cfg.health_port, log_level="warning")
    server = uvicorn.Server(uv_config)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown(*_):
        log.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    log.info("ATLAS Sprint-0 online. BTC-PERP streaming -> TimescaleDB")
    tasks = [
        asyncio.create_task(ingest_loop(adapter, writer, monitor), name="ingest"),
        asyncio.create_task(server.serve(), name="health-server"),
        asyncio.create_task(stop_event.wait(), name="stop-waiter"),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for t in done:
        if exc := t.exception():
            log.error("Task %s raised: %s", t.get_name(), exc)
    log.info("Shutting down...")
    server.should_exit = True
    await adapter.close()
    await archiver.stop()
    await writer.close()
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    log.info("ATLAS stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
ATLASEOF

cat > core/__init__.py << 'ATLASEOF'
ATLASEOF

cat > core/events.py << 'ATLASEOF'
from __future__ import annotations
import enum
import time
from typing import Annotated, Literal, Union
from pydantic import BaseModel, ConfigDict, Field

class EventKind(str, enum.Enum):
    TRADE = "TRADE"
    BOOK_DELTA = "BOOK_DELTA"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    OI = "OI"
    FUNDING = "FUNDING"
    LIQUIDATION = "LIQUIDATION"
    NEWS = "NEWS"
    CALENDAR = "CALENDAR"

class TradePayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.TRADE] = EventKind.TRADE
    price: float
    qty: float
    aggressor_side: str
    trade_id: int

class BookLevel(BaseModel):
    model_config = ConfigDict(frozen=True)
    price: float
    qty: float

class BookDeltaPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.BOOK_DELTA] = EventKind.BOOK_DELTA
    bids: list[BookLevel]
    asks: list[BookLevel]
    first_update_id: int
    last_update_id: int

class BookSnapshotPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.BOOK_SNAPSHOT] = EventKind.BOOK_SNAPSHOT
    bids: list[BookLevel]
    asks: list[BookLevel]
    last_update_id: int

class OIPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.OI] = EventKind.OI
    open_interest: float
    open_interest_value: float

class FundingPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.FUNDING] = EventKind.FUNDING
    funding_rate: float
    next_funding_time_ms: int

class LiquidationPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.LIQUIDATION] = EventKind.LIQUIDATION
    side: str
    price: float
    qty: float
    order_type: str

class NewsPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.NEWS] = EventKind.NEWS
    headline: str
    source: str
    url: str
    symbols: list[str]
    sentiment: float | None = None

class CalendarPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    kind: Literal[EventKind.CALENDAR] = EventKind.CALENDAR
    event_name: str
    impact: str
    currency: str
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None

AnyPayload = Annotated[
    Union[TradePayload, BookDeltaPayload, BookSnapshotPayload, OIPayload,
          FundingPayload, LiquidationPayload, NewsPayload, CalendarPayload],
    Field(discriminator="kind"),
]

class MarketEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    ts_exchange: int
    ts_received: int
    venue: str
    symbol: str
    kind: EventKind
    payload: AnyPayload
    seq: int = 0

    @staticmethod
    def now_ns() -> int:
        return time.time_ns()

    @property
    def latency_us(self) -> float:
        return (self.ts_received - self.ts_exchange) / 1_000
ATLASEOF

cat > core/config.py << 'ATLASEOF'
from __future__ import annotations
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore")
    database_url: str = "postgresql://atlas:atlasdev@localhost:5432/atlas"
    db_pool_min: int = 2
    db_pool_max: int = 10
    redis_url: str = "redis://localhost:6379"
    redis_stream_maxlen: int = 100_000
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_ws_base: str = "wss://fstream.binance.com"
    binance_rest_base: str = "https://fapi.binance.com"
    symbols: list[str] = Field(default=["BTCUSDT"])
    canonical_map: dict[str, str] = Field(default={"BTCUSDT": "BTC-PERP"})
    parquet_dir: str = "/data/parquet"
    archive_hour_utc: int = 0
    health_port: int = 8000
    heartbeat_interval_sec: float = 5.0
    gap_threshold_sec: float = 30.0
    log_level: str = "INFO"

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
ATLASEOF

cat > core/clock.py << 'ATLASEOF'
from __future__ import annotations
import time
from abc import ABC, abstractmethod

class Clock(ABC):
    @abstractmethod
    def now_ns(self) -> int: ...
    def now_ms(self) -> int: return self.now_ns() // 1_000_000
    def now_s(self) -> float: return self.now_ns() / 1e9

class LiveClock(Clock):
    def now_ns(self) -> int: return time.time_ns()

class ReplayClock(Clock):
    def __init__(self) -> None: self._ts_ns: int = 0
    def set(self, ts_ns: int) -> None: self._ts_ns = ts_ns
    def now_ns(self) -> int: return self._ts_ns

_live = LiveClock()
def live_clock() -> LiveClock: return _live
ATLASEOF

cat > core/bus.py << 'ATLASEOF'
from __future__ import annotations
import json
import logging
from typing import AsyncIterator
import redis.asyncio as aioredis
from core.events import MarketEvent

log = logging.getLogger(__name__)
STREAM_PREFIX = "atlas:events"

def stream_name(venue: str, kind: str) -> str:
    return f"{STREAM_PREFIX}:{venue}:{kind.lower()}"

class EventBus:
    def __init__(self, redis_url: str, maxlen: int = 100_000) -> None:
        self._url = redis_url
        self._maxlen = maxlen
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(self._url, decode_responses=True)
        await self._client.ping()

    async def close(self) -> None:
        if self._client: await self._client.aclose()

    async def publish(self, event: MarketEvent) -> None:
        assert self._client
        key = stream_name(event.venue, event.kind.value)
        data = {"ts_exchange": str(event.ts_exchange), "ts_received": str(event.ts_received),
                "venue": event.venue, "symbol": event.symbol, "kind": event.kind.value,
                "payload": event.payload.model_dump_json(), "seq": str(event.seq)}
        await self._client.xadd(key, data, maxlen=self._maxlen, approximate=True)
ATLASEOF

cat > adapters/__init__.py << 'ATLASEOF'
ATLASEOF

cat > adapters/base.py << 'ATLASEOF'
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import AsyncIterator
from core.events import MarketEvent

class EventSource(ABC):
    @abstractmethod
    async def connect(self) -> None: ...
    @abstractmethod
    async def stream(self) -> AsyncIterator[MarketEvent]: ...
    @abstractmethod
    async def close(self) -> None: ...
    @property
    @abstractmethod
    def name(self) -> str: ...
ATLASEOF

cat > adapters/binance_ws.py << 'ATLASEOF'
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
from core.events import (BookDeltaPayload, BookLevel, BookSnapshotPayload,
    EventKind, FundingPayload, LiquidationPayload, MarketEvent, OIPayload, TradePayload)

log = logging.getLogger(__name__)
VENUE = "binance-perp"
_BACKOFF = [1, 2, 4, 8, 16, 30]

def _backoff(attempt: int) -> float:
    return _BACKOFF[min(attempt, len(_BACKOFF) - 1)]

GapCallback = Callable[[str, int, int], Awaitable[None]]

class BinancePerpAdapter(EventSource):
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=10_000)
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._book_last_u: dict[str, int] = {}
        self._book_anchored: dict[str, bool] = {}
        self._book_degraded: dict[str, bool] = {}
        self._gap_start_ns: dict[str, int] = {}
        self._gap_cb: GapCallback | None = None

    def set_gap_callback(self, cb: GapCallback) -> None:
        self._gap_cb = cb

    @property
    def name(self) -> str:
        return VENUE

    def _canonical(self, symbol: str) -> str:
        return self._s.canonical_map.get(symbol.upper(), symbol.upper())

    async def connect(self) -> None:
        self._running = True
        for sym in self._s.symbols:
            self._tasks += [
                asyncio.create_task(self._ws_worker(sym), name=f"ws:{sym}"),
                asyncio.create_task(self._oi_poller(sym), name=f"oi:{sym}"),
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

    async def _ws_worker(self, symbol: str) -> None:
        sym_lower = symbol.lower()
        streams = [f"{sym_lower}@trade", f"{sym_lower}@depth@100ms", f"{sym_lower}@forceOrder"]
        url = f"{self._s.binance_ws_base}/stream?streams=" + "/".join(streams)
        attempt = 0
        while self._running:
            await self._initial_snapshot(symbol)
            try:
                log.info("WS connecting: %s (attempt %d)", symbol, attempt)
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, max_size=2**22) as ws:
                    attempt = 0
                    async for raw in ws:
                        if not self._running:
                            return
                        try:
                            await self._dispatch(json.loads(raw), symbol)
                        except Exception as exc:
                            log.warning("WS dispatch error (%s): %s", symbol, exc)
            except ConnectionClosed as exc:
                log.warning("WS closed (%s): %s - reconnecting", symbol, exc)
            except Exception as exc:
                log.error("WS error (%s): %s - reconnecting", symbol, exc)
            if not self._running:
                return
            delay = _backoff(attempt)
            attempt += 1
            await asyncio.sleep(delay)

    async def _dispatch(self, msg: dict, symbol: str) -> None:
        data = msg.get("data", msg)
        etype = data.get("e", "")
        ts_recv = time.time_ns()
        if etype in ("trade", "aggTrade"):
            self._enqueue(_parse_trade(data, self._canonical(symbol), ts_recv))
        elif etype == "depthUpdate":
            await self._handle_depth(data, symbol, ts_recv)
        elif etype == "forceOrder":
            self._enqueue(_parse_liquidation(data, self._canonical(symbol), ts_recv))

    async def _handle_depth(self, data: dict, symbol: str, ts_recv: int) -> None:
        U = data.get("U", 0)
        u = data.get("u", 0)
        pu = data.get("pu")
        last_u = self._book_last_u.get(symbol)

        if not self._book_anchored.get(symbol):
            if last_u is not None and u < last_u:
                return
            if last_u is not None and U > last_u + 1:
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

        if pu != last_u:
            log.error("DOM SEQ GAP on %s: pu=%s != last_u=%s -> DEGRADED", symbol, pu, last_u)
            if not self._book_degraded.get(symbol):
                self._book_degraded[symbol] = True
                self._gap_start_ns[symbol] = ts_recv
                asyncio.create_task(self._resync_book(symbol), name=f"resync:{symbol}")
            return
        self._book_last_u[symbol] = u
        if self._book_degraded.get(symbol):
            return
        self._emit_book_delta(data, symbol, ts_recv, U, u)

    def _emit_book_delta(self, data: dict, symbol: str, ts_recv: int, U: int, u: int) -> None:
        ts_ex_ns = data.get("T", 0) * 1_000_000 or ts_recv
        self._enqueue(MarketEvent(
            ts_exchange=ts_ex_ns, ts_received=ts_recv, venue=VENUE,
            symbol=self._canonical(symbol), kind=EventKind.BOOK_DELTA,
            payload=BookDeltaPayload(
                bids=[BookLevel(price=float(p), qty=float(q)) for p, q in data.get("b", [])[:10]],
                asks=[BookLevel(price=float(p), qty=float(q)) for p, q in data.get("a", [])[:10]],
                first_update_id=U, last_update_id=u,
            ), seq=u,
        ))

    async def _resync_book(self, symbol: str) -> None:
        log.info("Resyncing book for %s ...", symbol)
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
            log.error("Book resync failed for %s: %s", symbol, exc)
            self._book_degraded[symbol] = False

    async def _initial_snapshot(self, symbol: str) -> None:
        for _attempt in range(5):
            try:
                snap = await self.fetch_book_snapshot(symbol)
                self._enqueue(snap)
                self._book_degraded[symbol] = False
                log.info("Book snapshot anchored for %s (last_u=%d)", symbol, self._book_last_u.get(symbol, 0))
                return
            except Exception as exc:
                log.warning("Snapshot attempt %d failed for %s: %s", _attempt + 1, symbol, exc)
                await asyncio.sleep(2 ** _attempt)
        log.error("Could not snapshot %s after 5 attempts", symbol)

    async def _oi_poller(self, symbol: str) -> None:
        url = f"{self._s.binance_rest_base}/fapi/v1/openInterest"
        async with httpx.AsyncClient(timeout=10) as client:
            while self._running:
                try:
                    r = await client.get(url, params={"symbol": symbol})
                    r.raise_for_status()
                    d = r.json()
                    ts_recv = time.time_ns()
                    self._enqueue(MarketEvent(
                        ts_exchange=int(d.get("time", 0)) * 1_000_000, ts_received=ts_recv,
                        venue=VENUE, symbol=self._canonical(symbol), kind=EventKind.OI,
                        payload=OIPayload(open_interest=float(d["openInterest"]), open_interest_value=0.0),
                    ))
                except Exception as exc:
                    log.warning("OI poll error (%s): %s", symbol, exc)
                await asyncio.sleep(30)

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
                        ts_exchange=int(d.get("time", 0)) * 1_000_000, ts_received=ts_recv,
                        venue=VENUE, symbol=self._canonical(symbol), kind=EventKind.FUNDING,
                        payload=FundingPayload(
                            funding_rate=float(d.get("lastFundingRate", 0)),
                            next_funding_time_ms=int(d.get("nextFundingTime", 0)),
                        ),
                    ))
                except Exception as exc:
                    log.warning("Funding poll error (%s): %s", symbol, exc)
                await asyncio.sleep(30)

    async def fetch_book_snapshot(self, symbol: str) -> MarketEvent:
        url = f"{self._s.binance_rest_base}/fapi/v1/depth"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, params={"symbol": symbol, "limit": 20})
            r.raise_for_status()
            d = r.json()
        ts_recv = time.time_ns()
        last_u = int(d["lastUpdateId"])
        self._book_last_u[symbol] = last_u
        self._book_anchored[symbol] = False
        return MarketEvent(
            ts_exchange=ts_recv, ts_received=ts_recv, venue=VENUE,
            symbol=self._canonical(symbol), kind=EventKind.BOOK_SNAPSHOT,
            payload=BookSnapshotPayload(
                bids=[BookLevel(price=float(p), qty=float(q)) for p, q in d["bids"][:10]],
                asks=[BookLevel(price=float(p), qty=float(q)) for p, q in d["asks"][:10]],
                last_update_id=last_u,
            ), seq=last_u,
        )

    def _enqueue(self, event: MarketEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            log.warning("Event queue full - dropping %s", event.kind)

def _parse_trade(data: dict, canonical: str, ts_recv: int) -> MarketEvent:
    trade_id = int(data.get("a", data.get("t", 0)))
    return MarketEvent(
        ts_exchange=data["T"] * 1_000_000, ts_received=ts_recv, venue=VENUE, symbol=canonical,
        kind=EventKind.TRADE,
        payload=TradePayload(price=float(data["p"]), qty=float(data["q"]),
            aggressor_side="sell" if data["m"] else "buy", trade_id=trade_id),
        seq=trade_id,
    )

def _parse_liquidation(data: dict, canonical: str, ts_recv: int) -> MarketEvent:
    order = data.get("o", data)
    ts_ex_ns = int(order.get("T", 0)) * 1_000_000 or ts_recv
    return MarketEvent(
        ts_exchange=ts_ex_ns, ts_received=ts_recv, venue=VENUE, symbol=canonical,
        kind=EventKind.LIQUIDATION,
        payload=LiquidationPayload(side="buy" if order.get("S") == "BUY" else "sell",
            price=float(order.get("p", 0)), qty=float(order.get("q", 0)), order_type=order.get("o", "MARKET")),
    )
ATLASEOF

cat > storage/__init__.py << 'ATLASEOF'
ATLASEOF

cat > storage/timescale.py << 'ATLASEOF'
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
import asyncpg
from core.events import (BookDeltaPayload, BookSnapshotPayload, EventKind,
    FundingPayload, LiquidationPayload, MarketEvent, OIPayload, TradePayload)

log = logging.getLogger(__name__)

def _ns_to_dt(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)

class TimescaleWriter:
    def __init__(self, dsn: str, min_size: int = 2, max_size: int = 10) -> None:
        self._dsn = dsn
        self._min = min_size
        self._max = max_size
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=self._min, max_size=self._max)
        log.info("TimescaleDB pool connected (min=%d max=%d)", self._min, self._max)

    async def close(self) -> None:
        if self._pool: await self._pool.close()

    async def write(self, event: MarketEvent) -> None:
        assert self._pool
        try:
            match event.kind:
                case EventKind.TRADE: await self._write_trade(event)
                case EventKind.BOOK_DELTA | EventKind.BOOK_SNAPSHOT: await self._write_book(event)
                case EventKind.OI: await self._write_oi(event)
                case EventKind.FUNDING: await self._write_funding(event)
                case EventKind.LIQUIDATION: await self._write_liquidation(event)
        except Exception as exc:
            log.error("DB write error (kind=%s): %s", event.kind, exc)

    async def _write_trade(self, ev: MarketEvent) -> None:
        p: TradePayload = ev.payload
        await self._pool.execute(
            "INSERT INTO trades (ts_exchange,ts_received,venue,symbol,price,qty,side,trade_id,seq) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",
            _ns_to_dt(ev.ts_exchange), _ns_to_dt(ev.ts_received), ev.venue, ev.symbol, p.price, p.qty, p.aggressor_side, p.trade_id, ev.seq,
        )

    async def _write_book(self, ev: MarketEvent) -> None:
        p = ev.payload
        kind_str = "delta" if isinstance(p, BookDeltaPayload) else "snapshot"
        last_id = p.last_update_id
        bids_json = json.dumps([{"price": l.price, "qty": l.qty} for l in p.bids])
        asks_json = json.dumps([{"price": l.price, "qty": l.qty} for l in p.asks])
        await self._pool.execute(
            "INSERT INTO book_top (ts_exchange,ts_received,venue,symbol,kind,bids,asks,last_update_id) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8)",
            _ns_to_dt(ev.ts_exchange), _ns_to_dt(ev.ts_received), ev.venue, ev.symbol, kind_str, bids_json, asks_json, last_id,
        )

    async def _write_oi(self, ev: MarketEvent) -> None:
        p: OIPayload = ev.payload
        await self._pool.execute(
            "INSERT INTO open_interest (ts_exchange,ts_received,venue,symbol,open_interest,open_interest_value) VALUES ($1,$2,$3,$4,$5,$6)",
            _ns_to_dt(ev.ts_exchange), _ns_to_dt(ev.ts_received), ev.venue, ev.symbol, p.open_interest, p.open_interest_value,
        )

    async def _write_funding(self, ev: MarketEvent) -> None:
        p: FundingPayload = ev.payload
        next_dt = datetime.fromtimestamp(p.next_funding_time_ms / 1000, tz=timezone.utc)
        await self._pool.execute(
            "INSERT INTO funding (ts_exchange,ts_received,venue,symbol,funding_rate,next_funding_time) VALUES ($1,$2,$3,$4,$5,$6)",
            _ns_to_dt(ev.ts_exchange), _ns_to_dt(ev.ts_received), ev.venue, ev.symbol, p.funding_rate, next_dt,
        )

    async def _write_liquidation(self, ev: MarketEvent) -> None:
        p: LiquidationPayload = ev.payload
        await self._pool.execute(
            "INSERT INTO liquidations (ts_exchange,ts_received,venue,symbol,side,price,qty,order_type) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            _ns_to_dt(ev.ts_exchange), _ns_to_dt(ev.ts_received), ev.venue, ev.symbol, p.side, p.price, p.qty, p.order_type,
        )

    async def record_heartbeat(self, source: str, event_count: int, note: str = "") -> None:
        await self._pool.execute("INSERT INTO heartbeats (source,event_count,note) VALUES ($1,$2,$3)", source, event_count, note)

    async def record_gap(self, source: str, symbol: str, gap_start: datetime, gap_sec: float) -> None:
        await self._pool.execute("INSERT INTO data_gaps (source,symbol,gap_start,gap_sec) VALUES ($1,$2,$3,$4)", source, symbol, gap_start, gap_sec)

    async def record_book_gap(self, symbol: str, gap_start_ns: int, gap_end_ns: int) -> None:
        start_dt = datetime.fromtimestamp(gap_start_ns / 1e9, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(gap_end_ns / 1e9, tz=timezone.utc)
        gap_sec = (gap_end_ns - gap_start_ns) / 1e9
        await self._pool.execute(
            "INSERT INTO data_gaps (source,symbol,gap_start,gap_end,gap_sec,resolved) VALUES ($1,$2,$3,$4,$5,TRUE)",
            "binance-perp:book", symbol, start_dt, end_dt, gap_sec,
        )
ATLASEOF

cat > storage/archiver.py << 'ATLASEOF'
from __future__ import annotations
import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import asyncpg
import pandas as pd

log = logging.getLogger(__name__)
TABLES = ["trades", "book_top", "open_interest", "funding", "liquidations"]

class NightlyArchiver:
    def __init__(self, dsn: str, parquet_dir: str, archive_hour_utc: int = 0) -> None:
        self._dsn = dsn
        self._dir = Path(parquet_dir)
        self._hour = archive_hour_utc
        self._pool: asyncpg.Pool | None = None
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=2)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="archiver")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._pool: await self._pool.close()

    async def _loop(self) -> None:
        while self._running:
            now = datetime.now(timezone.utc)
            next_run = now.replace(hour=self._hour, minute=0, second=0, microsecond=0)
            if next_run <= now: next_run += timedelta(days=1)
            await asyncio.sleep((next_run - now).total_seconds())
            if not self._running: break
            await self.archive_date((datetime.now(timezone.utc) - timedelta(days=1)).date())

    async def archive_date(self, day: date) -> None:
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        for table in TABLES:
            try: await self._archive_table(table, start, end, day)
            except Exception as exc: log.error("Archive failed for %s/%s: %s", table, day, exc)

    async def _archive_table(self, table: str, start: datetime, end: datetime, day: date) -> None:
        rows = await self._pool.fetch(f"SELECT * FROM {table} WHERE ts_exchange >= $1 AND ts_exchange < $2", start, end)
        if not rows: return
        df = pd.DataFrame([dict(r) for r in rows])
        out_dir = self._dir / table
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{day}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow", compression="snappy")
        log.info("Archived %s/%s: %d rows", table, day, len(df))
ATLASEOF

cat > storage/migrations/001_initial.sql << 'ATLASEOF'
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE IF NOT EXISTS trades (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL, qty DOUBLE PRECISION NOT NULL,
    side TEXT NOT NULL, trade_id BIGINT NOT NULL, seq BIGINT DEFAULT 0
);
SELECT create_hypertable('trades', 'ts_exchange', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS trades_sym_ts ON trades (symbol, ts_exchange DESC);

CREATE TABLE IF NOT EXISTS book_top (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL, kind TEXT NOT NULL,
    bids JSONB NOT NULL, asks JSONB NOT NULL, last_update_id BIGINT NOT NULL
);
SELECT create_hypertable('book_top', 'ts_exchange', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS book_sym_ts ON book_top (symbol, ts_exchange DESC);

CREATE TABLE IF NOT EXISTS open_interest (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    open_interest DOUBLE PRECISION NOT NULL, open_interest_value DOUBLE PRECISION NOT NULL DEFAULT 0
);
SELECT create_hypertable('open_interest', 'ts_exchange', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS funding (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    funding_rate DOUBLE PRECISION NOT NULL, next_funding_time TIMESTAMPTZ NOT NULL
);
SELECT create_hypertable('funding', 'ts_exchange', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS liquidations (
    ts_exchange TIMESTAMPTZ NOT NULL, ts_received TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL, symbol TEXT NOT NULL,
    side TEXT NOT NULL, price DOUBLE PRECISION NOT NULL, qty DOUBLE PRECISION NOT NULL, order_type TEXT NOT NULL
);
SELECT create_hypertable('liquidations', 'ts_exchange', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS liq_sym_ts ON liquidations (symbol, ts_exchange DESC);

CREATE TABLE IF NOT EXISTS heartbeats (
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL, event_count BIGINT NOT NULL DEFAULT 0, note TEXT
);
SELECT create_hypertable('heartbeats', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS data_gaps (
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL, symbol TEXT NOT NULL,
    gap_start TIMESTAMPTZ NOT NULL, gap_end TIMESTAMPTZ,
    gap_sec DOUBLE PRECISION, resolved BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', ts_exchange) AS bucket, symbol, venue,
    first(price, ts_exchange) AS open, max(price) AS high, min(price) AS low,
    last(price, ts_exchange) AS close, sum(qty) AS volume,
    sum(CASE WHEN side='buy' THEN qty ELSE 0 END) - sum(CASE WHEN side='sell' THEN qty ELSE 0 END) AS delta
FROM trades GROUP BY bucket, symbol, venue WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1m',
    start_offset => INTERVAL '10 minutes', end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute', if_not_exists => TRUE);

SELECT add_compression_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('book_top', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('open_interest', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('funding', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('liquidations', INTERVAL '7 days', if_not_exists => TRUE);
ATLASEOF

cat > ops/__init__.py << 'ATLASEOF'
ATLASEOF

cat > ops/health.py << 'ATLASEOF'
from __future__ import annotations
import asyncio
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

log = logging.getLogger(__name__)

class GapDetector:
    def __init__(self, threshold_sec: float = 30.0) -> None:
        self._threshold = threshold_sec
        self._last_seen: dict[tuple[str, str], float] = {}
        self._in_gap: set[tuple[str, str]] = set()

    def record(self, source: str, symbol: str) -> None:
        key = (source, symbol)
        self._last_seen[key] = time.monotonic()
        self._in_gap.discard(key)

    def check(self) -> list[dict]:
        now = time.monotonic()
        alerts = []
        for key, last in self._last_seen.items():
            age = now - last
            if age > self._threshold:
                source, symbol = key
                if key not in self._in_gap:
                    self._in_gap.add(key)
                    log.warning("DATA GAP: %s/%s — no events for %.1fs", source, symbol, age)
                alerts.append({"source": source, "symbol": symbol, "age_sec": round(age, 1)})
        return alerts

    def status(self) -> dict[str, Any]:
        now = time.monotonic()
        return {k[0]+"/"+k[1]: {"last_seen_sec_ago": round(now-v,1), "gap": k in self._in_gap} for k,v in self._last_seen.items()}

class HealthMonitor:
    def __init__(self, gap_threshold_sec: float = 30.0, heartbeat_interval_sec: float = 5.0) -> None:
        self._gap = GapDetector(gap_threshold_sec)
        self._hb_interval = heartbeat_interval_sec
        self._counts: dict[str, int] = defaultdict(int)
        self._kind_counts: dict[str, int] = defaultdict(int)
        self._start_time = time.time()
        self._writer = None
        self._task: asyncio.Task | None = None
        self._running = False

    def attach_writer(self, writer: Any) -> None: self._writer = writer

    def record(self, source: str, symbol: str, kind: str) -> None:
        self._gap.record(source, symbol)
        self._counts[source] += 1
        self._kind_counts[kind] += 1

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="health-loop")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._hb_interval)
            gaps = self._gap.check()
            if self._writer:
                for source, count in list(self._counts.items()):
                    try: await self._writer.record_heartbeat(source, count)
                    except Exception as exc: log.warning("Heartbeat write failed: %s", exc)
                    for g in gaps:
                        try: await self._writer.record_gap(g["source"], g["symbol"], datetime.now(timezone.utc), g["age_sec"])
                        except Exception: pass

    def get_status(self) -> dict[str, Any]:
        return {
            "status": "ok" if not self._gap.check() else "degraded",
            "uptime_sec": round(time.time() - self._start_time),
            "event_counts": dict(self._counts),
            "kind_counts": dict(self._kind_counts),
            "sources": self._gap.status(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

_monitor: HealthMonitor | None = None

def create_health_app(monitor: HealthMonitor) -> FastAPI:
    global _monitor
    _monitor = monitor

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await monitor.start()
        yield
        await monitor.stop()

    app = FastAPI(title="ATLAS Health", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict:
        return _monitor.get_status()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        s = _monitor.get_status()
        lines = [f'atlas_uptime_seconds {s["uptime_sec"]}']
        for src, cnt in s["event_counts"].items():
            lines.append(f'atlas_events_total{{source="{src.replace("-","_")}"}} {cnt}')
        for kind, cnt in s["kind_counts"].items():
            lines.append(f'atlas_events_by_kind{{kind="{kind}"}} {cnt}')
        return "\n".join(lines) + "\n"

    return app
ATLASEOF

cat > ops/grafana/provisioning/dashboards/provider.yaml << 'ATLASEOF'
apiVersion: 1
providers:
  - name: ATLAS
    orgId: 1
    folder: ATLAS
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards/atlas
ATLASEOF

cat > ops/grafana/provisioning/datasources/timescale.yaml << 'ATLASEOF'
apiVersion: 1
datasources:
  - name: TimescaleDB
    type: postgres
    url: db:5432
    user: atlas
    secureJsonData:
      password: atlasdev
    jsonData:
      database: atlas
      sslmode: disable
      maxOpenConns: 5
      maxIdleConns: 2
      connMaxLifetime: 14400
      postgresVersion: 1600
      timescaledb: true
    isDefault: true
    editable: true
ATLASEOF

cat > ops/grafana/provisioning/dashboards/atlas/sprint0.json << 'ATLASEOF'
{"title":"ATLAS Sprint-0","uid":"atlas-s0","timezone":"UTC","refresh":"10s","time":{"from":"now-1h","to":"now"},"panels":[{"id":1,"title":"Trades / minute (BTC-PERP)","type":"timeseries","gridPos":{"x":0,"y":0,"w":12,"h":8},"targets":[{"datasource":"TimescaleDB","rawSql":"SELECT time_bucket('1 minute', ts_exchange) AS time, count(*) AS trades FROM trades WHERE $__timeFilter(ts_exchange) AND symbol='BTC-PERP' GROUP BY 1 ORDER BY 1","format":"time_series"}]},{"id":2,"title":"Trade Volume / minute","type":"timeseries","gridPos":{"x":12,"y":0,"w":12,"h":8},"targets":[{"datasource":"TimescaleDB","rawSql":"SELECT time_bucket('1 minute', ts_exchange) AS time, sum(qty) AS volume FROM trades WHERE $__timeFilter(ts_exchange) AND symbol='BTC-PERP' GROUP BY 1 ORDER BY 1","format":"time_series"}]},{"id":7,"title":"Heartbeats (last 1h)","type":"stat","gridPos":{"x":0,"y":8,"w":6,"h":4},"targets":[{"datasource":"TimescaleDB","rawSql":"SELECT count(*) AS heartbeats FROM heartbeats WHERE ts > NOW() - INTERVAL '1 hour'","format":"table"}]},{"id":8,"title":"Active Data Gaps","type":"stat","gridPos":{"x":6,"y":8,"w":6,"h":4},"fieldConfig":{"defaults":{"thresholds":{"steps":[{"color":"green","value":0},{"color":"red","value":1}]}}},"targets":[{"datasource":"TimescaleDB","rawSql":"SELECT count(*) AS gaps FROM data_gaps WHERE resolved=false AND detected_at > NOW() - INTERVAL '1 hour'","format":"table"}]},{"id":9,"title":"Total Trades","type":"stat","gridPos":{"x":12,"y":8,"w":6,"h":4},"targets":[{"datasource":"TimescaleDB","rawSql":"SELECT count(*) AS total FROM trades WHERE symbol='BTC-PERP'","format":"table"}]}]}
ATLASEOF

cat > tests/__init__.py << 'ATLASEOF'
ATLASEOF

cd ..
echo "All files created successfully"
echo "Starting ATLAS with Docker Compose..."
cd barbaros-trader

echo "Stage 1: starting db + redis ..."
sudo docker compose up -d db redis </dev/null

echo "Stage 2: waiting for TimescaleDB to become healthy ..."
for i in $(seq 1 60); do
  if sudo docker inspect -f '{{.State.Health.Status}}' barbaros-trader-db-1 </dev/null 2>/dev/null | grep -q healthy; then
    echo "  db is healthy (after $((i*5))s)"
    break
  fi
  sleep 5
done

echo "Stage 3: starting app + grafana ..."
sudo docker compose up -d </dev/null

echo "Stage 4: verifying app container is up (retry if needed) ..."
for i in $(seq 1 12); do
  state=$(sudo docker inspect -f '{{.State.Status}}' barbaros-trader-app-1 </dev/null 2>/dev/null || echo missing)
  if [ "$state" = "running" ]; then
    echo "  app container running"
    break
  fi
  echo "  app not running yet (state=$state) - re-running compose up"
  sudo docker compose up -d </dev/null
  sleep 10
done

echo "=== docker compose ps ==="
sudo docker compose ps </dev/null
echo "=== app logs (last 30) ==="
sudo docker compose logs app --tail=30 </dev/null 2>&1 || true

echo "==========================="
echo "ATLAS is running!"
PUBLIC_IP=$(curl -s ifconfig.me 2>/dev/null || echo "YOUR_SERVER_IP")
echo "Health: http://$PUBLIC_IP:8000/health"
echo "Grafana: http://$PUBLIC_IP:3001  (admin/admin)"
echo "==========================="
echo "Check status after 60s:"
echo "  sudo docker compose ps"
echo "  sudo docker compose logs app --tail=20"
