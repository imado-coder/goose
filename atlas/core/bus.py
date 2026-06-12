from __future__ import annotations
import json, logging
from typing import AsyncIterator
import redis.asyncio as aioredis
from core.events import MarketEvent

log = logging.getLogger(__name__)
STREAM_PREFIX = "atlas:events"

def stream_name(venue: str, kind: str) -> str:
    return f"{STREAM_PREFIX}:{venue}:{kind.lower()}"

class EventBus:
    def __init__(self, redis_url: str, maxlen: int = 100_000) -> None:
        self._url = redis_url; self._maxlen = maxlen
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(self._url, decode_responses=True)
        await self._client.ping()

    async def close(self) -> None:
        if self._client: await self._client.aclose()

    async def publish(self, event: MarketEvent) -> None:
        assert self._client
        await self._client.xadd(
            stream_name(event.venue, event.kind.value),
            {"ts_exchange": str(event.ts_exchange), "ts_received": str(event.ts_received),
             "venue": event.venue, "symbol": event.symbol, "kind": event.kind.value,
             "payload": event.payload.model_dump_json(), "seq": str(event.seq)},
            maxlen=self._maxlen, approximate=True)
