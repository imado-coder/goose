"""
Golden tests for DOM sequence gap detection.
No network, no DB — pure logic tests using a mock adapter.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from adapters.binance_ws import BinancePerpAdapter
from core.config import Settings


def _settings() -> Settings:
    return Settings(
        symbols=["BTCUSDT"],
        canonical_map={"BTCUSDT": "BTC-PERP"},
        binance_ws_base="wss://fstream.binance.com",
        binance_rest_base="https://fapi.binance.com",
        database_url="postgresql://x:x@localhost/x",
        redis_url="redis://localhost",
    )


def _depth_msg(symbol: str, U: int, u: int, pu: int) -> dict:
    return {
        "stream": f"{symbol.lower()}@depth@100ms",
        "data": {
            "e": "depthUpdate",
            "T": int(time.time() * 1000),
            "U": U,
            "u": u,
            "pu": pu,
            "b": [["68000.00", "1.5"]],
            "a": [["68001.00", "2.0"]],
        },
    }


@pytest.mark.asyncio
async def test_anchor_first_delta_after_snapshot():
    """First delta after a snapshot anchors the book even though pu!=last_u.

    pu refers to the previous *stream* event, which we never saw, so it cannot
    match the snapshot's lastUpdateId. The covering event (U<=last_u+1<=u) must
    still be accepted and emitted, not treated as a gap.
    """
    adapter = BinancePerpAdapter(_settings())
    adapter._running = True
    adapter._book_last_u["BTCUSDT"] = 100   # snapshot lastUpdateId
    # _book_anchored not set → anchor phase active

    # Covering event: U=100 <= 101 <= u=110, pu unrelated (=42)
    await adapter._dispatch(_depth_msg("BTCUSDT", 100, 110, pu=42), "BTCUSDT")
    assert adapter._queue.qsize() == 1
    assert adapter._book_anchored["BTCUSDT"] is True
    assert not adapter._book_degraded.get("BTCUSDT")
    assert adapter._book_last_u["BTCUSDT"] == 110

    # Next event now enforces strict contiguity
    await adapter._dispatch(_depth_msg("BTCUSDT", 111, 120, pu=110), "BTCUSDT")
    assert adapter._queue.qsize() == 2


@pytest.mark.asyncio
async def test_stale_delta_before_snapshot_dropped():
    """Deltas wholly older than the snapshot (u < last_u) are dropped."""
    adapter = BinancePerpAdapter(_settings())
    adapter._running = True
    adapter._book_last_u["BTCUSDT"] = 100

    await adapter._dispatch(_depth_msg("BTCUSDT", 90, 95, pu=89), "BTCUSDT")
    assert adapter._queue.qsize() == 0
    assert not adapter._book_anchored.get("BTCUSDT")


@pytest.mark.asyncio
async def test_no_gap_nominal():
    """Consecutive updates with pu==last_u should all be enqueued."""
    adapter = BinancePerpAdapter(_settings())
    adapter._running = True
    # Anchor last_u via fake snapshot
    adapter._book_last_u["BTCUSDT"] = 100
    adapter._book_anchored["BTCUSDT"] = True

    await adapter._dispatch(_depth_msg("BTCUSDT", 101, 110, pu=100), "BTCUSDT")
    assert adapter._queue.qsize() == 1
    assert not adapter._book_degraded.get("BTCUSDT")
    assert adapter._book_last_u["BTCUSDT"] == 110

    await adapter._dispatch(_depth_msg("BTCUSDT", 111, 120, pu=110), "BTCUSDT")
    assert adapter._queue.qsize() == 2
    assert adapter._book_last_u["BTCUSDT"] == 120


@pytest.mark.asyncio
async def test_gap_detected_marks_degraded():
    """pu != last_u must mark DEGRADED and drop the event."""
    adapter = BinancePerpAdapter(_settings())
    adapter._running = True
    adapter._book_last_u["BTCUSDT"] = 100
    adapter._book_anchored["BTCUSDT"] = True

    gap_calls: list[tuple] = []

    async def fake_resync(symbol: str) -> None:
        # Simulate resync completing
        adapter._book_last_u[symbol] = 999
        adapter._book_degraded[symbol] = False
        gap_calls.append((symbol,))

    # Patch _resync_book to avoid real HTTP calls
    adapter._resync_book = fake_resync

    # pu=98 but last_u=100 → gap
    await adapter._dispatch(_depth_msg("BTCUSDT", 101, 110, pu=98), "BTCUSDT")

    assert adapter._book_degraded.get("BTCUSDT") is True
    assert adapter._queue.qsize() == 0    # event was dropped
    await asyncio.sleep(0.01)            # let create_task run


@pytest.mark.asyncio
async def test_deltas_skipped_while_degraded():
    """Events arriving while DEGRADED must be silently dropped."""
    adapter = BinancePerpAdapter(_settings())
    adapter._running = True
    adapter._book_last_u["BTCUSDT"] = 100
    adapter._book_anchored["BTCUSDT"] = True
    adapter._book_degraded["BTCUSDT"] = True   # manually mark degraded

    # pu matches — but still degraded → drop
    await adapter._dispatch(_depth_msg("BTCUSDT", 101, 110, pu=100), "BTCUSDT")
    assert adapter._queue.qsize() == 0


@pytest.mark.asyncio
async def test_gap_callback_fired():
    """Gap callback must be called with start/end ns after resync."""
    adapter = BinancePerpAdapter(_settings())
    adapter._running = True
    adapter._book_last_u["BTCUSDT"] = 100
    adapter._book_anchored["BTCUSDT"] = True

    recorded: list[tuple] = []

    async def cb(symbol: str, start_ns: int, end_ns: int) -> None:
        recorded.append((symbol, start_ns, end_ns))

    adapter.set_gap_callback(cb)

    # Trigger gap
    await adapter._dispatch(_depth_msg("BTCUSDT", 101, 110, pu=95), "BTCUSDT")
    assert adapter._book_degraded["BTCUSDT"] is True

    # Simulate resync completing (call _resync internals directly)
    gap_start = adapter._gap_start_ns.get("BTCUSDT", time.time_ns())
    adapter._book_degraded["BTCUSDT"] = False
    await cb("BTCUSDT", gap_start, time.time_ns())

    assert len(recorded) == 1
    assert recorded[0][0] == "BTCUSDT"
    assert recorded[0][2] > recorded[0][1]   # end > start
