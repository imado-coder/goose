from __future__ import annotations
import asyncio, logging, signal, sys
import uvicorn
from adapters.binance_ws import BinancePerpAdapter
from core.config import get_settings
from ops.health import HealthMonitor, create_health_app
from storage.archiver import NightlyArchiver
from storage.timescale import TimescaleWriter

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
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
    adapter  = BinancePerpAdapter(cfg)
    writer   = TimescaleWriter(cfg.database_url, cfg.db_pool_min, cfg.db_pool_max)
    monitor  = HealthMonitor(gap_threshold_sec=cfg.gap_threshold_sec,
                             heartbeat_interval_sec=cfg.heartbeat_interval_sec)
    monitor.attach_writer(writer)
    archiver = NightlyArchiver(cfg.database_url, cfg.parquet_dir, cfg.archive_hour_utc)
    health_app = create_health_app(monitor)

    await writer.connect()
    await archiver.start()
    await adapter.connect()

    async def _on_book_gap(symbol, start_ns, end_ns):
        try: await writer.record_book_gap(symbol, start_ns, end_ns)
        except Exception as exc: log.warning("Gap record failed: %s", exc)

    adapter.set_gap_callback(_on_book_gap)

    server = uvicorn.Server(uvicorn.Config(health_app, host="0.0.0.0",
                                           port=cfg.health_port, log_level="warning"))
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop_event.set())

    log.info("ATLAS Sprint-0 online. BTC-PERP → TimescaleDB")
    tasks = [
        asyncio.create_task(ingest_loop(adapter, writer, monitor), name="ingest"),
        asyncio.create_task(server.serve(), name="health-server"),
        asyncio.create_task(stop_event.wait(), name="stop-waiter"),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for t in done:
        if exc := t.exception(): log.error("Task %s raised: %s", t.get_name(), exc)
    server.should_exit = True
    await adapter.close(); await archiver.stop(); await writer.close()
    for t in pending: t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    log.info("ATLAS stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
