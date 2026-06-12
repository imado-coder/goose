from __future__ import annotations
import asyncio, logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import asyncpg, pandas as pd

log = logging.getLogger(__name__)
TABLES = ["trades","book_top","open_interest","funding","liquidations"]

class NightlyArchiver:
    def __init__(self, dsn: str, parquet_dir: str, archive_hour_utc: int=0) -> None:
        self._dsn=dsn; self._dir=Path(parquet_dir); self._hour=archive_hour_utc
        self._pool=None; self._running=False; self._task=None

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=2)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._running=True
        self._task = asyncio.create_task(self._loop(), name="archiver")
        log.info("NightlyArchiver started (hour=%d UTC)", self._hour)

    async def stop(self) -> None:
        self._running=False
        if self._task: self._task.cancel(); await asyncio.gather(self._task, return_exceptions=True)
        if self._pool: await self._pool.close()

    async def _loop(self) -> None:
        while self._running:
            now=datetime.now(timezone.utc)
            nxt=now.replace(hour=self._hour,minute=0,second=0,microsecond=0)
            if nxt<=now: nxt+=timedelta(days=1)
            log.info("Archiver next run in %.0f s", (nxt-now).total_seconds())
            await asyncio.sleep((nxt-now).total_seconds())
            if not self._running: break
            await self.archive_date((datetime.now(timezone.utc)-timedelta(days=1)).date())

    async def archive_date(self, day: date) -> None:
        start=datetime(day.year,day.month,day.day,tzinfo=timezone.utc)
        end=start+timedelta(days=1)
        for table in TABLES:
            try: await self._dump(table,start,end,day)
            except Exception as exc: log.error("Archive %s/%s: %s",table,day,exc)

    async def _dump(self, table: str, start: datetime, end: datetime, day: date) -> None:
        rows=await self._pool.fetch(f"SELECT * FROM {table} WHERE ts_exchange>=$1 AND ts_exchange<$2",start,end)
        if not rows: return
        out=(self._dir/table); out.mkdir(parents=True,exist_ok=True)
        p=out/f"{day}.parquet"
        pd.DataFrame([dict(r) for r in rows]).to_parquet(p,index=False,engine="pyarrow",compression="snappy")
        log.info("Archived %s/%s: %d rows → %s", table, day, len(rows), p)
