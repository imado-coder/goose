from __future__ import annotations
import asyncio, logging, time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
log = logging.getLogger(__name__)

class GapDetector:
    def __init__(self, threshold_sec: float=30.0) -> None:
        self._thr=threshold_sec; self._last: dict[tuple,float]={}; self._in_gap: set=set()
    def record(self, source: str, symbol: str) -> None:
        k=(source,symbol); self._last[k]=time.monotonic()
        if k in self._in_gap: log.info("Gap RESOLVED %s/%s",source,symbol); self._in_gap.discard(k)
    def check(self) -> list[dict]:
        now,alerts=time.monotonic(),[]
        for k,last in self._last.items():
            age=now-last
            if age>self._thr:
                if k not in self._in_gap:
                    self._in_gap.add(k); log.warning("GAP %s/%s %.1fs",k[0],k[1],age)
                alerts.append({"source":k[0],"symbol":k[1],"age_sec":round(age,1)})
        return alerts
    def status(self) -> dict:
        now=time.monotonic()
        return {f"{k[0]}/{k[1]}":{"last_seen_sec_ago":round(now-v,1),"gap":(k in self._in_gap)} for k,v in self._last.items()}

class HealthMonitor:
    def __init__(self,gap_threshold_sec: float=30.0,heartbeat_interval_sec: float=5.0) -> None:
        self._gap=GapDetector(gap_threshold_sec); self._hb=heartbeat_interval_sec
        self._counts: dict[str,int]=defaultdict(int); self._kinds: dict[str,int]=defaultdict(int)
        self._t0=time.time(); self._writer=None; self._task=None; self._running=False
    def attach_writer(self,w: Any) -> None: self._writer=w
    def record(self,source: str,symbol: str,kind: str) -> None:
        self._gap.record(source,symbol); self._counts[source]+=1; self._kinds[kind]+=1
    async def start(self) -> None:
        self._running=True; self._task=asyncio.create_task(self._loop(),name="health")
    async def stop(self) -> None:
        self._running=False
        if self._task: self._task.cancel(); await asyncio.gather(self._task,return_exceptions=True)
    async def _loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._hb)
            gaps=self._gap.check()
            if self._writer:
                for src,cnt in list(self._counts.items()):
                    try: await self._writer.record_heartbeat(src,cnt)
                    except Exception as exc: log.warning("HB write: %s",exc)
                for g in gaps:
                    try: await self._writer.record_gap(g["source"],g["symbol"],datetime.now(timezone.utc),g["age_sec"])
                    except Exception: pass
    def get_status(self) -> dict:
        return {"status":"ok" if not self._gap.check() else "degraded",
                "uptime_sec":round(time.time()-self._t0),
                "event_counts":dict(self._counts),"kind_counts":dict(self._kinds),
                "sources":self._gap.status(),"timestamp":datetime.now(timezone.utc).isoformat()}

_monitor: HealthMonitor|None=None

def create_health_app(monitor: HealthMonitor) -> FastAPI:
    global _monitor; _monitor=monitor
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await monitor.start(); yield; await monitor.stop()
    app=FastAPI(title="ATLAS Health",lifespan=lifespan)
    @app.get("/health")
    async def health() -> dict: return _monitor.get_status()
    @app.get("/metrics",response_class=PlainTextResponse)
    async def metrics() -> str:
        s=_monitor.get_status()
        lines=[f'atlas_uptime_seconds {s["uptime_sec"]}']
        for src,cnt in s["event_counts"].items(): lines.append(f'atlas_events_total{{source="{src.replace("-","_")}"}} {cnt}')
        for k,cnt in s["kind_counts"].items(): lines.append(f'atlas_events_by_kind{{kind="{k}"}} {cnt}')
        for key,info in s["sources"].items():
            src,sym=key.split("/",1)
            lines+=[f'atlas_source_last_seen_sec{{source="{src}",symbol="{sym}"}} {info["last_seen_sec_ago"]}',
                    f'atlas_source_gap{{source="{src}",symbol="{sym}"}} {int(info["gap"])}']
        return "\n".join(lines)+"\n"
    return app
