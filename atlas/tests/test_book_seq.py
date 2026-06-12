from __future__ import annotations
import asyncio, time, pytest
from adapters.binance_ws import BinancePerpAdapter
from core.config import Settings

def _s(): return Settings(symbols=["BTCUSDT"],canonical_map={"BTCUSDT":"BTC-PERP"},
    binance_ws_base="wss://x",binance_rest_base="https://x",
    database_url="postgresql://x:x@localhost/x",redis_url="redis://localhost")

def _msg(sym,U,u,pu): return {"stream":f"{sym.lower()}@depth@100ms",
    "data":{"e":"depthUpdate","T":int(time.time()*1000),"U":U,"u":u,"pu":pu,
            "b":[["68000","1"]],"a":[["68001","1"]]}}

@pytest.mark.asyncio
async def test_nominal():
    a=BinancePerpAdapter(_s()); a._running=True; a._book_last_u["BTCUSDT"]=100
    await a._dispatch(_msg("BTCUSDT",101,110,100),"BTCUSDT")
    assert a._queue.qsize()==1 and a._book_last_u["BTCUSDT"]==110

@pytest.mark.asyncio
async def test_gap_degraded():
    a=BinancePerpAdapter(_s()); a._running=True; a._book_last_u["BTCUSDT"]=100
    async def fake(sym): a._book_degraded[sym]=False
    a._resync_book=fake
    await a._dispatch(_msg("BTCUSDT",101,110,98),"BTCUSDT")
    assert a._book_degraded.get("BTCUSDT") is True and a._queue.qsize()==0

@pytest.mark.asyncio
async def test_degraded_skips():
    a=BinancePerpAdapter(_s()); a._running=True
    a._book_last_u["BTCUSDT"]=100; a._book_degraded["BTCUSDT"]=True
    await a._dispatch(_msg("BTCUSDT",101,110,100),"BTCUSDT")
    assert a._queue.qsize()==0

@pytest.mark.asyncio
async def test_gap_callback():
    a=BinancePerpAdapter(_s()); a._running=True; a._book_last_u["BTCUSDT"]=100
    calls=[]
    async def cb(sym,s,e): calls.append((sym,s,e))
    a.set_gap_callback(cb)
    await a._dispatch(_msg("BTCUSDT",101,110,95),"BTCUSDT")
    gs=a._gap_start_ns.get("BTCUSDT",time.time_ns())
    a._book_degraded["BTCUSDT"]=False
    await cb("BTCUSDT",gs,time.time_ns())
    assert calls[0][2]>calls[0][1]
