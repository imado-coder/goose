import time, pytest
from core.events import (
    EventKind, FundingPayload, LiquidationPayload, MarketEvent,
    OIPayload, TradePayload, BookDeltaPayload, BookLevel,
)

def _trade():
    ns=time.time_ns()
    return MarketEvent(ts_exchange=ns-1_000_000,ts_received=ns,venue="binance-perp",
        symbol="BTC-PERP",kind=EventKind.TRADE,
        payload=TradePayload(price=68000.5,qty=0.01,aggressor_side="buy",trade_id=1),seq=1)

def test_trade_round_trip():
    ev=_trade(); assert ev.kind==EventKind.TRADE; assert ev.latency_us>0

def test_json_round_trip():
    ev=_trade(); ev2=MarketEvent.model_validate_json(ev.model_dump_json())
    assert ev2.payload.price==ev.payload.price

def test_oi():
    ns=time.time_ns()
    ev=MarketEvent(ts_exchange=ns,ts_received=ns,venue="binance-perp",symbol="BTC-PERP",
        kind=EventKind.OI,payload=OIPayload(open_interest=300000.0,open_interest_value=2e10))
    assert ev.payload.open_interest==300000.0

def test_liq():
    ns=time.time_ns()
    ev=MarketEvent(ts_exchange=ns,ts_received=ns,venue="binance-perp",symbol="BTC-PERP",
        kind=EventKind.LIQUIDATION,payload=LiquidationPayload(side="sell",price=67500.0,qty=2.5,order_type="MARKET"))
    assert ev.payload.side=="sell"

def test_immutability():
    ev=_trade()
    with pytest.raises(Exception): ev.venue="other"  # type: ignore
