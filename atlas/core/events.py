from __future__ import annotations
import enum, time
from typing import Annotated, Literal, Union
from pydantic import BaseModel, ConfigDict, Field

class EventKind(str, enum.Enum):
    TRADE="TRADE"; BOOK_DELTA="BOOK_DELTA"; BOOK_SNAPSHOT="BOOK_SNAPSHOT"
    OI="OI"; FUNDING="FUNDING"; LIQUIDATION="LIQUIDATION"
    NEWS="NEWS"; CALENDAR="CALENDAR"

class TradePayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.TRADE]=EventKind.TRADE
    price: float; qty: float; aggressor_side: str; trade_id: int

class BookLevel(BaseModel):
    model_config=ConfigDict(frozen=True)
    price: float; qty: float

class BookDeltaPayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.BOOK_DELTA]=EventKind.BOOK_DELTA
    bids: list[BookLevel]; asks: list[BookLevel]
    first_update_id: int; last_update_id: int

class BookSnapshotPayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.BOOK_SNAPSHOT]=EventKind.BOOK_SNAPSHOT
    bids: list[BookLevel]; asks: list[BookLevel]; last_update_id: int

class OIPayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.OI]=EventKind.OI
    open_interest: float; open_interest_value: float

class FundingPayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.FUNDING]=EventKind.FUNDING
    funding_rate: float; next_funding_time_ms: int

class LiquidationPayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.LIQUIDATION]=EventKind.LIQUIDATION
    side: str; price: float; qty: float; order_type: str

class NewsPayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.NEWS]=EventKind.NEWS
    headline: str; source: str; url: str; symbols: list[str]; sentiment: float|None=None

class CalendarPayload(BaseModel):
    model_config=ConfigDict(frozen=True)
    kind: Literal[EventKind.CALENDAR]=EventKind.CALENDAR
    event_name: str; impact: str; currency: str
    actual: str|None=None; forecast: str|None=None; previous: str|None=None

AnyPayload = Annotated[
    Union[TradePayload,BookDeltaPayload,BookSnapshotPayload,
          OIPayload,FundingPayload,LiquidationPayload,NewsPayload,CalendarPayload],
    Field(discriminator="kind"),
]

class MarketEvent(BaseModel):
    model_config=ConfigDict(frozen=True)
    ts_exchange: int; ts_received: int; venue: str; symbol: str
    kind: EventKind; payload: AnyPayload; seq: int=0
    @staticmethod
    def now_ns() -> int: return time.time_ns()
    @property
    def latency_us(self) -> float: return (self.ts_received-self.ts_exchange)/1_000
