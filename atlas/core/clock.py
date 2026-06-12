from __future__ import annotations
import time
from abc import ABC, abstractmethod

class Clock(ABC):
    @abstractmethod
    def now_ns(self) -> int: ...
    def now_ms(self) -> int: return self.now_ns()//1_000_000
    def now_s(self) -> float: return self.now_ns()/1e9

class LiveClock(Clock):
    def now_ns(self) -> int: return time.time_ns()

class ReplayClock(Clock):
    def __init__(self) -> None: self._ts_ns: int = 0
    def set(self, ts_ns: int) -> None: self._ts_ns = ts_ns
    def now_ns(self) -> int: return self._ts_ns

_live = LiveClock()
def live_clock() -> LiveClock: return _live
