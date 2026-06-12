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
