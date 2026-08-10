from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class EngineEvent:
    topic: str
    payload: Any
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class EventBus:
    def __init__(self):
        self._subscribers: dict[str, set[asyncio.Queue[EngineEvent]]] = defaultdict(set)

    async def publish(self, topic: str, payload: Any) -> None:
        event = EngineEvent(topic, payload)
        for queue in tuple(self._subscribers.get(topic, set())):
            await queue.put(event)

    def open_queue(self, topic: str) -> asyncio.Queue[EngineEvent]:
        queue: asyncio.Queue[EngineEvent] = asyncio.Queue()
        self._subscribers[topic].add(queue)
        return queue

    def close_queue(self, topic: str, queue: asyncio.Queue[EngineEvent]) -> None:
        self._subscribers[topic].discard(queue)

    async def subscribe(self, topic: str) -> AsyncIterator[EngineEvent]:
        queue = self.open_queue(topic)
        try:
            while True:
                yield await queue.get()
        finally:
            self.close_queue(topic, queue)
