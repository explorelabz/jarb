from __future__ import annotations

import asyncio
import heapq
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from itertools import count
from typing import Any


class EndpointGroup(StrEnum):
    HEDGE = "hedge"
    CANCEL = "cancel"
    KILL = "kill"
    PLACE = "place"
    QUERY = "query"


class Priority(IntEnum):
    CRITICAL = 0
    CANCEL = 10
    PLACE = 20
    QUERY = 30


@dataclass
class TokenBucket:
    rate: float
    capacity: float
    tokens: float = field(init=False)
    updated_at: float = field(default_factory=time.monotonic)

    def __post_init__(self) -> None:
        self.tokens = self.capacity

    def take(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated_at) * self.rate)
        self.updated_at = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


@dataclass(order=True)
class _QueuedRequest:
    priority: int
    sequence: int
    group: EndpointGroup = field(compare=False)
    operation: Callable[[], Awaitable[Any]] = field(compare=False)
    future: asyncio.Future = field(compare=False)
    attempts: int = field(default=0, compare=False)


class PriorityRateLimiter:
    """Endpoint-group token buckets with a global priority queue."""

    def __init__(self, limits: dict[EndpointGroup, tuple[float, float]] | None = None):
        defaults = {group: (8.0, 8.0) for group in EndpointGroup}
        defaults[EndpointGroup.HEDGE] = (15.0, 15.0)
        defaults.update(limits or {})
        self.buckets = {group: TokenBucket(*values) for group, values in defaults.items()}
        self._queue: list[_QueuedRequest] = []
        self._sequence = count()
        self._condition = asyncio.Condition()
        self._worker: asyncio.Task | None = None
        self._inflight: set[asyncio.Task] = set()
        self._blocked_until: dict[EndpointGroup, float] = {}

    async def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(self._run(), name="priority-rate-limiter")

    async def stop(self) -> None:
        if self._worker:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
            self._worker = None
        tasks = tuple(self._inflight)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._inflight.clear()
        async with self._condition:
            queued, self._queue = self._queue, []
        for request in queued:
            if not request.future.done():
                request.future.cancel()

    async def submit(self, group: EndpointGroup, priority: Priority,
                     operation: Callable[[], Awaitable[Any]]) -> Any:
        await self.start()
        future = asyncio.get_running_loop().create_future()
        async with self._condition:
            heapq.heappush(self._queue, _QueuedRequest(priority, next(self._sequence), group, operation, future))
            self._condition.notify()
        return await future

    async def _run(self) -> None:
        while True:
            async with self._condition:
                await self._condition.wait_for(lambda: bool(self._queue))
                request = heapq.heappop(self._queue)
            blocked_until = self._blocked_until.get(request.group, 0)
            if time.monotonic() < blocked_until:
                async with self._condition:
                    heapq.heappush(self._queue, request)
                await asyncio.sleep(.02)
                continue
            if not self.buckets[request.group].take():
                async with self._condition:
                    heapq.heappush(self._queue, request)
                await asyncio.sleep(.02)
                continue
            if request.future.cancelled():
                continue
            task = asyncio.create_task(self._execute(request), name=f"rate-limit-{request.group}")
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)

    async def _execute(self, request: _QueuedRequest) -> None:
        try:
            result = await request.operation()
        except asyncio.CancelledError:
            if not request.future.done():
                request.future.cancel()
            raise
        except Exception as exc:
            text = str(exc).lower()
            rate_limited = any(
                marker in text for marker in ("429", "too many", "rate limit", "リクエストが多すぎ")
            )
            max_attempts = 8 if request.group == EndpointGroup.HEDGE else 4
            if rate_limited and request.attempts < max_attempts and not request.future.done():
                request.attempts += 1
                delay = min(2 ** request.attempts * .1, 5)
                self._blocked_until[request.group] = time.monotonic() + delay
                await asyncio.sleep(delay)
                async with self._condition:
                    heapq.heappush(self._queue, request)
                    self._condition.notify()
                return
            if not request.future.done():
                request.future.set_exception(exc)
            return
        if not request.future.done():
            request.future.set_result(result)
