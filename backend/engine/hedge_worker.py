from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol

from ..adapters import GmoAdapter
from ..models import Side
from .domain import EventType, FillDelta, HedgeIntent, HedgeStatus
from .events import EventBus
from .rate_limit import EndpointGroup, Priority, PriorityRateLimiter
from .risk import RiskGate
from .state_store import StateStore


@dataclass(frozen=True)
class HedgeExecution:
    order_id: str
    filled_qty: Decimal
    filled_notional: Decimal = Decimal("0")
    submitted_at: str = ""
    confirmed_at: str = ""


class HedgeExecutor(Protocol):
    def __call__(self, symbol: str, side: str, qty: Decimal) -> Awaitable[HedgeExecution]: ...


class HedgeResolver(Protocol):
    def __call__(self, intent: HedgeIntent) -> Awaitable[HedgeExecution | None]: ...


class HedgeWorker:
    """Persists hedge intent before any order, aggregates dust, and retries partial FAK fills."""

    def __init__(self, store: StateStore, events: EventBus, executor: HedgeExecutor, risk: RiskGate, *,
                 min_sizes: dict[str, Decimal], dust_timeout_sec: float = 3.0,
                 delta_thresholds: dict[str, Decimal] | None = None, max_attempts: int = 4,
                 resolver: HedgeResolver | None = None,
                 on_execution: Callable[[str, str, HedgeExecution], Awaitable[None]] | None = None):
        self.store = store
        self.events = events
        self.executor = executor
        self.risk = risk
        self.min_sizes = min_sizes
        self.dust_timeout_sec = dust_timeout_sec
        self.delta_thresholds = delta_thresholds or min_sizes
        self.max_attempts = max_attempts
        self.resolver = resolver
        self.on_execution = on_execution
        self._wake = asyncio.Event()
        self._listener: asyncio.Task | None = None
        self._worker: asyncio.Task | None = None
        self._fill_queue: asyncio.Queue | None = None
        self._first_pending_at: dict[tuple[str, str], float] = {}

    async def start(self) -> None:
        if self._listener is None:
            await self._recover_inflight()
            self._fill_queue = self.events.open_queue(EventType.FILL)
            self._listener = asyncio.create_task(self._listen(), name="hedge-fill-listener")
            self._worker = asyncio.create_task(self._run(), name="hedge-worker")
            if await self.store.pending_hedges():
                self._wake.set()

    async def _recover_inflight(self) -> None:
        grouped: dict[str | None, list[HedgeIntent]] = defaultdict(list)
        for intent in await self.store.pending_hedges():
            if intent.status == HedgeStatus.HEDGING:
                grouped[intent.exchange_order_id].append(intent)
        for order_id, intents in grouped.items():
            if self.resolver is None or order_id is None:
                for intent in intents:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.ESCALATE,
                        error="unresolved in-flight hedge after restart",
                    )
                await self.risk.disarm(f"manual hedge reconciliation required for {intents[0].symbol}")
                continue
            try:
                result = await self.resolver(intents[0])
            except Exception as exc:
                for intent in intents:
                    await self.store.transition_hedge(intent.id, HedgeStatus.ESCALATE, error=str(exc)[:240])
                await self.risk.disarm(f"hedge recovery failed for {intents[0].symbol}")
                continue
            if result is None:
                for intent in intents:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.RETRY, error="confirmed absent during recovery",
                    )
            else:
                available = result.filled_qty
                average = result.filled_notional / result.filled_qty if result.filled_qty else Decimal("0")
                for intent in intents:
                    required = intent.qty - intent.filled_qty
                    allocated = min(required, available)
                    available -= allocated
                    total = intent.filled_qty + allocated
                    target = HedgeStatus.HEDGED if total >= intent.qty else HedgeStatus.RETRY
                    await self.store.transition_hedge(
                        intent.id, target, filled_qty=total,
                        filled_notional=intent.filled_notional + allocated * average,
                        latency_ms=self._latency_ms(intent, result.confirmed_at),
                        exchange_order_id=result.order_id,
                        error=None if target == HedgeStatus.HEDGED else "recovered partial hedge",
                    )

    async def stop(self) -> None:
        for task in (self._listener, self._worker):
            if task:
                task.cancel()
        for task in (self._listener, self._worker):
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._listener = self._worker = None
        if self._fill_queue is not None:
            self.events.close_queue(EventType.FILL, self._fill_queue)
            self._fill_queue = None

    async def _listen(self) -> None:
        if self._fill_queue is None:
            raise RuntimeError("fill queue is not initialized")
        while True:
            event = await self._fill_queue.get()
            fill: FillDelta = event.payload
            hedge_side = "SELL" if fill.side == "BUY" else "BUY"
            await self.store.create_hedge_intent(fill, hedge_side)
            self._wake.set()

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=.25)
            except TimeoutError:
                pass
            self._wake.clear()
            grouped: dict[tuple[str, str], list[HedgeIntent]] = defaultdict(list)
            for intent in await self.store.pending_hedges():
                grouped[(intent.symbol, intent.side)].append(intent)
            now = loop.time()
            for key, intents in grouped.items():
                remaining = sum((item.qty - item.filled_qty for item in intents), Decimal("0"))
                if remaining <= 0:
                    continue
                first = self._first_pending_at.setdefault(key, now)
                minimum = self.min_sizes[key[0]]
                delta_trigger = remaining >= self.delta_thresholds.get(key[0], minimum)
                timed_out = now - first >= self.dust_timeout_sec
                if remaining < minimum:
                    if timed_out:
                        for intent in intents:
                            await self.store.transition_hedge(
                                intent.id, HedgeStatus.ESCALATE, error="dust below GMO minimum after timeout",
                            )
                        await self.risk.disarm(f"unhedgeable {key[0]} dust {remaining}")
                        self._first_pending_at.pop(key, None)
                    continue
                if delta_trigger or timed_out:
                    await self._execute_group(key, intents, remaining)
                    self._first_pending_at.pop(key, None)

    async def _execute_group(self, key: tuple[str, str], intents: list[HedgeIntent], qty: Decimal) -> None:
        symbol, side = key
        active: list[HedgeIntent] = []
        for intent in intents:
            if intent.attempts >= self.max_attempts:
                await self.store.transition_hedge(intent.id, HedgeStatus.ESCALATE, error="retry limit exceeded")
                await self.risk.disarm(f"hedge retry limit exceeded for {symbol}")
                continue
            active.append(await self.store.transition_hedge(intent.id, HedgeStatus.HEDGING))
        if not active:
            return
        if hasattr(self.executor, "set_checkpoint"):
            async def checkpoint(order_id: str) -> None:
                for intent in active:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.HEDGING, exchange_order_id=order_id,
                    )
            self.executor.set_checkpoint(checkpoint)
        try:
            result = await self.executor(symbol, side, qty)
        except Exception as exc:
            for intent in active:
                target = HedgeStatus.ESCALATE if intent.attempts >= self.max_attempts else HedgeStatus.RETRY
                await self.store.transition_hedge(intent.id, target, error=str(exc)[:240])
            if any(item.attempts >= self.max_attempts for item in active):
                await self.risk.disarm(f"hedge failed repeatedly for {symbol}")
            else:
                await asyncio.sleep(min(2 ** max(item.attempts for item in active) * .1, 5))
                self._wake.set()
            return

        available = result.filled_qty
        average_price = result.filled_notional / result.filled_qty if result.filled_qty else Decimal("0")
        if self.on_execution is not None and result.filled_qty > 0:
            await self.on_execution(symbol, side, result)
        for intent in active:
            required = intent.qty - intent.filled_qty
            allocated = min(required, available)
            total_filled = intent.filled_qty + allocated
            available -= allocated
            target = HedgeStatus.HEDGED if total_filled >= intent.qty else HedgeStatus.RETRY
            await self.store.transition_hedge(
                intent.id, target, filled_qty=total_filled,
                filled_notional=intent.filled_notional + allocated * average_price,
                latency_ms=self._latency_ms(intent, result.confirmed_at),
                exchange_order_id=result.order_id,
                error=None if target == HedgeStatus.HEDGED else "GMO FAK partially filled",
            )
        if result.filled_qty < qty:
            self._wake.set()

    @staticmethod
    def _latency_ms(intent: HedgeIntent, confirmed_at: str = "") -> int:
        try:
            started = datetime.fromisoformat(intent.source_fill_at.replace("Z", "+00:00"))
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            confirmed = datetime.fromisoformat(confirmed_at.replace("Z", "+00:00")) \
                if confirmed_at else datetime.now(timezone.utc)
            if confirmed.tzinfo is None:
                confirmed = confirmed.replace(tzinfo=timezone.utc)
            return max(0, int((confirmed - started).total_seconds() * 1000))
        except ValueError:
            return 0


class GmoHedgeExecutor:
    """Submits GMO FAK orders and returns the exchange-confirmed executed quantity."""

    def __init__(self, adapter: GmoAdapter, limiter: PriorityRateLimiter,
                 size_steps: dict[str, Decimal], *, fill_timeout_sec: float = 5.0):
        self.adapter = adapter
        self.limiter = limiter
        self.size_steps = size_steps
        self.fill_timeout_sec = fill_timeout_sec
        self._checkpoint: Callable[[str], Awaitable[None]] | None = None

    def set_checkpoint(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._checkpoint = callback

    async def __call__(self, symbol: str, side: str, qty: Decimal) -> HedgeExecution:
        submitted_at = datetime.now(timezone.utc).isoformat()
        response = await self.limiter.submit(
            EndpointGroup.HEDGE, Priority.CRITICAL,
            lambda: self.adapter.market_order(
                symbol, side, qty, self.size_steps.get(symbol, Decimal("0.00000001")),
            ),
        )
        data = response.get("data", response)
        order_id = str(data.get("orderId") or data.get("order-id") or data.get("id"))
        if not order_id or order_id == "None":
            raise RuntimeError("GMO hedge response had no order id")
        if self._checkpoint is not None:
            await self._checkpoint(order_id)
        filled, notional, confirmed_at = await self._await_fills(
            order_id, qty, timeout_sec=self.fill_timeout_sec,
        )
        return HedgeExecution(
            order_id=order_id, filled_qty=filled, filled_notional=notional,
            submitted_at=submitted_at, confirmed_at=confirmed_at,
        )

    async def resolve(self, intent: HedgeIntent) -> HedgeExecution | None:
        if not intent.exchange_order_id:
            raise RuntimeError("GMO order id was not durably checkpointed")
        filled, notional, confirmed_at = await self._await_fills(
            intent.exchange_order_id, intent.qty, timeout_sec=self.fill_timeout_sec,
        )
        return HedgeExecution(
            intent.exchange_order_id, filled, notional,
            submitted_at=intent.created_at, confirmed_at=confirmed_at,
        )

    async def _await_fills(self, order_id: str, expected: Decimal, *,
                           timeout_sec: float = 5.0) -> tuple[Decimal, Decimal, str]:
        deadline = asyncio.get_running_loop().time() + timeout_sec
        last_error: Exception | None = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                payload = await self.limiter.submit(
                    EndpointGroup.QUERY, Priority.CRITICAL,
                    lambda: self.adapter.executions(order_id),
                )
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(.15)
                continue
            rows = self._rows(payload)
            filled = sum((Decimal(str(row.get("size", "0"))) for row in rows), Decimal("0"))
            notional = sum((
                Decimal(str(row.get("size", "0"))) * Decimal(str(row.get("price", "0")))
                for row in rows
            ), Decimal("0"))
            confirmed_at = datetime.now(timezone.utc).isoformat()
            if filled >= expected:
                return filled, notional, confirmed_at
            try:
                status = await self._order_status(order_id)
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(.15)
                continue
            if status in ("EXECUTED", "CANCELED"):
                return filled, notional, confirmed_at
            await asyncio.sleep(.15)
        detail = f"；最后查询错误：{last_error}" if last_error else ""
        raise RuntimeError(f"GMO 对冲 {order_id} 成交量未在 {timeout_sec:g}s 内确认{detail}")

    async def _order_status(self, order_id: str) -> str:
        payload = await self.limiter.submit(
            EndpointGroup.QUERY, Priority.CRITICAL,
            lambda: self.adapter.order(order_id),
        )
        data = payload.get("data", payload)
        if isinstance(data, dict):
            rows = data.get("list", data.get("orders"))
            if isinstance(rows, list):
                data = rows[0] if rows else {}
        elif isinstance(data, list):
            data = data[0] if data else {}
        return str(data.get("status", "")).upper() if isinstance(data, dict) else ""

    @staticmethod
    def _rows(payload: dict) -> list[dict]:
        rows = payload.get("data", [])
        if isinstance(rows, dict):
            rows = rows.get("list", rows.get("executions", []))
        return rows if isinstance(rows, list) else []
