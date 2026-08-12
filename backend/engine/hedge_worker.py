from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
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
    fee_jpy: Decimal = Decimal("0")
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
                 max_concurrent_groups_per_key: int = 4,
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
        self.max_concurrent_groups_per_key = max(1, max_concurrent_groups_per_key)
        self.resolver = resolver
        self.on_execution = on_execution
        self._wake = asyncio.Event()
        self._listener: asyncio.Task | None = None
        self._worker: asyncio.Task | None = None
        self._fill_queue: asyncio.Queue | None = None
        self._first_pending_at: dict[tuple[str, str], float] = {}
        self._active_groups: dict[tuple[str, str], set[asyncio.Task]] = defaultdict(set)
        self._active_intents: set[str] = set()

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
                    allocated_fee = result.fee_jpy * allocated / result.filled_qty \
                        if result.filled_qty else Decimal("0")
                    target = HedgeStatus.HEDGED if total >= intent.qty else HedgeStatus.RETRY
                    await self.store.transition_hedge(
                        intent.id, target, filled_qty=total,
                        filled_notional=intent.filled_notional + allocated * average,
                        fee_jpy=intent.fee_jpy + allocated_fee,
                        latency_ms=self._latency_ms(intent, result.confirmed_at),
                        exchange_order_id=result.order_id,
                        error=None if target == HedgeStatus.HEDGED else "recovered partial hedge",
                    )

    async def stop(self) -> None:
        active = tuple(task for tasks in self._active_groups.values() for task in tasks)
        for task in active:
            task.cancel()
        for task in (self._listener, self._worker):
            if task:
                task.cancel()
        for task in (self._listener, self._worker):
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        self._active_groups.clear()
        self._active_intents.clear()
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
                if intent.id in self._active_intents:
                    continue
                grouped[(intent.symbol, intent.side)].append(intent)
            now = loop.time()
            for key, intents in grouped.items():
                if len(self._active_groups[key]) >= self.max_concurrent_groups_per_key:
                    continue
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
                    self._first_pending_at.pop(key, None)
                    intent_ids = tuple(item.id for item in intents)
                    self._active_intents.update(intent_ids)
                    task = asyncio.create_task(
                        self._execute_group(key, intents, remaining),
                        name=f"hedge-{key[0]}-{key[1]}",
                    )
                    self._active_groups[key].add(task)
                    task.add_done_callback(
                        lambda completed, group=key, ids=intent_ids: self._group_done(group, ids, completed)
                    )

    def _group_done(self, key: tuple[str, str], intent_ids: tuple[str, ...], task: asyncio.Task) -> None:
        tasks = self._active_groups.get(key)
        if tasks is not None:
            tasks.discard(task)
            if not tasks:
                self._active_groups.pop(key, None)
        self._active_intents.difference_update(intent_ids)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                asyncio.create_task(self.store.audit(
                    "hedge.worker.error", "critical", f"{key[0]} {key[1]}: {str(error)[:200]}",
                ))
                asyncio.create_task(self.risk.disarm(f"hedge worker failed for {key[0]}"))
        self._wake.set()

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
            async def checkpoint(order_id: str, filled_qty: Decimal = Decimal("0"),
                                 filled_notional: Decimal = Decimal("0"),
                                 fee_jpy: Decimal = Decimal("0")) -> None:
                available = filled_qty
                average = filled_notional / filled_qty if filled_qty else Decimal("0")
                for intent in active:
                    required = intent.qty - intent.filled_qty
                    allocated = min(required, available)
                    available -= allocated
                    allocated_fee = fee_jpy * allocated / filled_qty if filled_qty else Decimal("0")
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.HEDGING,
                        filled_qty=intent.filled_qty + allocated,
                        filled_notional=intent.filled_notional + allocated * average,
                        fee_jpy=intent.fee_jpy + allocated_fee,
                        exchange_order_id=order_id,
                    )
            self.executor.set_checkpoint(checkpoint)
        try:
            result = await self.executor(symbol, side, qty)
        except Exception as exc:
            await self.store.audit(
                "hedge.retry", "warning", f"{symbol} {side}: {str(exc)[:200]}",
                metadata={
                    "intentIds": [intent.id for intent in active],
                    "attempts": max(intent.attempts for intent in active),
                    "qty": str(qty),
                },
            )
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
        for intent in active:
            required = intent.qty - intent.filled_qty
            allocated = min(required, available)
            total_filled = intent.filled_qty + allocated
            available -= allocated
            allocated_fee = result.fee_jpy * allocated / result.filled_qty \
                if result.filled_qty else Decimal("0")
            target = HedgeStatus.HEDGED if total_filled >= intent.qty else HedgeStatus.RETRY
            await self.store.transition_hedge(
                intent.id, target, filled_qty=total_filled,
                filled_notional=intent.filled_notional + allocated * average_price,
                fee_jpy=intent.fee_jpy + allocated_fee,
                latency_ms=self._latency_ms(intent, result.confirmed_at),
                exchange_order_id=result.order_id,
                error=None if target == HedgeStatus.HEDGED else "GMO FAK partially filled",
            )
        if self.on_execution is not None and result.filled_qty > 0:
            await self.on_execution(symbol, side, result)
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
    """Attempts GMO SOK maker hedge first, then cancels and FAK-hedges only the remainder."""

    def __init__(self, adapter: GmoAdapter, limiter: PriorityRateLimiter,
                 size_steps: dict[str, Decimal], *,
                 price_ticks: dict[str, Decimal] | None = None,
                 passive_price: Callable[[str, str], Decimal] | None = None,
                 maker_fee_bps: dict[str, Decimal] | None = None,
                 taker_fee_bps: dict[str, Decimal] | None = None,
                 passive_timeout_ms: dict[str, int] | None = None,
                 fill_timeout_sec: float = 5.0):
        self.adapter = adapter
        self.limiter = limiter
        self.size_steps = size_steps
        self.price_ticks = price_ticks or {}
        self.passive_price = passive_price
        self.maker_fee_bps = maker_fee_bps or {}
        self.taker_fee_bps = taker_fee_bps or {}
        self.passive_timeout_ms = passive_timeout_ms or {}
        self.fill_timeout_sec = fill_timeout_sec
        self._checkpoint: ContextVar[
            Callable[[str, Decimal, Decimal, Decimal], Awaitable[None]] | None
        ] = ContextVar(
            "gmo_hedge_checkpoint", default=None,
        )

    def set_checkpoint(self, callback: Callable[[str, Decimal, Decimal, Decimal], Awaitable[None]]) -> None:
        self._checkpoint.set(callback)

    async def __call__(self, symbol: str, side: str, qty: Decimal) -> HedgeExecution:
        if self.passive_price is None or not hasattr(self.adapter, "post_only_order"):
            return await self._market_execution(symbol, side, qty)
        return await self._hybrid_execution(symbol, side, qty)

    async def _hybrid_execution(self, symbol: str, side: str, qty: Decimal) -> HedgeExecution:
        submitted_at = datetime.now(timezone.utc).isoformat()
        step = self.size_steps.get(symbol, Decimal("0.00000001"))
        tick = self.price_ticks.get(symbol, Decimal("1"))
        passive_price = self.passive_price(symbol, side)
        response = await self.limiter.submit(
            EndpointGroup.HEDGE, Priority.CRITICAL,
            lambda: self.adapter.post_only_order(
                symbol, side, qty, passive_price, step, tick,
            ),
        )
        passive_id = self._order_id(response)
        if not passive_id:
            raise RuntimeError("GMO hedge response had no order id")
        await self._checkpoint_order(passive_id)
        passive_filled, passive_notional, terminal = await self._watch_passive(
            passive_id, qty, passive_price,
            timeout_sec=self.passive_timeout_ms.get(symbol, 800) / 1000,
        )
        if passive_filled < qty and terminal not in ("EXECUTED", "CANCELED", "EXPIRED"):
            passive_filled, passive_notional = await self._cancel_passive(
                passive_id, qty, passive_price,
            )
        passive_fee = passive_notional * self.maker_fee_bps.get(symbol, Decimal("-1")) / Decimal("10000")
        remaining = max(Decimal("0"), qty - passive_filled)
        if remaining <= 0:
            return HedgeExecution(
                passive_id, passive_filled, passive_notional, passive_fee,
                submitted_at=submitted_at, confirmed_at=datetime.now(timezone.utc).isoformat(),
            )
        fallback = await self._market_execution(
            symbol, side, remaining,
            carried_qty=passive_filled, carried_notional=passive_notional, carried_fee=passive_fee,
            submitted_at=submitted_at,
        )
        return HedgeExecution(
            order_id=f"{passive_id}+{fallback.order_id}",
            filled_qty=passive_filled + fallback.filled_qty,
            filled_notional=passive_notional + fallback.filled_notional,
            fee_jpy=passive_fee + fallback.fee_jpy,
            submitted_at=submitted_at, confirmed_at=fallback.confirmed_at,
        )

    async def _market_execution(self, symbol: str, side: str, qty: Decimal, *,
                                carried_qty: Decimal = Decimal("0"),
                                carried_notional: Decimal = Decimal("0"),
                                carried_fee: Decimal = Decimal("0"),
                                submitted_at: str = "") -> HedgeExecution:
        submitted_at = submitted_at or datetime.now(timezone.utc).isoformat()
        response = await self.limiter.submit(
            EndpointGroup.HEDGE, Priority.CRITICAL,
            lambda: self.adapter.market_order(
                symbol, side, qty, self.size_steps.get(symbol, Decimal("0.00000001")),
            ),
        )
        order_id = self._order_id(response)
        if not order_id:
            raise RuntimeError("GMO hedge response had no order id")
        await self._checkpoint_order(order_id, carried_qty, carried_notional, carried_fee)
        filled, notional, confirmed_at = await self._await_fills(
            order_id, qty, timeout_sec=self.fill_timeout_sec,
        )
        fee = notional * self.taker_fee_bps.get(symbol, Decimal("9")) / Decimal("10000")
        return HedgeExecution(order_id, filled, notional, fee, submitted_at, confirmed_at)

    async def resolve(self, intent: HedgeIntent) -> HedgeExecution | None:
        if not intent.exchange_order_id:
            raise RuntimeError("GMO order id was not durably checkpointed")
        remaining = max(Decimal("0"), intent.qty - intent.filled_qty)
        if remaining <= 0:
            return HedgeExecution(intent.exchange_order_id, Decimal("0"))
        detail = await self._order_detail(intent.exchange_order_id)
        if str(detail.get("timeInForce", "")).upper() == "SOK":
            passive_price = Decimal(str(detail.get("price", "0") or "0"))
            if passive_price <= 0 and self.passive_price is not None:
                passive_price = self.passive_price(intent.symbol, intent.side)
            status = str(detail.get("status", "")).upper()
            if status in ("EXECUTED", "CANCELED", "EXPIRED"):
                passive_filled, passive_notional = await self._execution_snapshot(intent.exchange_order_id)
                executed = Decimal(str(detail.get("executedSize", "0") or "0"))
                if executed > passive_filled:
                    passive_filled, passive_notional = executed, executed * passive_price
            else:
                passive_filled, passive_notional = await self._cancel_passive(
                    intent.exchange_order_id, remaining, passive_price,
                )
            passive_fee = passive_notional * self.maker_fee_bps.get(
                intent.symbol, Decimal("-1"),
            ) / Decimal("10000")
            fallback_qty = max(Decimal("0"), remaining - passive_filled)
            if fallback_qty <= 0:
                return HedgeExecution(
                    intent.exchange_order_id, passive_filled, passive_notional, passive_fee,
                    submitted_at=intent.created_at,
                    confirmed_at=datetime.now(timezone.utc).isoformat(),
                )
            fallback = await self._market_execution(
                intent.symbol, intent.side, fallback_qty,
                carried_qty=passive_filled, carried_notional=passive_notional,
                carried_fee=passive_fee, submitted_at=intent.created_at,
            )
            return HedgeExecution(
                f"{intent.exchange_order_id}+{fallback.order_id}",
                passive_filled + fallback.filled_qty,
                passive_notional + fallback.filled_notional,
                passive_fee + fallback.fee_jpy,
                intent.created_at, fallback.confirmed_at,
            )
        filled, notional, confirmed_at = await self._await_fills(
            intent.exchange_order_id, remaining, timeout_sec=self.fill_timeout_sec,
        )
        is_maker = str(detail.get("timeInForce", "")).upper() == "SOK"
        fee_bps = self.maker_fee_bps.get(intent.symbol, Decimal("-1")) if is_maker \
            else self.taker_fee_bps.get(intent.symbol, Decimal("9"))
        return HedgeExecution(
            intent.exchange_order_id, filled, notional, notional * fee_bps / Decimal("10000"),
            submitted_at=intent.created_at, confirmed_at=confirmed_at,
        )

    async def _watch_passive(self, order_id: str, expected: Decimal, price: Decimal, *,
                             timeout_sec: float) -> tuple[Decimal, Decimal, str]:
        # SOK uses one fixed limit price, so cumulative executedSize from the order
        # record is sufficient to calculate notional exactly. Polling executions and
        # orders every 100ms for every concurrent hedge starves the QUERY token bucket
        # and increases (rather than decreases) real exposure time.
        paper_poll_interval = getattr(self.adapter, "passive_poll_interval_sec", None)
        if paper_poll_interval is None:
            await asyncio.sleep(timeout_sec)
            detail = await self._order_detail(order_id)
            status = str(detail.get("status", "")).upper()
            filled = min(expected, Decimal(str(detail.get("executedSize", "0") or "0")))
            return filled, filled * price, status
        deadline = asyncio.get_running_loop().time() + timeout_sec
        while True:
            detail = await self._order_detail(order_id)
            status = str(detail.get("status", "")).upper()
            filled = min(expected, Decimal(str(detail.get("executedSize", "0") or "0")))
            if status in ("EXECUTED", "CANCELED", "EXPIRED"):
                return filled, filled * price, status
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return filled, filled * price, status
            await asyncio.sleep(min(float(paper_poll_interval), remaining))

    async def _cancel_passive(self, order_id: str, expected: Decimal,
                              price: Decimal) -> tuple[Decimal, Decimal]:
        try:
            await self.limiter.submit(
                EndpointGroup.HEDGE, Priority.CRITICAL,
                lambda: self.adapter.cancel_order(order_id),
            )
        except Exception:
            detail = await self._order_detail(order_id)
            if str(detail.get("status", "")).upper() not in ("EXECUTED", "CANCELED", "EXPIRED"):
                raise
        deadline = asyncio.get_running_loop().time() + min(self.fill_timeout_sec, 3.0)
        while asyncio.get_running_loop().time() < deadline:
            detail = await self._order_detail(order_id)
            status = str(detail.get("status", "")).upper()
            filled = min(expected, Decimal(str(detail.get("executedSize", "0") or "0")))
            if status in ("EXECUTED", "CANCELED", "EXPIRED"):
                return filled, filled * price
            await asyncio.sleep(.1)
        raise RuntimeError(f"GMO SOK 对冲 {order_id} 未在撤单超时内进入终态")

    async def _execution_snapshot(self, order_id: str) -> tuple[Decimal, Decimal]:
        payload = await self.limiter.submit(
            EndpointGroup.QUERY, Priority.CRITICAL,
            lambda: self.adapter.executions(order_id),
        )
        rows = self._rows(payload)
        filled = sum((Decimal(str(row.get("size", "0"))) for row in rows), Decimal("0"))
        notional = sum((
            Decimal(str(row.get("size", "0"))) * Decimal(str(row.get("price", "0")))
            for row in rows
        ), Decimal("0"))
        return filled, notional

    async def _checkpoint_order(self, order_id: str, filled_qty: Decimal = Decimal("0"),
                                filled_notional: Decimal = Decimal("0"),
                                fee_jpy: Decimal = Decimal("0")) -> None:
        checkpoint = self._checkpoint.get()
        if checkpoint is not None:
            await checkpoint(order_id, filled_qty, filled_notional, fee_jpy)

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
            if status in ("EXECUTED", "CANCELED") and filled > 0:
                return filled, notional, confirmed_at
            # A terminal order status can become visible just before executions are
            # queryable. Returning zero here would immediately submit a duplicate hedge.
            # Keep polling until rows arrive; a genuinely zero-filled FAK times out and
            # escalates instead of being retried blindly.
            await asyncio.sleep(.15)
        detail = f"；最后查询错误：{last_error}" if last_error else ""
        raise RuntimeError(f"GMO 对冲 {order_id} 成交量未在 {timeout_sec:g}s 内确认{detail}")

    async def _order_status(self, order_id: str) -> str:
        return str((await self._order_detail(order_id)).get("status", "")).upper()

    async def _order_detail(self, order_id: str) -> dict:
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
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _order_id(payload: dict) -> str | None:
        data = payload.get("data", payload)
        if isinstance(data, str | int):
            return str(data)
        if isinstance(data, dict):
            value = data.get("orderId") or data.get("order-id") or data.get("id")
            return str(value) if value is not None else None
        return None

    @staticmethod
    def _rows(payload: dict) -> list[dict]:
        rows = payload.get("data", [])
        if isinstance(rows, dict):
            rows = rows.get("list", rows.get("executions", []))
        return rows if isinstance(rows, list) else []
