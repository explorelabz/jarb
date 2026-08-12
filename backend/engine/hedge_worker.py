from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Protocol

from ..adapters import ExchangeAPIError, GmoAdapter
from ..models import Side
from .domain import (
    EventType, HedgeIntent, HedgePreconditionError, HedgeStatus,
    HedgeSubmission, HedgeSubmissionUnknown,
)
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


@dataclass(frozen=True)
class HedgeSubmissionRecovery:
    order_id: str
    execution: HedgeExecution


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
            repaired = await self.store.backfill_missing_hedge_intents()
            if repaired:
                await self.store.audit(
                    "hedge.intent.recovered", "critical",
                    f"recreated {repaired} missing hedge intent(s) from durable fills",
                )
            await self._recover_durable_submissions()
            await self._recover_inflight()
            self._fill_queue = self.events.open_queue(EventType.FILL)
            self._listener = asyncio.create_task(self._listen(), name="hedge-fill-listener")
            self._worker = asyncio.create_task(self._run(), name="hedge-worker")
            if await self.store.pending_hedges():
                self._wake.set()

    async def _recover_inflight(self) -> None:
        pending_submission_intents = {
            intent_id for submission in await self.store.pending_hedge_submissions()
            for intent_id in submission.intent_ids
        }
        grouped: dict[str | None, list[HedgeIntent]] = defaultdict(list)
        for intent in await self.store.pending_hedges():
            if intent.status != HedgeStatus.HEDGING or intent.id in pending_submission_intents:
                continue
            submissions = await self.store.submissions_for_intent(intent.id)
            latest = submissions[-1] if submissions else None
            if (
                latest is not None and latest.status == "RESOLVED"
                and latest.exchange_order_id == intent.exchange_order_id
            ):
                target = HedgeStatus.HEDGED if intent.filled_qty >= intent.qty else HedgeStatus.RETRY
                await self.store.transition_hedge(
                    intent.id, target,
                    error=None if target == HedgeStatus.HEDGED else "resolved durable submission was partial",
                )
                continue
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

    async def _recover_durable_submissions(self) -> None:
        submissions = await self.store.pending_hedge_submissions()
        if not submissions:
            return
        recover = getattr(self.executor, "recover_submission", None)
        if recover is None:
            await self.risk.disarm("durable hedge submission recovery is unavailable")
            return
        for submission in submissions:
            try:
                recovered: HedgeSubmissionRecovery | None = await recover(submission)
            except Exception as exc:
                await self.store.audit(
                    "hedge.submission.unresolved", "critical",
                    f"{submission.id} remains unresolved: {str(exc)[:200]}",
                    metadata={
                        "symbol": submission.symbol, "side": submission.side,
                        "qty": str(submission.qty), "submittedAt": submission.submitted_at,
                    },
                )
                await self.risk.disarm(
                    f"manual reconciliation required for hedge submission {submission.id}"
                )
                continue
            if recovered is None:
                await self.store.set_hedge_submission_status(
                    submission.id, "ABSENT", error="exchange history proved no matching order",
                )
            else:
                if not submission.exchange_order_id:
                    await self.store.acknowledge_hedge_submission(
                        submission.id, recovered.order_id,
                    )
                execution = recovered.execution
                await self.store.finish_hedge_submission(
                    submission.id, order_id=recovered.order_id,
                    filled_qty=execution.filled_qty,
                    filled_notional=execution.filled_notional,
                    fee_jpy=execution.fee_jpy,
                )
            for intent_id in submission.intent_ids:
                intent = await self.store.hedge_intent(intent_id)
                if intent is None or intent.status != HedgeStatus.HEDGING:
                    continue
                target = HedgeStatus.HEDGED if intent.filled_qty >= intent.qty else HedgeStatus.RETRY
                await self.store.transition_hedge(
                    intent.id, target,
                    error=None if target == HedgeStatus.HEDGED else "durable submission recovered",
                    exchange_order_id=(recovered.order_id if recovered else None),
                    latency_ms=self._latency_ms(
                        intent, recovered.execution.confirmed_at if recovered else "",
                    ),
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
            await self._fill_queue.get()
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
            blocked_ids = {
                intent_id for submission in await self.store.pending_hedge_submissions()
                for intent_id in submission.intent_ids
            }
            for intent in await self.store.pending_hedges():
                if intent.id in self._active_intents or intent.id in blocked_ids:
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
        inflight = [
            intent for intent in intents
            if intent.status == HedgeStatus.HEDGING and intent.exchange_order_id
        ]
        if inflight:
            await self._resolve_inflight(inflight)
            self._wake.set()
            return
        active: list[HedgeIntent] = []
        for intent in intents:
            if intent.attempts >= self.max_attempts:
                await self.store.transition_hedge(intent.id, HedgeStatus.ESCALATE, error="retry limit exceeded")
                await self.risk.disarm(f"hedge retry limit exceeded for {symbol}")
                continue
            active.append(await self.store.transition_hedge(intent.id, HedgeStatus.HEDGING))
        if not active:
            return
        active_qty = sum(
            (intent.qty - intent.filled_qty for intent in active), Decimal("0"),
        )
        if active_qty <= 0:
            return
        checkpointed_order_id: str | None = None
        durable_submissions = hasattr(self.executor, "set_submission_hooks")
        if durable_submissions:
            async def prepare_submission(
                execution_type: str, submission_qty: Decimal, submitted_at: str,
            ) -> str:
                submission = await self.store.prepare_hedge_submission(
                    [intent.id for intent in active], symbol=symbol, side=side,
                    qty=submission_qty, execution_type=execution_type,
                    submitted_at=submitted_at,
                )
                return submission.id

            async def acknowledge_submission(submission_id: str, order_id: str) -> None:
                nonlocal checkpointed_order_id
                checkpointed_order_id = order_id
                await self.store.acknowledge_hedge_submission(submission_id, order_id)
                for intent in active:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.HEDGING, exchange_order_id=order_id,
                    )

            async def complete_submission(
                submission_id: str, order_id: str, filled_qty: Decimal,
                filled_notional: Decimal, fee_jpy: Decimal,
            ) -> None:
                await self.store.finish_hedge_submission(
                    submission_id, order_id=order_id, filled_qty=filled_qty,
                    filled_notional=filled_notional, fee_jpy=fee_jpy,
                )

            async def reject_submission(submission_id: str, error: str) -> None:
                await self.store.set_hedge_submission_status(
                    submission_id, "ABSENT", error=error[:240],
                )

            self.executor.set_submission_hooks(
                prepare_submission, acknowledge_submission, complete_submission,
                reject_submission,
            )
        if hasattr(self.executor, "set_checkpoint"):
            async def checkpoint(order_id: str, filled_qty: Decimal = Decimal("0"),
                                 filled_notional: Decimal = Decimal("0"),
                                 fee_jpy: Decimal = Decimal("0")) -> None:
                nonlocal checkpointed_order_id
                # Mark the group as externally submitted before any SQLite write.
                # Even a checkpoint failure must not permit a blind second order.
                checkpointed_order_id = order_id
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
            result = await self.executor(symbol, side, active_qty)
        except Exception as exc:
            await self.store.audit(
                "hedge.retry", "warning", f"{symbol} {side}: {str(exc)[:200]}",
                metadata={
                    "intentIds": [intent.id for intent in active],
                    "attempts": max(intent.attempts for intent in active),
                    "qty": str(active_qty),
                    "exchangeOrderId": checkpointed_order_id,
                },
            )
            if isinstance(exc, HedgeSubmissionUnknown):
                for intent in active:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.HEDGING,
                        error=f"unresolved durable submission {exc.submission_id}: {str(exc)[:160]}",
                    )
                await self.risk.disarm(f"unresolved hedge submission {exc.submission_id} for {symbol}")
                return
            if isinstance(exc, HedgePreconditionError):
                for intent in active:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.RETRY, error=str(exc)[:240],
                    )
                await self.risk.disarm(f"hedge precondition failed for {symbol}")
                self._wake.set()
                return
            if checkpointed_order_id is not None:
                for intent in active:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.HEDGING,
                        exchange_order_id=checkpointed_order_id, error=str(exc)[:240],
                    )
                await self.risk.disarm(f"unresolved hedge order {checkpointed_order_id} for {symbol}")
                self._wake.set()
                return
            # Without an order id a response timeout is still ambiguous: GMO may have
            # accepted the FAK. Manual reconciliation is safer than a second order.
            for intent in active:
                await self.store.transition_hedge(
                    intent.id, HedgeStatus.ESCALATE,
                    error=f"ambiguous hedge submission: {str(exc)[:200]}",
                )
            await self.risk.disarm(f"manual hedge reconciliation required for {symbol}")
            return

        if durable_submissions:
            refreshed = [await self.store.hedge_intent(intent.id) for intent in active]
            for intent in refreshed:
                if intent is None:
                    continue
                target = HedgeStatus.HEDGED if intent.filled_qty >= intent.qty else HedgeStatus.RETRY
                await self.store.transition_hedge(
                    intent.id, target,
                    latency_ms=self._latency_ms(intent, result.confirmed_at),
                    exchange_order_id=result.order_id,
                    error=None if target == HedgeStatus.HEDGED else "GMO hedge partially filled",
                )
            if self.on_execution is not None and result.filled_qty > 0:
                await self.on_execution(symbol, side, result)
            if result.filled_qty < active_qty:
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
        if result.filled_qty < active_qty:
            self._wake.set()

    async def _resolve_inflight(self, intents: list[HedgeIntent]) -> None:
        grouped: dict[str, list[HedgeIntent]] = defaultdict(list)
        for intent in intents:
            if intent.exchange_order_id:
                grouped[intent.exchange_order_id].append(intent)
        for order_id, group in grouped.items():
            if self.resolver is None:
                for intent in group:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.ESCALATE,
                        error="no resolver for checkpointed hedge order",
                    )
                await self.risk.disarm(f"manual hedge reconciliation required for {group[0].symbol}")
                continue
            try:
                result = await self.resolver(group[0])
            except Exception as exc:
                for intent in group:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.HEDGING,
                        exchange_order_id=order_id, error=str(exc)[:240],
                    )
                await self.store.audit(
                    "hedge.resolve.pending", "critical",
                    f"{group[0].symbol} order {order_id} remains unresolved: {str(exc)[:200]}",
                    metadata={"intentIds": [intent.id for intent in group]},
                )
                await self.risk.disarm(f"unresolved hedge order {order_id} for {group[0].symbol}")
                return
            if result is None:
                for intent in group:
                    await self.store.transition_hedge(
                        intent.id, HedgeStatus.RETRY,
                        error="exchange confirmed checkpointed order absent",
                    )
                continue
            available = result.filled_qty
            average = result.filled_notional / result.filled_qty if result.filled_qty else Decimal("0")
            for intent in group:
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
                    error=None if target == HedgeStatus.HEDGED else "resolved partial hedge",
                )
            if self.on_execution is not None and result.filled_qty > 0:
                await self.on_execution(group[0].symbol, group[0].side, result)

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
                 fill_timeout_sec: float = 5.0,
                 orphan_recovery_grace_sec: float = 15.0):
        self.adapter = adapter
        self.limiter = limiter
        self.size_steps = size_steps
        self.price_ticks = price_ticks or {}
        self.passive_price = passive_price
        self.maker_fee_bps = maker_fee_bps or {}
        self.taker_fee_bps = taker_fee_bps or {}
        self.passive_timeout_ms = passive_timeout_ms or {}
        self.fill_timeout_sec = fill_timeout_sec
        self.orphan_recovery_grace_sec = max(0.0, orphan_recovery_grace_sec)
        self._checkpoint: ContextVar[
            Callable[[str, Decimal, Decimal, Decimal], Awaitable[None]] | None
        ] = ContextVar(
            "gmo_hedge_checkpoint", default=None,
        )
        self._submission_hooks: ContextVar[
            tuple[Callable, Callable, Callable, Callable] | None
        ] = ContextVar(
            "gmo_hedge_submission_hooks", default=None,
        )

    def set_checkpoint(self, callback: Callable[[str, Decimal, Decimal, Decimal], Awaitable[None]]) -> None:
        self._checkpoint.set(callback)

    def set_submission_hooks(
        self, prepare: Callable[[str, Decimal, str], Awaitable[str]],
        acknowledge: Callable[[str, str], Awaitable[None]],
        complete: Callable[[str, str, Decimal, Decimal, Decimal], Awaitable[None]],
        reject: Callable[[str, str], Awaitable[None]],
    ) -> None:
        self._submission_hooks.set((prepare, acknowledge, complete, reject))

    async def _prepare_submission(
        self, execution_type: str, qty: Decimal, submitted_at: str,
    ) -> str | None:
        hooks = self._submission_hooks.get()
        return await hooks[0](execution_type, qty, submitted_at) if hooks else None

    async def _acknowledge_submission(self, submission_id: str | None, order_id: str) -> None:
        hooks = self._submission_hooks.get()
        if hooks and submission_id:
            await hooks[1](submission_id, order_id)

    async def _complete_submission(
        self, submission_id: str | None, order_id: str, filled_qty: Decimal,
        filled_notional: Decimal, fee_jpy: Decimal,
    ) -> None:
        hooks = self._submission_hooks.get()
        if hooks and submission_id:
            await hooks[2](submission_id, order_id, filled_qty, filled_notional, fee_jpy)

    async def __call__(self, symbol: str, side: str, qty: Decimal) -> HedgeExecution:
        if self.passive_price is None or not hasattr(self.adapter, "post_only_order"):
            return await self._market_execution(symbol, side, qty)
        return await self._hybrid_execution(symbol, side, qty)

    async def _hybrid_execution(self, symbol: str, side: str, qty: Decimal) -> HedgeExecution:
        submitted_at = datetime.now(timezone.utc).isoformat()
        step = self.size_steps.get(symbol, Decimal("0.00000001"))
        tick = self.price_ticks.get(symbol, Decimal("1"))
        passive_price = self.passive_price(symbol, side)
        passive_submission = await self._prepare_submission("SOK", qty, submitted_at)
        try:
            response = await self.limiter.submit(
                EndpointGroup.HEDGE, Priority.CRITICAL,
                lambda: self.adapter.post_only_order(
                    symbol, side, qty, passive_price, step, tick,
                ),
            )
        except HedgePreconditionError as exc:
            if passive_submission:
                await self.store_submission_absent(passive_submission, str(exc))
            raise
        except ExchangeAPIError as exc:
            if passive_submission:
                # A structured exchange rejection proves no order was accepted.
                await self.store_submission_absent(passive_submission, str(exc))
            raise HedgePreconditionError(str(exc)) from exc
        except Exception as exc:
            if passive_submission:
                raise HedgeSubmissionUnknown(passive_submission, str(exc)) from exc
            raise
        passive_id = self._order_id(response)
        if not passive_id:
            if passive_submission:
                raise HedgeSubmissionUnknown(passive_submission, "GMO SOK response had no order id")
            raise RuntimeError("GMO hedge response had no order id")
        await self._acknowledge_submission(passive_submission, passive_id)
        if not passive_submission:
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
        await self._complete_submission(
            passive_submission, passive_id, passive_filled, passive_notional, passive_fee,
        )
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
            # Persist one real exchange identifier only. The fallback checkpoint
            # already replaced the passive id after durably carrying its fills.
            order_id=fallback.order_id,
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
        submission_id = await self._prepare_submission("FAK", qty, submitted_at)
        try:
            response = await self.limiter.submit(
                EndpointGroup.HEDGE, Priority.CRITICAL,
                lambda: self.adapter.market_order(
                    symbol, side, qty, self.size_steps.get(symbol, Decimal("0.00000001")),
                ),
            )
        except HedgePreconditionError as exc:
            if submission_id:
                await self.store_submission_absent(submission_id, str(exc))
            raise
        except ExchangeAPIError as exc:
            if submission_id:
                await self.store_submission_absent(submission_id, str(exc))
            raise HedgePreconditionError(str(exc)) from exc
        except Exception as exc:
            if submission_id:
                raise HedgeSubmissionUnknown(submission_id, str(exc)) from exc
            raise
        order_id = self._order_id(response)
        if not order_id:
            if submission_id:
                raise HedgeSubmissionUnknown(submission_id, "GMO FAK response had no order id")
            raise RuntimeError("GMO hedge response had no order id")
        await self._acknowledge_submission(submission_id, order_id)
        if not submission_id:
            await self._checkpoint_order(order_id, carried_qty, carried_notional, carried_fee)
        filled, notional, confirmed_at = await self._await_fills(
            order_id, qty, timeout_sec=self.fill_timeout_sec,
        )
        fee = notional * self.taker_fee_bps.get(symbol, Decimal("9")) / Decimal("10000")
        await self._complete_submission(submission_id, order_id, filled, notional, fee)
        return HedgeExecution(order_id, filled, notional, fee, submitted_at, confirmed_at)

    async def store_submission_absent(self, submission_id: str, error: str) -> None:
        hooks = self._submission_hooks.get()
        if hooks:
            await hooks[3](submission_id, error)

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
                fallback.order_id,
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

    async def recover_submission(
        self, submission: HedgeSubmission,
    ) -> HedgeSubmissionRecovery | None:
        """Recover ACKED or response-lost submissions without issuing a new order."""
        if submission.exchange_order_id:
            execution = await self._recover_known_submission(
                submission, submission.exchange_order_id,
            )
            return HedgeSubmissionRecovery(submission.exchange_order_id, execution)
        order_id, execution = await self._find_orphan_submission(submission)
        if order_id is None:
            return None
        if execution is None:
            execution = await self._recover_known_submission(submission, order_id)
        return HedgeSubmissionRecovery(order_id, execution)

    async def _recover_known_submission(
        self, submission: HedgeSubmission, order_id: str,
    ) -> HedgeExecution:
        submitted_at = submission.submitted_at
        if submission.execution_type == "SOK":
            detail = await self._order_detail(order_id)
            price = Decimal(str(detail.get("price", "0") or "0"))
            status = str(detail.get("status", "")).upper()
            if status not in ("EXECUTED", "CANCELED", "EXPIRED"):
                filled, notional = await self._cancel_passive(order_id, submission.qty, price)
            else:
                filled, notional = await self._execution_snapshot(order_id)
                executed = Decimal(str(detail.get("executedSize", "0") or "0"))
                if executed > filled:
                    filled, notional = executed, executed * price
            fee = notional * self.maker_fee_bps.get(submission.symbol, Decimal("-1")) / Decimal("10000")
            return HedgeExecution(
                order_id, filled, notional, fee, submitted_at,
                datetime.now(timezone.utc).isoformat(),
            )
        filled, notional, confirmed_at = await self._await_fills(
            order_id, submission.qty, timeout_sec=self.fill_timeout_sec,
        )
        fee = notional * self.taker_fee_bps.get(submission.symbol, Decimal("9")) / Decimal("10000")
        return HedgeExecution(order_id, filled, notional, fee, submitted_at, confirmed_at)

    async def _find_orphan_submission(
        self, submission: HedgeSubmission,
    ) -> tuple[str | None, HedgeExecution | None]:
        if not hasattr(self.adapter, "latest_executions") or not hasattr(self.adapter, "active_orders"):
            raise RuntimeError("GMO orphan-order history APIs are unavailable")
        submitted = datetime.fromisoformat(submission.submitted_at.replace("Z", "+00:00"))
        if submitted.tzinfo is None:
            submitted = submitted.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - submitted > timedelta(hours=23):
            raise RuntimeError("submission is older than GMO latestExecutions retention")
        age_sec = (datetime.now(timezone.utc) - submitted).total_seconds()
        if age_sec < self.orphan_recovery_grace_sec:
            await asyncio.sleep(self.orphan_recovery_grace_sec - max(0.0, age_sec))
        lower = submitted - timedelta(seconds=15)
        upper = submitted + timedelta(minutes=5)
        if hasattr(self.adapter, "executions_by_symbol_window"):
            payload = await self.limiter.submit(
                EndpointGroup.QUERY, Priority.CRITICAL,
                lambda: self.adapter.executions_by_symbol_window(
                    submission.symbol, lower, upper, count=100, max_pages=10,
                ),
            )
            execution_rows = self._rows(payload)
            executions_covered = bool(payload.get("windowCovered", False))
        else:
            execution_rows, executions_covered = await self._paged_recovery_rows(
                lambda page: self.adapter.latest_executions(
                    submission.symbol, page=page, count=100,
                ), lower,
            )
        active_rows, active_covered = await self._paged_recovery_rows(
            lambda page: self.adapter.active_orders(
                submission.symbol, page=page, count=100,
            ), lower,
        )
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in execution_rows:
            if str(row.get("side", "")).upper() != submission.side:
                continue
            timestamp = self._exchange_timestamp(row.get("timestamp"))
            if timestamp is None or not lower <= timestamp <= upper:
                continue
            order_id = row.get("orderId", row.get("order-id"))
            if order_id is not None:
                grouped[str(order_id)].append(row)
        active_candidates: set[str] = set()
        for row in active_rows:
            timestamp = self._exchange_timestamp(row.get("timestamp"))
            execution_type = str(row.get("executionType", "")).upper()
            time_in_force = str(row.get("timeInForce", "")).upper()
            if (
                timestamp is not None and lower <= timestamp <= upper
                and str(row.get("side", "")).upper() == submission.side
                and Decimal(str(row.get("size", "0"))) == submission.qty
                and (
                    submission.execution_type == "FAK" and execution_type == "MARKET"
                    or submission.execution_type == "SOK" and time_in_force == "SOK"
                )
            ):
                order_id = row.get("orderId", row.get("order-id"))
                if order_id is not None:
                    active_candidates.add(str(order_id))
        execution_candidates: set[str] = set()
        for order_id, rows in grouped.items():
            total = sum((Decimal(str(row.get("size", "0"))) for row in rows), Decimal("0"))
            if total <= submission.qty and await self._order_matches_submission(
                order_id, submission, lower, upper,
            ):
                execution_candidates.add(order_id)
        candidates = execution_candidates | active_candidates
        if len(candidates) > 1:
            raise RuntimeError(
                f"{len(candidates)} GMO orders match durable submission {submission.id}"
            )
        if not candidates:
            if not executions_covered or not active_covered:
                raise RuntimeError("GMO history pagination did not cover submission time window")
            return None, None
        order_id = next(iter(candidates))
        rows = grouped.get(order_id, [])
        if not rows:
            return order_id, None
        filled = sum((Decimal(str(row.get("size", "0"))) for row in rows), Decimal("0"))
        notional = sum((
            Decimal(str(row.get("size", "0"))) * Decimal(str(row.get("price", "0")))
            for row in rows
        ), Decimal("0"))
        fee = sum((Decimal(str(row.get("fee", "0"))) for row in rows), Decimal("0"))
        confirmed = max(
            (str(row.get("timestamp", submission.submitted_at)) for row in rows),
            default=submission.submitted_at,
        )
        return order_id, HedgeExecution(
            order_id, filled, notional, fee, submission.submitted_at, confirmed,
        )

    async def _order_matches_submission(
        self, order_id: str, submission: HedgeSubmission,
        lower: datetime, upper: datetime,
    ) -> bool:
        detail = await self._order_detail(order_id)
        timestamp = self._exchange_timestamp(detail.get("timestamp"))
        execution_type = str(detail.get("executionType", "")).upper()
        time_in_force = str(detail.get("timeInForce", "")).upper()
        raw_symbol = str(detail.get("symbol", "")).upper()
        symbol = raw_symbol if raw_symbol.endswith("_JPY") else f"{raw_symbol}_JPY"
        return (
            timestamp is not None and lower <= timestamp <= upper
            and symbol == submission.symbol
            and str(detail.get("side", "")).upper() == submission.side
            and Decimal(str(detail.get("size", "0"))) == submission.qty
            and (
                submission.execution_type == "FAK" and execution_type == "MARKET"
                or submission.execution_type == "SOK" and time_in_force == "SOK"
            )
        )

    async def _paged_recovery_rows(
        self, request: Callable[[int], Awaitable[dict]], lower: datetime,
    ) -> tuple[list[dict], bool]:
        rows: list[dict] = []
        covered = False
        for page in range(1, 11):
            payload = await self.limiter.submit(
                EndpointGroup.QUERY, Priority.CRITICAL,
                lambda page=page: request(page),
            )
            batch = self._rows(payload)
            rows.extend(batch)
            timestamps = [
                timestamp for timestamp in (
                    self._exchange_timestamp(row.get("timestamp")) for row in batch
                ) if timestamp is not None
            ]
            if len(batch) < 100 or timestamps and min(timestamps) <= lower:
                covered = True
                break
        return rows, covered

    @staticmethod
    def _exchange_timestamp(value) -> datetime | None:
        if not value:
            return None
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return result if result.tzinfo else result.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    async def _watch_passive(self, order_id: str, expected: Decimal, price: Decimal, *,
                             timeout_sec: float) -> tuple[Decimal, Decimal, str]:
        # SOK uses one fixed limit price, so cumulative executedSize from the order
        # record is sufficient to calculate notional exactly. Paper and live share
        # this bounded backoff path; all queries still pass through the same limiter.
        deadline = asyncio.get_running_loop().time() + timeout_sec
        poll_interval = .1
        while True:
            detail = await self._order_detail(order_id)
            status = str(detail.get("status", "")).upper()
            filled = min(expected, Decimal(str(detail.get("executedSize", "0") or "0")))
            if status in ("EXECUTED", "CANCELED", "EXPIRED"):
                return filled, filled * price, status
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return filled, filled * price, status
            await asyncio.sleep(min(poll_interval, remaining))
            poll_interval = min(poll_interval * 1.5, .5)

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
            if status in ("EXECUTED", "CANCELED", "EXPIRED") and filled > 0:
                return filled, notional, confirmed_at
            # A terminal order status can become visible just before executions are
            # queryable. Returning zero here would immediately submit a duplicate hedge.
            # Keep polling until rows arrive; a genuinely zero-filled FAK times out and
            # escalates instead of being retried blindly.
            await asyncio.sleep(.15)
        # A final ordered snapshot establishes the only safe zero-fill retry case.
        # Every ambiguous/partially observable terminal state remains unresolved.
        try:
            filled, notional = await self._execution_snapshot(order_id)
            order = await self._order_detail(order_id)
            status = str(order.get("status", "")).upper()
            executed = Decimal(str(order.get("executedSize", "0") or "0"))
            confirmed_at = datetime.now(timezone.utc).isoformat()
            if filled >= expected or (
                status in ("EXECUTED", "CANCELED", "EXPIRED") and filled > 0
            ):
                return filled, notional, confirmed_at
            if status in ("CANCELED", "EXPIRED") and filled == 0 and executed == 0:
                return Decimal("0"), Decimal("0"), confirmed_at
        except Exception as exc:
            last_error = exc
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
