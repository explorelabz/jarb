from __future__ import annotations

import asyncio
import hmac
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .state_store import StateStore


class AlertNotifier(Protocol):
    async def send(self, message: str) -> bool: ...


@dataclass(frozen=True)
class RiskLimits:
    max_single_order_jpy: float = 250_000
    max_daily_volume_jpy: float = 5_000_000
    max_daily_loss_jpy: float = 100_000
    max_abs_delta: float = .05
    max_hedge_failures: int = 3
    max_hedge_p95_ms: int = 1_000
    arm_ttl_sec: int = 3_600


@dataclass(frozen=True)
class RiskSnapshot:
    market_age_ms: int = 0
    stale_market_ms: int = 800
    order_notional_jpy: float = 0
    daily_volume_jpy: float = 0
    daily_pnl_jpy: float = 0
    abs_delta: float = 0
    hedge_failures: int = 0
    hedge_p95_ms: int = 0


class RiskGate:
    def __init__(self, store: StateStore, limits: RiskLimits | None = None, *,
                 confirmation_phrase: str | None = None, kill_sentinel: Path | str | None = None,
                 require_dual_approval: bool = False, approval_ttl_sec: int = 300,
                 notifier: AlertNotifier | None = None):
        self.store = store
        self.limits = limits or RiskLimits()
        self.confirmation_phrase = confirmation_phrase if confirmation_phrase is not None else os.getenv("ARM_CONFIRMATION_PHRASE", "")
        self.kill_sentinel = Path(kill_sentinel or os.getenv("KILL_SENTINEL", "data/KILL"))
        self.require_dual_approval = require_dual_approval
        self.approval_ttl_sec = approval_ttl_sec
        self.notifier = notifier
        self.pending_arm_actor: str | None = None
        self.pending_arm_until = 0.0
        self.armed_until = 0.0
        self.recovery_complete = False
        self.killed = False
        self.last_reason: str | None = None
        self._lock = asyncio.Lock()

    @property
    def armed(self) -> bool:
        return not self.killed and self.recovery_complete and time.time() < self.armed_until

    async def restore(self) -> None:
        state = await self.store.get_state("risk", {})
        self.armed_until = 0.0  # restart always returns to DISARMED
        self.killed = bool(state.get("killed", False)) or self.kill_sentinel.exists()
        self.recovery_complete = False
        self.last_reason = "startup reconciliation required"
        self.pending_arm_actor = None
        self.pending_arm_until = 0.0

    async def mark_recovery_complete(self) -> None:
        self.recovery_complete = True
        self.last_reason = "operator arm required"
        await self._persist("recovery complete; operator arm required")

    async def arm(self, phrase: str, actor: str) -> bool:
        async with self._lock:
            if not self.confirmation_phrase:
                await self.store.audit(
                    "risk.arm.denied", "warning", "arm confirmation phrase is not configured", actor=actor,
                )
                raise ValueError("未配置 ARM_CONFIRMATION_PHRASE，禁止实盘 Arm")
            if not hmac.compare_digest(phrase, self.confirmation_phrase):
                await self.store.audit("risk.arm.denied", "warning", "arm confirmation phrase rejected", actor=actor)
                raise ValueError("确认短语错误")
            if self.killed:
                raise ValueError("kill switch 仍处于启用状态")
            if not self.recovery_complete:
                raise ValueError("启动恢复对账尚未完成")
            now = time.time()
            if self.require_dual_approval:
                if self.pending_arm_until <= now:
                    self.pending_arm_actor = None
                    self.pending_arm_until = 0.0
                if self.pending_arm_actor is None:
                    self.pending_arm_actor = actor
                    self.pending_arm_until = now + self.approval_ttl_sec
                    self.last_reason = "等待第二位操作员复核 Arm"
                    await self._persist("first arm approval recorded", actor)
                    return False
                if hmac.compare_digest(actor, self.pending_arm_actor):
                    raise ValueError("第二次 Arm 必须由不同操作员复核")
                first_actor = self.pending_arm_actor
                self.pending_arm_actor = None
                self.pending_arm_until = 0.0
                await self.store.audit(
                    "risk.arm.dual_approved", "critical",
                    f"dual approval completed by {first_actor} and {actor}", actor=actor,
                )
            self.armed_until = time.time() + self.limits.arm_ttl_sec
            self.last_reason = None
            await self._persist("armed", actor)
            return True

    async def disarm(self, reason: str, actor: str = "system") -> None:
        async with self._lock:
            self.armed_until = 0.0
            self.pending_arm_actor = None
            self.pending_arm_until = 0.0
            self.last_reason = reason
            await self._persist(f"disarmed: {reason}", actor)
            if self.notifier:
                try:
                    await self.notifier.send(f"⚠️ JARB 已 DISARM：{reason}（操作者：{actor}）")
                except Exception as exc:
                    # A notification failure must never interfere with a safety disarm.
                    await self.store.audit(
                        "alert.webhook.failed", "warning", f"risk disarm alert: {str(exc)[:240]}", actor=actor,
                    )

    async def kill(self, reason: str, actor: str = "system") -> None:
        async with self._lock:
            self.killed = True
            self.armed_until = 0.0
            self.pending_arm_actor = None
            self.pending_arm_until = 0.0
            self.last_reason = reason
            await self._persist(f"killed: {reason}", actor, level="critical")

    async def reset_kill(self, actor: str) -> None:
        async with self._lock:
            if self.kill_sentinel.exists():
                raise ValueError(f"外部 kill 哨兵仍存在：{self.kill_sentinel}")
            self.killed = False
            self.armed_until = 0.0
            self.pending_arm_actor = None
            self.pending_arm_until = 0.0
            self.recovery_complete = False
            self.last_reason = "kill reset; recovery and arm required"
            await self._persist("kill reset", actor)

    async def evaluate(self, snapshot: RiskSnapshot) -> tuple[bool, str | None]:
        if self.armed_until > 0 and time.time() >= self.armed_until:
            await self.disarm("arm expired")
        if self.kill_sentinel.exists() and not self.killed:
            await self.kill("external kill sentinel detected")
        checks = (
            (snapshot.market_age_ms > snapshot.stale_market_ms, "market data stale"),
            (snapshot.order_notional_jpy > self.limits.max_single_order_jpy, "single order limit exceeded"),
            (snapshot.daily_volume_jpy > self.limits.max_daily_volume_jpy, "daily volume limit exceeded"),
            (snapshot.daily_pnl_jpy < -self.limits.max_daily_loss_jpy, "daily loss limit exceeded"),
            (snapshot.abs_delta > self.limits.max_abs_delta, "delta limit exceeded"),
            (snapshot.hedge_failures >= self.limits.max_hedge_failures, "hedge failure limit exceeded"),
            (snapshot.hedge_p95_ms > self.limits.max_hedge_p95_ms, "hedge latency limit exceeded"),
        )
        for triggered, reason in checks:
            if triggered:
                if self.armed:
                    await self.disarm(reason)
                return False, reason
        if not self.armed:
            return False, self.last_reason or "engine is disarmed"
        return True, None

    async def _persist(self, message: str, actor: str = "system", level: str = "warning") -> None:
        await self.store.set_state("risk", {
            "armedUntil": self.armed_until, "killed": self.killed,
            "recoveryComplete": self.recovery_complete, "reason": self.last_reason,
            "pendingArmActor": self.pending_arm_actor,
            "pendingArmUntil": self.pending_arm_until,
        })
        await self.store.audit("risk.state", level, message, actor=actor)
