from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .execution_gateway import ExecutionGateway
from .risk import RiskGate
from .state_store import StateStore


class RecoveryCoordinator:
    """Blocks arming until clock sync and durable order reconciliation have completed."""

    def __init__(self, store: StateStore, risk: RiskGate, *, gateway: ExecutionGateway | None = None,
                 gmo=None, bittrade=None, cancel_existing: bool = False,
                 reconcile_fills: Callable[[list[dict]], Awaitable[None]] | None = None):
        self.store = store
        self.risk = risk
        self.gateway = gateway
        self.gmo = gmo
        self.bittrade = bittrade
        self.cancel_existing = cancel_existing
        self.reconcile_fills = reconcile_fills

    async def run(self) -> None:
        await self.risk.disarm("startup reconciliation in progress")
        for venue, adapter in (("GMO", self.gmo), ("BitTrade", self.bittrade)):
            if adapter is None or not hasattr(adapter, "sync_time"):
                continue
            try:
                offset = await adapter.sync_time()
                await self.store.set_state(f"clock-offset:{venue.lower()}", offset)
            except Exception as exc:
                await self.store.audit("recovery.clock.error", "warning", f"{venue}: {str(exc)[:200]}")
        local_open = await self.store.open_orders()
        reconciliation_orders = await self.store.orders_for_fill_reconciliation()
        remote_open: list[dict] = []
        if self.bittrade is not None and hasattr(self.bittrade, "open_orders"):
            try:
                payload = await self.bittrade.open_orders()
                data = payload.get("data", [])
                remote_open = data if isinstance(data, list) else data.get("orders", [])
            except Exception as exc:
                await self.store.audit("recovery.orders.error", "critical", str(exc)[:240])
                return
        known_exchange_ids = {str(row["exchange_order_id"]) for row in local_open if row.get("exchange_order_id")}
        unmanaged = [row for row in remote_open if str(row.get("id")) not in known_exchange_ids]
        if unmanaged:
            await self.store.audit(
                "recovery.orders.unmanaged", "critical",
                f"{len(unmanaged)} remote open orders are not owned by this StateStore",
            )
            return
        if local_open:
            if not self.cancel_existing or self.gateway is None:
                await self.store.audit(
                    "recovery.orders.blocked", "critical",
                    f"{len(local_open)} durable open orders require reconciliation",
                )
                return
            await self.gateway.cancel_all()
        if self.reconcile_fills is not None:
            try:
                await self.reconcile_fills(reconciliation_orders)
            except Exception as exc:
                await self.store.audit("recovery.fills.error", "critical", str(exc)[:240])
                return
            for _ in range(300):
                if not await self.store.pending_hedges():
                    break
                await asyncio.sleep(.1)
            if await self.store.pending_hedges():
                await self.store.audit("recovery.hedge.timeout", "critical", "hedge recovery did not converge")
                return
        if await self.store.escalated_hedges():
            await self.store.audit("recovery.hedge.escalated", "critical", "manual hedge resolution required")
            return
        await self.risk.mark_recovery_complete()
