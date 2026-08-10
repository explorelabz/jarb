from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class OrderState(StrEnum):
    NEW = "NEW"
    PLACING = "PLACING"
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"
    FILLED = "FILLED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


ORDER_TRANSITIONS: dict[OrderState, set[OrderState]] = {
    OrderState.NEW: {OrderState.PLACING, OrderState.FAILED},
    OrderState.PLACING: {OrderState.OPEN, OrderState.PARTIAL, OrderState.FILLED, OrderState.FAILED, OrderState.UNKNOWN},
    OrderState.OPEN: {OrderState.PARTIAL, OrderState.FILLED, OrderState.CANCELING, OrderState.UNKNOWN},
    OrderState.PARTIAL: {OrderState.PARTIAL, OrderState.FILLED, OrderState.CANCELING, OrderState.UNKNOWN},
    OrderState.CANCELING: {OrderState.CANCELED, OrderState.FILLED, OrderState.PARTIAL, OrderState.UNKNOWN},
    OrderState.UNKNOWN: {OrderState.OPEN, OrderState.PARTIAL, OrderState.FILLED, OrderState.CANCELED, OrderState.FAILED},
    OrderState.CANCELED: set(),
    OrderState.FILLED: set(),
    OrderState.FAILED: set(),
}


class HedgeStatus(StrEnum):
    PENDING = "PENDING_HEDGE"
    HEDGING = "HEDGING"
    HEDGED = "HEDGED"
    RETRY = "RETRY"
    ESCALATE = "ESCALATE"


HEDGE_TRANSITIONS: dict[HedgeStatus, set[HedgeStatus]] = {
    HedgeStatus.PENDING: {HedgeStatus.HEDGING, HedgeStatus.ESCALATE},
    HedgeStatus.HEDGING: {HedgeStatus.HEDGED, HedgeStatus.RETRY, HedgeStatus.ESCALATE},
    HedgeStatus.RETRY: {HedgeStatus.HEDGING, HedgeStatus.ESCALATE},
    HedgeStatus.HEDGED: set(),
    HedgeStatus.ESCALATE: set(),
}


class EventType(StrEnum):
    MARKET = "market.updated"
    FILL = "fill.incremental"
    HEDGE_DUE = "hedge.due"
    RISK = "risk.changed"
    CONTROL = "control.changed"


@dataclass(frozen=True)
class FillDelta:
    fill_id: int
    client_order_id: str
    order_id: str
    trade_id: str
    symbol: str
    side: str
    cumulative_qty: Decimal
    incremental_qty: Decimal
    price: Decimal
    fee: Decimal


@dataclass(frozen=True)
class HedgeIntent:
    id: str
    client_fill_id: int
    symbol: str
    side: str
    qty: Decimal
    filled_qty: Decimal
    filled_notional: Decimal
    status: HedgeStatus
    attempts: int
    latency_ms: int
    created_at: str
    exchange_order_id: str | None = None
