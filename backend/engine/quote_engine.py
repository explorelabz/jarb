from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RequotePolicy:
    price_deviation_bps: Decimal = Decimal("2")
    depth_change_ratio: Decimal = Decimal("0.25")
    min_remaining_ratio: Decimal = Decimal("0.4")


@dataclass(frozen=True)
class WorkingQuote:
    price: Decimal
    original_qty: Decimal
    remaining_qty: Decimal
    reference_depth: Decimal


class QuoteEngine:
    def __init__(self, policy: RequotePolicy | None = None):
        self.policy = policy or RequotePolicy()

    def should_requote(self, current: WorkingQuote | None, *, target_price: Decimal,
                       target_qty: Decimal, current_depth: Decimal) -> bool:
        if current is None:
            return target_qty > 0
        price_bps = abs(target_price - current.price) / current.price * Decimal("10000")
        depth_change = abs(current_depth - current.reference_depth) / max(current.reference_depth, Decimal("1e-18"))
        remaining_ratio = current.remaining_qty / max(current.original_qty, Decimal("1e-18"))
        return (
            price_bps > self.policy.price_deviation_bps
            or depth_change > self.policy.depth_change_ratio
            or remaining_ratio < self.policy.min_remaining_ratio
        )
