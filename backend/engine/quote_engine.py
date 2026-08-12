from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP


@dataclass(frozen=True)
class RequotePolicy:
    price_deviation_bps: Decimal = Decimal("8")
    depth_change_ratio: Decimal = Decimal("0.6")
    min_remaining_ratio: Decimal = Decimal("0.25")


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


def target_price(levels: list[tuple[Decimal, Decimal]], gmo_hedge_price: Decimal,
                 edge_bps: Decimal, queue_budget_jpy: Decimal, tick: Decimal,
                 side: str, *, opposite_best: Decimal | None = None) -> Decimal | None:
    """Join one tick ahead of the first profitable level within a JPY queue budget."""
    if not levels or gmo_hedge_price <= 0 or tick <= 0:
        return None
    ahead = Decimal("0")
    edge = edge_bps / Decimal("10000")
    if side == "SELL":
        boundary = gmo_hedge_price * (Decimal("1") + edge)
        boundary = (boundary / tick).to_integral_value(rounding=ROUND_UP) * tick
        for price, size in levels:  # asks, ascending
            budget_qty = queue_budget_jpy / price if price > 0 else Decimal("0")
            if price >= boundary and ahead <= budget_qty:
                result = max(boundary, price - tick)
                return result if opposite_best is None or result > opposite_best else None
            ahead += max(Decimal("0"), size)
        return None
    if side == "BUY":
        boundary = gmo_hedge_price * (Decimal("1") - edge)
        boundary = (boundary / tick).to_integral_value(rounding=ROUND_DOWN) * tick
        for price, size in levels:  # bids, descending
            budget_qty = queue_budget_jpy / price if price > 0 else Decimal("0")
            if price <= boundary and ahead <= budget_qty:
                result = min(boundary, price + tick)
                return result if opposite_best is None or result < opposite_best else None
            ahead += max(Decimal("0"), size)
        return None
    raise ValueError("side must be BUY or SELL")
