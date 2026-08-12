from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class QueuePosition:
    """FIFO volume ahead of a paper order, split by price priority."""

    ahead_better: Decimal = Decimal("0")
    ahead_same: Decimal = Decimal("0")

    @classmethod
    def from_levels(cls, levels: list[tuple[Decimal, Decimal]], side: str,
                    price: Decimal) -> QueuePosition:
        return cls(
            ahead_better=sum((size for level_price, size in levels if (
                level_price < price if side == "SELL" else level_price > price
            )), Decimal("0")),
            ahead_same=sum((
                size for level_price, size in levels if level_price == price
            ), Decimal("0")),
        )

    @property
    def ahead_qty(self) -> Decimal:
        return self.ahead_better + self.ahead_same

    def clear(self) -> None:
        self.ahead_better = Decimal("0")
        self.ahead_same = Decimal("0")

    def consume(self, available: Decimal) -> Decimal:
        """Consume same-price FIFO volume before better-price volume."""
        consumed_same = min(self.ahead_same, available)
        self.ahead_same -= consumed_same
        available -= consumed_same
        consumed_better = min(self.ahead_better, available)
        self.ahead_better -= consumed_better
        return consumed_same + consumed_better

    def resync(self, levels: list[tuple[Decimal, Decimal]], side: str,
               price: Decimal) -> None:
        snapshot = self.from_levels(levels, side, price)
        self.ahead_better = min(self.ahead_better, snapshot.ahead_better)
        self.ahead_same = min(self.ahead_same, snapshot.ahead_same)
