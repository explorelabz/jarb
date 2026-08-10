from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class CachedBalance:
    available: Decimal
    reserved: Decimal = Decimal("0")
    updated_at: float = 0

    @property
    def spendable(self) -> Decimal:
        return max(Decimal("0"), self.available - self.reserved)


class BalanceCache:
    def __init__(self, safety_factor: Decimal = Decimal("0.7"), ttl_sec: float = 10):
        self.safety_factor = safety_factor
        self.ttl_sec = ttl_sec
        self._balances: dict[tuple[str, str], CachedBalance] = {}
        self._allocations: dict[tuple[str, str], Decimal] = {}
        self._lock = asyncio.Lock()

    async def update(self, venue: str, asset: str, available: Decimal) -> None:
        async with self._lock:
            current = self._balances.get((venue, asset), CachedBalance(Decimal("0")))
            current.available = available
            current.updated_at = time.monotonic()
            self._balances[(venue, asset)] = current

    async def apply_local_delta(self, venue: str, asset: str, delta: Decimal) -> None:
        async with self._lock:
            current = self._balances.setdefault((venue, asset), CachedBalance(Decimal("0")))
            current.available += delta

    async def clear_balances(self) -> None:
        async with self._lock:
            self._balances.clear()

    def available(self, venue: str, asset: str) -> Decimal:
        return self._balances.get((venue, asset), CachedBalance(Decimal("0"))).available

    def reserved(self, venue: str, asset: str) -> Decimal:
        return self._balances.get((venue, asset), CachedBalance(Decimal("0"))).reserved

    def assets(self, venue: str) -> set[str]:
        normalized = venue.lower()
        return {
            asset for item_venue, asset in set(self._balances) | set(self._allocations)
            if item_venue == normalized
        }

    def stale(self) -> bool:
        return not self._balances or any(time.monotonic() - value.updated_at > self.ttl_sec for value in self._balances.values())

    def has(self, venue: str, asset: str) -> bool:
        return (venue, asset) in self._balances

    def configure_allocations(self, allocations: dict[str, dict[str, Decimal]]) -> None:
        self._allocations = {
            (venue.lower(), asset.upper()): max(Decimal("0"), Decimal(str(amount)))
            for venue, assets in allocations.items() for asset, amount in assets.items()
        }

    def allocation(self, venue: str, asset: str) -> Decimal:
        return self._allocations.get((venue.lower(), asset.upper()), Decimal("0"))

    def pair_blockers(self, base_asset: str, *, require_actual: bool) -> list[str]:
        required = (
            ("bittrade", "JPY"), ("bittrade", base_asset),
            ("gmo", "JPY"), ("gmo", base_asset),
        )
        blockers = [f"{venue}:{asset}:底仓" for venue, asset in required if self.allocation(venue, asset) <= 0]
        if require_actual:
            blockers.extend(
                f"{venue}:{asset}:实际余额" for venue, asset in required
                if self._get(venue, asset) <= 0
            )
        return list(dict.fromkeys(blockers))

    def quote_capacity(self, *, side: str, base_asset: str, price: Decimal, strategy_limit: Decimal,
                       hedge_depth: Decimal) -> Decimal:
        bittrade_jpy = self._get("bittrade", "JPY")
        bittrade_base = self._get("bittrade", base_asset)
        gmo_jpy = self._get("gmo", "JPY")
        gmo_base = self._get("gmo", base_asset)
        if side == "BUY":  # BitTrade buys base; hedge sells base at GMO
            venue_capacity = min(bittrade_jpy / price, gmo_base)
        else:  # BitTrade sells base; hedge buys base at GMO
            venue_capacity = min(bittrade_base, gmo_jpy / price)
        return max(Decimal("0"), min(strategy_limit, hedge_depth, venue_capacity) * self.safety_factor)

    def _get(self, venue: str, asset: str) -> Decimal:
        spendable = self._balances.get((venue, asset), CachedBalance(Decimal("0"))).spendable
        return min(spendable, self.allocation(venue, asset))
