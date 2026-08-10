from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ..adapters import GmoAdapter
from ..models import MarketTop
from .domain import EventType
from .events import EventBus


class MarketFeed:
    """GMO market-data boundary. REST polling is usable now and WS can publish the same event shape."""

    def __init__(self, adapter: GmoAdapter, events: EventBus):
        self.adapter = adapter
        self.events = events
        self.latest: dict[str, MarketTop] = {}

    async def refresh(self, symbols: dict[str, str]) -> dict[str, MarketTop]:
        values = await asyncio.gather(*(self.adapter.ticker(base) for base in symbols.values()))
        result = dict(zip(symbols, values, strict=True))
        for symbol, market in result.items():
            self.latest[symbol] = market
            await self.events.publish(EventType.MARKET, market)
        return result

    def age_ms(self, symbol: str) -> int:
        market = self.latest.get(symbol)
        if market is None:
            return 2 ** 31 - 1
        try:
            observed = datetime.fromisoformat(market.timestamp.replace("Z", "+00:00"))
        except ValueError:
            return 2 ** 31 - 1
        return max(0, int((datetime.now(timezone.utc) - observed).total_seconds() * 1000))
