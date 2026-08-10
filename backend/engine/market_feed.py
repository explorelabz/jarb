from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import websockets

from ..adapters import GmoAdapter
from ..models import MarketTop
from .domain import EventType
from .events import EventBus


class MarketFeed:
    """GMO market-data boundary shared by the primary WebSocket and REST watchdog."""

    def __init__(self, adapter: GmoAdapter, events: EventBus):
        self.adapter = adapter
        self.events = events
        self.latest: dict[str, MarketTop] = {}
        self.latest_transport: dict[str, str] = {}
        self.last_ws_error: str | None = None

    async def refresh(self, symbols: dict[str, str]) -> dict[str, MarketTop]:
        values = await asyncio.gather(*(self.adapter.ticker(base) for base in symbols.values()))
        result = dict(zip(symbols, values, strict=True))
        for symbol, market in result.items():
            await self.update(market, transport="rest")
        return result

    async def update(self, market: MarketTop, *, transport: str) -> None:
        self.latest[market.symbol] = market
        self.latest_transport[market.symbol] = transport
        if transport == "ws":
            self.last_ws_error = None
        await self.events.publish(EventType.MARKET, market)

    def age_ms(self, symbol: str) -> int:
        market = self.latest.get(symbol)
        if market is None:
            return 2 ** 31 - 1
        try:
            observed = datetime.fromisoformat(market.timestamp.replace("Z", "+00:00"))
        except ValueError:
            return 2 ** 31 - 1
        return max(0, int((datetime.now(timezone.utc) - observed).total_seconds() * 1000))


class GmoPublicWS:
    URL = "wss://api.coin.z.com/ws/public/v1"

    def __init__(self, bases: list[str], feed: MarketFeed):
        self.bases = list(dict.fromkeys(base.upper() for base in bases))
        self.feed = feed

    async def run(self) -> None:
        while True:
            try:
                async with websockets.connect(self.URL, ping_interval=20, close_timeout=3) as ws:
                    for base in self.bases:
                        await ws.send(json.dumps({
                            "command": "subscribe", "channel": "orderbooks", "symbol": base,
                        }, separators=(",", ":")))
                        await asyncio.sleep(1.1)  # GMO ERR-5003: subscribe/unsubscribe is limited to 1 req/s/IP
                    async for raw in ws:
                        payload = json.loads(raw)
                        if payload.get("channel") != "orderbooks":
                            continue
                        bids, asks = payload.get("bids") or [], payload.get("asks") or []
                        if not bids or not asks:
                            continue
                        base = str(payload["symbol"]).upper()
                        symbol = base if base.endswith("_JPY") else f"{base}_JPY"
                        bid_levels = [(float(row["price"]), float(row["size"])) for row in bids]
                        ask_levels = [(float(row["price"]), float(row["size"])) for row in asks]
                        market = MarketTop(
                            symbol=symbol,
                            bid=float(bids[0]["price"]), ask=float(asks[0]["price"]),
                            bidSize=float(bids[0]["size"]), askSize=float(asks[0]["size"]),
                            bids=bid_levels, asks=ask_levels,
                            timestamp=str(payload.get("timestamp") or datetime.now(timezone.utc).isoformat()),
                            source="GMO",
                        )
                        await self.feed.update(market, transport="ws")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.feed.last_ws_error = str(exc)[:240]
                await asyncio.sleep(1)
