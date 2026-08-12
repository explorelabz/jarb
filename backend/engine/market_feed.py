from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal

import websockets

from ..adapters import GmoAdapter
from ..models import MarketTop
from .domain import EventType
from .events import EventBus
from .paper_matcher import PublicTrade


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

    def __init__(self, bases: list[str], feed: MarketFeed, *,
                 on_trade: Callable[[PublicTrade], Awaitable[None]] | None = None):
        self.bases = list(dict.fromkeys(base.upper() for base in bases))
        self.feed = feed
        self.on_trade = on_trade
        self._trade_seq = 0

    @staticmethod
    def _timestamp_ms(value: str) -> int:
        try:
            observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            return int(observed.timestamp() * 1000)
        except (TypeError, ValueError):
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    async def _subscribe(self, ws) -> None:
        for base in self.bases:
            await ws.send(json.dumps({
                "command": "subscribe", "channel": "orderbooks", "symbol": base,
            }, separators=(",", ":")))
            await asyncio.sleep(1.1)  # GMO ERR-5003: subscribe/unsubscribe is limited to 1 req/s/IP
        if self.on_trade is not None:
            for base in self.bases:
                await ws.send(json.dumps({
                    "command": "subscribe", "channel": "trades", "symbol": base,
                    "option": "TAKER_ONLY",
                }, separators=(",", ":")))
                await asyncio.sleep(1.1)

    async def _consume(self, ws) -> None:
        async for raw in ws:
            payload = json.loads(raw)
            channel = payload.get("channel")
            if channel == "trades" and self.on_trade is not None:
                base = str(payload.get("symbol", "")).upper()
                if not base or payload.get("price") is None or payload.get("size") is None:
                    continue
                self._trade_seq += 1
                timestamp = str(
                    payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
                )
                symbol = base if base.endswith("_JPY") else f"{base}_JPY"
                await self.on_trade(PublicTrade(
                    symbol=symbol,
                    price=Decimal(str(payload["price"])),
                    qty=Decimal(str(payload["size"])),
                    taker_side=str(payload.get("side", "")).upper(),
                    ts_ms=self._timestamp_ms(timestamp),
                    trade_id=f"GMO-{self._trade_seq}-{timestamp}",
                ))
                continue
            if channel != "orderbooks":
                continue
            bids, asks = payload.get("bids") or [], payload.get("asks") or []
            if not bids or not asks:
                continue
            base = str(payload["symbol"]).upper()
            symbol = base if base.endswith("_JPY") else f"{base}_JPY"
            bid_levels_exact = [(Decimal(str(row["price"])), Decimal(str(row["size"]))) for row in bids]
            ask_levels_exact = [(Decimal(str(row["price"])), Decimal(str(row["size"]))) for row in asks]
            bid_levels = [(float(price), float(size)) for price, size in bid_levels_exact]
            ask_levels = [(float(price), float(size)) for price, size in ask_levels_exact]
            market = MarketTop(
                symbol=symbol,
                bid=float(bids[0]["price"]), ask=float(asks[0]["price"]),
                bidSize=float(bids[0]["size"]), askSize=float(asks[0]["size"]),
                bids=bid_levels, asks=ask_levels,
                bidExact=bid_levels_exact[0][0], askExact=ask_levels_exact[0][0],
                bidsExact=bid_levels_exact, asksExact=ask_levels_exact,
                timestamp=str(payload.get("timestamp") or datetime.now(timezone.utc).isoformat()),
                source="GMO",
            )
            await self.feed.update(market, transport="ws")

    async def run(self) -> None:
        while True:
            try:
                async with websockets.connect(self.URL, ping_interval=20, close_timeout=3) as ws:
                    async with asyncio.TaskGroup() as tasks:
                        tasks.create_task(self._subscribe(ws))
                        tasks.create_task(self._consume(ws))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.feed.last_ws_error = str(exc)[:240]
                await asyncio.sleep(1)
