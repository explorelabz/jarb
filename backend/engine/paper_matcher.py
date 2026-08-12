from __future__ import annotations

import asyncio
import gzip
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

import websockets

from .fill_tracker import CumulativeFillEvent, FillTracker


def _symbol_pair(symbol: str) -> str:
    return symbol.replace("_", "").lower()


def _decode_message(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        try:
            raw = gzip.decompress(raw).decode()
        except (OSError, EOFError):
            raw = raw.decode()
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else {}


async def _reply_to_ping(socket, payload: dict[str, Any]) -> bool:
    if "ping" not in payload:
        return False
    await socket.send(json.dumps({"pong": payload["ping"]}, separators=(",", ":")))
    return True


@dataclass(frozen=True)
class PublicTrade:
    symbol: str
    price: Decimal
    qty: Decimal
    taker_side: str
    ts_ms: int
    trade_id: str


@dataclass(frozen=True)
class DepthSnapshot:
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    version: int
    received_at: float


class DepthProvider(Protocol):
    def levels(self, symbol: str, side: str) -> list[tuple[Decimal, Decimal]]: ...


class BitTradeDepthFeed:
    """Fresh full-depth snapshots from BitTrade's gzip public WebSocket."""

    URL = "wss://api-cloud.bittrade.co.jp/ws"

    def __init__(self, symbols: list[str], *, stale_after_sec: float = 3.0):
        self.symbols = list(dict.fromkeys(symbol.upper() for symbol in symbols))
        self.stale_after_sec = stale_after_sec
        self.snapshots: dict[str, DepthSnapshot] = {}
        self.last_error: str | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="bittrade-public-depth")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    def levels(self, symbol: str, side: str) -> list[tuple[Decimal, Decimal]]:
        snapshot = self.snapshots.get(symbol.upper())
        if snapshot is None:
            raise RuntimeError(f"BitTrade {symbol} WebSocket 深度尚未就绪")
        age = time.monotonic() - snapshot.received_at
        if age > self.stale_after_sec:
            raise RuntimeError(f"BitTrade {symbol} WebSocket 深度已过期（{age:.1f}s）")
        if side == "BUY":
            return list(snapshot.bids)
        if side == "SELL":
            return list(snapshot.asks)
        raise ValueError("side must be BUY or SELL")

    def book(self, symbol: str) -> tuple[
        list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]
    ]:
        return self.levels(symbol, "BUY"), self.levels(symbol, "SELL")

    def status(self) -> dict[str, Any]:
        now = time.monotonic()
        return {
            symbol: {
                "ready": True,
                "ageMs": max(0, int((now - snapshot.received_at) * 1000)),
                "bestBid": str(snapshot.bids[0][0]),
                "bestAsk": str(snapshot.asks[0][0]),
                "levels": min(len(snapshot.bids), len(snapshot.asks)),
            }
            for symbol, snapshot in self.snapshots.items()
        }

    def update(self, symbol: str, bids, asks, *, version: int = 0) -> None:
        normalized = symbol.upper()
        previous = self.snapshots.get(normalized)
        if previous is not None and version and previous.version and version <= previous.version:
            return

        def normalize(rows, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
            values = [
                (Decimal(str(row[0])), Decimal(str(row[1])))
                for row in rows if len(row) >= 2 and Decimal(str(row[1])) > 0
            ]
            values.sort(key=lambda row: row[0], reverse=reverse)
            return tuple(values)

        bid_levels = normalize(bids, True)
        ask_levels = normalize(asks, False)
        if not bid_levels or not ask_levels:
            return
        self.snapshots[normalized] = DepthSnapshot(
            bid_levels, ask_levels, version, time.monotonic(),
        )

    async def _run(self) -> None:
        backoff = 1.0
        pair_to_symbol = {_symbol_pair(symbol): symbol for symbol in self.symbols}
        while True:
            try:
                async with websockets.connect(self.URL, ping_interval=None, close_timeout=3) as socket:
                    for pair in pair_to_symbol:
                        await socket.send(json.dumps({
                            "sub": f"market.{pair}.depth.step0", "id": f"jarb-depth-{pair}",
                        }, separators=(",", ":")))
                    backoff = 1.0
                    async for raw in socket:
                        payload = _decode_message(raw)
                        if await _reply_to_ping(socket, payload):
                            continue
                        channel = str(payload.get("ch", ""))
                        if ".depth." not in channel:
                            continue
                        pair = channel.split(".")[1]
                        symbol = pair_to_symbol.get(pair)
                        data = payload.get("tick", payload.get("data", {}))
                        if symbol is None or not isinstance(data, dict):
                            continue
                        self.update(
                            symbol, data.get("bids", []), data.get("asks", []),
                            version=int(data.get("version", payload.get("ts", 0)) or 0),
                        )
                        self.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)[:240]
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 15.0)


class BitTradeTradeStream:
    """BitTrade public maker/taker trades, with heartbeat and trade-id de-duplication."""

    URL = "wss://api-cloud.bittrade.co.jp/ws"

    def __init__(self, symbols: list[str]):
        self.symbols = list(dict.fromkeys(symbol.upper() for symbol in symbols))

    async def stream(self) -> AsyncIterator[PublicTrade]:
        backoff = 1.0
        pair_to_symbol = {_symbol_pair(symbol): symbol for symbol in self.symbols}
        seen: set[str] = set()
        seen_order: deque[str] = deque()
        while True:
            try:
                async with websockets.connect(self.URL, ping_interval=None, close_timeout=3) as socket:
                    for pair in pair_to_symbol:
                        await socket.send(json.dumps({
                            "sub": f"market.{pair}.trade.detail", "id": f"jarb-trade-{pair}",
                        }, separators=(",", ":")))
                    backoff = 1.0
                    async for raw in socket:
                        payload = _decode_message(raw)
                        if await _reply_to_ping(socket, payload):
                            continue
                        channel = str(payload.get("ch", ""))
                        if ".trade.detail" not in channel:
                            continue
                        pair = channel.split(".")[1]
                        symbol = pair_to_symbol.get(pair)
                        tick = payload.get("tick", {})
                        if symbol is None or not isinstance(tick, dict):
                            continue
                        rows = tick.get("data", [])
                        if not isinstance(rows, list):
                            continue
                        rows = sorted(rows, key=lambda row: (int(row.get("ts", 0)), str(row.get("tradeId", row.get("id", "")))))
                        for row in rows:
                            trade_id = str(row.get("tradeId", row.get("id", "")))
                            if not trade_id:
                                trade_id = f"{symbol}:{row.get('ts')}:{row.get('price')}:{row.get('amount')}:{row.get('direction')}"
                            scoped_id = f"{symbol}:{trade_id}"
                            if scoped_id in seen:
                                continue
                            seen.add(scoped_id)
                            seen_order.append(scoped_id)
                            if len(seen_order) > 20_000:
                                seen.discard(seen_order.popleft())
                            yield PublicTrade(
                                symbol=symbol,
                                price=Decimal(str(row["price"])),
                                qty=Decimal(str(row["amount"])),
                                taker_side="BUY" if str(row.get("direction", "")).lower() == "buy" else "SELL",
                                ts_ms=int(row.get("ts", tick.get("ts", payload.get("ts", 0))) or 0),
                                trade_id=trade_id,
                            )
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 15.0)


@dataclass
class PaperOrder:
    client_order_id: str
    symbol: str
    side: str
    price: Decimal
    qty: Decimal
    ahead_qty: Decimal
    filled: Decimal = Decimal("0")
    seq: int = 0
    active: bool = False
    placed_seq: int = 0


class PaperMatchingEngine:
    """Queue-aware Paper maker matching driven only by public BitTrade trades."""

    def __init__(self, fill_tracker: FillTracker, depth_provider: DepthProvider,
                 maker_fee_bps: Callable[[str], Decimal] | None = None, *,
                 initial_stats: dict[str, Any] | None = None,
                 stats_callback: Callable[[dict[str, Any]], Awaitable[None]] | None = None):
        self.orders: dict[str, PaperOrder] = {}
        self.fill_tracker = fill_tracker
        self.depth_provider = depth_provider
        self.maker_fee_bps = maker_fee_bps or (lambda _symbol: Decimal("0"))
        self._lock = asyncio.Lock()
        self._placed_seq = 0
        self._seen_trades: set[str] = set()
        self._seen_trade_order: deque[str] = deque()
        initial_stats = initial_stats or {}
        self.through_fills = int(initial_stats.get("throughFills", 0))
        self.at_level_fills = int(initial_stats.get("atLevelFills", 0))
        self.through_qty = Decimal(str(initial_stats.get("throughQty", "0")))
        self.at_level_qty = Decimal(str(initial_stats.get("atLevelQty", "0")))
        self.stats_callback = stats_callback
        self.public_trades_seen = 0
        self.last_trade_ts_ms = 0

    async def on_place(self, client_order_id: str, symbol: str, side: str,
                       price: Decimal, qty: Decimal) -> PaperOrder:
        levels = self.depth_provider.levels(symbol, side)
        ahead = sum((size for level_price, size in levels if (
            level_price <= price if side == "SELL" else level_price >= price
        )), Decimal("0"))
        async with self._lock:
            self._placed_seq += 1
            order = PaperOrder(
                client_order_id, symbol, side, price, qty, ahead,
                placed_seq=self._placed_seq,
            )
            self.orders[client_order_id] = order
            return order

    async def on_activate(self, client_order_id: str) -> None:
        async with self._lock:
            order = self.orders.get(client_order_id)
            if order is not None:
                order.active = True

    async def on_cancel(self, client_order_id: str) -> None:
        async with self._lock:
            self.orders.pop(client_order_id, None)

    async def on_cancel_all(self) -> None:
        async with self._lock:
            self.orders.clear()

    async def on_trade(self, trade: PublicTrade) -> None:
        scoped_trade_id = f"{trade.symbol}:{trade.trade_id}"
        async with self._lock:
            if scoped_trade_id in self._seen_trades:
                return
            self._seen_trades.add(scoped_trade_id)
            self._seen_trade_order.append(scoped_trade_id)
            if len(self._seen_trade_order) > 20_000:
                self._seen_trades.discard(self._seen_trade_order.popleft())
            self.public_trades_seen += 1
            self.last_trade_ts_ms = max(self.last_trade_ts_ms, trade.ts_ms)

            candidates = sorted(
                (order for order in self.orders.values() if order.active and order.symbol == trade.symbol),
                key=lambda order: order.placed_seq,
            )
            at_level_available = trade.qty
            for order in candidates:
                if trade.taker_side == "BUY" and order.side != "SELL":
                    continue
                if trade.taker_side == "SELL" and order.side != "BUY":
                    continue
                through = trade.price > order.price if order.side == "SELL" else trade.price < order.price
                at_level = trade.price == order.price
                if not through and not at_level:
                    continue
                remaining = order.qty - order.filled
                if remaining <= 0:
                    continue
                match_kind = "through" if through else "at_level"
                if through:
                    exec_qty = remaining
                else:
                    consumed = min(order.ahead_qty, at_level_available)
                    order.ahead_qty -= consumed
                    at_level_available -= consumed
                    exec_qty = min(remaining, at_level_available)
                    at_level_available -= exec_qty
                if exec_qty <= 0:
                    continue
                order.filled += exec_qty
                order.seq += 1
                fee = -order.price * exec_qty * self.maker_fee_bps(order.symbol) / Decimal("10000")
                await self.fill_tracker.ingest(CumulativeFillEvent(
                    client_order_id=order.client_order_id,
                    order_id=f"PAPER-{order.client_order_id}",
                    trade_id=f"PAPER-{trade.trade_id}-{order.client_order_id}-{order.seq}",
                    symbol=order.symbol,
                    side=order.side,
                    cumulative_qty=order.filled,
                    price=order.price,
                    fee=fee,
                    occurred_at=datetime.fromtimestamp(trade.ts_ms / 1000, timezone.utc).isoformat(),
                ))
                if match_kind == "through":
                    self.through_fills += 1
                    self.through_qty += exec_qty
                else:
                    self.at_level_fills += 1
                    self.at_level_qty += exec_qty
                if self.stats_callback is not None:
                    await self.stats_callback(self.stats())
                if order.filled >= order.qty:
                    self.orders.pop(order.client_order_id, None)

    async def resync_once(self) -> None:
        async with self._lock:
            for order in self.orders.values():
                if not order.active:
                    continue
                levels = self.depth_provider.levels(order.symbol, order.side)
                snapshot_ahead = sum((size for price, size in levels if (
                    price < order.price if order.side == "SELL" else price > order.price
                )), Decimal("0"))
                order.ahead_qty = min(order.ahead_qty, snapshot_ahead)

    async def resync_queue(self, interval_sec: float = 2.0) -> None:
        while True:
            await asyncio.sleep(interval_sec)
            try:
                await self.resync_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # A stale/missing depth snapshot must never manufacture queue progress.
                continue

    def stats(self) -> dict[str, Any]:
        total = self.through_fills + self.at_level_fills
        return {
            "openOrders": len(self.orders),
            "throughFills": self.through_fills,
            "atLevelFills": self.at_level_fills,
            "throughQty": str(self.through_qty),
            "atLevelQty": str(self.at_level_qty),
            "throughRatio": self.through_fills / total if total else 0.0,
            "publicTradesSeen": self.public_trades_seen,
            "lastTradeTsMs": self.last_trade_ts_ms,
        }
