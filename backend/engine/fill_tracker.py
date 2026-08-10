from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol
from urllib.parse import urlencode

import websockets

from ..adapters import BitTradeAdapter

from .domain import EventType, FillDelta
from .events import EventBus
from .state_store import StateStore


@dataclass(frozen=True)
class CumulativeFillEvent:
    client_order_id: str
    order_id: str
    trade_id: str
    symbol: str
    side: str
    cumulative_qty: Decimal
    price: Decimal
    fee: Decimal = Decimal("0")
    occurred_at: str = ""


class FillSource(Protocol):
    def __call__(self) -> Awaitable[list[CumulativeFillEvent]]: ...


class FillTracker:
    """Normalizes WS and REST events through one cumulative, database-idempotent path."""

    def __init__(self, store: StateStore, events: EventBus, *, rest_source: FillSource | None = None,
                 poll_interval_sec: float = 5.0,
                 on_fill: Callable[[FillDelta], Awaitable[None]] | None = None):
        self.store = store
        self.events = events
        self.rest_source = rest_source
        self.poll_interval_sec = poll_interval_sec
        self.on_fill = on_fill
        self._tasks: list[asyncio.Task] = []
        self._callback_tasks: set[asyncio.Task] = set()

    async def ingest(self, event: CumulativeFillEvent) -> FillDelta | None:
        delta = await self.store.record_cumulative_fill(
            client_order_id=event.client_order_id,
            order_id=event.order_id,
            trade_id=event.trade_id,
            symbol=event.symbol,
            side=event.side,
            cumulative_qty=event.cumulative_qty,
            price=event.price,
            fee=event.fee,
            occurred_at=event.occurred_at or datetime.now(timezone.utc).isoformat(),
        )
        if delta is not None:
            # Hedge creation is the critical path. Publish the durably recorded delta
            # before balance/UI projection so a slow dashboard query cannot delay GMO.
            await self.events.publish(EventType.FILL, delta)
            if self.on_fill is not None:
                task = asyncio.create_task(self._notify_fill(delta), name="fill-projection")
                self._callback_tasks.add(task)
                task.add_done_callback(self._callback_tasks.discard)
        return delta

    async def start(self, ws_stream: AsyncIterator[CumulativeFillEvent] | None = None) -> None:
        if ws_stream is not None:
            self._tasks.append(asyncio.create_task(self._consume_ws(ws_stream), name="bittrade-private-ws"))
        if self.rest_source is not None:
            self._tasks.append(asyncio.create_task(self._poll_rest(), name="bittrade-fill-fallback"))

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        callbacks = tuple(self._callback_tasks)
        for task in callbacks:
            task.cancel()
        if callbacks:
            await asyncio.gather(*callbacks, return_exceptions=True)
        self._callback_tasks.clear()

    async def _notify_fill(self, delta: FillDelta) -> None:
        try:
            await self.on_fill(delta)
        except Exception as exc:
            await self.store.audit("fill.projection.error", "warning", str(exc)[:240])

    async def _consume_ws(self, stream: AsyncIterator[CumulativeFillEvent]) -> None:
        async for event in stream:
            await self.ingest(event)

    async def _poll_rest(self) -> None:
        while True:
            try:
                for event in await self.rest_source():
                    await self.ingest(event)
                checkpoint = getattr(self.rest_source, "checkpoint", None)
                if checkpoint is not None:
                    await checkpoint()
            except Exception as exc:
                await self.store.audit("fill.rest.error", "warning", str(exc)[:240])
            await asyncio.sleep(self.poll_interval_sec)


class BitTradePrivateWS:
    URL = "wss://api-cloud.bittrade.co.jp/ws/v2"

    def __init__(self, adapter: BitTradeAdapter, symbols: list[str], *,
                 on_disconnect: Callable[[Exception], Awaitable[None]] | None = None,
                 on_reconnect: Callable[[], Awaitable[None]] | None = None):
        self.adapter = adapter
        self.symbols = symbols
        self.on_disconnect = on_disconnect
        self.on_reconnect = on_reconnect

    async def stream(self) -> AsyncIterator[CumulativeFillEvent]:
        backoff = 1.0
        connected_once = False
        while True:
            try:
                async with websockets.connect(self.URL, ping_interval=None, close_timeout=3) as socket:
                    await socket.send(json.dumps(self._auth_payload(), separators=(",", ":")))
                    auth = await self._receive(socket)
                    if auth.get("code") != 200:
                        raise RuntimeError(f"BitTrade WS authentication failed: {auth}")
                    for symbol in self.symbols:
                        topic = f"trade.clearing#{symbol.lower().replace('_', '')}#1"
                        await socket.send(json.dumps({"action": "sub", "ch": topic}))
                    if connected_once and self.on_reconnect is not None:
                        await self.on_reconnect()
                    connected_once = True
                    backoff = 1.0
                    while True:
                        payload = await asyncio.wait_for(self._receive(socket), timeout=30)
                        data = payload.get("data", {})
                        if payload.get("action") == "ping":
                            await socket.send(json.dumps({"action": "pong", "data": payload.get("data", {})}))
                            continue
                        if "ping" in payload:
                            await socket.send(json.dumps({"pong": payload["ping"]}))
                            continue
                        if data.get("eventType") != "trade":
                            continue
                        order_id = str(data["orderId"])
                        detail = await self.adapter.order(order_id)
                        order = detail.get("data", {})
                        client_id = str(data.get("clientOrderId") or order.get("client-order-id") or "")
                        if not client_id:
                            continue
                        cumulative = order.get("field-amount", order.get("filled-amount", "0"))
                        yield CumulativeFillEvent(
                            client_order_id=client_id,
                            order_id=order_id,
                            trade_id=str(data["tradeId"]),
                            symbol=self._symbol(data["symbol"]),
                            side=str(data["orderSide"]).upper(),
                            cumulative_qty=Decimal(str(cumulative)),
                            price=Decimal(str(data["tradePrice"])),
                            fee=Decimal(str(data.get("transactFee", "0"))),
                            occurred_at=datetime.fromtimestamp(
                                int(data["tradeTime"]) / 1000, timezone.utc,
                            ).isoformat(),
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if connected_once and self.on_disconnect is not None:
                    await self.on_disconnect(exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _auth_payload(self) -> dict:
        timestamp = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() + self.adapter.time_offset_sec, timezone.utc,
        ).strftime("%Y-%m-%dT%H:%M:%S")
        params = {
            "accessKey": self.adapter.access_key,
            "signatureMethod": "HmacSHA256",
            "signatureVersion": "2.1",
            "timestamp": timestamp,
        }
        canonical = urlencode(sorted(params.items()))
        source = f"GET\n{self.adapter.HOST}\n/ws/v2\n{canonical}"
        signature = base64.b64encode(
            hmac.new(self.adapter.secret_key.encode(), source.encode(), hashlib.sha256).digest(),
        ).decode()
        return {"action": "req", "ch": "auth", "params": {
            "authType": "api", **params, "signature": signature,
        }}

    @staticmethod
    async def _receive(socket) -> dict:
        message = await socket.recv()
        if isinstance(message, bytes):
            import gzip
            message = gzip.decompress(message).decode()
        return json.loads(message)

    @staticmethod
    def _symbol(value: str) -> str:
        normalized = value.upper()
        return normalized[:-3] + "_JPY" if normalized.endswith("JPY") else normalized


class BitTradeRestFillSource:
    def __init__(self, adapter: BitTradeAdapter, store: StateStore):
        self.adapter = adapter
        self.store = store
        self._pending_checkpoint: int | None = None

    async def __call__(self) -> list[CumulativeFillEvent]:
        open_orders = await self.store.open_orders()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        last_processed = await self.store.get_state("last_processed_ts", None)
        start_ms = int(last_processed) - 5_000 if last_processed is not None else now_ms - 86_400_000
        payload = await self.adapter.recent_matches(start_time=str(max(0, start_ms)))
        rows = payload.get("data", [])
        if isinstance(rows, dict):
            rows = rows.get("data", rows.get("list", []))
        candidates = await self.store.orders_for_fill_reconciliation(limit=10_000)
        by_exchange = {
            str(order["exchange_order_id"]): order for order in candidates if order.get("exchange_order_id")
        }
        by_client = {order["client_order_id"]: order for order in candidates}
        affected: dict[str, dict] = {order["client_order_id"]: order for order in open_orders}
        for row in rows if isinstance(rows, list) else []:
            exchange_id = row.get("order-id", row.get("orderId", row.get("order_id")))
            client_id = row.get("client-order-id", row.get("clientOrderId", row.get("client_order_id")))
            order = by_exchange.get(str(exchange_id)) if exchange_id is not None else None
            if order is None and client_id is not None:
                order = by_client.get(str(client_id))
            if order is not None:
                affected[order["client_order_id"]] = order
        self._pending_checkpoint = now_ms
        return await self.for_orders(list(affected.values()))

    async def checkpoint(self) -> None:
        if self._pending_checkpoint is not None:
            await self.store.set_state("last_processed_ts", self._pending_checkpoint)
            self._pending_checkpoint = None

    async def for_orders(self, orders: list[dict]) -> list[CumulativeFillEvent]:
        events: list[CumulativeFillEvent] = []
        for order in orders:
            exchange_id = order.get("exchange_order_id")
            if not exchange_id:
                continue
            payload = await self.adapter.matches(exchange_id)
            cumulative = Decimal("0")
            rows = payload.get("data", [])
            if isinstance(rows, dict):
                rows = rows.get("data", rows.get("list", []))
            rows = sorted(rows, key=lambda row: (row.get("created-at", 0), row.get("trade-id", 0)))
            for row in rows:
                cumulative += Decimal(str(row.get("filled-amount", "0")))
                timestamp = int(row.get("created-at", 0))
                events.append(CumulativeFillEvent(
                    client_order_id=order["client_order_id"], order_id=str(exchange_id),
                    trade_id=str(row.get("trade-id", row.get("id"))), symbol=order["symbol"],
                    side=order["side"], cumulative_qty=cumulative,
                    price=Decimal(str(row.get("price", order["price"]))),
                    fee=Decimal(str(row.get("filled-fees", "0"))),
                    occurred_at=datetime.fromtimestamp(timestamp / 1000, timezone.utc).isoformat(),
                ))
        return events
