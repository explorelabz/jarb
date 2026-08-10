from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Protocol

import httpx

from ..adapters import DecimalQuote, ExchangeAPIError
from .domain import OrderState
from .rate_limit import EndpointGroup, Priority, PriorityRateLimiter
from .risk import RiskGate, RiskSnapshot
from .state_store import StateStore


class MakerVenue(Protocol):
    async def place_quote(self, symbol: str, quote: DecimalQuote, client_order_id: str,
                          size_step: Decimal, price_tick: Decimal) -> dict: ...
    async def cancel(self, order_id: str) -> dict: ...
    async def batch_cancel(self, *, order_ids: list[str] | None = None,
                           client_order_ids: list[str] | None = None) -> dict: ...
    async def cancel_all_open(self, symbols: list[str] | None = None) -> dict: ...
    async def order(self, order_id: str) -> dict: ...
    async def open_orders(self, symbol: str | None = None) -> dict: ...


class ExecutionGateway:
    """The only component allowed to mutate maker orders."""

    def __init__(self, venue: MakerVenue, store: StateStore, risk: RiskGate,
                 limiter: PriorityRateLimiter):
        self.venue = venue
        self.store = store
        self.risk = risk
        self.limiter = limiter
        self._replace_locks: dict[tuple[str, str], asyncio.Lock] = {}

    async def place(self, *, symbol: str, side: str, qty: Decimal, price: Decimal,
                    size_step: Decimal, price_tick: Decimal, snapshot: RiskSnapshot) -> dict:
        allowed, reason = await self.risk.evaluate(RiskSnapshot(
            **{**snapshot.__dict__, "order_notional_jpy": float(qty * price)},
        ))
        if not allowed:
            raise RuntimeError(f"order rejected by RiskGate: {reason}")
        sequence = await self.store.next_sequence(f"order-seq:{symbol}:{side}")
        client_order_id = f"{symbol.replace('_', '')}-{side}-{sequence}"
        await self.store.create_order(client_order_id, symbol, side, qty, price)
        await self.store.transition_order(client_order_id, OrderState.PLACING)
        quote = DecimalQuote(side=side, price=price, size=qty, source_price=price)
        try:
            payload = await self.limiter.submit(
                EndpointGroup.PLACE, Priority.PLACE,
                lambda: self.venue.place_quote(
                    symbol, quote, client_order_id, size_step, price_tick,
                ),
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            return await self.store.transition_order(client_order_id, OrderState.UNKNOWN, error=str(exc)[:240])
        except Exception as exc:
            return await self.store.transition_order(client_order_id, OrderState.FAILED, error=str(exc)[:240])
        exchange_order_id = self._order_id(payload)
        if not exchange_order_id:
            return await self.store.transition_order(
                client_order_id, OrderState.UNKNOWN, error="place response had no order id",
            )
        return await self.store.transition_order(
            client_order_id, OrderState.OPEN, exchange_order_id=exchange_order_id,
        )

    async def replace(self, current: dict | None, **new_order: Any) -> dict:
        key = (new_order["symbol"], new_order["side"])
        lock = self._replace_locks.setdefault(key, asyncio.Lock())
        async with lock:
            if current is not None:
                canceled = await self.cancel(current)
                if OrderState(canceled["state"]) != OrderState.CANCELED:
                    raise RuntimeError(f"old quote not confirmed canceled: {canceled['state']}")
            return await self.place(**new_order)

    async def cancel(self, order: dict) -> dict:
        client_id = order["client_order_id"]
        exchange_id = order.get("exchange_order_id")
        if not exchange_id:
            return await self.store.transition_order(
                client_id, OrderState.UNKNOWN, error="cannot cancel without exchange order id",
            )
        await self.store.transition_order(client_id, OrderState.CANCELING)
        try:
            await self.limiter.submit(
                EndpointGroup.CANCEL, Priority.CANCEL, lambda: self.venue.cancel(exchange_id),
            )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            return await self.store.transition_order(client_id, OrderState.UNKNOWN, error=str(exc)[:240])
        except ExchangeAPIError as exc:
            text = f"{exc.code or ''} {exc}".lower()
            if any(marker in text for marker in ("not found", "already filled", "order-orderstate-error")):
                return await self.confirm(client_id, exchange_id)
            return await self.store.transition_order(client_id, OrderState.UNKNOWN, error=str(exc)[:240])
        return await self._wait_terminal(client_id, exchange_id)

    async def confirm(self, client_order_id: str, exchange_order_id: str) -> dict:
        try:
            payload = await self.limiter.submit(
                EndpointGroup.QUERY, Priority.QUERY, lambda: self.venue.order(exchange_order_id),
            )
        except Exception as exc:
            return await self.store.transition_order(
                client_order_id, OrderState.UNKNOWN, exchange_order_id=exchange_order_id,
                error=str(exc)[:240],
            )
        state = self._exchange_state(payload)
        row = next(
            (item for item in await self.store.open_orders() if item["client_order_id"] == client_order_id), None,
        )
        if row is None:
            return {"client_order_id": client_order_id, "state": state}
        current = OrderState(row["state"])
        if current == OrderState.CANCELING and state == OrderState.OPEN:
            return row
        if state == OrderState.CANCELED and current != OrderState.UNKNOWN:
            if current != OrderState.CANCELING:
                await self.store.transition_order(client_order_id, OrderState.UNKNOWN)
        return await self.store.transition_order(client_order_id, state, exchange_order_id=exchange_order_id)

    async def cancel_all(self, *, timeout_sec: float = 15.0) -> None:
        local = await self.store.open_orders()
        symbols = sorted({row["symbol"] for row in local}) or None
        await self.limiter.submit(
            EndpointGroup.KILL, Priority.CRITICAL,
            lambda: self.venue.cancel_all_open(symbols),
        )
        ids = [row["client_order_id"] for row in local]
        for start in range(0, len(ids), 50):
            chunk = ids[start:start + 50]
            if chunk:
                try:
                    await self.limiter.submit(
                        EndpointGroup.KILL, Priority.CRITICAL,
                        lambda chunk=chunk: self.venue.batch_cancel(client_order_ids=chunk),
                    )
                except Exception as exc:
                    await self.store.audit("orders.cancel_all.error", "critical", str(exc)[:240])
        deadline = asyncio.get_running_loop().time() + timeout_sec
        while asyncio.get_running_loop().time() < deadline:
            payload = await self.limiter.submit(
                EndpointGroup.QUERY, Priority.CANCEL, lambda: self.venue.open_orders(),
            )
            remote = self._orders(payload)
            remote_client_ids = {
                str(item.get("client-order-id")) for item in remote if item.get("client-order-id")
            }
            remaining = [row for row in await self.store.open_orders() if row["client_order_id"] in remote_client_ids]
            if not remaining:
                for row in await self.store.open_orders():
                    current = OrderState(row["state"])
                    if current != OrderState.CANCELING:
                        await self.store.transition_order(row["client_order_id"], OrderState.UNKNOWN)
                    await self.store.transition_order(row["client_order_id"], OrderState.CANCELED)
                return
            await asyncio.sleep(.25)
        raise TimeoutError("cancel-all did not converge to zero open orders")

    async def _wait_terminal(self, client_id: str, exchange_id: str) -> dict:
        for _ in range(20):
            row = await self.confirm(client_id, exchange_id)
            if OrderState(row["state"]) in (OrderState.CANCELED, OrderState.FILLED):
                return row
            await asyncio.sleep(.1)
        return await self.store.transition_order(client_id, OrderState.UNKNOWN, error="cancel confirmation timeout")

    @staticmethod
    def _order_id(payload: dict) -> str | None:
        data = payload.get("data")
        if isinstance(data, str | int):
            return str(data)
        if isinstance(data, dict):
            value = data.get("order-id") or data.get("orderId") or data.get("id")
            return str(value) if value is not None else None
        return None

    @staticmethod
    def _orders(payload: dict) -> list[dict]:
        data = payload.get("data", [])
        return data if isinstance(data, list) else data.get("orders", []) if isinstance(data, dict) else []

    @classmethod
    def _exchange_state(cls, payload: dict) -> OrderState:
        data = payload.get("data", payload)
        state = str(data.get("state", "") if isinstance(data, dict) else "").lower()
        return {
            "submitted": OrderState.OPEN,
            "created": OrderState.OPEN,
            "partial-filled": OrderState.PARTIAL,
            "filled": OrderState.FILLED,
            "canceled": OrderState.CANCELED,
            "partial-canceled": OrderState.CANCELED,
        }.get(state, OrderState.UNKNOWN)
