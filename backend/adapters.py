from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import httpx

from .models import MarketTop, QuoteLevel, Side


class ExchangeAPIError(RuntimeError):
    def __init__(self, venue: str, message: str, *, code: str | None = None):
        super().__init__(f"{venue} API failed: {message}")
        self.venue = venue
        self.code = code


@dataclass(frozen=True)
class DecimalQuote:
    side: Side
    price: Decimal
    size: Decimal
    source_price: Decimal


def decimal_string(value: Decimal | float | int | str, step: Decimal | float | str = "0.00000001") -> str:
    """Serialize an order value on an explicit decimal grid without leaking binary-float tails."""
    number = Decimal(str(value))
    quantum = Decimal(str(step))
    aligned = (number / quantum).to_integral_value(rounding=ROUND_DOWN) * quantum
    text = format(aligned, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


class GmoAdapter:
    PUBLIC = "https://api.coin.z.com/public/v1"
    PRIVATE = "https://api.coin.z.com/private"

    def __init__(self, api_key: str = "", secret_key: str = "", client: httpx.AsyncClient | None = None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.client = client or httpx.AsyncClient(timeout=3.0)
        self.time_offset_ms = 0

    async def ticker(self, symbol: str = "BTC") -> MarketTop:
        response = await self.client.get(f"{self.PUBLIC}/orderbooks", params={"symbol": symbol})
        response.raise_for_status()
        payload = response.json()
        book = payload.get("data", {})
        if payload.get("status") != 0 or not book.get("bids") or not book.get("asks"):
            raise RuntimeError(f"GMO orderbook failed: {payload.get('messages', payload)}")
        bid, ask = book["bids"][0], book["asks"][0]
        return MarketTop(symbol=f"{symbol}_JPY", bid=float(bid["price"]), ask=float(ask["price"]),
                         bidSize=float(bid["size"]), askSize=float(ask["size"]),
                         timestamp=payload.get("responsetime", datetime.now(timezone.utc).isoformat()), source="GMO")

    async def symbols(self) -> list[dict]:
        response = await self.client.get(f"{self.PUBLIC}/symbols")
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != 0 or not isinstance(payload.get("data"), list):
            raise RuntimeError(f"GMO symbols failed: {payload.get('messages', payload)}")
        return payload["data"]

    async def market_order(self, symbol: str, side: Side, size: Decimal | float,
                           size_step: Decimal | float = Decimal("0.00000001")) -> dict:
        body = {"symbol": symbol.replace("_JPY", ""), "side": side, "executionType": "MARKET", "timeInForce": "FAK",
                "size": decimal_string(size, size_step)}
        return await self._private("POST", "/v1/order", body=body)

    async def executions(self, order_id: str) -> dict:
        return await self._private("GET", "/v1/executions", query={"orderId": order_id})

    async def balances(self) -> dict:
        return await self._private("GET", "/v1/account/assets")

    async def sync_time(self) -> int:
        response = await self.client.get(f"{self.PUBLIC}/status")
        response.raise_for_status()
        payload = response.json()
        server_text = payload.get("responsetime")
        if server_text:
            server_ms = int(datetime.fromisoformat(server_text.replace("Z", "+00:00")).timestamp() * 1000)
            self.time_offset_ms = server_ms - int(time.time() * 1000)
        return self.time_offset_ms

    def set_credentials(self, api_key: str, secret_key: str) -> None:
        self.api_key = api_key
        self.secret_key = secret_key

    async def verify_credentials(self) -> None:
        await self._private("GET", "/v1/account/assets")

    async def _private(self, method: str, path: str, query: dict[str, str] | None = None, body: dict | None = None) -> dict:
        if not self.api_key or not self.secret_key:
            raise RuntimeError("GMO credentials are not configured")
        timestamp = str(int(time.time() * 1000) + self.time_offset_ms)
        body_text = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""
        signature_text = timestamp + method + path + (body_text if method == "POST" else "")
        signature = hmac.new(self.secret_key.encode(), signature_text.encode(), hashlib.sha256).hexdigest()
        response = await self.client.request(method, f"{self.PRIVATE}{path}", params=query, content=body_text or None,
            headers={"Content-Type": "application/json", "API-KEY": self.api_key, "API-TIMESTAMP": timestamp, "API-SIGN": signature})
        payload = response.json()
        if response.is_error or payload.get("status") != 0:
            messages = payload.get("messages", payload)
            code = messages[0].get("message_code") if isinstance(messages, list) and messages else None
            raise ExchangeAPIError("GMO", str(messages), code=code)
        return payload


class BitTradeAdapter:
    HOST = "api-cloud.bittrade.co.jp"
    BASE = f"https://{HOST}"

    def __init__(self, access_key: str = "", secret_key: str = "", account_id: str = "", client: httpx.AsyncClient | None = None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.account_id = account_id
        self.client = client or httpx.AsyncClient(timeout=3.0)
        self.time_offset_sec = 0.0

    async def place_quote(self, symbol: str, quote: QuoteLevel | DecimalQuote, client_order_id: str,
                          size_step: Decimal | float = Decimal("0.00000001"),
                          price_tick: Decimal | float = Decimal("0.00000001")) -> dict:
        body = {"account-id": self.account_id, "amount": decimal_string(quote.size, size_step),
                "price": decimal_string(quote.price, price_tick), "source": "api",
                "symbol": symbol.lower().replace("_", ""), "type": "buy-limit-maker" if quote.side == "BUY" else "sell-limit-maker",
                "client-order-id": client_order_id}
        return await self._private("POST", "/v1/order/orders/place", body=body)

    async def cancel(self, order_id: str) -> dict:
        return await self._private("POST", f"/v1/order/orders/{order_id}/submitcancel")

    async def batch_cancel(self, *, order_ids: list[str] | None = None,
                           client_order_ids: list[str] | None = None) -> dict:
        if bool(order_ids) == bool(client_order_ids):
            raise ValueError("provide exactly one of order_ids or client_order_ids")
        body = {"order-ids": order_ids} if order_ids else {"client-order-ids": client_order_ids}
        return await self._private("POST", "/v1/order/orders/batchcancel", body=body)

    async def cancel_all_open(self, symbols: list[str] | None = None) -> dict:
        query = {"account-id": self.account_id}
        if symbols:
            query["symbol"] = ",".join(symbol.lower().replace("_", "") for symbol in symbols)
        return await self._private("POST", "/v1/order/orders/batchCancelOpenOrders", query=query)

    async def order(self, order_id: str) -> dict:
        return await self._private("GET", f"/v1/order/orders/{order_id}")

    async def open_orders(self, symbol: str | None = None) -> dict:
        query = {"account-id": self.account_id}
        if symbol:
            query["symbol"] = symbol.lower().replace("_", "")
        return await self._private("GET", "/v1/order/openOrders", query=query)

    async def recent_matches(self, symbol: str | None = None, *, start_time: str | None = None) -> dict:
        query = {"account-id": self.account_id}
        if symbol:
            query["symbol"] = symbol.lower().replace("_", "")
        if start_time:
            query["start-time"] = start_time
        return await self._private("GET", "/v1/order/matchresults", query=query)

    async def balances(self) -> dict:
        return await self._private("GET", f"/v1/account/accounts/{self.account_id}/balance")

    async def matches(self, order_id: str) -> dict:
        return await self._private("GET", f"/v1/order/orders/{order_id}/matchresults")

    async def symbols(self) -> list[dict]:
        response = await self.client.get(f"{self.BASE}/v1/common/symbols")
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "ok" or not isinstance(payload.get("data"), list):
            raise RuntimeError(f"BitTrade symbols failed: {payload.get('err-msg', payload)}")
        return payload["data"]

    async def sync_time(self) -> float:
        response = await self.client.get(f"{self.BASE}/v1/common/timestamp")
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "ok":
            raise ExchangeAPIError("BitTrade", str(payload.get("err-msg", payload)))
        raw = float(payload["data"])
        server_sec = raw / 1000 if raw > 10_000_000_000 else raw
        self.time_offset_sec = server_sec - time.time()
        return self.time_offset_sec

    def set_credentials(self, access_key: str, secret_key: str, account_id: str) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.account_id = account_id

    async def verify_credentials(self) -> None:
        if not self.access_key or not self.secret_key:
            raise RuntimeError("BitTrade credentials are not configured")
        await self._private("GET", "/v1/account/accounts")

    async def _private(self, method: str, path: str, query: dict[str, str] | None = None, body: dict | None = None) -> dict:
        if not self.access_key or not self.secret_key or not self.account_id:
            raise RuntimeError("BitTrade credentials are not configured")
        timestamp = datetime.fromtimestamp(time.time() + self.time_offset_sec, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        auth = {"AccessKeyId": self.access_key, "SignatureMethod": "HmacSHA256", "SignatureVersion": "2", "Timestamp": timestamp, **(query or {})}
        canonical = urlencode(sorted(auth.items()))
        signature_text = f"{method}\n{self.HOST}\n{path}\n{canonical}"
        auth["Signature"] = base64.b64encode(hmac.new(self.secret_key.encode(), signature_text.encode(), hashlib.sha256).digest()).decode()
        response = await self.client.request(method, f"{self.BASE}{path}", params=auth, json=body,
            headers={"Accept-Language": "ja-JP", "Content-Type": "application/json" if method == "POST" else "application/x-www-form-urlencoded"})
        payload = response.json()
        if response.is_error or payload.get("status") != "ok":
            raise ExchangeAPIError(
                "BitTrade", str(payload.get("err-msg", response.reason_phrase)),
                code=str(payload.get("err-code")) if payload.get("err-code") is not None else None,
            )
        return payload
