from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx

from .models import MarketTop, QuoteLevel, Side


class GmoAdapter:
    PUBLIC = "https://api.coin.z.com/public/v1"
    PRIVATE = "https://api.coin.z.com/private"

    def __init__(self, api_key: str = "", secret_key: str = "", client: httpx.AsyncClient | None = None):
        self.api_key = api_key
        self.secret_key = secret_key
        self.client = client or httpx.AsyncClient(timeout=3.0)

    async def ticker(self, symbol: str = "BTC") -> MarketTop:
        response = await self.client.get(f"{self.PUBLIC}/ticker", params={"symbol": symbol})
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != 0 or not payload.get("data"):
            raise RuntimeError(f"GMO ticker failed: {payload.get('messages', payload)}")
        item = payload["data"][0]
        return MarketTop(symbol=f"{symbol}_JPY", bid=round(float(item["bid"])), ask=round(float(item["ask"])),
                         bidSize=0.5, askSize=0.5, timestamp=item["timestamp"], source="GMO")

    async def market_order(self, symbol: str, side: Side, size: float) -> dict:
        body = {"symbol": symbol.replace("_JPY", ""), "side": side, "executionType": "MARKET", "timeInForce": "FAK", "size": str(size)}
        return await self._private("POST", "/v1/order", body=body)

    async def executions(self, order_id: str) -> dict:
        return await self._private("GET", "/v1/executions", query={"orderId": order_id})

    async def _private(self, method: str, path: str, query: dict[str, str] | None = None, body: dict | None = None) -> dict:
        if not self.api_key or not self.secret_key:
            raise RuntimeError("GMO credentials are not configured")
        timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        body_text = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""
        signature_text = timestamp + method + path + (body_text if method == "POST" else "")
        signature = hmac.new(self.secret_key.encode(), signature_text.encode(), hashlib.sha256).hexdigest()
        response = await self.client.request(method, f"{self.PRIVATE}{path}", params=query, content=body_text or None,
            headers={"Content-Type": "application/json", "API-KEY": self.api_key, "API-TIMESTAMP": timestamp, "API-SIGN": signature})
        payload = response.json()
        if response.is_error or payload.get("status") != 0:
            raise RuntimeError(f"GMO private API failed: {payload.get('messages', payload)}")
        return payload


class BitTradeAdapter:
    HOST = "api-cloud.bittrade.co.jp"
    BASE = f"https://{HOST}"

    def __init__(self, access_key: str = "", secret_key: str = "", account_id: str = "", client: httpx.AsyncClient | None = None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.account_id = account_id
        self.client = client or httpx.AsyncClient(timeout=3.0)

    async def place_quote(self, symbol: str, quote: QuoteLevel, client_order_id: str) -> dict:
        body = {"account-id": self.account_id, "amount": str(quote.size), "price": str(quote.price), "source": "api",
                "symbol": symbol.lower().replace("_", ""), "type": "buy-limit-maker" if quote.side == "BUY" else "sell-limit-maker",
                "client-order-id": client_order_id}
        return await self._private("POST", "/v1/order/orders/place", body=body)

    async def cancel(self, order_id: str) -> dict:
        return await self._private("POST", f"/v1/order/orders/{order_id}/submitcancel")

    async def matches(self, order_id: str) -> dict:
        return await self._private("GET", f"/v1/order/orders/{order_id}/matchresults")

    async def _private(self, method: str, path: str, query: dict[str, str] | None = None, body: dict | None = None) -> dict:
        if not self.access_key or not self.secret_key or not self.account_id:
            raise RuntimeError("BitTrade credentials are not configured")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        auth = {"AccessKeyId": self.access_key, "SignatureMethod": "HmacSHA256", "SignatureVersion": "2", "Timestamp": timestamp, **(query or {})}
        canonical = urlencode(sorted(auth.items()))
        signature_text = f"{method}\n{self.HOST}\n{path}\n{canonical}"
        auth["Signature"] = base64.b64encode(hmac.new(self.secret_key.encode(), signature_text.encode(), hashlib.sha256).digest()).decode()
        response = await self.client.request(method, f"{self.BASE}{path}", params=auth, json=body,
            headers={"Accept-Language": "ja-JP", "Content-Type": "application/json" if method == "POST" else "application/x-www-form-urlencoded"})
        payload = response.json()
        if response.is_error or payload.get("status") == "error":
            raise RuntimeError(f"BitTrade API failed: {payload.get('err-msg', response.reason_phrase)}")
        return payload
