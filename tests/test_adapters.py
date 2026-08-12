from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pytest

from backend.adapters import BitTradeAdapter, GmoAdapter, decimal_string
from backend.models import QuoteLevel


@pytest.mark.asyncio
async def test_gmo_market_top_uses_real_best_level_depth():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/public/v1/orderbooks"
        return httpx.Response(200, json={
            "status": 0,
            "data": {
                "bids": [{"price": "17400000", "size": "0.1234"}],
                "asks": [{"price": "17410000", "size": "0.0567"}],
                "symbol": "BTC",
            },
            "responsetime": "2026-08-10T00:00:00Z",
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        market = await GmoAdapter(client=client).ticker("BTC")
    assert market.bidSize == .1234
    assert market.askSize == .0567
    assert market.bids == [(17400000.0, .1234)]
    assert market.asks == [(17410000.0, .0567)]
    assert market.decimal_bid() == Decimal("17400000")
    assert market.decimal_asks() == [(Decimal("17410000"), Decimal("0.0567"))]
    assert "bidExact" not in market.model_dump()


@pytest.mark.asyncio
async def test_gmo_post_only_order_uses_limit_sok_and_cancel_endpoint():
    requests: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        requests.append((request.method, request.url.path, body))
        return httpx.Response(200, json={"status": 0, "data": {"orderId": 123}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = GmoAdapter("key", "secret", client)
        await adapter.post_only_order(
            "BTC_JPY", "BUY", Decimal("0.01"), Decimal("100.9"),
            Decimal("0.0001"), Decimal("1"),
        )
        await adapter.cancel_order("123")
    assert requests[0][1:] == ("/private/v1/order", {
        "symbol": "BTC", "side": "BUY", "executionType": "LIMIT",
        "timeInForce": "SOK", "price": "100", "size": "0.01",
    })
    assert requests[1][1:] == ("/private/v1/cancelOrder", {"orderId": 123})


@pytest.mark.asyncio
async def test_bittrade_rejects_every_non_ok_status():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "maintenance", "err-msg": "temporarily unavailable"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = BitTradeAdapter("access", "secret", "account", client)
        with pytest.raises(RuntimeError, match="temporarily unavailable"):
            await adapter.cancel("order-1")


@pytest.mark.asyncio
async def test_order_json_uses_decimal_grid_without_float_tails():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json={"status": "ok", "data": "order-1"})

    quote = QuoteLevel(side="BUY", price=100.1 + .2, size=.1 + .2, sourcePrice=100)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = BitTradeAdapter("access", "secret", "account", client)
        await adapter.place_quote("BTC_JPY", quote, "client-1", size_step=.1, price_tick=.1)

    assert decimal_string(.1 + .2, .1) == "0.3"
    assert decimal_string(100, 1) == "100"
    assert captured["amount"] == "0.3"
    assert captured["price"] == "100.3"


@pytest.mark.asyncio
async def test_bittrade_public_depth_uses_market_depth_endpoint():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/market/depth"
        assert request.url.params["symbol"] == "btcjpy"
        assert request.url.params["type"] == "step0"
        return httpx.Response(200, json={"status": "ok", "tick": {
            "bids": [[100, 2]], "asks": [[101, 3]],
        }})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        payload = await BitTradeAdapter(client=client).depth("BTC_JPY")
    assert payload["tick"]["bids"][0][0] == 100


@pytest.mark.asyncio
async def test_gmo_order_status_uses_private_orders_endpoint():
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["orderId"] = request.url.params["orderId"]
        return httpx.Response(200, json={"status": 0, "data": {"list": [{"status": "EXECUTED"}]}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        payload = await GmoAdapter("key", "secret", client).order("G-1")
    assert captured == {"path": "/private/v1/orders", "orderId": "G-1"}
    assert payload["data"]["list"][0]["status"] == "EXECUTED"


@pytest.mark.asyncio
async def test_gmo_execution_window_pages_by_symbol_and_proves_coverage():
    pages: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/private/v1/latestExecutions"
        assert request.url.params["symbol"] == "BTC"
        pages.append(request.url.params["page"])
        if request.url.params["page"] == "1":
            rows = [
                {"orderId": str(index), "timestamp": "2026-08-13T00:01:00Z"}
                for index in range(100)
            ]
        else:
            rows = [{"orderId": "window", "timestamp": "2026-08-12T23:59:59Z"}]
        return httpx.Response(200, json={"status": 0, "data": {"list": rows}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        payload = await GmoAdapter("key", "secret", client).executions_by_symbol_window(
            "BTC_JPY",
            datetime(2026, 8, 13, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 13, 0, 2, tzinfo=timezone.utc),
        )
    assert pages == ["1", "2"]
    assert payload["windowCovered"] is True
    assert len(payload["data"]["list"]) == 100
