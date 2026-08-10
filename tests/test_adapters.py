from __future__ import annotations

import json

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
