from decimal import Decimal

import pytest

from backend.engine.paper_exchange import FakeGmo, PaperBroker
from backend.models import MarketTop, utc_now


def market(*, bid="99", ask="101", bids=None, asks=None) -> MarketTop:
    return MarketTop(
        symbol="BTC_JPY", bid=float(bid), ask=float(ask), bidSize=1, askSize=1,
        bids=bids or [(float(bid), 1.0)], asks=asks or [(float(ask), 1.0)],
        timestamp=utc_now(), source="GMO",
    )


@pytest.mark.asyncio
async def test_fake_gmo_market_order_requires_and_consumes_live_book_depth():
    broker = PaperBroker()
    gmo = FakeGmo(broker)
    with pytest.raises(RuntimeError, match="真实 GMO 盘口"):
        await gmo.market_order("BTC_JPY", "BUY", Decimal("1"), Decimal(".1"))

    broker.set_market(market(
        asks=[(100.0, .4), (102.0, .6)], bids=[(99.0, 1.0)],
    ))
    response = await gmo.market_order("BTC_JPY", "BUY", Decimal("1"), Decimal(".1"))
    order_id = response["data"]["orderId"]
    executions = await gmo.executions(order_id)
    assert executions["data"][0]["size"] == "1.0"
    assert executions["data"][0]["price"] == "101.2"


@pytest.mark.asyncio
async def test_fake_gmo_sok_fills_only_when_live_book_touches_limit_and_caps_by_depth():
    broker = PaperBroker()
    broker.scenarios.gmoPostOnlyFillDelayMs = 0
    gmo = FakeGmo(broker)
    broker.set_market(market(ask="102", asks=[(102.0, 2.0)]))
    response = await gmo.post_only_order(
        "BTC_JPY", "BUY", Decimal("1"), Decimal("101"), Decimal(".1"), Decimal("1"),
    )
    order_id = response["data"]["orderId"]

    broker.set_market(market(ask="100", asks=[(100.0, .3), (102.0, 2.0)]))
    detail = await gmo.order(order_id)
    assert detail["data"]["list"][0]["executedSize"] == "0.3"
    assert detail["data"]["list"][0]["status"] == "ORDERED"
    executions = await gmo.executions(order_id)
    assert executions["data"] == [{"orderId": order_id, "size": "0.3", "price": "101"}]
