import asyncio
import sqlite3
import time
from decimal import Decimal

import pytest

from backend.engine.paper_exchange import FakeGmo, PaperBroker
from backend.engine.domain import HedgeStatus, OrderState
from backend.engine.events import EventBus
from backend.engine.hedge_worker import GmoHedgeExecutor, HedgeWorker
from backend.engine.rate_limit import PriorityRateLimiter
from backend.engine.risk import RiskGate
from backend.engine.state_store import StateStore
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
    assert detail["data"]["list"][0]["status"] == "EXECUTED"
    executions = await gmo.executions(order_id)
    assert executions["data"] == [{"orderId": order_id, "size": "0.3", "price": "101"}]


@pytest.mark.asyncio
async def test_fake_gmo_sok_waits_for_new_market_and_consumes_same_price_queue():
    broker = PaperBroker()
    broker.scenarios.gmoPostOnlyFillDelayMs = 0
    gmo = FakeGmo(broker)
    broker.set_market(market(
        bid="99", ask="101", bids=[(99.0, 2.0)], asks=[(101.0, 1.0)],
    ))
    response = await gmo.post_only_order(
        "BTC_JPY", "BUY", Decimal("1"), Decimal("99"), Decimal(".1"), Decimal("1"),
    )
    order_id = response["data"]["orderId"]

    # Re-reading the placement snapshot neither fills nor consumes queue twice.
    first = await gmo.order(order_id)
    second = await gmo.order(order_id)
    assert first["data"]["list"][0]["status"] == "ORDERED"
    assert second["data"]["list"][0]["executedSize"] == "0"
    assert broker.gmo_orders[order_id]["ahead_same"] == Decimal("2.0")

    # A locked book at our price consumes the earlier FIFO queue first.
    broker.set_market(market(
        bid="99", ask="99", bids=[(99.0, 2.0)], asks=[(99.0, 1.0)],
    ))
    queued = await gmo.order(order_id)
    assert queued["data"]["list"][0]["status"] == "ORDERED"
    assert broker.gmo_orders[order_id]["ahead_same"] == Decimal("1.0")

    broker.set_market(market(
        bid="99", ask="99", bids=[(99.0, 1.0)], asks=[(99.0, 1.5)],
    ))
    filled = await gmo.order(order_id)
    assert filled["data"]["list"][0]["status"] == "EXECUTED"
    assert filled["data"]["list"][0]["executedSize"] == "0.5"


@pytest.mark.asyncio
async def test_fake_gmo_sok_returns_early_when_later_market_crosses_limit():
    broker = PaperBroker()
    broker.scenarios.gmoPostOnlyFillDelayMs = 0
    broker.set_market(market(bid="99", ask="101"))
    gmo = FakeGmo(broker)
    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        gmo, limiter, {"BTC_JPY": Decimal(".1")},
        price_ticks={"BTC_JPY": Decimal("1")},
        passive_price=lambda _symbol, _side: Decimal("99"),
        passive_timeout_ms={"BTC_JPY": 800}, fill_timeout_sec=.5,
    )

    async def cross_market():
        await asyncio.sleep(.08)
        broker.set_market(market(bid="98", ask="98", asks=[(98.0, 1.0)]))

    started = time.monotonic()
    crossing = asyncio.create_task(cross_market())
    try:
        execution = await executor("BTC_JPY", "BUY", Decimal(".1"))
    finally:
        await crossing
        await limiter.stop()
    assert time.monotonic() - started < .5
    assert execution.filled_qty == Decimal(".1")
    assert "+" not in execution.order_id


@pytest.mark.asyncio
async def test_canceled_sok_remains_queryable_when_market_is_stale():
    broker = PaperBroker()
    broker.scenarios.gmoPostOnlyFillDelayMs = 0
    broker.set_market(market())
    gmo = FakeGmo(broker)
    response = await gmo.post_only_order(
        "BTC_JPY", "BUY", Decimal("1"), Decimal("99"), Decimal(".1"), Decimal("1"),
    )
    order_id = response["data"]["orderId"]
    broker._live_market_updated_at["BTC_JPY"] -= 10
    with pytest.raises(RuntimeError, match="盘口已过期"):
        await gmo.order(order_id)

    await gmo.cancel_order(order_id)
    detail = await gmo.order(order_id)
    assert detail["data"]["list"][0]["status"] == "CANCELED"


@pytest.mark.asyncio
async def test_stale_gmo_market_is_classified_as_retryable_hedge_failure(tmp_path):
    store = StateStore(tmp_path / "state.db", trading_mode="paper")
    await store.initialize()
    await store.create_order(
        "BTCJPY-SELL-1", "BTC_JPY", "SELL", Decimal(".1"), Decimal("100"),
        trading_mode="paper",
    )
    await store.transition_order("BTCJPY-SELL-1", OrderState.PLACING)
    await store.transition_order(
        "BTCJPY-SELL-1", OrderState.OPEN, exchange_order_id="PAPER-1",
    )
    fill = await store.record_cumulative_fill(
        client_order_id="BTCJPY-SELL-1", order_id="PAPER-1", trade_id="T1",
        symbol="BTC_JPY", side="SELL", cumulative_qty=Decimal(".1"),
        price=Decimal("100"), fee=Decimal("0"), occurred_at="2026-08-12T00:00:00Z",
    )
    assert fill is not None
    intent = await store.create_hedge_intent(fill, "BUY")

    broker = PaperBroker()
    broker.set_market(market())
    broker._live_market_updated_at["BTC_JPY"] -= 10
    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        FakeGmo(broker), limiter, {"BTC_JPY": Decimal(".1")},
        price_ticks={"BTC_JPY": Decimal("1")},
        passive_price=lambda _symbol, _side: Decimal("99"),
    )
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    worker = HedgeWorker(
        store, EventBus(), executor, risk,
        min_sizes={"BTC_JPY": Decimal(".1")}, max_attempts=4,
    )
    try:
        await worker._execute_group(("BTC_JPY", "BUY"), [intent], Decimal(".1"))
        pending = await store.pending_hedges()
        assert len(pending) == 1
        assert pending[0].status == HedgeStatus.RETRY
        with sqlite3.connect(tmp_path / "state.db") as db:
            last_error = db.execute(
                "SELECT last_error FROM hedge_intents WHERE id=?", (intent.id,),
            ).fetchone()[0]
        assert "盘口已过期" in last_error
    finally:
        await limiter.stop()
        await store.close()
