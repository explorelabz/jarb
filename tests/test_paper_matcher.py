from __future__ import annotations

import time
from decimal import Decimal

import pytest

from backend.engine.domain import OrderState
from backend.engine.events import EventBus
from backend.engine.execution_gateway import ExecutionGateway
from backend.engine.fill_tracker import FillTracker
from backend.engine.paper_matcher import BitTradeDepthFeed, PaperMatchingEngine, PublicTrade
from backend.engine.rate_limit import PriorityRateLimiter
from backend.engine.risk import RiskGate, RiskSnapshot
from backend.engine.state_store import StateStore


class MemoryDepth:
    def __init__(self):
        self.bids = [(Decimal("101"), Decimal(".4")), (Decimal("100"), Decimal(".6"))]
        self.asks = [(Decimal("99"), Decimal(".4")), (Decimal("100"), Decimal(".6"))]

    def levels(self, _symbol: str, side: str):
        return list(self.bids if side == "BUY" else self.asks)


async def open_order(store: StateStore, client_id: str, side: str, price: str, qty: str) -> None:
    await store.create_order(client_id, "BTC_JPY", side, Decimal(qty), Decimal(price))
    await store.transition_order(client_id, OrderState.PLACING)
    await store.transition_order(client_id, OrderState.OPEN, exchange_order_id=f"PAPER-{client_id}")


@pytest.mark.asyncio
async def test_same_price_trades_consume_queue_before_filling_and_through_trade_completes(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    tracker = FillTracker(store, EventBus())
    depth = MemoryDepth()
    matcher = PaperMatchingEngine(tracker, depth, lambda _symbol: Decimal("-1"))
    await open_order(store, "BTCJPY-SELL-1", "SELL", "100", "1")
    order = await matcher.on_place("BTCJPY-SELL-1", "BTC_JPY", "SELL", Decimal("100"), Decimal("1"))
    await matcher.on_activate(order.client_order_id)

    now_ms = int(time.time() * 1000)
    await matcher.on_trade(PublicTrade("BTC_JPY", Decimal("100"), Decimal(".7"), "BUY", now_ms, "T1"))
    assert (await store.order(order.client_order_id))["cumulative_filled"] == "0"
    assert order.ahead_qty == Decimal(".3")

    at_level = PublicTrade("BTC_JPY", Decimal("100"), Decimal(".5"), "BUY", now_ms + 1, "T2")
    await matcher.on_trade(at_level)
    await matcher.on_trade(at_level)  # public trade-id de-duplication
    row = await store.order(order.client_order_id)
    assert row["state"] == "PARTIAL"
    assert Decimal(row["cumulative_filled"]) == Decimal(".2")

    await matcher.on_trade(PublicTrade("BTC_JPY", Decimal("101"), Decimal(".01"), "BUY", now_ms + 2, "T3"))
    row = await store.order(order.client_order_id)
    assert row["state"] == "FILLED"
    assert Decimal(row["cumulative_filled"]) == Decimal("1")
    assert matcher.stats()["atLevelFills"] == 1
    assert matcher.stats()["throughFills"] == 1
    assert matcher.stats()["publicTradesSeen"] == 3
    await store.close()


@pytest.mark.asyncio
async def test_wrong_taker_side_and_non_crossing_trade_do_not_fill_buy_order(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    matcher = PaperMatchingEngine(FillTracker(store, EventBus()), MemoryDepth())
    await open_order(store, "BTCJPY-BUY-1", "BUY", "100", "1")
    order = await matcher.on_place("BTCJPY-BUY-1", "BTC_JPY", "BUY", Decimal("100"), Decimal("1"))
    await matcher.on_activate(order.client_order_id)
    now_ms = int(time.time() * 1000)

    await matcher.on_trade(PublicTrade("BTC_JPY", Decimal("99"), Decimal("2"), "BUY", now_ms, "T1"))
    await matcher.on_trade(PublicTrade("BTC_JPY", Decimal("101"), Decimal("2"), "SELL", now_ms + 1, "T2"))
    assert (await store.order(order.client_order_id))["cumulative_filled"] == "0"
    await store.close()


@pytest.mark.asyncio
async def test_queue_resync_only_moves_forward_and_excludes_same_level_late_orders(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    depth = MemoryDepth()
    matcher = PaperMatchingEngine(FillTracker(store, EventBus()), depth)
    await open_order(store, "BTCJPY-SELL-1", "SELL", "100", "1")
    order = await matcher.on_place("BTCJPY-SELL-1", "BTC_JPY", "SELL", Decimal("100"), Decimal("1"))
    await matcher.on_activate(order.client_order_id)
    assert order.ahead_qty == Decimal("1.0")

    depth.asks = [(Decimal("99"), Decimal(".1")), (Decimal("100"), Decimal("9"))]
    await matcher.resync_once()
    assert order.ahead_qty == Decimal(".1")
    depth.asks = [(Decimal("99"), Decimal(".8")), (Decimal("100"), Decimal("12"))]
    await matcher.resync_once()
    assert order.ahead_qty == Decimal(".1")
    await store.close()


def test_depth_feed_keeps_freshest_version_and_normalizes_book_order():
    feed = BitTradeDepthFeed(["BTC_JPY"])
    feed.update("BTC_JPY", [[99, 1], [101, 2]], [[104, 1], [103, 2]], version=2)
    feed.update("BTC_JPY", [[1, 99]], [[2, 99]], version=1)
    bids, asks = feed.book("BTC_JPY")
    assert bids == [(Decimal("101"), Decimal("2")), (Decimal("99"), Decimal("1"))]
    assert asks == [(Decimal("103"), Decimal("2")), (Decimal("104"), Decimal("1"))]
    assert feed.status()["BTC_JPY"]["bestBid"] == "101"


class ForbiddenVenue:
    async def place_quote(self, *args, **kwargs):
        raise AssertionError("Paper placement must not call BitTrade HTTP")


@pytest.mark.asyncio
async def test_execution_gateway_routes_paper_place_and_cancel_to_matcher(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    await risk.mark_recovery_complete()
    await risk.arm("ARM", "tester")
    limiter = PriorityRateLimiter()
    matcher = PaperMatchingEngine(FillTracker(store, EventBus()), MemoryDepth())
    gateway = ExecutionGateway(ForbiddenVenue(), store, risk, limiter, paper_engine=matcher)

    row = await gateway.place(
        symbol="BTC_JPY", side="SELL", qty=Decimal("1"), price=Decimal("100"),
        size_step=Decimal(".1"), price_tick=Decimal("1"), snapshot=RiskSnapshot(),
    )
    assert row["state"] == "OPEN"
    assert row["client_order_id"] in matcher.orders
    canceled = await gateway.cancel(row)
    assert canceled["state"] == "CANCELED"
    assert row["client_order_id"] not in matcher.orders
    await limiter.stop()
    await store.close()
