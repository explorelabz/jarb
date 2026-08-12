from __future__ import annotations

import sqlite3
import time
from decimal import Decimal

import pytest

from backend.engine.domain import OrderState
from backend.engine.events import EventBus
from backend.engine.execution_gateway import ExecutionGateway
from backend.engine.fill_tracker import FillTracker
from backend.engine.paper_matcher import (
    BitTradeDepthFeed, DepthUnavailableError, PaperMatchingEngine, PublicTrade,
)
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
async def test_same_price_trades_consume_queue_and_through_trade_is_volume_limited(tmp_path):
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
    assert row["state"] == "PARTIAL"
    assert Decimal(row["cumulative_filled"]) == Decimal(".21")
    await matcher.on_trade(PublicTrade("BTC_JPY", Decimal("102"), Decimal("2"), "BUY", now_ms + 3, "T4"))
    row = await store.order(order.client_order_id)
    assert row["state"] == "FILLED"
    assert Decimal(row["cumulative_filled"]) == Decimal("1")
    assert matcher.stats()["atLevelFills"] == 1
    assert matcher.stats()["throughFills"] == 2
    assert matcher.stats()["publicTradesSeen"] == 4
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
async def test_queue_resync_preserves_earlier_same_level_and_only_moves_forward(tmp_path):
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
    assert order.ahead_better == Decimal(".1")
    assert order.ahead_same == Decimal(".6")
    assert order.ahead_qty == Decimal(".7")
    depth.asks = [(Decimal("99"), Decimal(".8")), (Decimal("100"), Decimal("12"))]
    await matcher.resync_once()
    assert order.ahead_qty == Decimal(".7")
    depth.asks = [(Decimal("99"), Decimal(".8")), (Decimal("100"), Decimal(".2"))]
    await matcher.resync_once()
    assert order.ahead_qty == Decimal(".3")
    await store.close()


@pytest.mark.asyncio
async def test_resync_does_not_let_small_same_price_trade_jump_the_queue(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    depth = MemoryDepth()
    depth.asks = [(Decimal("100"), Decimal("2"))]
    matcher = PaperMatchingEngine(FillTracker(store, EventBus()), depth)
    await open_order(store, "BTCJPY-SELL-1", "SELL", "100", "1")
    order = await matcher.on_place("BTCJPY-SELL-1", "BTC_JPY", "SELL", Decimal("100"), Decimal("1"))
    await matcher.on_activate(order.client_order_id)

    await matcher.resync_once()
    assert order.ahead_qty == Decimal("2")
    await matcher.on_trade(PublicTrade(
        "BTC_JPY", Decimal("100"), Decimal(".1"), "BUY", int(time.time() * 1000), "T1",
    ))
    assert (await store.order(order.client_order_id))["cumulative_filled"] == "0"
    assert order.ahead_qty == Decimal("1.9")
    await store.close()


@pytest.mark.asyncio
async def test_one_through_print_shares_its_quantity_across_orders(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    matcher = PaperMatchingEngine(FillTracker(store, EventBus()), MemoryDepth())
    orders = []
    for index in (1, 2):
        client_id = f"BTCJPY-SELL-{index}"
        await open_order(store, client_id, "SELL", "100", "1")
        order = await matcher.on_place(client_id, "BTC_JPY", "SELL", Decimal("100"), Decimal("1"))
        await matcher.on_activate(client_id)
        orders.append(order)

    await matcher.on_trade(PublicTrade(
        "BTC_JPY", Decimal("101"), Decimal("1.2"), "BUY", int(time.time() * 1000), "T1",
    ))
    first = await store.order(orders[0].client_order_id)
    second = await store.order(orders[1].client_order_id)
    assert Decimal(first["cumulative_filled"]) == Decimal("1")
    assert Decimal(second["cumulative_filled"]) == Decimal(".2")
    await store.close()


@pytest.mark.asyncio
async def test_stats_callback_is_throttled_and_shutdown_flushes(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    snapshots = []

    async def capture(stats):
        snapshots.append(stats)

    matcher = PaperMatchingEngine(
        FillTracker(store, EventBus()), MemoryDepth(), stats_callback=capture,
        stats_flush_interval_sec=60, stats_flush_fill_count=50,
    )
    await open_order(store, "BTCJPY-SELL-1", "SELL", "100", "1")
    order = await matcher.on_place("BTCJPY-SELL-1", "BTC_JPY", "SELL", Decimal("100"), Decimal("1"))
    await matcher.on_activate(order.client_order_id)
    await matcher.on_trade(PublicTrade(
        "BTC_JPY", Decimal("101"), Decimal(".1"), "BUY", int(time.time() * 1000), "T1",
    ))
    assert snapshots == []
    await matcher.flush_stats()
    assert snapshots[-1]["throughQty"] == "0.1"
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


@pytest.mark.asyncio
async def test_paper_gateway_observes_risk_limit_without_blocking_order(tmp_path):
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
        symbol="BTC_JPY", side="SELL", qty=Decimal("3000"), price=Decimal("100"),
        size_step=Decimal(".1"), price_tick=Decimal("1"), snapshot=RiskSnapshot(),
    )
    assert row["state"] == "OPEN"
    assert risk.armed
    assert gateway.paper_risk_stats()["wouldReject"] == 1
    assert gateway.paper_risk_stats()["reasons"] == {"single order limit exceeded": 1}
    for _ in range(2):
        repeated = await gateway.place(
            symbol="BTC_JPY", side="SELL", qty=Decimal("3000"), price=Decimal("100"),
            size_step=Decimal(".1"), price_tick=Decimal("1"), snapshot=RiskSnapshot(),
        )
        assert repeated["state"] == "OPEN"
    assert gateway.paper_risk_stats()["wouldReject"] == 3
    with sqlite3.connect(tmp_path / "state.db") as db:
        audit_count = db.execute(
            "SELECT COUNT(*) FROM audit_events WHERE event_type='risk.paper.would_reject'",
        ).fetchone()[0]
    assert audit_count == 1
    await limiter.stop()
    await store.close()


@pytest.mark.asyncio
async def test_stale_depth_has_retryable_paper_error_code(tmp_path):
    class StaleDepth:
        def levels(self, _symbol, _side):
            raise DepthUnavailableError("stale")

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    await risk.mark_recovery_complete()
    await risk.arm("ARM", "tester")
    limiter = PriorityRateLimiter()
    matcher = PaperMatchingEngine(FillTracker(store, EventBus()), StaleDepth())
    gateway = ExecutionGateway(ForbiddenVenue(), store, risk, limiter, paper_engine=matcher)

    row = await gateway.place(
        symbol="BTC_JPY", side="SELL", qty=Decimal("1"), price=Decimal("100"),
        size_step=Decimal(".1"), price_tick=Decimal("1"), snapshot=RiskSnapshot(),
    )
    assert row["state"] == "FAILED"
    assert row["last_error"] == "depth_unavailable"
    assert risk.armed
    await limiter.stop()
    await store.close()
