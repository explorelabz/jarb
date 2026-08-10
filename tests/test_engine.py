from __future__ import annotations

import asyncio
import json
import sqlite3
from decimal import Decimal

import pytest
import httpx

from backend.engine.balance import BalanceCache, CachedBalance
from backend.engine.alerting import LarkWebhookNotifier
from backend.engine.domain import HedgeStatus, OrderState
from backend.engine.events import EventBus
from backend.adapters import ExchangeAPIError
from backend.engine.execution_gateway import ExecutionGateway
from backend.engine.fill_tracker import BitTradeRestFillSource
from backend.engine.hedge_worker import GmoHedgeExecutor, HedgeExecution, HedgeWorker
from backend.engine.market_feed import GmoPublicWS, MarketFeed
from backend.engine.quote_engine import QuoteEngine, RequotePolicy, WorkingQuote, target_price
from backend.engine.rate_limit import EndpointGroup, Priority, PriorityRateLimiter
from backend.engine.risk import RiskGate, RiskLimits, RiskSnapshot
from backend.engine.state_store import StateStore


@pytest.mark.asyncio
async def test_cumulative_fills_are_database_idempotent(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-1", "BTC_JPY", "BUY", Decimal("1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-1", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-1", OrderState.OPEN, exchange_order_id="9001")

    first = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-1", order_id="9001", trade_id="T1", symbol="BTC_JPY", side="BUY",
        cumulative_qty=Decimal("0.2"), price=Decimal("100"), fee=Decimal("0"), occurred_at="now",
    )
    duplicate = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-1", order_id="9001", trade_id="T1", symbol="BTC_JPY", side="BUY",
        cumulative_qty=Decimal("0.2"), price=Decimal("100"), fee=Decimal("0"), occurred_at="now",
    )
    second = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-1", order_id="9001", trade_id="T2", symbol="BTC_JPY", side="BUY",
        cumulative_qty=Decimal("0.35"), price=Decimal("101"), fee=Decimal("0"), occurred_at="later",
    )
    old = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-1", order_id="9001", trade_id="T0", symbol="BTC_JPY", side="BUY",
        cumulative_qty=Decimal("0.1"), price=Decimal("99"), fee=Decimal("0"), occurred_at="earlier",
    )

    assert first and first.incremental_qty == Decimal("0.2")
    assert duplicate is None
    assert second and second.incremental_qty == Decimal("0.15")
    assert old is None
    assert (await store.open_orders())[0]["cumulative_filled"] == "0.35"
    await store.close()


@pytest.mark.asyncio
async def test_partial_fill_during_cancel_preserves_state_machine(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-SELL-1", "BTC_JPY", "SELL", Decimal("1"), Decimal("100"))
    await store.transition_order("BTCJPY-SELL-1", OrderState.PLACING)
    await store.transition_order("BTCJPY-SELL-1", OrderState.OPEN, exchange_order_id="9002")
    await store.transition_order("BTCJPY-SELL-1", OrderState.CANCELING)
    await store.record_cumulative_fill(
        client_order_id="BTCJPY-SELL-1", order_id="9002", trade_id="T-CANCEL",
        symbol="BTC_JPY", side="SELL", cumulative_qty=Decimal("0.25"),
        price=Decimal("100"), fee=Decimal("0"), occurred_at="2026-08-10T00:00:00Z",
    )
    row = await store.order("BTCJPY-SELL-1")
    assert row["state"] == "CANCELING"
    assert Decimal(row["cumulative_filled"]) == Decimal("0.25")
    await store.transition_order("BTCJPY-SELL-1", OrderState.CANCELED)
    await store.close()


@pytest.mark.asyncio
async def test_state_store_migrates_existing_hedge_latency_origin_column(tmp_path):
    path = tmp_path / "legacy.db"
    connection = sqlite3.connect(path)
    connection.execute("""
        CREATE TABLE hedge_intents (
            id TEXT PRIMARY KEY, client_fill_id INTEGER NOT NULL UNIQUE, symbol TEXT NOT NULL,
            side TEXT NOT NULL, qty TEXT NOT NULL, filled_qty TEXT NOT NULL DEFAULT '0',
            filled_notional TEXT NOT NULL DEFAULT '0', status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, latency_ms INTEGER NOT NULL DEFAULT 0,
            exchange_order_id TEXT, last_error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    connection.commit()
    connection.close()
    store = StateStore(path)
    await store.initialize()
    await store.close()
    connection = sqlite3.connect(path)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(hedge_intents)")}
    connection.close()
    assert "source_fill_at" in columns
    assert "fee_jpy" in columns


@pytest.mark.asyncio
async def test_hedge_intent_is_persisted_and_partial_fak_is_retried(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-1", "BTC_JPY", "BUY", Decimal("0.2"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-1", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-1", OrderState.OPEN, exchange_order_id="9001")
    fill = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-1", order_id="9001", trade_id="T1", symbol="BTC_JPY", side="BUY",
        cumulative_qty=Decimal("0.2"), price=Decimal("100"), fee=Decimal("0"), occurred_at="now",
    )
    assert fill
    events = EventBus()
    risk = RiskGate(store, RiskLimits(max_abs_delta=1), confirmation_phrase="ARM")
    await risk.restore()
    await risk.mark_recovery_complete()
    calls = 0

    async def execute(symbol: str, side: str, qty: Decimal) -> HedgeExecution:
        nonlocal calls
        calls += 1
        return HedgeExecution(str(calls), Decimal("0.1") if calls == 1 else qty)

    worker = HedgeWorker(
        store, events, execute, risk, min_sizes={"BTC_JPY": Decimal("0.1")},
        delta_thresholds={"BTC_JPY": Decimal("0.1")}, dust_timeout_sec=.05,
    )
    await worker.start()
    await asyncio.sleep(.01)
    await events.publish("fill.incremental", fill)
    for _ in range(100):
        pending = await store.pending_hedges()
        if calls >= 2 and not pending:
            break
        await asyncio.sleep(.01)
    assert calls == 2
    assert not await store.pending_hedges()
    await worker.stop()
    await store.close()


@pytest.mark.asyncio
async def test_risk_gate_requires_recovery_and_expires(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    risk = RiskGate(store, RiskLimits(arm_ttl_sec=1), confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    with pytest.raises(ValueError, match="恢复对账"):
        await risk.arm("ARM", "tester")
    await risk.mark_recovery_complete()
    await risk.arm("ARM", "tester")
    assert risk.armed
    monkeypatch.setattr("backend.engine.risk.time.time", lambda: risk.armed_until + 1)
    allowed, reason = await risk.evaluate(RiskSnapshot())
    assert not allowed and reason == "arm expired"
    await store.close()


def test_quote_and_balance_gates_reduce_churn_and_oversizing():
    engine = QuoteEngine(RequotePolicy(
        price_deviation_bps=Decimal("2"), depth_change_ratio=Decimal("0.2"),
        min_remaining_ratio=Decimal("0.5"),
    ))
    current = WorkingQuote(Decimal("100"), Decimal("1"), Decimal("1"), Decimal("2"))
    assert not engine.should_requote(current, target_price=Decimal("100.01"), target_qty=Decimal("1"), current_depth=Decimal("2"))
    assert engine.should_requote(current, target_price=Decimal("100.03"), target_qty=Decimal("1"), current_depth=Decimal("2"))

    cache = BalanceCache(safety_factor=Decimal("0.7"))
    cache._balances[("bittrade", "JPY")] = CachedBalance(Decimal("1000"))
    cache._balances[("bittrade", "BTC")] = CachedBalance(Decimal("2"))
    cache._balances[("gmo", "JPY")] = CachedBalance(Decimal("1000"))
    cache._balances[("gmo", "BTC")] = CachedBalance(Decimal("0.5"))
    cache.configure_allocations({
        "bittrade": {"JPY": Decimal("1000"), "BTC": Decimal("2")},
        "gmo": {"JPY": Decimal("1000"), "BTC": Decimal("0.5")},
    })
    assert cache.quote_capacity(
        side="BUY", base_asset="BTC", price=Decimal("100"), strategy_limit=Decimal("2"),
        hedge_depth=Decimal("1"),
    ) == Decimal("0.35")
    cache.configure_allocations({
        "bittrade": {"JPY": Decimal("1000"), "BTC": Decimal("2")},
        "gmo": {"JPY": Decimal("0"), "BTC": Decimal("0.5")},
    })
    assert cache.pair_blockers("BTC", require_actual=False) == ["gmo:JPY:底仓"]


def test_default_requote_policy_preserves_queue_and_depth_aware_price_steps_inside():
    policy = RequotePolicy()
    assert policy.price_deviation_bps == Decimal("8")
    assert policy.depth_change_ratio == Decimal("0.6")
    assert policy.min_remaining_ratio == Decimal("0.25")
    sell = target_price(
        [(Decimal("101"), Decimal(".01")), (Decimal("102"), Decimal(".01"))],
        Decimal("100"), Decimal("100"), Decimal(".05"), Decimal("1"), "SELL",
    )
    buy = target_price(
        [(Decimal("99"), Decimal(".01")), (Decimal("98"), Decimal(".01"))],
        Decimal("100"), Decimal("100"), Decimal(".05"), Decimal("1"), "BUY",
    )
    assert sell == Decimal("101")
    assert buy == Decimal("99")


class FakeMakerVenue:
    def __init__(self):
        self.state = "submitted"
        self.actions: list[str] = []

    async def place_quote(self, symbol, quote, client_order_id, size_step, price_tick):
        self.actions.append(f"place:{client_order_id}")
        self.state = "submitted"
        return {"status": "ok", "data": "2"}

    async def cancel(self, order_id):
        self.actions.append(f"cancel:{order_id}")
        self.state = "canceled"
        return {"status": "ok", "data": order_id}

    async def order(self, order_id):
        return {"status": "ok", "data": {"id": order_id, "state": self.state}}

    async def batch_cancel(self, **kwargs):
        return {"status": "ok", "data": {"success": []}}

    async def cancel_all_open(self, symbols=None):
        self.state = "canceled"
        return {"status": "ok"}

    async def open_orders(self, symbol=None):
        return {"status": "ok", "data": []}


@pytest.mark.asyncio
async def test_gateway_cancel_before_place_and_monotonic_ids(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    await risk.mark_recovery_complete()
    await risk.arm("ARM", "tester")
    limiter = PriorityRateLimiter()
    venue = FakeMakerVenue()
    gateway = ExecutionGateway(venue, store, risk, limiter)
    kwargs = dict(
        symbol="BTC_JPY", side="BUY", qty=Decimal("0.1"), price=Decimal("100"),
        size_step=Decimal("0.1"), price_tick=Decimal("1"), snapshot=RiskSnapshot(),
    )
    first = await gateway.place(**kwargs)
    second = await gateway.replace(first, **kwargs)
    assert first["client_order_id"].endswith("-1")
    assert second["client_order_id"].endswith("-2")
    assert venue.actions == [f"place:{first['client_order_id']}", "cancel:2", f"place:{second['client_order_id']}"]
    await limiter.stop()
    await store.close()


@pytest.mark.asyncio
async def test_gateway_treats_filled_during_replace_as_normal_and_ingests_matches(tmp_path):
    class FilledOnCancelVenue(FakeMakerVenue):
        async def cancel(self, order_id):
            self.actions.append(f"cancel:{order_id}")
            self.state = "filled"
            return {"status": "ok"}

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    await risk.mark_recovery_complete()
    await risk.arm("ARM", "tester")
    limiter = PriorityRateLimiter()
    venue = FilledOnCancelVenue()
    gateway = ExecutionGateway(venue, store, risk, limiter)
    reconciled: list[str] = []

    async def reconcile_order(order):
        reconciled.append(order["client_order_id"])
        await store.record_cumulative_fill(
            client_order_id=order["client_order_id"], order_id=order["exchange_order_id"],
            trade_id="T-FULL", symbol=order["symbol"], side=order["side"],
            cumulative_qty=Decimal(order["qty"]), price=Decimal(order["price"]),
            fee=Decimal("0"), occurred_at="2026-08-10T00:00:00Z",
        )

    gateway.set_fill_reconciler(reconcile_order)
    kwargs = dict(
        symbol="BTC_JPY", side="BUY", qty=Decimal("0.1"), price=Decimal("100"),
        size_step=Decimal("0.1"), price_tick=Decimal("1"), snapshot=RiskSnapshot(),
    )
    first = await gateway.place(**kwargs)
    result = await gateway.replace(first, **kwargs)
    assert result["state"] == "FILLED"
    assert Decimal(result["cumulative_filled"]) == Decimal("0.1")
    assert reconciled == [first["client_order_id"]]
    assert venue.actions == [f"place:{first['client_order_id']}", "cancel:2"]
    await limiter.stop()
    await store.close()


@pytest.mark.asyncio
async def test_account_level_match_sweep_uses_persisted_cursor(tmp_path):
    class MatchAdapter:
        def __init__(self):
            self.start_time = None

        async def recent_matches(self, symbol=None, *, start_time=None):
            self.start_time = start_time
            return {"status": "ok", "data": [{"order-id": "9001", "created-at": 2000}]}

        async def matches(self, order_id):
            return {"status": "ok", "data": [{
                "order-id": order_id, "trade-id": "T1", "filled-amount": "0.1",
                "price": "100", "filled-fees": "0", "created-at": 2000,
            }]}

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-1", "BTC_JPY", "BUY", Decimal("1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-1", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-1", OrderState.OPEN, exchange_order_id="9001")
    await store.set_state("last_processed_ts", 10_000)
    adapter = MatchAdapter()
    source = BitTradeRestFillSource(adapter, store)
    events = await source()
    assert adapter.start_time == "5000"
    assert len(events) == 1 and events[0].trade_id == "T1"
    await source.checkpoint()
    assert await store.get_state("last_processed_ts") > 10_000
    await store.close()


@pytest.mark.asyncio
async def test_live_arm_requires_two_distinct_operators(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    risk = RiskGate(
        store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL",
        require_dual_approval=True,
    )
    await risk.restore()
    await risk.mark_recovery_complete()
    assert not await risk.arm("ARM", "operator-a")
    with pytest.raises(ValueError, match="不同操作员"):
        await risk.arm("ARM", "operator-a")
    assert await risk.arm("ARM", "operator-b")
    assert risk.armed
    await store.close()


@pytest.mark.asyncio
async def test_rate_limiter_dispatches_critical_request_while_place_is_slow():
    limiter = PriorityRateLimiter()
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()

    async def slow_place():
        slow_started.set()
        await release_slow.wait()
        return "place"

    async def fast_hedge():
        return "hedge"

    place = asyncio.create_task(limiter.submit(EndpointGroup.PLACE, Priority.PLACE, slow_place))
    await slow_started.wait()
    hedge = asyncio.create_task(limiter.submit(EndpointGroup.HEDGE, Priority.CRITICAL, fast_hedge))
    assert await asyncio.wait_for(hedge, timeout=.2) == "hedge"
    release_slow.set()
    assert await place == "place"
    await limiter.stop()


@pytest.mark.asyncio
async def test_gmo_hedge_waits_for_delayed_execution_confirmation():
    class FakeGmo:
        def __init__(self):
            self.market_orders = 0
            self.execution_queries = 0

        async def market_order(self, symbol, side, qty, size_step):
            self.market_orders += 1
            return {"status": 0, "data": {"orderId": "G-1"}}

        async def executions(self, order_id):
            self.execution_queries += 1
            if self.execution_queries == 1:
                return {"status": 0, "data": []}
            return {"status": 0, "data": [{"size": "0.1", "price": "100"}]}

        async def order(self, order_id):
            return {"status": 0, "data": {"list": [{"status": "ORDERED"}]}}

    adapter = FakeGmo()
    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        adapter, limiter, {"BTC_JPY": Decimal("0.1")}, fill_timeout_sec=.5,
    )
    execution = await executor("BTC_JPY", "SELL", Decimal("0.1"))
    assert adapter.market_orders == 1
    assert adapter.execution_queries == 2
    assert execution.filled_qty == Decimal("0.1")
    assert execution.filled_notional == Decimal("10.0")
    assert execution.submitted_at and execution.confirmed_at
    await limiter.stop()


@pytest.mark.asyncio
async def test_gmo_terminal_status_does_not_turn_empty_execution_rows_into_duplicate_hedge():
    class TerminalBeforeExecutionsGmo:
        def __init__(self):
            self.execution_queries = 0

        async def market_order(self, symbol, side, qty, size_step):
            return {"status": 0, "data": {"orderId": "G-RACE"}}

        async def executions(self, order_id):
            self.execution_queries += 1
            if self.execution_queries == 1:
                return {"status": 0, "data": []}
            return {"status": 0, "data": [{"size": "0.1", "price": "100"}]}

        async def order(self, order_id):
            return {"status": 0, "data": {"list": [{"status": "EXECUTED"}]}}

    adapter = TerminalBeforeExecutionsGmo()
    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        adapter, limiter, {"BTC_JPY": Decimal("0.1")}, fill_timeout_sec=.5,
    )
    execution = await executor("BTC_JPY", "SELL", Decimal("0.1"))
    assert adapter.execution_queries == 2
    assert execution.filled_qty == Decimal("0.1")
    await limiter.stop()


@pytest.mark.asyncio
async def test_gmo_sok_partial_fill_is_canceled_before_fak_fallback_and_uses_blended_fee():
    class HybridGmo:
        def __init__(self):
            self.canceled = False
            self.market_sizes: list[Decimal] = []

        async def post_only_order(self, symbol, side, qty, price, size_step, price_tick):
            return {"status": 0, "data": {"orderId": "P-1"}}

        async def cancel_order(self, order_id):
            self.canceled = True
            return {"status": 0, "data": order_id}

        async def market_order(self, symbol, side, qty, size_step):
            assert self.canceled
            self.market_sizes.append(qty)
            return {"status": 0, "data": {"orderId": "M-1"}}

        async def executions(self, order_id):
            if order_id == "P-1":
                return {"status": 0, "data": [{"size": "0.06", "price": "101"}]}
            return {"status": 0, "data": [{"size": "0.04", "price": "102"}]}

        async def order(self, order_id):
            if order_id == "P-1":
                status = "CANCELED" if self.canceled else "ORDERED"
                return {"status": 0, "data": {"list": [{
                    "status": status, "executedSize": "0.06", "timeInForce": "SOK",
                }]}}
            return {"status": 0, "data": {"list": [{
                "status": "EXECUTED", "executedSize": "0.04", "timeInForce": "FAK",
            }]}}

    adapter = HybridGmo()
    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        adapter, limiter, {"BTC_JPY": Decimal("0.01")},
        price_ticks={"BTC_JPY": Decimal("1")},
        passive_price=lambda symbol, side: Decimal("101"),
        maker_fee_bps={"BTC_JPY": Decimal("-1")},
        taker_fee_bps={"BTC_JPY": Decimal("5")},
        passive_timeout_ms={"BTC_JPY": 100}, fill_timeout_sec=.5,
    )
    execution = await executor("BTC_JPY", "SELL", Decimal("0.1"))
    assert adapter.market_sizes == [Decimal("0.04")]
    assert execution.filled_qty == Decimal("0.10")
    assert execution.filled_notional == Decimal("10.14")
    assert execution.fee_jpy == Decimal("6.06") * Decimal("-1") / Decimal("10000") \
        + Decimal("4.08") * Decimal("5") / Decimal("10000")
    await limiter.stop()


@pytest.mark.asyncio
async def test_gmo_public_ws_serializes_subscriptions_and_publishes_best_level(monkeypatch):
    sent: list[dict] = []
    delays: list[float] = []
    block = asyncio.Event()

    class FakeSocket:
        async def send(self, raw):
            sent.append(json.loads(raw))

        def __aiter__(self):
            return self.messages()

        async def messages(self):
            yield json.dumps({
                "channel": "orderbooks", "symbol": "BTC", "timestamp": "2026-08-10T00:00:00Z",
                "bids": [{"price": "100", "size": "2"}],
                "asks": [{"price": "101", "size": "3"}],
            })
            await block.wait()

    class FakeConnect:
        async def __aenter__(self):
            return FakeSocket()

        async def __aexit__(self, *_):
            return False

    async def no_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("backend.engine.market_feed.websockets.connect", lambda *_, **__: FakeConnect())
    monkeypatch.setattr("backend.engine.market_feed.asyncio.sleep", no_sleep)
    events = EventBus()
    feed = MarketFeed(object(), events)
    queue = events.open_queue("market.updated")
    task = asyncio.create_task(GmoPublicWS(["BTC", "ETH"], feed).run())
    event = await asyncio.wait_for(queue.get(), timeout=.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert [row["symbol"] for row in sent[:2]] == ["BTC", "ETH"]
    assert delays[:2] == [1.1, 1.1]
    assert event.payload.bid == 100
    assert event.payload.askSize == 3
    assert feed.latest_transport["BTC_JPY"] == "ws"


@pytest.mark.asyncio
async def test_post_only_reject_is_classified_without_unknown_state(tmp_path):
    class RejectingVenue(FakeMakerVenue):
        async def place_quote(self, *args, **kwargs):
            raise ExchangeAPIError("BitTrade", "order would immediately match", code="maker-reject")

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    await risk.mark_recovery_complete()
    await risk.arm("ARM", "tester")
    limiter = PriorityRateLimiter()
    gateway = ExecutionGateway(RejectingVenue(), store, risk, limiter)
    result = await gateway.place(
        symbol="BTC_JPY", side="SELL", qty=Decimal("0.1"), price=Decimal("100"),
        size_step=Decimal("0.1"), price_tick=Decimal("1"), snapshot=RiskSnapshot(),
    )
    assert result["state"] == "FAILED"
    assert result["last_error"] == "post_only_reject"
    await limiter.stop()
    await store.close()


@pytest.mark.asyncio
async def test_lark_inventory_alert_is_deduplicated():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.host == "open.larksuite.com"
        return httpx.Response(200, json={"code": 0, "msg": "success"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    notifier = LarkWebhookNotifier(client, cooldown_sec=300)
    notifier.configure("https://open.larksuite.com/open-apis/bot/v2/hook/test")
    assert await notifier.send_once("BTC_JPY", "disabled")
    assert not await notifier.send_once("BTC_JPY", "disabled again")
    assert calls == 1
    with pytest.raises(ValueError, match="官方 HTTPS"):
        notifier.configure("https://example.com/hook")
    await client.aclose()
