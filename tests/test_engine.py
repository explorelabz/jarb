from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import httpx

from backend.engine.balance import BalanceCache, CachedBalance
from backend.engine.alerting import LarkWebhookNotifier
from backend.engine.domain import HedgeStatus, OrderState
from backend.engine.events import EventBus
from backend.adapters import ExchangeAPIError
from backend.engine.execution_gateway import ExecutionGateway
from backend.engine.fill_tracker import BitTradePrivateWS, BitTradeRestFillSource
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
async def test_fill_and_hedge_intent_are_atomic_and_legacy_gaps_are_backfilled(tmp_path):
    path = tmp_path / "state.db"
    store = StateStore(path)
    await store.initialize()
    await store.create_order("BTCJPY-BUY-ATOMIC", "BTC_JPY", "BUY", Decimal("1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-ATOMIC", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-ATOMIC", OrderState.OPEN, exchange_order_id="9001-A")
    fill = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-ATOMIC", order_id="9001-A", trade_id="T-ATOMIC",
        symbol="BTC_JPY", side="BUY", cumulative_qty=Decimal("0.2"),
        price=Decimal("100"), fee=Decimal("0"), occurred_at="2026-08-13T00:00:00Z",
    )
    assert fill is not None
    pending = await store.pending_hedges()
    assert len(pending) == 1
    assert pending[0].client_fill_id == fill.fill_id
    assert pending[0].side == "SELL"

    # Simulate a database created by the old two-commit implementation.
    with sqlite3.connect(path) as db:
        db.execute("DELETE FROM hedge_intents WHERE client_fill_id=?", (fill.fill_id,))
    assert not await store.pending_hedges()
    assert await store.backfill_missing_hedge_intents() == 1
    assert (await store.pending_hedges())[0].client_fill_id == fill.fill_id
    assert await store.backfill_missing_hedge_intents() == 0
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


@pytest.mark.asyncio
async def test_risk_disarm_sends_lark_alert_without_blocking_safety(tmp_path):
    class CaptureNotifier:
        messages: list[tuple[str, str]] = []

        async def send(self, message: str) -> bool:
            self.messages.append(message)
            return True

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    notifier = CaptureNotifier()
    risk = RiskGate(store, confirmation_phrase="ARM", notifier=notifier)
    await risk.restore()
    await risk.mark_recovery_complete()
    await risk.arm("ARM", "alice")
    await risk.disarm("market data stale", "system")

    assert not risk.armed
    assert notifier.messages == ["⚠️ JARB 已 DISARM：market data stale（操作者：system）"]
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


def test_target_price_never_joins_or_crosses_the_opposite_best():
    assert target_price(
        [(Decimal("101"), Decimal("1"))], Decimal("100"), Decimal("0"),
        Decimal("1"), Decimal("1"), "SELL", opposite_best=Decimal("100"),
    ) is None
    assert target_price(
        [(Decimal("99"), Decimal("1"))], Decimal("100"), Decimal("0"),
        Decimal("1"), Decimal("1"), "BUY", opposite_best=Decimal("100"),
    ) is None


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
async def test_rest_match_sweep_recovers_missing_exchange_and_client_ids(tmp_path):
    class MatchAdapter:
        async def recent_matches(self, symbol=None, *, start_time=None):
            return {"data": [{
                "order-id": "EX-REST-1", "trade-id": "T-REST-1", "symbol": "btcjpy",
                    "side": "buy-limit-maker", "price": "100", "created-at": int(time.time() * 1000),
            }]}

        async def matches(self, order_id):
            return {"data": [{
                "trade-id": "T-REST-1", "filled-amount": "0.1", "price": "100",
                "filled-fees": "0", "created-at": int(time.time() * 1000),
            }]}

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-REST", "BTC_JPY", "BUY", Decimal("0.1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-REST", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-REST", OrderState.UNKNOWN)
    events = await BitTradeRestFillSource(MatchAdapter(), store)()
    assert len(events) == 1
    assert events[0].client_order_id == "BTCJPY-BUY-REST"
    assert events[0].order_id == "EX-REST-1"
    assert (await store.order("BTCJPY-BUY-REST"))["exchange_order_id"] == "EX-REST-1"
    await store.close()


@pytest.mark.asyncio
async def test_private_fill_without_client_id_recovers_unique_unknown_order(tmp_path):
    class FillAdapter:
        access_key = "key"
        secret_key = "secret"
        time_offset_sec = 0
        HOST = "example.invalid"

        async def order(self, order_id):
            return {"data": {"field-amount": "0.2", "price": "100"}}

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-LOST-ID", "BTC_JPY", "BUY", Decimal("0.2"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-LOST-ID", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-LOST-ID", OrderState.UNKNOWN)
    source = BitTradePrivateWS(FillAdapter(), ["BTC_JPY"], store=store)
    event = await source._trade_event({
        "orderId": "EX-LOST-1", "tradeId": "T-LOST-1", "symbol": "btcjpy",
        "orderSide": "buy", "tradePrice": "100", "tradeTime": int(time.time() * 1000),
    })
    assert event.client_order_id == "BTCJPY-BUY-LOST-ID"
    assert event.cumulative_qty == Decimal("0.2")
    assert (await store.order("BTCJPY-BUY-LOST-ID"))["exchange_order_id"] == "EX-LOST-1"
    await store.close()


@pytest.mark.asyncio
async def test_private_fill_without_client_id_fails_closed_when_mapping_is_ambiguous(tmp_path):
    class FillAdapter:
        access_key = "key"
        secret_key = "secret"
        time_offset_sec = 0
        HOST = "example.invalid"

        async def order(self, order_id):
            return {"data": {"field-amount": "0.1", "price": "100"}}

    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    for suffix in ("A", "B"):
        client_id = f"BTCJPY-BUY-AMBIG-{suffix}"
        await store.create_order(client_id, "BTC_JPY", "BUY", Decimal("0.1"), Decimal("100"))
        await store.transition_order(client_id, OrderState.PLACING)
        await store.transition_order(client_id, OrderState.UNKNOWN)
    source = BitTradePrivateWS(FillAdapter(), ["BTC_JPY"], store=store)
    with pytest.raises(RuntimeError, match="no resolvable clientOrderId"):
        await source._trade_event({
            "orderId": "EX-AMBIG", "tradeId": "T-AMBIG", "symbol": "btcjpy",
            "orderSide": "buy", "tradePrice": "100", "tradeTime": int(time.time() * 1000),
        })
    with sqlite3.connect(tmp_path / "state.db") as db:
        assert db.execute(
            "SELECT COUNT(*) FROM audit_events WHERE event_type='fill.ws.unresolved' AND level='critical'"
        ).fetchone()[0] == 1
    await store.close()


@pytest.mark.asyncio
async def test_private_fill_recovery_refuses_unknown_order_outside_time_window(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-OLD", "BTC_JPY", "BUY", Decimal("0.1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-OLD", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-OLD", OrderState.UNKNOWN)
    resolved = await store.resolve_client_order_for_exchange_fill(
        "EX-FUTURE", symbol="BTC_JPY", side="BUY", price=Decimal("100"),
        occurred_at="2030-01-01T00:00:00Z", window_sec=600,
    )
    assert resolved is None
    assert (await store.order("BTCJPY-BUY-OLD"))["exchange_order_id"] is None
    await store.close()


@pytest.mark.asyncio
async def test_cancel_all_keeps_unknown_without_exchange_id_unresolved(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-UNKNOWN", "BTC_JPY", "BUY", Decimal("0.1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-UNKNOWN", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-UNKNOWN", OrderState.UNKNOWN)
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    limiter = PriorityRateLimiter()
    gateway = ExecutionGateway(FakeMakerVenue(), store, risk, limiter)
    with pytest.raises(RuntimeError, match="manual reconciliation required"):
        await gateway.cancel_all(timeout_sec=.05)
    assert (await store.order("BTCJPY-BUY-UNKNOWN"))["state"] == OrderState.UNKNOWN
    with sqlite3.connect(tmp_path / "state.db") as db:
        assert db.execute(
            "SELECT COUNT(*) FROM audit_events WHERE event_type='orders.cancel_all.unresolved'"
        ).fetchone()[0] == 1
    await limiter.stop()
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
async def test_execute_group_recalculates_qty_after_retry_limit_filter(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    intents = []
    for suffix, qty in (("STALE", Decimal("0.3")), ("ACTIVE", Decimal("0.1"))):
        client_id = f"BTCJPY-BUY-{suffix}"
        await store.create_order(client_id, "BTC_JPY", "BUY", qty, Decimal("100"))
        await store.transition_order(client_id, OrderState.PLACING)
        await store.transition_order(client_id, OrderState.OPEN, exchange_order_id=f"BT-{suffix}")
        fill = await store.record_cumulative_fill(
            client_order_id=client_id, order_id=f"BT-{suffix}", trade_id=f"T-{suffix}",
            symbol="BTC_JPY", side="BUY", cumulative_qty=qty, price=Decimal("100"),
            fee=Decimal("0"), occurred_at="2026-08-13T00:00:00Z",
        )
        intents.append(next(
            intent for intent in await store.pending_hedges()
            if intent.client_fill_id == fill.fill_id
        ))
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute("UPDATE hedge_intents SET attempts=4 WHERE id=?", (intents[0].id,))

    submitted: list[Decimal] = []

    async def execute(_symbol, _side, qty):
        submitted.append(qty)
        return HedgeExecution("GMO-ACTIVE", qty, qty * Decimal("101"))

    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    await risk.restore()
    worker = HedgeWorker(
        store, EventBus(), execute, risk, min_sizes={"BTC_JPY": Decimal("0.1")}, max_attempts=4,
    )
    refreshed = await store.pending_hedges()
    await worker._execute_group(("BTC_JPY", "SELL"), refreshed, Decimal("0.4"))
    assert submitted == [Decimal("0.1")]
    assert len(await store.escalated_hedges()) == 1
    assert not await store.pending_hedges()
    await store.close()


@pytest.mark.asyncio
async def test_response_lost_fak_is_recovered_from_gmo_history_without_resubmission(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-ORPHAN", "BTC_JPY", "BUY", Decimal("0.1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-ORPHAN", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-ORPHAN", OrderState.OPEN, exchange_order_id="BT-ORPHAN")
    fill = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-ORPHAN", order_id="BT-ORPHAN", trade_id="T-ORPHAN",
        symbol="BTC_JPY", side="BUY", cumulative_qty=Decimal("0.1"),
        price=Decimal("100"), fee=Decimal("0"), occurred_at=datetime.now(timezone.utc).isoformat(),
    )
    intent = next(item for item in await store.pending_hedges() if item.client_fill_id == fill.fill_id)
    intent = await store.transition_hedge(intent.id, HedgeStatus.HEDGING)
    submitted_at = datetime.now(timezone.utc).isoformat()
    submission = await store.prepare_hedge_submission(
        [intent.id], symbol="BTC_JPY", side="SELL", qty=Decimal("0.1"),
        execution_type="FAK", submitted_at=submitted_at,
    )

    class RecoveryGmo:
        market_orders = 0

        async def market_order(self, *args):
            self.market_orders += 1
            raise AssertionError("recovery must not submit another FAK")

        async def latest_executions(self, symbol, *, page=1, count=100):
            return {"data": {"list": [{
                "orderId": "GMO-ORPHAN", "symbol": "BTC", "side": "SELL",
                "size": "0.1", "price": "101", "fee": "0.001",
                "timestamp": submitted_at,
            }] if page == 1 else []}}

        async def active_orders(self, symbol, *, page=1, count=100):
            return {"data": {"list": []}}

        async def order(self, order_id):
            return {"data": {"list": [{
                "orderId": order_id, "symbol": "BTC", "side": "SELL", "size": "0.1",
                "executionType": "MARKET", "timeInForce": "FAK", "timestamp": submitted_at,
            }]}}

    adapter = RecoveryGmo()
    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        adapter, limiter, {"BTC_JPY": Decimal("0.1")}, orphan_recovery_grace_sec=0,
    )
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    worker = HedgeWorker(
        store, EventBus(), executor, risk, min_sizes={"BTC_JPY": Decimal("0.1")},
        resolver=executor.resolve,
    )
    await worker.start()
    recovered = await store.hedge_intent(intent.id)
    assert recovered.status == HedgeStatus.HEDGED
    assert recovered.filled_qty == Decimal("0.1")
    assert recovered.exchange_order_id == "GMO-ORPHAN"
    assert (await store.submissions_for_intent(intent.id))[0].status == "RESOLVED"
    assert adapter.market_orders == 0
    await worker.stop()
    await limiter.stop()
    await store.close()


@pytest.mark.asyncio
async def test_process_exit_after_fak_acceptance_leaves_durable_recovery_barrier(tmp_path):
    db_path = tmp_path / "crash.db"
    script = textwrap.dedent(f"""
        import asyncio
        import os
        from datetime import datetime, timezone
        from decimal import Decimal
        from backend.engine.domain import HedgeStatus, OrderState
        from backend.engine.hedge_worker import GmoHedgeExecutor
        from backend.engine.rate_limit import PriorityRateLimiter
        from backend.engine.state_store import StateStore

        async def main():
            store = StateStore({str(db_path)!r})
            await store.initialize()
            await store.create_order('BT-CRASH', 'BTC_JPY', 'BUY', Decimal('0.1'), Decimal('100'))
            await store.transition_order('BT-CRASH', OrderState.PLACING)
            await store.transition_order('BT-CRASH', OrderState.OPEN, exchange_order_id='BT-1')
            fill = await store.record_cumulative_fill(
                client_order_id='BT-CRASH', order_id='BT-1', trade_id='T-1',
                symbol='BTC_JPY', side='BUY', cumulative_qty=Decimal('0.1'),
                price=Decimal('100'), fee=Decimal('0'),
                occurred_at=datetime.now(timezone.utc).isoformat(),
            )
            intent = next(item for item in await store.pending_hedges() if item.client_fill_id == fill.fill_id)
            await store.transition_hedge(intent.id, HedgeStatus.HEDGING)

            class AcceptedGmo:
                async def market_order(self, *args):
                    return {{'status': 0, 'data': {{'orderId': 'GMO-CRASH'}}}}

            limiter = PriorityRateLimiter()
            executor = GmoHedgeExecutor(AcceptedGmo(), limiter, {{'BTC_JPY': Decimal('0.1')}})

            async def prepare(kind, qty, submitted_at):
                row = await store.prepare_hedge_submission(
                    [intent.id], symbol='BTC_JPY', side='SELL', qty=qty,
                    execution_type=kind, submitted_at=submitted_at,
                )
                return row.id

            async def acknowledge(_submission_id, _order_id):
                os._exit(17)

            async def complete(*_args):
                raise AssertionError('process should exit before completion')

            async def reject(*_args):
                raise AssertionError('accepted order must not be rejected')

            executor.set_submission_hooks(prepare, acknowledge, complete, reject)
            await executor('BTC_JPY', 'SELL', Decimal('0.1'))

        asyncio.run(main())
    """)
    crashed = subprocess.run(
        [sys.executable, "-c", script], cwd=str(Path(__file__).parents[1]),
        capture_output=True, text=True, timeout=10,
    )
    assert crashed.returncode == 17, crashed.stderr

    store = StateStore(db_path)
    await store.initialize()
    submissions = await store.pending_hedge_submissions()
    assert len(submissions) == 1 and submissions[0].status == "SUBMITTING"
    submitted_at = submissions[0].submitted_at

    class RecoveryGmo:
        market_orders = 0

        async def market_order(self, *args):
            self.market_orders += 1
            raise AssertionError("restart must reconcile before any retry")

        async def latest_executions(self, symbol, *, page=1, count=100):
            return {"data": {"list": [{
                "orderId": "GMO-CRASH", "symbol": "BTC", "side": "SELL",
                "size": "0.1", "price": "101", "fee": "0",
                "timestamp": submitted_at,
            }] if page == 1 else []}}

        async def active_orders(self, symbol, *, page=1, count=100):
            return {"data": {"list": []}}

        async def order(self, order_id):
            return {"data": {"list": [{
                "orderId": order_id, "symbol": "BTC", "side": "SELL", "size": "0.1",
                "executionType": "MARKET", "timeInForce": "FAK", "timestamp": submitted_at,
            }]}}

    adapter = RecoveryGmo()
    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        adapter, limiter, {"BTC_JPY": Decimal("0.1")}, orphan_recovery_grace_sec=0,
    )
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    worker = HedgeWorker(
        store, EventBus(), executor, risk, min_sizes={"BTC_JPY": Decimal("0.1")},
        resolver=executor.resolve,
    )
    await worker.start()
    assert adapter.market_orders == 0
    assert not await store.pending_hedge_submissions()
    assert not await store.pending_hedges()
    await worker.stop()
    await limiter.stop()
    await store.close()


@pytest.mark.asyncio
async def test_response_lost_sok_fallback_fak_recovers_only_the_remainder(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-HYBRID", "BTC_JPY", "BUY", Decimal("0.1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-HYBRID", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-HYBRID", OrderState.OPEN, exchange_order_id="BT-HYBRID")
    fill = await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-HYBRID", order_id="BT-HYBRID", trade_id="T-HYBRID",
        symbol="BTC_JPY", side="BUY", cumulative_qty=Decimal("0.1"),
        price=Decimal("100"), fee=Decimal("0"), occurred_at=datetime.now(timezone.utc).isoformat(),
    )
    intent = next(item for item in await store.pending_hedges() if item.client_fill_id == fill.fill_id)
    intent = await store.transition_hedge(
        intent.id, HedgeStatus.HEDGING, filled_qty=Decimal("0.06"),
        filled_notional=Decimal("6.06"), exchange_order_id="GMO-SOK",
    )
    submitted_at = datetime.now(timezone.utc).isoformat()
    await store.prepare_hedge_submission(
        [intent.id], symbol="BTC_JPY", side="SELL", qty=Decimal("0.04"),
        execution_type="FAK", submitted_at=submitted_at,
    )

    class RecoveryGmo:
        async def latest_executions(self, symbol, *, page=1, count=100):
            return {"data": {"list": [{
                "orderId": "GMO-FAK", "symbol": "BTC", "side": "SELL",
                "size": "0.04", "price": "102", "fee": "0.002",
                "timestamp": submitted_at,
            }] if page == 1 else []}}

        async def active_orders(self, symbol, *, page=1, count=100):
            return {"data": {"list": []}}

        async def order(self, order_id):
            return {"data": {"list": [{
                "orderId": order_id, "symbol": "BTC", "side": "SELL", "size": "0.04",
                "executionType": "MARKET", "timeInForce": "FAK", "timestamp": submitted_at,
            }]}}

    limiter = PriorityRateLimiter()
    executor = GmoHedgeExecutor(
        RecoveryGmo(), limiter, {"BTC_JPY": Decimal("0.01")},
        orphan_recovery_grace_sec=0,
    )
    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    worker = HedgeWorker(
        store, EventBus(), executor, risk, min_sizes={"BTC_JPY": Decimal("0.01")},
        resolver=executor.resolve,
    )
    await worker.start()
    recovered = await store.hedge_intent(intent.id)
    assert recovered.status == HedgeStatus.HEDGED
    assert recovered.filled_qty == Decimal("0.10")
    assert recovered.filled_notional == Decimal("10.14")
    await worker.stop()
    await limiter.stop()
    await store.close()


@pytest.mark.asyncio
async def test_checkpointed_fak_timeout_is_resolved_without_submitting_another_order(tmp_path):
    store = StateStore(tmp_path / "state.db")
    await store.initialize()
    await store.create_order("BTCJPY-BUY-FAK", "BTC_JPY", "BUY", Decimal("0.1"), Decimal("100"))
    await store.transition_order("BTCJPY-BUY-FAK", OrderState.PLACING)
    await store.transition_order("BTCJPY-BUY-FAK", OrderState.OPEN, exchange_order_id="BT-FAK")
    await store.record_cumulative_fill(
        client_order_id="BTCJPY-BUY-FAK", order_id="BT-FAK", trade_id="T-FAK",
        symbol="BTC_JPY", side="BUY", cumulative_qty=Decimal("0.1"),
        price=Decimal("100"), fee=Decimal("0"), occurred_at="2026-08-13T00:00:00Z",
    )
    intent = (await store.pending_hedges())[0]

    class TimeoutAfterCheckpoint:
        def __init__(self):
            self.calls = 0
            self.checkpoint = None

        def set_checkpoint(self, callback):
            self.checkpoint = callback

        async def __call__(self, symbol, side, qty):
            self.calls += 1
            await self.checkpoint("GMO-FAK-1")
            raise TimeoutError("executions temporarily delayed")

    executor = TimeoutAfterCheckpoint()
    resolved: list[str] = []

    async def resolver(checkpointed):
        resolved.append(checkpointed.exchange_order_id)
        return HedgeExecution(
            "GMO-FAK-1", Decimal("0.1"), Decimal("10"),
            confirmed_at="2026-08-13T00:00:01Z",
        )

    risk = RiskGate(store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL")
    worker = HedgeWorker(
        store, EventBus(), executor, risk, min_sizes={"BTC_JPY": Decimal("0.1")},
        resolver=resolver,
    )
    await worker._execute_group(("BTC_JPY", "SELL"), [intent], Decimal("0.1"))
    inflight = (await store.pending_hedges())[0]
    assert inflight.status == HedgeStatus.HEDGING
    assert inflight.exchange_order_id == "GMO-FAK-1"

    await worker._execute_group(("BTC_JPY", "SELL"), [inflight], Decimal("0.1"))
    assert executor.calls == 1
    assert resolved == ["GMO-FAK-1"]
    assert not await store.pending_hedges()
    await store.close()


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
    assert execution.order_id == "M-1"
    assert "+" not in execution.order_id
    assert execution.filled_qty == Decimal("0.10")
    assert execution.filled_notional == Decimal("10.14")
    assert execution.fee_jpy == Decimal("6.06") * Decimal("-1") / Decimal("10000") \
        + Decimal("4.08") * Decimal("5") / Decimal("10000")
    await limiter.stop()


@pytest.mark.asyncio
async def test_gmo_public_ws_serializes_subscriptions_and_publishes_best_level(monkeypatch):
    sent: list[dict] = []
    delays: list[float] = []
    trades = []
    trade_seen = asyncio.Event()
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
            yield json.dumps({
                "channel": "trades", "symbol": "BTC", "timestamp": "2026-08-10T00:00:00.125Z",
                "price": "100", "size": "0.2", "side": "SELL",
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
    async def on_trade(trade):
        trades.append(trade)
        trade_seen.set()

    task = asyncio.create_task(GmoPublicWS(["BTC", "ETH"], feed, on_trade=on_trade).run())
    event = await asyncio.wait_for(queue.get(), timeout=.2)
    await asyncio.wait_for(trade_seen.wait(), timeout=.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert [(row["symbol"], row["channel"]) for row in sent[:4]] == [
        ("BTC", "orderbooks"), ("BTC", "trades"),
        ("ETH", "orderbooks"), ("ETH", "trades"),
    ]
    assert sent[1]["option"] == "TAKER_ONLY"
    assert delays[:4] == [1.1, 1.1, 1.1, 1.1]
    assert event.payload.bid == 100
    assert event.payload.askSize == 3
    assert event.payload.decimal_bid() == Decimal("100")
    assert event.payload.decimal_asks() == [(Decimal("101"), Decimal("3"))]
    assert feed.latest_transport["BTC_JPY"] == "ws"
    assert trades[0].symbol == "BTC_JPY"
    assert trades[0].taker_side == "SELL"
    assert trades[0].qty == Decimal("0.2")
    assert trades[0].ts_ms == 1786320000125


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
