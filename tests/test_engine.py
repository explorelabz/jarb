from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
import httpx

from backend.engine.balance import BalanceCache, CachedBalance
from backend.engine.alerting import LarkWebhookNotifier
from backend.engine.domain import HedgeStatus, OrderState
from backend.engine.events import EventBus
from backend.engine.execution_gateway import ExecutionGateway
from backend.engine.hedge_worker import HedgeExecution, HedgeWorker
from backend.engine.quote_engine import QuoteEngine, RequotePolicy, WorkingQuote
from backend.engine.rate_limit import PriorityRateLimiter
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
