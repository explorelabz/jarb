from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest

from backend.engine.domain import HedgeStatus, OrderState
from backend.engine.paper_matcher import PublicTrade
from backend.engine.state_store import StateStore
from backend.models import (
    ClientFill, ConnectionUpdate, MarketTop, PaperScenarioUpdate, RiskLimitsUpdate, StrategyConfig, utc_now,
)
from backend.service import TradingService


class FakeGmo:
    api_key = ""
    secret_key = ""

    def set_credentials(self, api_key: str, secret_key: str) -> None:
        self.api_key = api_key
        self.secret_key = secret_key

    async def ticker(self, symbol: str) -> MarketTop:
        return MarketTop(symbol=f"{symbol}_JPY", bid=20_000_000, ask=20_010_000, bidSize=.5, askSize=.4,
                         timestamp=utc_now(), source="GMO")

    async def market_stream(self, bases: list[str], feed) -> None:
        """A deterministic stand-in for the public GMO market-data stream."""
        while True:
            for base in bases:
                await feed.update(await self.ticker(base.removesuffix("_JPY")), transport="ws")
            await asyncio.sleep(.2)

    async def symbols(self) -> list[dict]:
        return [
            {"symbol": "BTC", "minOrderSize": ".0001", "maxOrderSize": "5", "sizeStep": ".0001", "tickSize": "1"},
            {"symbol": "ETH", "minOrderSize": ".01", "maxOrderSize": "10", "sizeStep": ".001", "tickSize": "1"},
            {"symbol": "DOGE", "minOrderSize": "10", "maxOrderSize": "100000", "sizeStep": "1", "tickSize": ".001"},
            {"symbol": "BTC_JPY", "minOrderSize": ".01", "maxOrderSize": "5", "sizeStep": ".01", "tickSize": "1"},
        ]


class FakeBittrade:
    access_key = ""
    secret_key = ""
    account_id = ""

    def set_credentials(self, access_key: str, secret_key: str, account_id: str) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.account_id = account_id

    async def symbols(self) -> list[dict]:
        return [
            {"base-currency": "btc", "quote-currency": "jpy", "state": "online", "api-trading": "enabled",
             "amount-precision": 4, "price-precision": 0, "limit-order-min-order-amt": .001, "limit-order-max-order-amt": 100},
            {"base-currency": "eth", "quote-currency": "jpy", "state": "online", "api-trading": "enabled",
             "amount-precision": 3, "price-precision": 0, "limit-order-min-order-amt": .001, "limit-order-max-order-amt": 100},
            {"base-currency": "doge", "quote-currency": "jpy", "state": "suspend", "api-trading": "enabled",
             "amount-precision": 0, "price-precision": 3, "limit-order-min-order-amt": 1, "limit-order-max-order-amt": 100000},
            {"base-currency": "xrp", "quote-currency": "jpy", "state": "online", "api-trading": "enabled",
             "amount-precision": 2, "price-precision": 3, "limit-order-min-order-amt": 1, "limit-order-max-order-amt": 100000},
        ]

    async def depth(self, symbol: str) -> dict:
        bids = [[str(19_996_000 - 4_000 * index), ".0002"] for index in range(20)]
        asks = [[str(20_014_002 + 4_002 * index), ".0002"] for index in range(20)]
        return {"tick": {
            "bids": bids,
            "asks": asks,
        }}


class FakeDepthFeed:
    def __init__(self, _symbols: list[str]):
        self.books = {
            "BTC_JPY": (
                [(Decimal(str(19_996_000 - 4_000 * index)), Decimal(".0002")) for index in range(20)],
                [(Decimal(str(20_014_002 + 4_002 * index)), Decimal(".0002")) for index in range(20)],
            ),
            "ETH_JPY": (
                [(Decimal(str(519_000 - 200 * index)), Decimal(".01")) for index in range(20)],
                [(Decimal(str(521_000 + 200 * index)), Decimal(".01")) for index in range(20)],
            ),
        }

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def levels(self, symbol: str, side: str):
        bids, asks = self.books[symbol]
        return list(bids if side == "BUY" else asks)

    def book(self, symbol: str):
        return self.levels(symbol, "BUY"), self.levels(symbol, "SELL")


async def idle_public_trades(_symbols: list[str]):
    while True:
        await asyncio.sleep(3600)
        if False:
            yield PublicTrade("BTC_JPY", Decimal("0"), Decimal("0"), "BUY", 0, "never")


async def cross_first_paper_order(service: TradingService, trade_id: str = "PUBLIC-1") -> dict:
    for _ in range(100):
        engine = service.paper_engine
        paper_orders = list(engine.orders.values()) if engine is not None else []
        if paper_orders:
            paper_order = paper_orders[0]
            order = await service.state_store.order(paper_order.client_order_id)
            assert order is not None
            break
        await asyncio.sleep(.05)
    else:
        raise AssertionError("Paper quote was not registered in the queue matcher")
    assert service.paper_engine is not None
    side = paper_order.side
    price = paper_order.price
    await service.paper_engine.on_trade(PublicTrade(
        symbol=paper_order.symbol,
        price=price + Decimal("1") if side == "SELL" else price - Decimal("1"),
        qty=paper_order.qty,
        taker_side="BUY" if side == "SELL" else "SELL",
        ts_ms=int(time.time() * 1000),
        trade_id=trade_id,
    ))
    return order


def paper_market_adapters() -> dict:
    """Paper execution tests use controlled *live-source* public market adapters."""
    return {
        "market_gmo": FakeGmo(), "market_bittrade": FakeBittrade(),
        "bittrade_depth_factory": FakeDepthFeed,
        "paper_trade_stream_factory": idle_public_trades,
    }


@pytest.mark.asyncio
async def test_execution_price_keeps_exchange_decimal_and_delta_breach_kills_risk_gate(tmp_path):
    service = TradingService(
        StrategyConfig(deltaLimit=.005), mode="paper", gmo=FakeGmo(), bittrade=FakeBittrade(),
        db_path=tmp_path / "state.db",
    )
    await service.state_store.initialize()
    service.risk_gate.kill_sentinel = tmp_path / "KILL"
    await service.risk_gate.restore()
    runtime = service.state.symbolStates["BTC_JPY"]
    runtime.instrument.priceTick = .00000001
    exact_market = MarketTop(
        symbol="BTC_JPY", bid=float(Decimal("0.123456789")), ask=float(Decimal("0.123456799")),
        bidSize=1, askSize=1, bidExact=Decimal("0.123456789"), askExact=Decimal("0.123456799"),
        timestamp=utc_now(), source="GMO",
    )
    runtime.market = exact_market
    service.market_feed.latest["BTC_JPY"] = exact_market
    assert service._gmo_passive_price("BTC_JPY", "BUY") == Decimal("0.12345678")
    assert service._gmo_passive_price("BTC_JPY", "SELL") == Decimal("0.12345680")

    service.clients["BTC_JPY"] = [ClientFill(
        id="C-DELTA", orderId="BT-DELTA", symbol="BTC_JPY", side="BUY",
        price=100, size=.006, fee=0, role="maker", timestamp=utc_now(),
    )]
    await service._recalculate("BTC_JPY")
    assert service.risk_gate.killed is True
    assert service.state.killSwitch is True
    assert "delta limit exceeded" in service.risk_gate.last_reason
    await service.state_store.close()
    await service.notifier.close()


@pytest.mark.asyncio
async def test_online_mode_uses_live_market_without_exposing_credentials():
    service = TradingService(StrategyConfig(), mode="live", gmo=FakeGmo(), bittrade=FakeBittrade())
    await service.configure_connection(ConnectionUpdate(
        mode="live", confirmOnline=True, gmoApiKey="public-abcd", gmoSecretKey="private-secret",
    ))

    assert service.state.mode == "live"
    assert service.state.market.source == "GMO"
    assert service.state.market.bid == 20_000_000
    assert service.state.connection.status == "connected"
    assert service.state.connection.gmoKeyHint == "••••abcd"
    assert "private-secret" not in service.state.model_dump_json()


@pytest.mark.asyncio
async def test_only_common_online_api_symbols_can_be_selected():
    service = TradingService(StrategyConfig(), gmo=FakeGmo(), bittrade=FakeBittrade())
    common = await service.common_symbols()
    assert [item.symbol for item in common] == ["BTC_JPY", "ETH_JPY"]
    assert common[0].minOrderSize == .001

    await service.configure({"symbol": "ETH_JPY"})
    assert service.state.config.symbol == "ETH_JPY"
    assert service.state.instrument.baseAsset == "ETH"

    with pytest.raises(ValueError, match="共同可交易"):
        await service.configure({"symbol": "XRP_JPY"})


@pytest.mark.asyncio
async def test_runtime_uses_per_symbol_gmo_taker_fee():
    class FeeGmo(FakeGmo):
        async def symbols(self):
            return await super().symbols() + [
                {"symbol": "LTC", "minOrderSize": ".01", "maxOrderSize": "100",
                 "sizeStep": ".01", "tickSize": ".1"},
            ]

    class FeeBittrade(FakeBittrade):
        async def symbols(self):
            return await super().symbols() + [
                {"base-currency": "ltc", "quote-currency": "jpy", "state": "online",
                 "api-trading": "enabled", "amount-precision": 2, "price-precision": 1,
                 "limit-order-min-order-amt": .01, "limit-order-max-order-amt": 100},
            ]

    service = TradingService(StrategyConfig(), gmo=FeeGmo(), bittrade=FeeBittrade())
    await service.configure({"symbols": ["BTC_JPY", "LTC_JPY"], "gmoFeeBps": 0})
    assert service.state.symbolStates["BTC_JPY"].config.gmoFeeBps == 5
    assert service.state.symbolStates["LTC_JPY"].config.gmoFeeBps == 9

    await service.configure({
        "symbols": ["BTC_JPY", "LTC_JPY"],
        "gmoFeeBpsByAsset": {"BTC": 6.5, "LTC": 7.25},
    })
    assert service.state.symbolStates["BTC_JPY"].config.gmoFeeBps == 6.5
    assert service.state.symbolStates["LTC_JPY"].config.gmoFeeBps == 7.25


@pytest.mark.asyncio
async def test_risk_limits_are_runtime_configurable():
    service = TradingService(StrategyConfig(), gmo=FakeGmo(), bittrade=FakeBittrade())
    limits = await service.configure_risk_limits(RiskLimitsUpdate(
        maxSingleOrderJpy=100_000, maxDailyVolumeJpy=2_000_000,
        maxDailyLossJpy=50_000, maxAbsDelta=.002,
        maxHedgeFailures=2, maxHedgeP95Ms=2500, armTtlSec=1_800,
    ))
    assert limits["maxSingleOrderJpy"] == 100_000
    assert service.risk_gate.limits.max_abs_delta == .002
    assert service.risk_status()["limits"]["maxHedgeP95Ms"] == 2500
    assert service.risk_status()["limits"]["armTtlSec"] == 86_400

    with pytest.raises(ValueError, match="SOK 等待时间"):
        await service.configure_risk_limits(RiskLimitsUpdate(maxHedgeP95Ms=1000))


@pytest.mark.asyncio
async def test_paper_configuration_keeps_engine_armed_and_uses_day_long_lease(tmp_path):
    db_path = tmp_path / "state.db"
    previous = StateStore(db_path)
    await previous.initialize()
    await previous.set_state("risk.limits", {"arm_ttl_sec": 3_600})
    await previous.close()

    service = TradingService(StrategyConfig(maxQuoteSize=.002), db_path=db_path, **paper_market_adapters())
    await service.start()
    try:
        assert service.risk_gate.armed, service.risk_gate.last_reason
        assert service.risk_status()["limits"]["armTtlSec"] == 86_400

        await service.configure({"symbols": ["BTC_JPY"]})
        await service.configure_risk_limits(RiskLimitsUpdate(maxDailyLossJpy=90_000))
        await service.configure_inventory(
            {"JPY": 1_000_000, "BTC": 1}, {"JPY": 1_000_000, "BTC": 1},
        )

        assert service.risk_gate.armed, service.risk_gate.last_reason
        assert service.state.running
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_paper_records_hedge_latency_without_disarming(tmp_path):
    service = TradingService(
        StrategyConfig(maxQuoteSize=.002), db_path=tmp_path / "state.db", **paper_market_adapters(),
    )
    await service.start()
    try:
        service.state.metrics.hedgeP95Ms = 99_999
        await service._enforce_risk()
        assert service.risk_gate.armed
        assert service.state.running
        assert service._last_live_risk_snapshot.hedge_p95_ms == 99_999
        observed = service.paper_scenario_summary()["matching"]["risk"]
        assert observed["wouldReject"] == 1
        assert observed["reasons"] == {"hedge latency limit exceeded": 1}
        assert observed["currentReason"] == "hedge latency limit exceeded"

        service.market_feed.latest["BTC_JPY"] = service.state.market.model_copy(
            update={"timestamp": "2020-01-01T00:00:00Z"},
        )
        await service._enforce_risk()
        assert service.risk_gate.armed
        assert service.state.running
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_lark_heartbeat_reports_arm_orders_and_today_fills(tmp_path):
    class CaptureNotifier:
        webhook_url = "https://open.larksuite.com/open-apis/bot/v2/hook/test"
        messages: list[tuple[str, str]] = []

        async def send_once(self, key: str, message: str) -> bool:
            self.messages.append((key, message))
            return True

    service = TradingService(StrategyConfig(), db_path=tmp_path / "state.db")
    await service.state_store.initialize()
    service._engine_ready = True
    original_notifier = service.notifier
    notifier = CaptureNotifier()
    service.notifier = notifier
    await service._send_heartbeat()

    assert notifier.messages == [("heartbeat", "💓 JARB 心跳：disarmed / 挂单 0 / 今日成交 0 笔 / 模式 paper")]
    await service.state_store.close()
    await original_notifier.close()


def test_quote_size_is_capped_by_directional_delta_headroom():
    limit = Decimal("0.005")
    assert TradingService._delta_headroom("BUY", Decimal("0.003"), limit) == Decimal("0.002")
    assert TradingService._delta_headroom("SELL", Decimal("0.003"), limit) == Decimal("0.008")
    assert TradingService._delta_headroom("BUY", Decimal("0.006"), limit) == 0


@pytest.mark.asyncio
async def test_multiple_symbols_run_and_reconcile_independently(tmp_path):
    service = TradingService(
        StrategyConfig(maxQuoteSize=.002), db_path=tmp_path / "state.db", **paper_market_adapters(),
    )
    await service.configure({"symbols": ["BTC_JPY", "ETH_JPY"]})
    await service.configure_inventory(
        {"JPY": 1_000_000, "BTC": 1, "ETH": 10},
        {"JPY": 1_000_000, "BTC": 1, "ETH": 10},
    )

    await service.start()
    try:
        for _ in range(30):
            if all(runtime.market.source == "GMO" for runtime in service.state.symbolStates.values()):
                break
            await asyncio.sleep(.1)
        assert service.state.activeSymbols == ["BTC_JPY", "ETH_JPY"]
        assert set(service.state.symbolStates) == {"BTC_JPY", "ETH_JPY"}
        assert all(runtime.market.source == "GMO" for runtime in service.state.symbolStates.values())
        assert set(service.export_reconciliation()["symbols"]) == {"BTC_JPY", "ETH_JPY"}
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_paper_fill_updates_both_venue_holdings(tmp_path):
    service = TradingService(
        StrategyConfig(maxQuoteSize=.002), db_path=tmp_path / "state.db", **paper_market_adapters(),
    )
    await service.configure_inventory(
        {"JPY": 1_000_000, "BTC": 1},
        {"JPY": 1_000_000, "BTC": 1},
    )
    await service.start()
    try:
        await cross_first_paper_order(service)
        for _ in range(200):
            if service.state.metrics.fillCount and not await service.state_store.pending_hedges():
                await asyncio.sleep(.1)  # allow the coalesced UI/holdings projection to publish
                break
            await asyncio.sleep(.1)
        holdings = service.state.holdings
        assert holdings.source == "paper"
        assert holdings.bittrade["BTC"].opening == 1
        assert service.state.metrics.fillCount > 0
        combined = holdings.bittrade["BTC"].change + holdings.gmo["BTC"].change
        assert combined == pytest.approx(service.state.reconciliation.delta)
        combined_jpy = holdings.bittrade["JPY"].change + holdings.gmo["JPY"].change
        mid = (service.state.market.bid + service.state.market.ask) / 2
        marked_equity = combined_jpy + combined * mid
        assert marked_equity == pytest.approx(service.state.pnl.net, abs=.05)
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_durable_live_fills_project_to_delta_pnl_and_dashboard(tmp_path):
    service = TradingService(
        StrategyConfig(), mode="live", gmo=FakeGmo(), bittrade=FakeBittrade(),
        db_path=tmp_path / "state.db", require_dual_arm_approval=False,
    )
    await service.state_store.initialize()
    service._engine_ready = True
    try:
        await service.state_store.create_order(
            "BTCJPY-SELL-1", "BTC_JPY", "SELL", Decimal("0.01"), Decimal("20050000"),
        )
        await service.state_store.transition_order("BTCJPY-SELL-1", OrderState.PLACING)
        await service.state_store.transition_order(
            "BTCJPY-SELL-1", OrderState.OPEN, exchange_order_id="BT-1",
        )
        fill = await service.state_store.record_cumulative_fill(
            client_order_id="BTCJPY-SELL-1", order_id="BT-1", trade_id="T-1",
            symbol="BTC_JPY", side="SELL", cumulative_qty=Decimal("0.01"),
            price=Decimal("20050000"), fee=Decimal("0"), occurred_at=utc_now(),
        )
        assert fill is not None
        await service._refresh_live_projection("BTC_JPY")
        assert service.state.symbolStates["BTC_JPY"].reconciliation.delta == pytest.approx(-.01)

        intent = await service.state_store.create_hedge_intent(fill, "BUY")
        await service.state_store.transition_hedge(intent.id, HedgeStatus.HEDGING)
        await service.state_store.transition_hedge(
            intent.id, HedgeStatus.HEDGED, filled_qty=Decimal("0.01"),
            filled_notional=Decimal("200100"), latency_ms=125, exchange_order_id="GMO-1",
        )
        await service._refresh_live_projection("BTC_JPY")
        runtime = service.state.symbolStates["BTC_JPY"]
        assert runtime.reconciliation.delta == 0
        assert runtime.fillCount == 1
        assert runtime.pnl.net != 0
        assert runtime.trades[0].latencyMs == 125
        assert service.export_reconciliation()["symbols"]["BTC_JPY"]["matchedTrades"]
    finally:
        service._engine_ready = False
        await service.state_store.close()


@pytest.mark.asyncio
async def test_paper_public_trade_generates_fill_and_hedge(tmp_path):
    db_path = tmp_path / "state.db"
    service = TradingService(
        StrategyConfig(maxQuoteSize=.002), db_path=db_path, **paper_market_adapters(),
    )
    await service.start()
    try:
        await cross_first_paper_order(service)
        for _ in range(100):
            if service.state.metrics.fillCount:
                break
            await asyncio.sleep(.1)
        assert service.state.metrics.fillCount >= 1
        assert service.state.trades
        assert abs(service.state.reconciliation.delta) <= service.state.config.deltaLimit
        exported = []
        for _ in range(100):
            exported = await service.export_orders("paper")
            if any(row["hedge_status"] == "HEDGED" for row in exported):
                break
            await asyncio.sleep(.1)
        assert exported
        assert exported[-1]["trading_mode"] == "paper"
        assert any(row["hedge_status"] == "HEDGED" for row in exported)
    finally:
        await service.stop()

    restarted = TradingService(
        StrategyConfig(maxQuoteSize=.002), db_path=db_path, **paper_market_adapters(),
    )
    await restarted.start()
    try:
        assert restarted.state.metrics.fillCount >= 1
        assert restarted.state.trades
        holdings = restarted.state.holdings
        assert holdings.bittrade["JPY"].change is not None
        assert holdings.gmo["JPY"].change is not None
        assert restarted.risk_gate.recovery_complete is True
    finally:
        await restarted.stop()


@pytest.mark.asyncio
async def test_zero_inventory_disables_entire_pair():
    service = TradingService(StrategyConfig())
    await service.configure_inventory(
        {"JPY": 1_000_000, "BTC": 1},
        {"JPY": 0, "BTC": 1},
    )

    assert service.state.disabledSymbols["BTC_JPY"] == ["gmo:JPY:底仓"]
    assert service.state.disabledSymbols["BTC_JPY"] == ["gmo:JPY:底仓"]


@pytest.mark.asyncio
async def test_paper_fault_switches_are_runtime_configurable():
    service = TradingService(StrategyConfig())
    result = await service.configure_paper_scenarios(PaperScenarioUpdate(**{
        "gmoPartialFak": True, "delayedExecutions": True, "randomRateLimit": True,
    }))
    assert result["gmoPartialFak"] is True
    assert result["matching"]["throughFills"] == 0
    assert "autoMatch" not in result
