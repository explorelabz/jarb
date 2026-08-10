from __future__ import annotations

import pytest

from backend.models import ConnectionUpdate, MarketTop, SimulatedFillRequest, StrategyConfig
from backend.service import TradingService


class FakeGmo:
    api_key = ""
    secret_key = ""

    def set_credentials(self, api_key: str, secret_key: str) -> None:
        self.api_key = api_key
        self.secret_key = secret_key

    async def ticker(self, symbol: str) -> MarketTop:
        return MarketTop(symbol=f"{symbol}_JPY", bid=20_000_000, ask=20_010_000, bidSize=.5, askSize=.4,
                         timestamp="2026-08-09T00:00:00Z", source="GMO")

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


@pytest.mark.asyncio
async def test_online_mode_uses_live_market_without_exposing_credentials():
    service = TradingService(StrategyConfig(), gmo=FakeGmo(), bittrade=FakeBittrade())
    await service.configure_connection(ConnectionUpdate(
        mode="online", confirmOnline=True, gmoApiKey="public-abcd", gmoSecretKey="private-secret",
    ))

    assert service.state.mode == "online"
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
async def test_multiple_symbols_run_and_reconcile_independently():
    service = TradingService(StrategyConfig(), gmo=FakeGmo(), bittrade=FakeBittrade())
    await service.configure({"symbols": ["BTC_JPY", "ETH_JPY"]})
    await service.configure_inventory(
        {"JPY": 1_000_000, "BTC": 1, "ETH": 10},
        {"JPY": 1_000_000, "BTC": 1, "ETH": 10},
    )

    assert service.state.activeSymbols == ["BTC_JPY", "ETH_JPY"]
    assert set(service.state.symbolStates) == {"BTC_JPY", "ETH_JPY"}
    await service.simulate_fill(SimulatedFillRequest(symbol="BTC_JPY", side="SELL", size=.01))
    await service.simulate_fill(SimulatedFillRequest(symbol="ETH_JPY", side="BUY", size=.01))

    btc = service.state.symbolStates["BTC_JPY"]
    eth = service.state.symbolStates["ETH_JPY"]
    assert len(btc.trades) == len(eth.trades) == 1
    assert btc.reconciliation.delta == eth.reconciliation.delta == 0
    assert service.state.metrics.fillCount == 2
    assert set(service.export_reconciliation()["symbols"]) == {"BTC_JPY", "ETH_JPY"}


@pytest.mark.asyncio
async def test_zero_inventory_disables_entire_pair():
    service = TradingService(StrategyConfig(), gmo=FakeGmo(), bittrade=FakeBittrade())
    await service.configure_inventory(
        {"JPY": 1_000_000, "BTC": 1},
        {"JPY": 0, "BTC": 1},
    )

    assert service.state.disabledSymbols["BTC_JPY"] == ["gmo:JPY:底仓"]
    with pytest.raises(ValueError, match="禁止交易"):
        await service.simulate_fill(SimulatedFillRequest(symbol="BTC_JPY", side="SELL", size=.01))


@pytest.mark.asyncio
async def test_pnl_and_latency_use_full_history_beyond_display_limit():
    service = TradingService(StrategyConfig(bittradeMakerFeeBps=1), gmo=FakeGmo(), bittrade=FakeBittrade())
    for _ in range(60):
        await service.simulate_fill(SimulatedFillRequest(symbol="BTC_JPY", side="SELL", size=.01))

    runtime = service.state.symbolStates["BTC_JPY"]
    full_history = service.matched_trades["BTC_JPY"]
    latencies = sorted(trade.latencyMs for trade in full_history)
    expected_p95 = latencies[min(len(latencies) - 1, int(len(latencies) * .95))]

    assert len(runtime.trades) == 50
    assert len(full_history) == runtime.fillCount == service.state.metrics.fillCount == 60
    assert runtime.pnl.net == pytest.approx(sum(trade.netPnl for trade in full_history))
    assert runtime.pnl.clientFees < 0
    assert runtime.hedgeP95Ms == service.state.metrics.hedgeP95Ms == expected_p95
    assert runtime.reconciliation.delta == 0
