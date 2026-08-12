from __future__ import annotations

import importlib
import sys

import pytest

import backend.core as core_module
from backend import core_fallback
from backend.core import make_quotes, matched_trade, opposite_side, reconcile, validate_config
from backend.models import ClientFill, HedgeFill, MarketTop, StrategyConfig, gmo_taker_bps


def config(**updates) -> StrategyConfig:
    return StrategyConfig(**updates)


def test_quote_defaults_restore_live_candidates_and_use_jpy_queue_budget():
    defaults = StrategyConfig()
    assert defaults.spreadBps == 12
    assert defaults.queueBudgetJpy == 1_500_000


def test_quotes_are_outside_gmo_and_depth_capped():
    market = MarketTop(symbol="BTC_JPY", bid=14_990_000, ask=15_010_000, bidSize=.02, askSize=.4, timestamp="", source="GMO")
    quotes = make_quotes(market, config(spreadBps=10))
    assert quotes[0].price == 14_975_010
    assert quotes[0].size == .02
    assert quotes[1].price == 15_025_010
    assert quotes[1].size == .05


def test_quotes_follow_decimal_price_tick():
    market = MarketTop(symbol="DOGE_JPY", bid=24.123, ask=24.127, bidSize=100, askSize=100, timestamp="", source="GMO")
    quotes = make_quotes(market, config(maxQuoteSize=10, spreadBps=10), price_tick=.001)
    assert quotes[0].price == 24.098
    assert quotes[1].price == 24.152


def test_quotes_use_cumulative_gmo_depth_inside_slippage_band():
    market = MarketTop(
        symbol="BTC_JPY", bid=100, ask=101, bidSize=.01, askSize=.01,
        bids=[(100, .01), (99.98, .02), (99.96, .04), (99.90, 1)],
        asks=[(101, .01), (101.02, .02), (101.04, .04), (101.10, 1)],
        timestamp="", source="GMO",
    )
    quotes = make_quotes(market, config(maxQuoteSize=1, spreadBps=10, maxHedgeSlippageBps=5))
    assert quotes[0].size == pytest.approx(.07)
    assert quotes[1].size == pytest.approx(.07)


def test_hedge_direction_is_opposite():
    assert opposite_side("BUY") == "SELL"
    assert opposite_side("SELL") == "BUY"


def test_audit_scenario_a_is_27023_jpy():
    client = ClientFill(id="c", orderId="co", symbol="BTC_JPY", side="SELL", price=15_025_000, size=1, fee=15_025, role="taker", timestamp="")
    hedge = HedgeFill(id="h", orderId="ho", clientFillId="c", symbol="BTC_JPY", side="BUY", price=15_010_000, size=1, fee=3_002, latencyMs=80, timestamp="", status="filled")
    pnl = matched_trade(client, hedge)
    assert pnl.spreadPnl == 15_000
    assert pnl.netPnl == 27_023


def test_reconciliation_uses_fixed_1e8_units():
    clients = [ClientFill(id="c", orderId="co", symbol="BTC_JPY", side="SELL", price=1, size=.2, fee=0, role="maker", timestamp="")]
    assert reconcile("BTC_JPY", clients, []).delta == -.2
    hedges = [HedgeFill(id="h", orderId="ho", clientFillId="c", symbol="BTC_JPY", side="BUY", price=1, size=.2, fee=0, latencyMs=1, timestamp="", status="filled")]
    assert reconcile("BTC_JPY", clients, hedges).status == "matched"


def test_maker_profitability_floor():
    with pytest.raises(ValueError, match="价差必须高于"):
        validate_config(config(spreadBps=3))


def test_latency_limit_leaves_room_for_sok_fallback_confirmation():
    with pytest.raises(ValueError, match="SOK 等待时间"):
        validate_config(config(maxHedgeLatencyMs=1900, gmoPostOnlyTimeoutMs=800))


def test_bittrade_maker_fee_is_included_in_profitability_floor():
    with pytest.raises(ValueError, match="价差必须高于"):
        validate_config(config(spreadBps=4, bittradeMakerFeeBps=1))
    assert validate_config(config(spreadBps=4, bittradeMakerFeeBps=-1)) > 0


def test_gmo_taker_fee_is_selected_per_base_asset():
    assert gmo_taker_bps("btc") == 5
    assert gmo_taker_bps("DAI") == 5
    assert gmo_taker_bps("DOGE") == 9


def test_core_imports_and_operates_without_optional_rust_extension(monkeypatch):
    with monkeypatch.context() as patch:
        patch.setitem(sys.modules, "hedge_core", None)
        fallback = importlib.reload(core_module)
        assert fallback.core_runtime() == "Python/Decimal fallback"
        quotes = fallback.make_quotes(
            MarketTop(
                symbol="DOGE_JPY", bid=24.123, ask=24.127, bidSize=100, askSize=100,
                timestamp="", source="GMO",
            ),
            config(maxQuoteSize=10, spreadBps=10),
            price_tick=.001,
        )
        assert [quote.price for quote in quotes] == [24.098, 24.152]
    importlib.reload(core_module)


def test_rust_and_decimal_fallback_outputs_are_consistent():
    native = pytest.importorskip("hedge_core")
    cases = [
        (100, 101, [(100, .1), (99.99, .2)], [(101, .1), (101.01, .2)], 8, .25, .01, 5),
        (24.123, 24.127, [(24.123, 100)], [(24.127, 100)], 10, 10, .001, 3),
        (17_400_000, 17_410_000, [(17_400_000, .02)], [(17_410_000, .03)], 25, .05, 1, 3),
    ]
    for args in cases:
        native_rows = native.make_quotes(*args)
        fallback_rows = core_fallback.make_quotes(*args)
        for native_row, fallback_row in zip(native_rows, fallback_rows, strict=True):
            assert native_row[0] == fallback_row[0]
            assert native_row[1:] == pytest.approx(fallback_row[1:], abs=1e-10)
    assert native.trade_pnl("BUY", 100, 101, .125, .01, .02) == pytest.approx(
        core_fallback.trade_pnl("BUY", 100, 101, .125, .01, .02), abs=1e-12,
    )
    rows = [("BUY", .100000005), ("SELL", .05)]
    assert native.reconcile(rows, [("SELL", .050000005)]) == pytest.approx(
        core_fallback.reconcile(rows, [("SELL", .050000005)]), abs=1e-12,
    )
    for side in ("BUY", "SELL"):
        assert native.hedge_side(side) == core_fallback.hedge_side(side)
    for args in ((12, 5, 3), (25.5, -1, 4.25)):
        assert native.validate_profitability(*args) == pytest.approx(
            core_fallback.validate_profitability(*args), abs=1e-12,
        )
