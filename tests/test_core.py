from __future__ import annotations

import pytest

from backend.core import make_quotes, matched_trade, opposite_side, reconcile, validate_config
from backend.models import ClientFill, HedgeFill, MarketTop, StrategyConfig


def config(**updates) -> StrategyConfig:
    return StrategyConfig(**updates)


def test_quotes_are_outside_gmo_and_depth_capped():
    market = MarketTop(symbol="BTC_JPY", bid=14_990_000, ask=15_010_000, bidSize=.02, askSize=.4, timestamp="", source="GMO")
    quotes = make_quotes(market, config())
    assert quotes[0].price == 14_975_010
    assert quotes[0].size == .02
    assert quotes[1].price == 15_025_010
    assert quotes[1].size == .05


def test_quotes_follow_decimal_price_tick():
    market = MarketTop(symbol="DOGE_JPY", bid=24.123, ask=24.127, bidSize=100, askSize=100, timestamp="", source="GMO")
    quotes = make_quotes(market, config(maxQuoteSize=10), price_tick=.001)
    assert quotes[0].price == 24.098
    assert quotes[1].price == 24.152


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


def test_bittrade_maker_fee_is_included_in_profitability_floor():
    with pytest.raises(ValueError, match="价差必须高于"):
        validate_config(config(spreadBps=4, bittradeMakerFeeBps=1))
    assert validate_config(config(spreadBps=4, bittradeMakerFeeBps=-1)) > 0
