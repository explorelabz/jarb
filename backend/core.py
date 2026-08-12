from __future__ import annotations

from typing import TYPE_CHECKING

try:
    import hedge_core as native
    NATIVE_CORE_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised in an isolated import test
    from . import core_fallback as native
    NATIVE_CORE_AVAILABLE = False

from .models import (
    ClientFill, HedgeFill, MarketTop, MatchedTrade, QuoteLevel, Reconciliation,
    StrategyConfig, expected_gmo_fee_bps, utc_now,
)

if TYPE_CHECKING:
    from typing import Literal


def core_runtime() -> str:
    return "Rust/PyO3" if NATIVE_CORE_AVAILABLE else "Python/Decimal fallback"


def validate_config(config: StrategyConfig) -> float:
    minimum_latency_limit = config.gmoPostOnlyTimeoutMs + 1200
    if config.maxHedgeLatencyMs < minimum_latency_limit:
        raise ValueError(
            f"maxHedgeLatencyMs 必须至少为 {minimum_latency_limit}ms "
            "（SOK 等待时间 + 撤单/FAK 确认余量）"
        )
    return native.validate_profitability(
        config.spreadBps,
        expected_gmo_fee_bps(config) + config.bittradeMakerFeeBps,
        max(config.expectedSlippageBps, config.maxHedgeSlippageBps),
    )


def make_quotes(market: MarketTop, config: StrategyConfig, price_tick: float = 1) -> list[QuoteLevel]:
    bids = market.bids or [(market.bid, market.bidSize)]
    asks = market.asks or [(market.ask, market.askSize)]
    rows = native.make_quotes(
        market.bid, market.ask, bids, asks, config.spreadBps,
        config.maxQuoteSize, price_tick, config.maxHedgeSlippageBps,
    )
    return [QuoteLevel(side=side, price=price, size=size, sourcePrice=source) for side, price, size, source in rows]


def opposite_side(side: str) -> str:
    return native.hedge_side(side)


def matched_trade(client: ClientFill, hedge: HedgeFill) -> MatchedTrade:
    spread, net = native.trade_pnl(client.side, client.price, hedge.price, hedge.size, client.fee, hedge.fee)
    return MatchedTrade(
        id=client.id, timestamp=client.timestamp, symbol=client.symbol, clientSide=client.side,
        size=hedge.size, clientPrice=client.price, hedgePrice=hedge.price, spreadPnl=spread,
        clientFee=client.fee, hedgeCost=hedge.fee, netPnl=net, latencyMs=hedge.latencyMs,
        status=hedge.status,
    )


def reconcile(symbol: str, clients: list[ClientFill], hedges: list[HedgeFill]) -> Reconciliation:
    client_net, hedge_net, delta = native.reconcile(
        [(fill.side, fill.size) for fill in clients], [(fill.side, fill.size) for fill in hedges]
    )
    return Reconciliation(
        symbol=symbol, clientNet=client_net, hedgeNet=hedge_net, delta=delta,
        status="matched" if delta == 0 else "exception", checkedAt=utc_now(),
    )
