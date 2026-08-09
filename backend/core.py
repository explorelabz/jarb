from __future__ import annotations

from typing import TYPE_CHECKING

try:
    import hedge_core as native
except ImportError as exc:  # pragma: no cover - startup guard
    raise RuntimeError("Rust 核心未安装，请运行 npm run rust:build") from exc

from .models import ClientFill, HedgeFill, MarketTop, MatchedTrade, QuoteLevel, Reconciliation, StrategyConfig, utc_now

if TYPE_CHECKING:
    from typing import Literal


def validate_config(config: StrategyConfig) -> float:
    return native.validate_profitability(config.spreadBps, config.gmoFeeBps, config.expectedSlippageBps)


def make_quotes(market: MarketTop, config: StrategyConfig) -> list[QuoteLevel]:
    rows = native.make_quotes(market.bid, market.ask, market.bidSize, market.askSize, config.spreadBps, config.maxQuoteSize)
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
