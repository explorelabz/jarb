from __future__ import annotations

import os
from dataclasses import dataclass

from .engine.risk import ARM_TTL_BY_MODE, RiskLimits
from .models import StrategyConfig, gmo_maker_bps, gmo_taker_bps


def number(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def boolean(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def fee_overrides(value: str) -> dict[str, float]:
    result: dict[str, float] = {}
    for item in value.split(","):
        if not item.strip():
            continue
        asset, separator, raw = item.partition("=")
        if not separator:
            continue
        try:
            result[asset.strip().upper()] = float(raw)
        except ValueError:
            continue
    return result


configured_symbol = os.getenv("SYMBOL", "BTC_JPY").upper()
gmo_fee_overrides = fee_overrides(os.getenv("GMO_TAKER_FEE_BPS_OVERRIDES", ""))
gmo_maker_fee_overrides = fee_overrides(os.getenv("GMO_MAKER_FEE_BPS_OVERRIDES", ""))

strategy_config = StrategyConfig(
    symbol=configured_symbol,
    spreadBps=number("SPREAD_BPS", 12),
    bittradeMakerFeeBps=number("BITTRADE_MAKER_FEE_BPS", 0),
    gmoFeeBps=gmo_taker_bps(configured_symbol.removesuffix("_JPY"), gmo_fee_overrides),
    gmoMakerFeeBps=gmo_maker_bps(configured_symbol.removesuffix("_JPY"), gmo_maker_fee_overrides),
    expectedPassiveFillRatio=number("EXPECTED_PASSIVE_FILL_RATIO", .8),
    gmoPostOnlyTimeoutMs=int(number("GMO_POST_ONLY_TIMEOUT_MS", 800)),
    maxHedgeSlippageBps=number("MAX_HEDGE_SLIPPAGE_BPS", 3),
    expectedSlippageBps=number("EXPECTED_SLIPPAGE_BPS", 3),
    queueBudgetJpy=number("BITTRADE_QUEUE_BUDGET_JPY", 1_500_000),
    maxQuoteSize=number("MAX_QUOTE_SIZE", 0.05),
    deltaLimit=number("DELTA_LIMIT", 0.005),
    maxHedgeLatencyMs=int(number("MAX_HEDGE_LATENCY_MS", 2500)),
    staleMarketMs=int(number("STALE_MARKET_MS", 3000)),
    quoteRefreshMs=int(number("QUOTE_REFRESH_MS", 1000)),
)

requested_mode = "live" if os.getenv("TRADING_MODE", "paper").strip().lower() in {"online", "live"} else "paper"
require_dual_arm_approval = boolean("REQUIRE_DUAL_ARM_APPROVAL", False)
zero_activity_alert_minutes = number("ZERO_ACTIVITY_ALERT_MINUTES", 10)
risk_limits = RiskLimits(
    max_single_order_jpy=number("MAX_SINGLE_ORDER_JPY", 250_000),
    max_daily_volume_jpy=number("MAX_DAILY_VOLUME_JPY", 5_000_000),
    max_daily_loss_jpy=number("MAX_DAILY_LOSS_JPY", 100_000),
    max_abs_delta=number("MAX_ABS_DELTA", strategy_config.deltaLimit),
    max_hedge_failures=int(number("MAX_HEDGE_FAILURES", 3)),
    max_hedge_p95_ms=int(number("MAX_HEDGE_P95_MS", strategy_config.maxHedgeLatencyMs)),
    arm_ttl_sec=ARM_TTL_BY_MODE[requested_mode],
)


@dataclass(frozen=True)
class Credentials:
    bittrade_access_key: str = os.getenv("BITTRADE_ACCESS_KEY", "")
    bittrade_secret_key: str = os.getenv("BITTRADE_SECRET_KEY", "")
    bittrade_account_id: str = os.getenv("BITTRADE_ACCOUNT_ID", "")
    gmo_api_key: str = os.getenv("GMO_API_KEY", "")
    gmo_secret_key: str = os.getenv("GMO_SECRET_KEY", "")


credentials = Credentials()
