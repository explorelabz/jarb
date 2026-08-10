from __future__ import annotations

import os
from dataclasses import dataclass

from .models import StrategyConfig


def number(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


strategy_config = StrategyConfig(
    symbol=os.getenv("SYMBOL", "BTC_JPY"),
    spreadBps=number("SPREAD_BPS", 10),
    bittradeMakerFeeBps=number("BITTRADE_MAKER_FEE_BPS", 0),
    gmoFeeBps=number("GMO_FEE_BPS", 2),
    expectedSlippageBps=number("EXPECTED_SLIPPAGE_BPS", 1.5),
    maxQuoteSize=number("MAX_QUOTE_SIZE", 0.05),
    deltaLimit=number("DELTA_LIMIT", 0.005),
    maxHedgeLatencyMs=int(number("MAX_HEDGE_LATENCY_MS", 1000)),
    staleMarketMs=int(number("STALE_MARKET_MS", 5000)),
    quoteRefreshMs=int(number("QUOTE_REFRESH_MS", 1000)),
)

requested_mode = "online" if os.getenv("TRADING_MODE") in {"online", "live"} else "simulation"


@dataclass(frozen=True)
class Credentials:
    bittrade_access_key: str = os.getenv("BITTRADE_ACCESS_KEY", "")
    bittrade_secret_key: str = os.getenv("BITTRADE_SECRET_KEY", "")
    bittrade_account_id: str = os.getenv("BITTRADE_ACCOUNT_ID", "")
    gmo_api_key: str = os.getenv("GMO_API_KEY", "")
    gmo_secret_key: str = os.getenv("GMO_SECRET_KEY", "")


credentials = Credentials()
