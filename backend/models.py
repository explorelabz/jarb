from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Side = Literal["BUY", "SELL"]

GMO_TAKER_BPS: dict[str, float] = {"BTC": 5.0, "ETH": 5.0, "XRP": 5.0, "DAI": 5.0}
GMO_TAKER_BPS_DEFAULT = 9.0
GMO_MAKER_BPS: dict[str, float] = {"BTC": -1.0, "ETH": -1.0, "XRP": -1.0, "DAI": -1.0}
GMO_MAKER_BPS_DEFAULT = -3.0


def gmo_taker_bps(base_asset: str, overrides: dict[str, float] | None = None) -> float:
    asset = base_asset.upper()
    if overrides and asset in overrides:
        return float(overrides[asset])
    return GMO_TAKER_BPS.get(asset, GMO_TAKER_BPS_DEFAULT)


def gmo_maker_bps(base_asset: str, overrides: dict[str, float] | None = None) -> float:
    asset = base_asset.upper()
    if overrides and asset in overrides:
        return float(overrides[asset])
    return GMO_MAKER_BPS.get(asset, GMO_MAKER_BPS_DEFAULT)


def expected_gmo_fee_bps(config: "StrategyConfig") -> float:
    """Expected blended GMO fee for SOK maker fills plus FAK fallback fills."""
    passive = min(1.0, max(0.0, config.expectedPassiveFillRatio))
    return config.gmoMakerFeeBps * passive + config.gmoFeeBps * (1.0 - passive)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StrategyConfig(BaseModel):
    symbol: str = "BTC_JPY"
    spreadBps: float = Field(12, gt=0)
    bittradeMakerFeeBps: float = Field(0, ge=-100, le=100)
    gmoFeeBps: float = Field(5, ge=0)
    gmoMakerFeeBps: float = Field(-1, ge=-100, le=100)
    expectedPassiveFillRatio: float = Field(.8, ge=0, le=1)
    gmoPostOnlyTimeoutMs: int = Field(800, ge=100, le=10_000)
    maxHedgeSlippageBps: float = Field(3, ge=0, le=100)
    expectedSlippageBps: float = Field(3, ge=0)
    queueBudgetJpy: float = Field(1_500_000, ge=0)
    maxQuoteSize: float = Field(0.05, gt=0)
    deltaLimit: float = Field(0.005, gt=0)
    maxHedgeLatencyMs: int = Field(2500, gt=0)
    staleMarketMs: int = Field(3000, gt=0)
    quoteRefreshMs: int = Field(1000, ge=100)


class MarketTop(BaseModel):
    symbol: str
    bid: float
    ask: float
    bidSize: float
    askSize: float
    bids: list[tuple[float, float]] = Field(default_factory=list)
    asks: list[tuple[float, float]] = Field(default_factory=list)
    # The public API remains numeric/JSON-compatible, while execution paths can
    # retain the exchange's original decimal strings without an f64 round trip.
    bidExact: Decimal | None = Field(default=None, exclude=True)
    askExact: Decimal | None = Field(default=None, exclude=True)
    bidsExact: list[tuple[Decimal, Decimal]] = Field(default_factory=list, exclude=True)
    asksExact: list[tuple[Decimal, Decimal]] = Field(default_factory=list, exclude=True)
    timestamp: str
    source: Literal["GMO", "SIM"]

    def decimal_bid(self) -> Decimal:
        return self.bidExact if self.bidExact is not None else Decimal(str(self.bid))

    def decimal_ask(self) -> Decimal:
        return self.askExact if self.askExact is not None else Decimal(str(self.ask))

    def decimal_bids(self) -> list[tuple[Decimal, Decimal]]:
        return self.bidsExact or [(Decimal(str(price)), Decimal(str(qty))) for price, qty in self.bids]

    def decimal_asks(self) -> list[tuple[Decimal, Decimal]]:
        return self.asksExact or [(Decimal(str(price)), Decimal(str(qty))) for price, qty in self.asks]


class QuoteLevel(BaseModel):
    side: Side
    price: float
    size: float
    sourcePrice: float


class ClientFill(BaseModel):
    id: str
    orderId: str
    symbol: str
    side: Side
    price: float
    size: float
    fee: float
    role: Literal["maker", "taker"]
    timestamp: str


class HedgeFill(BaseModel):
    id: str
    orderId: str
    clientFillId: str
    symbol: str
    side: Side
    price: float
    size: float
    fee: float
    latencyMs: int
    timestamp: str
    status: Literal["filled", "partial", "failed"]


class MatchedTrade(BaseModel):
    id: str
    timestamp: str
    symbol: str
    clientSide: Side
    size: float
    clientPrice: float
    hedgePrice: float
    spreadPnl: float
    clientFee: float
    hedgeCost: float
    netPnl: float
    latencyMs: int
    status: str


class Reconciliation(BaseModel):
    symbol: str
    clientNet: float
    hedgeNet: float
    delta: float
    status: Literal["matched", "exception"]
    checkedAt: str


class AuditEvent(BaseModel):
    id: str
    timestamp: str
    level: Literal["info", "warning", "critical"]
    type: str
    message: str
    metadata: dict[str, object] | None = None


class Pnl(BaseModel):
    spread: float = 0
    clientFees: float = 0
    hedgeCosts: float = 0
    net: float = 0


class Metrics(BaseModel):
    hedgeP95Ms: int = 0
    fillCount: int = 0
    exceptionCount: int = 0
    uptimeSec: int = 0
    coreCalcP99Us: float = 0
    quoteSelectionNoneCount: int = 0
    noQuoteDurationSec: int = 0
    noFillDurationSec: int = 0
    lastQuoteAt: str | None = None
    lastFillAt: str | None = None


class ConnectionState(BaseModel):
    status: Literal["paper", "connecting", "connected", "error"] = "paper"
    gmoConfigured: bool = False
    gmoKeyHint: str | None = None
    bittradeConfigured: bool = False
    bittradeKeyHint: str | None = None
    lastError: str | None = None


class AssetHolding(BaseModel):
    configured: float = 0
    opening: float | None = None
    available: float | None = None
    reserved: float = 0
    change: float | None = None


class HoldingsState(BaseModel):
    source: Literal["paper", "exchange", "configured"] = "configured"
    updatedAt: str | None = None
    bittrade: dict[str, AssetHolding] = Field(default_factory=dict)
    gmo: dict[str, AssetHolding] = Field(default_factory=dict)


class InstrumentRules(BaseModel):
    symbol: str
    baseAsset: str
    quoteAsset: str = "JPY"
    minOrderSize: float = Field(gt=0)
    maxOrderSize: float = Field(gt=0)
    sizeStep: float = Field(gt=0)
    priceTick: float = Field(gt=0)


class SymbolRuntime(BaseModel):
    instrument: InstrumentRules
    config: StrategyConfig
    market: MarketTop
    quotes: list[QuoteLevel]
    position: float = 0
    reconciliation: Reconciliation
    pnl: Pnl = Field(default_factory=Pnl)
    trades: list[MatchedTrade] = Field(default_factory=list)
    fillCount: int = 0
    hedgeP95Ms: int = 0


class SystemState(BaseModel):
    mode: Literal["paper", "live"]
    running: bool
    killSwitch: bool
    market: MarketTop
    quotes: list[QuoteLevel]
    position: float
    reconciliation: Reconciliation
    pnl: Pnl
    metrics: Metrics
    trades: list[MatchedTrade]
    events: list[AuditEvent]
    config: StrategyConfig
    connection: ConnectionState = Field(default_factory=ConnectionState)
    instrument: InstrumentRules
    activeSymbols: list[str] = Field(default_factory=list)
    symbolStates: dict[str, SymbolRuntime] = Field(default_factory=dict)
    disabledSymbols: dict[str, list[str]] = Field(default_factory=dict)
    holdings: HoldingsState = Field(default_factory=HoldingsState)


class PaperFillRequest(BaseModel):
    symbol: str | None = None
    side: Side
    size: float = Field(gt=0)
    role: Literal["maker", "taker"] = "maker"


class ControlRequest(BaseModel):
    action: Literal["pause", "resume", "kill", "reset-kill"]


class ArmRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phrase: str = Field(min_length=1, max_length=128)


class RiskLimitsUpdate(BaseModel):
    maxSingleOrderJpy: float | None = Field(default=None, gt=0)
    maxDailyVolumeJpy: float | None = Field(default=None, gt=0)
    maxDailyLossJpy: float | None = Field(default=None, gt=0)
    maxAbsDelta: float | None = Field(default=None, gt=0)
    maxHedgeFailures: int | None = Field(default=None, gt=0)
    maxHedgeP95Ms: int | None = Field(default=None, gt=0)
    armTtlSec: int | None = Field(default=None, gt=0)


class InventoryUpdate(BaseModel):
    bittrade: dict[str, float] = Field(default_factory=dict)
    gmo: dict[str, float] = Field(default_factory=dict)
    webhookUrl: str | None = Field(default=None, max_length=1024)
    clearWebhook: bool = False


class ConnectionUpdate(BaseModel):
    mode: Literal["paper", "live"]
    confirmOnline: bool = False
    clearGmoCredentials: bool = False
    clearBittradeCredentials: bool = False
    gmoApiKey: str | None = Field(default=None, max_length=512)
    gmoSecretKey: str | None = Field(default=None, max_length=512)
    bittradeAccessKey: str | None = Field(default=None, max_length=512)
    bittradeSecretKey: str | None = Field(default=None, max_length=512)
    bittradeAccountId: str | None = Field(default=None, max_length=128)


class PaperScenarioUpdate(BaseModel):
    autoMatch: bool | None = None
    partialFills: bool | None = None
    dustFills: bool | None = None
    duplicateEvents: bool | None = None
    outOfOrderEvents: bool | None = None
    cancelAlreadyFilled: bool | None = None
    cancelRaceFill: bool | None = None
    gmoPartialFak: bool | None = None
    gmoPostOnlyFillRatio: float | None = Field(default=None, ge=0, le=1)
    gmoPostOnlyFillDelayMs: int | None = Field(default=None, ge=0, le=10_000)
    delayedExecutions: bool | None = None
    postOnlyReject: bool | None = None
    randomRateLimit: bool | None = None
    randomNetworkTimeout: bool | None = None
    autoMatchProbability: float | None = Field(default=None, ge=0, le=1)
    dustProbability: float | None = Field(default=None, ge=0, le=1)
    duplicateProbability: float | None = Field(default=None, ge=0, le=1)
    outOfOrderProbability: float | None = Field(default=None, ge=0, le=1)
    cancelRaceProbability: float | None = Field(default=None, ge=0, le=1)
    gmoFillRatio: float | None = Field(default=None, gt=0, le=1)
    executionDelayMinMs: int | None = Field(default=None, ge=0, le=10_000)
    executionDelayMaxMs: int | None = Field(default=None, ge=0, le=10_000)
    rateLimitProbability: float | None = Field(default=None, ge=0, le=1)
    networkTimeoutProbability: float | None = Field(default=None, ge=0, le=1)
    seed: int | None = None


# Kept as an import alias for older API clients and tests.
SimulatedFillRequest = PaperFillRequest
