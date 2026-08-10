from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

Side = Literal["BUY", "SELL"]

GMO_TAKER_BPS: dict[str, float] = {"BTC": 5.0, "ETH": 5.0, "XRP": 5.0, "DAI": 5.0}
GMO_TAKER_BPS_DEFAULT = 9.0


def gmo_taker_bps(base_asset: str) -> float:
    return GMO_TAKER_BPS.get(base_asset.upper(), GMO_TAKER_BPS_DEFAULT)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StrategyConfig(BaseModel):
    symbol: str = "BTC_JPY"
    spreadBps: float = Field(25, gt=0)
    bittradeMakerFeeBps: float = Field(0, ge=-100, le=100)
    gmoFeeBps: float = Field(5, ge=0)
    expectedSlippageBps: float = Field(1.5, ge=0)
    maxQuoteSize: float = Field(0.05, gt=0)
    deltaLimit: float = Field(0.005, gt=0)
    maxHedgeLatencyMs: int = Field(1000, gt=0)
    staleMarketMs: int = Field(800, gt=0)
    quoteRefreshMs: int = Field(1000, ge=100)


class MarketTop(BaseModel):
    symbol: str
    bid: float
    ask: float
    bidSize: float
    askSize: float
    timestamp: str
    source: Literal["GMO", "SIM"]


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


class ConnectionState(BaseModel):
    status: Literal["simulation", "connecting", "connected", "error"] = "simulation"
    gmoConfigured: bool = False
    gmoKeyHint: str | None = None
    bittradeConfigured: bool = False
    bittradeKeyHint: str | None = None
    lastError: str | None = None


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
    mode: Literal["simulation", "online"]
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


class SimulatedFillRequest(BaseModel):
    symbol: str | None = None
    side: Side
    size: float = Field(gt=0)
    role: Literal["maker", "taker"] = "maker"


class ControlRequest(BaseModel):
    action: Literal["pause", "resume", "kill", "reset-kill"]


class ArmRequest(BaseModel):
    phrase: str = Field(min_length=1, max_length=128)
    actor: str = Field(default="operator", min_length=1, max_length=128)


class InventoryUpdate(BaseModel):
    bittrade: dict[str, float] = Field(default_factory=dict)
    gmo: dict[str, float] = Field(default_factory=dict)
    webhookUrl: str | None = Field(default=None, max_length=1024)
    clearWebhook: bool = False


class ConnectionUpdate(BaseModel):
    mode: Literal["simulation", "online"]
    confirmOnline: bool = False
    clearGmoCredentials: bool = False
    clearBittradeCredentials: bool = False
    gmoApiKey: str | None = Field(default=None, max_length=512)
    gmoSecretKey: str | None = Field(default=None, max_length=512)
    bittradeAccessKey: str | None = Field(default=None, max_length=512)
    bittradeSecretKey: str | None = Field(default=None, max_length=512)
    bittradeAccountId: str | None = Field(default=None, max_length=128)
