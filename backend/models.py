from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

Side = Literal["BUY", "SELL"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StrategyConfig(BaseModel):
    symbol: str = "BTC_JPY"
    spreadBps: float = Field(10, gt=0)
    gmoFeeBps: float = Field(2, ge=0)
    expectedSlippageBps: float = Field(1.5, ge=0)
    maxQuoteSize: float = Field(0.05, gt=0)
    deltaLimit: float = Field(0.005, gt=0)
    maxHedgeLatencyMs: int = Field(1000, gt=0)
    staleMarketMs: int = Field(5000, gt=0)
    quoteRefreshMs: int = Field(1000, ge=100)


class MarketTop(BaseModel):
    symbol: str
    bid: int
    ask: int
    bidSize: float
    askSize: float
    timestamp: str
    source: Literal["GMO", "SIM"]


class QuoteLevel(BaseModel):
    side: Side
    price: int
    size: float
    sourcePrice: int


class ClientFill(BaseModel):
    id: str
    orderId: str
    symbol: str
    side: Side
    price: int
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
    price: int
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
    clientPrice: int
    hedgePrice: int
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


class SystemState(BaseModel):
    mode: Literal["simulation", "live"]
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


class SimulatedFillRequest(BaseModel):
    side: Side
    size: float = Field(gt=0)
    role: Literal["maker", "taker"] = "maker"


class ControlRequest(BaseModel):
    action: Literal["pause", "resume", "kill", "reset-kill"]
