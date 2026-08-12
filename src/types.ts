export type Side = 'BUY' | 'SELL'

export interface RiskStatus {
  armed: boolean
  armedUntil: number
  recoveryComplete: boolean
  killed: boolean
  reason: string | null
  pendingArmActor: string | null
  pendingArmUntil: number
  requiresDualApproval: boolean
  limits: RiskLimits
}

export interface RiskLimits {
  maxSingleOrderJpy: number
  maxDailyVolumeJpy: number
  maxDailyLossJpy: number
  maxAbsDelta: number
  maxHedgeFailures: number
  maxHedgeP95Ms: number
  armTtlSec: number
}

export interface InventoryState {
  bittrade: Record<string, number>
  gmo: Record<string, number>
  webhookConfigured: boolean
  webhookHint: string | null
  disabledSymbols: Record<string, string[]>
}

export interface PaperScenarios {
  gmoPartialFak: boolean
  gmoPostOnlyFillDelayMs: number
  delayedExecutions: boolean
  postOnlyReject: boolean
  randomRateLimit: boolean
  randomNetworkTimeout: boolean
  gmoFillRatio: number
  executionDelayMinMs: number
  executionDelayMaxMs: number
  rateLimitProbability: number
  networkTimeoutProbability: number
  seed: number
  matching?: {
    openOrders: number
    throughFills: number
    atLevelFills: number
    throughQty: string
    atLevelQty: string
    throughRatio: number
    publicTradesSeen: number
    lastTradeTsMs: number
    gmoPassive: {
      publicTradesSeen: number
      fillEvents: number
      fillQty: string
      fillsWithoutPublicTrade: number
    }
    publicDepth: Record<string, {
      ready: boolean
      ageMs: number
      bestBid: string
      bestAsk: string
      levels: number
    }>
  }
}

export interface AssetHolding {
  configured: number
  opening: number | null
  available: number | null
  reserved: number
  change: number | null
}

export interface HoldingsState {
  source: 'paper' | 'exchange' | 'configured'
  updatedAt: string | null
  bittrade: Record<string, AssetHolding>
  gmo: Record<string, AssetHolding>
}

export interface InstrumentRules {
  symbol: string
  baseAsset: string
  quoteAsset: string
  minOrderSize: number
  maxOrderSize: number
  sizeStep: number
  priceTick: number
}

export interface SymbolRuntime {
  instrument: InstrumentRules
  config: SystemState['config']
  market: SystemState['market']
  quotes: SystemState['quotes']
  position: number
  reconciliation: SystemState['reconciliation']
  pnl: SystemState['pnl']
  trades: SystemState['trades']
  fillCount: number
  hedgeP95Ms: number
}

export interface SystemState {
  mode: 'paper' | 'live'
  running: boolean
  killSwitch: boolean
  market: { symbol: string; bid: number; ask: number; bidSize: number; askSize: number; bids: [number, number][]; asks: [number, number][]; timestamp: string; source: string }
  quotes: Array<{ side: Side; price: number; size: number; sourcePrice: number }>
  position: number
  reconciliation: { symbol: string; clientNet: number; hedgeNet: number; delta: number; status: 'matched' | 'exception'; checkedAt: string }
  pnl: { spread: number; clientFees: number; hedgeCosts: number; net: number }
  metrics: { hedgeP95Ms: number; fillCount: number; exceptionCount: number; uptimeSec: number; coreCalcP99Us: number }
  trades: Array<{
    id: string; timestamp: string; symbol: string; clientSide: Side; size: number; clientPrice: number;
    hedgePrice: number; spreadPnl: number; clientFee: number; hedgeCost: number; netPnl: number;
    latencyMs: number; status: string
  }>
  events: Array<{ id: string; timestamp: string; level: 'info' | 'warning' | 'critical'; type: string; message: string }>
  config: {
    symbol: string; spreadBps: number; bittradeMakerFeeBps: number; gmoFeeBps: number; gmoMakerFeeBps: number;
    expectedPassiveFillRatio: number; gmoPostOnlyTimeoutMs: number; maxHedgeSlippageBps: number;
    expectedSlippageBps: number; queueBudget: number; maxQuoteSize: number; deltaLimit: number;
    maxHedgeLatencyMs: number; staleMarketMs: number
  }
  connection: {
    status: 'paper' | 'connecting' | 'connected' | 'error'
    gmoConfigured: boolean; gmoKeyHint: string | null
    bittradeConfigured: boolean; bittradeKeyHint: string | null
    lastError: string | null
  }
  instrument: InstrumentRules
  activeSymbols: string[]
  symbolStates: Record<string, SymbolRuntime>
  disabledSymbols: Record<string, string[]>
  holdings: HoldingsState
}
