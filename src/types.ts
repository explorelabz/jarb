export type Side = 'BUY' | 'SELL'

export interface SystemState {
  mode: 'simulation' | 'live'
  running: boolean
  killSwitch: boolean
  market: { symbol: string; bid: number; ask: number; bidSize: number; askSize: number; timestamp: string; source: string }
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
    symbol: string; spreadBps: number; gmoFeeBps: number; expectedSlippageBps: number;
    maxQuoteSize: number; deltaLimit: number; maxHedgeLatencyMs: number; staleMarketMs: number
  }
}
