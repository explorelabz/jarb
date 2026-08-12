import { useEffect, useMemo, useState } from 'react'
import {
  Pulse, ArrowClockwise, ArrowsLeftRight, CheckCircle, DownloadSimple, Gauge,
  Lightning, Pause, Play, ShieldCheck, Siren, SlidersHorizontal, Warning,
} from '@phosphor-icons/react'
import type { InstrumentRules, InventoryState, PaperScenarios, RiskLimits, RiskStatus, SystemState } from './types'

const jpy = new Intl.NumberFormat('ja-JP', { maximumFractionDigits: 0 })
const decimal = new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 8 })
const time = (value: string) => new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(new Date(value))
const signed = (value: number, suffix = '') => `${value > 0 ? '+' : ''}${decimal.format(value)}${suffix}`
const stepPrecision = (step: number) => {
  const text = step.toString().toLowerCase()
  if (text.includes('e-')) return Number(text.split('e-')[1])
  return text.includes('.') ? text.split('.')[1].length : 0
}
const price = (value: number, tick: number) => new Intl.NumberFormat('ja-JP', {
  minimumFractionDigits: stepPrecision(tick), maximumFractionDigits: stepPrecision(tick),
}).format(value)
const blendedGmoFee = (config: SystemState['config']) => config.gmoMakerFeeBps * config.expectedPassiveFillRatio
  + config.gmoFeeBps * (1 - config.expectedPassiveFillRatio)

let operatorToken = ''
const pendingApprovals: Record<string, string> = {}
const setOperatorToken = (token: string) => { operatorToken = token.trim() }
const promptOperatorToken = (message = '输入操作员访问 Token') => {
  const token = window.prompt(message)?.trim() ?? ''
  if (token) setOperatorToken(token)
  return token
}

async function authorizedFetch(path: string, init?: RequestInit, retry = true): Promise<Response> {
  let token = operatorToken
  if (!token) token = promptOperatorToken()
  if (!token) throw new Error('需要操作员 Token')
  const response = await fetch(path, {
    ...init,
    headers: { Authorization: `Bearer ${token}`, ...init?.headers },
  })
  if (response.status === 401 && retry) {
    operatorToken = ''
    const replacement = promptOperatorToken('Token 无效或已轮换，请重新输入操作员访问 Token')
    if (!replacement) return response
    return authorizedFetch(path, init, false)
  }
  return response
}

async function api(path: string, init?: RequestInit) {
  const approvalId = pendingApprovals[path]
  const response = await authorizedFetch(path, {
    ...init, headers: {
      'Content-Type': 'application/json',
      ...(approvalId ? { 'X-JARB-Approval': approvalId } : {}),
      ...init?.headers,
    },
  })
  const data = await response.json()
  if (data.approvalRequired && data.approvalId) {
    pendingApprovals[path] = data.approvalId
    throw new Error(`${data.message}。审批 ID：${data.approvalId}`)
  }
  if (response.ok && approvalId) delete pendingApprovals[path]
  if (!response.ok) throw new Error(data.error ?? data.detail ?? '请求失败')
  return data
}

async function downloadApi(path: string) {
  const response = await authorizedFetch(path)
  if (!response.ok) {
    const data = await response.json().catch(() => ({}))
    throw new Error(data.error ?? data.detail ?? '导出失败')
  }
  const blobUrl = URL.createObjectURL(await response.blob())
  const disposition = response.headers.get('Content-Disposition') ?? ''
  const filename = disposition.match(/filename="?([^";]+)"?/i)?.[1] ?? 'jarb-export'
  const link = document.createElement('a')
  link.href = blobUrl
  link.download = filename
  link.click()
  URL.revokeObjectURL(blobUrl)
}

function Skeleton() {
  return <main className="shell loading-shell" aria-label="正在连接交易服务">
    <div className="skeleton skeleton-head" />
    <div className="skeleton-grid"><div className="skeleton tall" /><div className="skeleton tall" /></div>
    <div className="skeleton skeleton-table" />
  </main>
}

function Metric({ label, value, note, tone = '' }: { label: string; value: string; note: string; tone?: string }) {
  return <div className={`metric ${tone}`}>
    <span>{label}</span><strong>{value}</strong><small>{note}</small>
  </div>
}

function MarketLadder({ state }: { state: SystemState }) {
  const bid = state.quotes.find(q => q.side === 'BUY')
  const ask = state.quotes.find(q => q.side === 'SELL')
  const tick = state.instrument.priceTick
  const indicativeBid = Math.floor(state.market.bid * (1 - state.config.spreadBps / 10_000) / tick) * tick
  const indicativeAsk = Math.ceil(state.market.ask * (1 + state.config.spreadBps / 10_000) / tick) * tick
  const mid = (state.market.ask + state.market.bid) / 2
  return <section className="market-panel panel" aria-labelledby="market-title">
    <div className="section-head">
      <div><span className="eyebrow">LIVE PRICE LADDER</span><h2 id="market-title">价格阶梯</h2></div>
      <div className={`feed-status ${state.connection.status === 'error' ? 'feed-error' : ''}`}><i /><span>{state.market.source === 'GMO' ? 'GMO 线上行情' : 'Paper 行情流'}</span><b>{time(state.market.timestamp)}</b></div>
    </div>
    <div className="ladder">
      <div className="ladder-row client ask">
        <div><span>BitTrade Ask</span><small>{ask ? `客户买入价 · 可挂 ${decimal.format(ask.size)} ${state.instrument.baseAsset}` : '参考价 · 当前未挂单'}</small></div>
        <strong>¥{price(ask?.price ?? indicativeAsk, tick)}</strong>
      </div>
      <div className="spread-band"><span>卖出加价</span><b>+{state.config.spreadBps.toFixed(1)} bps</b></div>
      <div className="ladder-row external">
        <div><span>GMO Ask</span><small>对冲买入基准</small></div><strong>¥{price(state.market.ask, state.instrument.priceTick)}</strong>
      </div>
      <div className="midline"><span>外部价差</span><b>¥{price(state.market.ask - state.market.bid, state.instrument.priceTick)}</b><em>中间价 ¥{price(mid, state.instrument.priceTick)}</em></div>
      <div className="ladder-row external">
        <div><span>GMO Bid</span><small>对冲卖出基准</small></div><strong>¥{price(state.market.bid, state.instrument.priceTick)}</strong>
      </div>
      <div className="spread-band"><span>买入减价</span><b>−{state.config.spreadBps.toFixed(1)} bps</b></div>
      <div className="ladder-row client bid">
        <div><span>BitTrade Bid</span><small>{bid ? `客户卖出价 · 可挂 ${decimal.format(bid.size)} ${state.instrument.baseAsset}` : '参考价 · 当前未挂单'}</small></div>
        <strong>¥{price(bid?.price ?? indicativeBid, tick)}</strong>
      </div>
    </div>
  </section>
}

function Reconciliation({ state }: { state: SystemState }) {
  const matched = state.reconciliation.status === 'matched'
  const limitRatio = Math.min(100, Math.abs(state.reconciliation.delta / state.config.deltaLimit) * 100)
  return <section className="recon-panel panel" aria-labelledby="recon-title">
    <div className="section-head">
      <div><span className="eyebrow">RECONCILIATION</span><h2 id="recon-title">仓位与对账</h2></div>
      <span className={`status-chip ${matched ? 'ok' : 'danger'}`}>{matched ? <CheckCircle size={16} weight="fill" /> : <Warning size={16} weight="fill" />}{matched ? '已归零' : '存在差额'}</span>
    </div>
    <div className="delta-hero">
      <span>实时 Delta</span><strong>{signed(state.reconciliation.delta)} <small>{state.instrument.baseAsset}</small></strong>
      <div className="limit"><i style={{ transform: `scaleX(${Math.max(0.006, limitRatio / 100)})` }} /><span>阈值 ±{state.config.deltaLimit} {state.instrument.baseAsset}</span></div>
    </div>
    <div className="recon-equation">
      <div><span>BitTrade 客户净额</span><b>{signed(state.reconciliation.clientNet)}</b></div>
      <i>+</i><div><span>GMO 对冲净额</span><b>{signed(state.reconciliation.hedgeNet)}</b></div>
      <i>=</i><div className="result"><span>未对冲余额</span><b>{signed(state.reconciliation.delta)}</b></div>
    </div>
    <div className="risk-lines">
      <div><span><Gauge size={18} /> 对冲延迟 P95</span><b>{state.metrics.hedgeP95Ms || '—'}{state.metrics.hedgeP95Ms ? ' ms' : ''}</b></div>
      <div><span><ShieldCheck size={18} /> SOK 混合净边际</span><b>{(state.config.spreadBps - state.config.bittradeMakerFeeBps - blendedGmoFee(state.config) - Math.max(state.config.expectedSlippageBps, state.config.maxHedgeSlippageBps)).toFixed(1)} bps</b></div>
      <div><span><Lightning size={18} /> Rust 核心 P99</span><b>{state.metrics.coreCalcP99Us ? `${state.metrics.coreCalcP99Us.toFixed(2)} µs` : '采样中'}</b></div>
      <div><span><Pulse size={18} /> 运行时长</span><b>{Math.floor(state.metrics.uptimeSec / 60)}m {state.metrics.uptimeSec % 60}s</b></div>
    </div>
  </section>
}

function Holdings({ state }: { state: SystemState }) {
  const base = state.instrument.baseAsset
  const assets = ['JPY', base]
  const venues = [
    { key: 'bittrade', name: 'BitTrade', role: '做市账户' },
    { key: 'gmo', name: 'GMO', role: '对冲账户' },
  ] as const
  const sourceLabel = state.holdings.source === 'paper'
    ? 'Paper 账本'
    : state.holdings.source === 'exchange' ? '交易所余额' : '等待余额同步'
  const formatAmount = (value: number, asset: string) => asset === 'JPY' ? `¥${jpy.format(value)}` : decimal.format(value)
  const formatChange = (value: number | null, asset: string) => value === null
    ? '—'
    : `${value > 0 ? '+' : value < 0 ? '−' : ''}${formatAmount(Math.abs(value), asset)}`
  const combinedChange = (asset: string) => {
    const maker = state.holdings.bittrade[asset]?.change
    const hedge = state.holdings.gmo[asset]?.change
    return maker == null || hedge == null ? null : maker + hedge
  }
  const baseChange = combinedChange(base)
  const jpyChange = combinedChange('JPY')

  return <section className="holdings-panel panel" aria-labelledby="holdings-title">
    <div className="section-head holdings-head">
      <div><span className="eyebrow">VENUE HOLDINGS / {base}_JPY</span><h2 id="holdings-title">双交易所持仓</h2></div>
      <div className="holdings-meta">
        <span className={`holdings-source ${state.holdings.source}`}><i />{sourceLabel}</span>
        <small>{state.holdings.updatedAt ? `更新于 ${time(state.holdings.updatedAt)}` : '尚未取得私有余额'}</small>
      </div>
    </div>
    <div className="holdings-summary" aria-label="合并持仓变化">
      <div><span>合并 {base} 变化</span><b className={baseChange === 0 ? 'neutral' : (baseChange ?? 0) > 0 ? 'positive' : 'negative'}>{formatChange(baseChange, base)} <small>{base}</small></b></div>
      <div><span>合并 JPY 变化</span><b className={(jpyChange ?? 0) >= 0 ? 'positive' : 'negative'}>{formatChange(jpyChange, 'JPY')}</b></div>
      <p>{baseChange === null ? 'Live 模式会在私有余额同步完成后显示实际持仓。' : Math.abs(baseChange) < 1e-10 ? '双边资产已闭环；日元变化包含成交价差与手续费。' : `仍有 ${decimal.format(Math.abs(baseChange))} ${base} 未完成资产闭环。`}</p>
    </div>
    <div className="holdings-grid">
      {venues.map(venue => <article className="venue-holdings" key={venue.key}>
        <header><div><strong>{venue.name}</strong><span>{venue.role}</span></div><ArrowsLeftRight size={19} /></header>
        <div className="holding-labels"><span>资产</span><span>当前可用</span><span>本轮变化</span></div>
        {assets.map(asset => {
          const holding = state.holdings[venue.key][asset]
          const change = holding?.change ?? null
          return <div className="holding-row" key={asset}>
            <div className="holding-asset"><b>{asset}</b><small>配置 {formatAmount(holding?.configured ?? 0, asset)}</small></div>
            <strong className="holding-current">{holding?.available == null ? '—' : formatAmount(holding.available, asset)}</strong>
            <div className={`holding-change ${change == null || change === 0 ? 'neutral' : change > 0 ? 'positive' : 'negative'}`}>
              <b>{formatChange(change, asset)}</b>
              <small>{holding?.opening == null ? '等待同步' : `起始 ${formatAmount(holding.opening, asset)}`}</small>
            </div>
          </div>
        })}
      </article>)}
    </div>
  </section>
}

function Trades({ state }: { state: SystemState }) {
  return <section className="table-section" aria-labelledby="trade-title">
    <div className="section-head wide">
      <div><span className="eyebrow">MATCHED EXECUTIONS</span><h2 id="trade-title">双边成交明细</h2></div>
      <div className="trade-head-actions">
        <span className="muted">最近 {state.trades.length} 笔 · BitTrade 成交 → GMO 反向对冲</span>
        <button className="export-link" onClick={() => void downloadApi('/api/orders/export?format=csv&mode=all')}><DownloadSimple size={15} />导出全部 CSV</button>
        <button className="export-link" onClick={() => void downloadApi('/api/orders/export?format=json&mode=all')}><DownloadSimple size={15} />JSON</button>
      </div>
    </div>
    {state.trades.length === 0 ? <div className="empty-state">
      <ArrowsLeftRight size={30} /><div><b>尚无双边成交</b><span>Paper 撮合器会通过真实引擎链路生成订单、成交与对冲。</span></div>
    </div> : <div className="table-wrap"><table>
      <thead><tr><th>时间 / 交易编号</th><th>客户方向</th><th className="num">数量 {state.instrument.baseAsset}</th><th className="num">BitTrade 成交</th><th className="num">GMO 对冲</th><th className="num">延迟</th><th className="num">净收益 JPY</th></tr></thead>
      <tbody>{state.trades.map(trade => <tr key={trade.id}>
        <td><b>{time(trade.timestamp)}</b><small>{trade.id}</small></td>
        <td><span className={`side ${trade.clientSide.toLowerCase()}`}>{trade.clientSide === 'SELL' ? '客户买入' : '客户卖出'}</span></td>
        <td className="num">{decimal.format(trade.size)}</td>
        <td className="num">¥{price(trade.clientPrice, state.instrument.priceTick)}</td>
        <td className="num">¥{price(trade.hedgePrice, state.instrument.priceTick)}</td>
        <td className="num">{trade.latencyMs} ms</td>
        <td className={`num pnl ${trade.netPnl >= 0 ? 'positive' : 'negative'}`}>{trade.netPnl >= 0 ? '+' : ''}¥{jpy.format(trade.netPnl)}</td>
      </tr>)}</tbody>
    </table></div>}
  </section>
}

function Settings({ state, risk, onClose, onSaved }: { state: SystemState; risk: RiskStatus | null; onClose: () => void; onSaved: () => void }) {
  const [form, setForm] = useState(state.config)
  const [mode, setMode] = useState<SystemState['mode']>(state.mode)
  const [confirmed, setConfirmed] = useState(false)
  const [secrets, setSecrets] = useState({ gmoApiKey: '', gmoSecretKey: '', bittradeAccessKey: '', bittradeSecretKey: '', bittradeAccountId: '' })
  const [clearGmo, setClearGmo] = useState(false)
  const [clearBittrade, setClearBittrade] = useState(false)
  const [symbols, setSymbols] = useState<InstrumentRules[]>([])
  const [selectedSymbols, setSelectedSymbols] = useState(state.activeSymbols)
  const [symbolsLoading, setSymbolsLoading] = useState(true)
  const [symbolsError, setSymbolsError] = useState('')
  const [inventory, setInventory] = useState<InventoryState>({ bittrade: {}, gmo: {}, webhookConfigured: false, webhookHint: null, disabledSymbols: {} })
  const [webhookUrl, setWebhookUrl] = useState('')
  const [clearWebhook, setClearWebhook] = useState(false)
  const defaultGmoFee = (asset: string) => ['BTC', 'ETH', 'XRP', 'DAI'].includes(asset) ? 5 : 9
  const defaultGmoMakerFee = (asset: string) => ['BTC', 'ETH', 'XRP', 'DAI'].includes(asset) ? -1 : -3
  const [gmoFees, setGmoFees] = useState<Record<string, number>>(Object.fromEntries(
    Object.values(state.symbolStates).map(runtime => [runtime.instrument.baseAsset, runtime.config.gmoFeeBps]),
  ))
  const [gmoMakerFees, setGmoMakerFees] = useState<Record<string, number>>(Object.fromEntries(
    Object.values(state.symbolStates).map(runtime => [runtime.instrument.baseAsset, runtime.config.gmoMakerFeeBps]),
  ))
  const [riskLimits, setRiskLimits] = useState<RiskLimits>(risk?.limits ?? {
    maxSingleOrderJpy: 250000, maxDailyVolumeJpy: 5000000, maxDailyLossJpy: 100000,
    maxAbsDelta: .005, maxHedgeFailures: 3, maxHedgeP95Ms: 2500, armTtlSec: 3600,
  })
  const [paperScenarios, setPaperScenarios] = useState<PaperScenarios | null>(null)
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const set = (key: keyof typeof form, value: string) => setForm(v => ({ ...v, [key]: key === 'symbol' ? value : Number(value) }))
  const setSecret = (key: keyof typeof secrets, value: string) => setSecrets(v => ({ ...v, [key]: value }))
  useEffect(() => {
    let active = true
    api('/api/symbols').then(data => { if (active) setSymbols(data.symbols) })
      .catch(e => { if (active) setSymbolsError(e instanceof Error ? e.message : '币种加载失败') })
      .finally(() => { if (active) setSymbolsLoading(false) })
    api('/api/inventory').then(data => { if (active) setInventory(data) })
      .catch(e => { if (active) setSymbolsError(e instanceof Error ? e.message : '底仓配置加载失败') })
    api('/api/risk/limits').then(data => { if (active) setRiskLimits(data) }).catch(() => undefined)
    if (state.mode === 'paper') api('/api/paper/scenarios').then(data => { if (active) setPaperScenarios(data) }).catch(() => undefined)
    return () => { active = false }
  }, [])
  const toggleSymbol = (symbol: string) => setSelectedSymbols(value => value.includes(symbol)
    ? value.filter(item => item !== symbol)
    : [...value, symbol])
  const setInventoryAmount = (venue: 'bittrade' | 'gmo', asset: string, value: string) => {
    const amount = Math.max(0, Number(value) || 0)
    setInventory(current => ({ ...current, [venue]: { ...current[venue], [asset]: amount } }))
  }
  const save = async () => {
    setSaving(true); setError('')
    try {
      await api('/api/strategy', { method: 'PATCH', body: JSON.stringify({
        ...form, symbols: selectedSymbols,
        gmoFeeBpsByAsset: Object.fromEntries(selectedSymbols.map(symbol => {
          const asset = symbol.replace('_JPY', '')
          return [asset, gmoFees[asset] ?? defaultGmoFee(asset)]
        })),
        gmoMakerFeeBpsByAsset: Object.fromEntries(selectedSymbols.map(symbol => {
          const asset = symbol.replace('_JPY', '')
          return [asset, gmoMakerFees[asset] ?? defaultGmoMakerFee(asset)]
        })),
      }) })
      await api('/api/risk/limits', { method: 'PATCH', body: JSON.stringify(riskLimits) })
      if (state.mode === 'paper' && paperScenarios) {
        await api('/api/paper/scenarios', { method: 'PATCH', body: JSON.stringify(paperScenarios) })
      }
      const assets = ['JPY', ...selectedSymbols.map(symbol => symbol.replace('_JPY', ''))]
      await api('/api/inventory', { method: 'PATCH', body: JSON.stringify({
        bittrade: Object.fromEntries(assets.map(asset => [asset, inventory.bittrade[asset] ?? 0])),
        gmo: Object.fromEntries(assets.map(asset => [asset, inventory.gmo[asset] ?? 0])),
        webhookUrl: webhookUrl.trim() || undefined, clearWebhook,
      }) })
      const credentials = Object.fromEntries(Object.entries(secrets).filter(([, value]) => value.trim()))
      await api('/api/connection', { method: 'PATCH', body: JSON.stringify({
        mode, confirmOnline: confirmed, clearGmoCredentials: clearGmo,
        clearBittradeCredentials: clearBittrade, ...credentials,
      }) })
      onSaved(); onClose()
    }
    catch (e) { setError(e instanceof Error ? e.message : '保存失败') }
    finally { setSaving(false) }
  }
  return <div className="drawer-backdrop" onMouseDown={onClose} role="presentation">
    <aside className="drawer" onMouseDown={e => e.stopPropagation()} aria-label="策略参数">
      <div className="drawer-head"><div><span className="eyebrow">STRATEGY SETTINGS</span><h2>策略参数</h2></div><button className="text-button" onClick={onClose}>关闭</button></div>
      <p className="drawer-intro">参数将同时作用于报价、风险闸门和审计记录。BitTrade Maker 费率必须按账户实际费率填写，负数表示返佣。</p>
      <div className="settings-section">
        <div className="settings-title"><div><b>同时运行的对冲币种</b><small>可多选；仅显示双方均在线且开放 API 交易的 JPY 币种</small></div><em>{selectedSymbols.length}/8</em></div>
        <div className="symbol-grid" aria-busy={symbolsLoading}>
          {selectedSymbols.filter(symbol => !symbols.some(item => item.symbol === symbol)).map(symbol => <button type="button" key={symbol} className="selected unavailable" onClick={() => toggleSymbol(symbol)}>
            <b>{symbol.replace('_JPY', '')}</b><small>当前不可用 · 点击停用</small>
          </button>)}
          {symbols.map(item => <button type="button" key={item.symbol} className={selectedSymbols.includes(item.symbol) ? 'selected' : ''} onClick={() => toggleSymbol(item.symbol)} disabled={!selectedSymbols.includes(item.symbol) && selectedSymbols.length >= 8}>
            <b>{item.baseAsset}</b><small>JPY · 最小 {decimal.format(item.minOrderSize)}</small>
          </button>)}
        </div>
        {symbolsLoading && <p className="field-note">正在核对两家交易所的币种列表…</p>}
        {symbolsError && <p className="field-error">{symbolsError}</p>}
        {!symbolsLoading && !symbolsError && <p className={selectedSymbols.length ? 'field-note' : 'field-error'}>{selectedSymbols.length ? '各币种独立维护行情、仓位、Delta、成交和 P&L。' : '请至少选择一个币种。'}</p>}
      </div>
      <div className="settings-section">
        <div className="settings-title"><div><b>运行模式</b><small>Paper / Live 在交易所边界分叉，切换后需重启服务</small></div><span className={`connection-dot ${state.connection.status}`} /> </div>
        <div className="mode-selector">
          <button className={mode === 'paper' ? 'active' : ''} onClick={() => setMode('paper')} type="button">Paper 模式</button>
          <button className={mode === 'live' ? 'active online' : ''} onClick={() => setMode('live')} type="button">Live 模式</button>
        </div>
        {mode === 'live' && <label className="confirm-row"><input type="checkbox" checked={confirmed} onChange={e => setConfirmed(e.target.checked)} /><span>我确认 Live 模式会连接真实账户；下单仍需在主界面输入确认短语单独 Arm</span></label>}
      </div>

      {state.mode === 'paper' && paperScenarios && <div className="settings-section">
        <div className="settings-title"><div><b>Paper 撮合与对冲</b><small>BitTrade 逐笔成交按队列位置撮合；以下开关只作用于模拟 GMO 对冲边界</small></div></div>
        <p className="field-note">穿价成交 {paperScenarios.matching?.throughFills ?? 0} · 同价排队命中 {paperScenarios.matching?.atLevelFills ?? 0} · 穿价占比 {((paperScenarios.matching?.throughRatio ?? 0) * 100).toFixed(1)}%</p>
        <p className={(paperScenarios.matching?.gmoPassive?.fillsWithoutPublicTrade ?? 0) === 0 ? 'field-note' : 'field-error'}>GMO SOK 成交 {paperScenarios.matching?.gmoPassive?.fillEvents ?? 0} · 无公开成交驱动 {paperScenarios.matching?.gmoPassive?.fillsWithoutPublicTrade ?? 0}</p>
        <div className="paper-scenario-grid">
          {([
            ['gmoPartialFak', 'GMO FAK 部分成交'], ['delayedExecutions', 'GMO 成交延迟'],
            ['postOnlyReject', 'GMO Post-only 拒单'], ['randomRateLimit', '随机 429'],
            ['randomNetworkTimeout', '随机网络超时'],
          ] as Array<[keyof PaperScenarios, string]>).map(([key, label]) => <label className="confirm-row" key={key}>
            <input type="checkbox" checked={Boolean(paperScenarios[key])} onChange={e => setPaperScenarios(value => value ? ({ ...value, [key]: e.target.checked }) : value)} />
            <span>{label}</span>
          </label>)}
        </div>
        <div className="form-pair">
          <label><span>GMO 部分成交比例</span><input className="secret-input" type="number" min="0.01" max="1" step="0.05" value={paperScenarios.gmoFillRatio} onChange={e => setPaperScenarios(v => v ? ({ ...v, gmoFillRatio: Number(e.target.value) }) : v)} /></label>
          <label><span>GMO SOK 模拟成交延迟 ms</span><input className="secret-input" type="number" min="0" max="10000" value={paperScenarios.gmoPostOnlyFillDelayMs} onChange={e => setPaperScenarios(v => v ? ({ ...v, gmoPostOnlyFillDelayMs: Number(e.target.value) }) : v)} /></label>
          <label><span>executions 最短延迟 ms</span><input className="secret-input" type="number" min="0" max="10000" value={paperScenarios.executionDelayMinMs} onChange={e => setPaperScenarios(v => v ? ({ ...v, executionDelayMinMs: Number(e.target.value) }) : v)} /></label>
          <label><span>executions 最长延迟 ms</span><input className="secret-input" type="number" min="0" max="10000" value={paperScenarios.executionDelayMaxMs} onChange={e => setPaperScenarios(v => v ? ({ ...v, executionDelayMaxMs: Number(e.target.value) }) : v)} /></label>
        </div>
      </div>}

      <div className="settings-section credentials-section">
        <div className="settings-title"><div><b>GMO Coin API</b><small>{state.connection.gmoConfigured ? `已配置 ${state.connection.gmoKeyHint}` : '未配置 · Live 公开行情无需密钥'}</small></div>{state.connection.gmoConfigured && <button type="button" className="clear-button" onClick={() => setClearGmo(v => !v)}>{clearGmo ? '保留' : '清除'}</button>}</div>
        <label><span>API Key</span><input className="secret-input" type="password" autoComplete="new-password" placeholder={state.connection.gmoConfigured ? '留空则保留现有密钥' : '输入 GMO API Key'} value={secrets.gmoApiKey} onChange={e => setSecret('gmoApiKey', e.target.value)} /></label>
        <label><span>Secret Key</span><input className="secret-input" type="password" autoComplete="new-password" placeholder={state.connection.gmoConfigured ? '留空则保留现有密钥' : '输入 GMO Secret Key'} value={secrets.gmoSecretKey} onChange={e => setSecret('gmoSecretKey', e.target.value)} /></label>
      </div>

      <div className="settings-section inventory-section">
        <div className="settings-title"><div><b>双交易所底仓</b><small>任一币对的四项底仓有一项为 0，整对禁止交易</small></div></div>
        <div className="inventory-grid inventory-head"><span>资产</span><b>BitTrade</b><b>GMO</b></div>
        {['JPY', ...selectedSymbols.map(symbol => symbol.replace('_JPY', ''))].map(asset => <div className="inventory-grid" key={asset}>
          <span>{asset}</span>
          <input type="number" min="0" step={asset === 'JPY' ? '10000' : '0.0001'} value={inventory.bittrade[asset] ?? 0} onChange={e => setInventoryAmount('bittrade', asset, e.target.value)} />
          <input type="number" min="0" step={asset === 'JPY' ? '10000' : '0.0001'} value={inventory.gmo[asset] ?? 0} onChange={e => setInventoryAmount('gmo', asset, e.target.value)} />
        </div>)}
        <p className="field-note">例如 BTC/JPY 必须同时具备 BitTrade JPY、BitTrade BTC、GMO JPY、GMO BTC。</p>
        <label><span>Lark 报警 Webhook</span><input className="secret-input" type="password" autoComplete="off" placeholder={inventory.webhookConfigured ? `已配置 ${inventory.webhookHint}，留空则保留` : '粘贴 Lark Bot Webhook'} value={webhookUrl} onChange={e => setWebhookUrl(e.target.value)} /></label>
        {inventory.webhookConfigured && <button type="button" className="clear-button" onClick={() => setClearWebhook(value => !value)}>{clearWebhook ? '保留 Webhook' : '清除 Webhook'}</button>}
      </div>

      <div className="settings-section credentials-section">
        <div className="settings-title"><div><b>BitTrade API</b><small>{state.connection.bittradeConfigured ? `已配置 ${state.connection.bittradeKeyHint}` : '未配置'}</small></div>{state.connection.bittradeConfigured && <button type="button" className="clear-button" onClick={() => setClearBittrade(v => !v)}>{clearBittrade ? '保留' : '清除'}</button>}</div>
        <label><span>Access Key</span><input className="secret-input" type="password" autoComplete="new-password" placeholder={state.connection.bittradeConfigured ? '留空则保留现有密钥' : '输入 BitTrade Access Key'} value={secrets.bittradeAccessKey} onChange={e => setSecret('bittradeAccessKey', e.target.value)} /></label>
        <label><span>Secret Key</span><input className="secret-input" type="password" autoComplete="new-password" placeholder={state.connection.bittradeConfigured ? '留空则保留现有密钥' : '输入 BitTrade Secret Key'} value={secrets.bittradeSecretKey} onChange={e => setSecret('bittradeSecretKey', e.target.value)} /></label>
        <label><span>Account ID</span><input className="secret-input" type="text" autoComplete="off" placeholder={state.connection.bittradeConfigured ? '留空则保留现有账户' : '输入 BitTrade Account ID'} value={secrets.bittradeAccountId} onChange={e => setSecret('bittradeAccountId', e.target.value)} /></label>
        <p className="security-note"><ShieldCheck size={15} />密钥仅保存在后端进程内存中，不写入浏览器、状态流或审计日志；服务重启后需重新填写。</p>
      </div>

      <div className="settings-divider"><span>报价与风控</span></div>
      <label><span>BitTrade 加价幅度</span><div className="input-with-unit"><input type="number" step="0.1" value={form.spreadBps} onChange={e => set('spreadBps', e.target.value)} /><i>bps</i></div><small>必须高于手续费与预期滑点之和</small></label>
      <div className="form-pair">
        <label><span>BitTrade Maker 费率</span><div className="input-with-unit"><input type="number" step="0.01" value={form.bittradeMakerFeeBps} onChange={e => set('bittradeMakerFeeBps', e.target.value)} /><i>bps</i></div></label>
        <label><span>预期滑点</span><div className="input-with-unit"><input type="number" step="0.1" value={form.expectedSlippageBps} onChange={e => set('expectedSlippageBps', e.target.value)} /><i>bps</i></div></label>
        <label><span>累计深度滑点范围</span><div className="input-with-unit"><input type="number" min="0" step="0.1" value={form.maxHedgeSlippageBps} onChange={e => set('maxHedgeSlippageBps', e.target.value)} /><i>bps</i></div></label>
        <label><span>BitTrade 排队预算</span><div className="input-with-unit"><input type="number" min="0" step="0.001" value={form.queueBudget} onChange={e => set('queueBudget', e.target.value)} /><i>币</i></div></label>
      </div>
      <div className="settings-section fee-section">
        <div className="settings-title"><div><b>GMO Taker 费率（每币种）</b><small>可按账户实际费率覆盖；未设置时采用公开费率表</small></div></div>
        <div className="fee-grid">
          {selectedSymbols.map(symbol => {
            const asset = symbol.replace('_JPY', '')
            return <label key={asset}><span>{asset}</span><div className="input-with-unit"><input type="number" min="0" max="100" step="0.01" value={gmoFees[asset] ?? defaultGmoFee(asset)} onChange={e => setGmoFees(current => ({ ...current, [asset]: Number(e.target.value) }))} /><i>bps</i></div></label>
          })}
        </div>
      </div>
      <div className="form-pair">
        <label><span>预计 SOK 成交比例</span><div className="input-with-unit"><input type="number" min="0" max="1" step="0.05" value={form.expectedPassiveFillRatio} onChange={e => set('expectedPassiveFillRatio', e.target.value)} /><i>ratio</i></div></label>
        <label><span>SOK 等待后兜底</span><div className="input-with-unit"><input type="number" min="100" max="10000" step="50" value={form.gmoPostOnlyTimeoutMs} onChange={e => set('gmoPostOnlyTimeoutMs', e.target.value)} /><i>ms</i></div></label>
        <label><span>预计 GMO 混合费率</span><div className="input-with-unit"><input readOnly value={((gmoMakerFees[state.instrument.baseAsset] ?? defaultGmoMakerFee(state.instrument.baseAsset)) * form.expectedPassiveFillRatio + (gmoFees[state.instrument.baseAsset] ?? defaultGmoFee(state.instrument.baseAsset)) * (1 - form.expectedPassiveFillRatio)).toFixed(2)} /><i>bps</i></div></label>
      </div>
      <div className="settings-section fee-section">
        <div className="settings-title"><div><b>GMO SOK Maker 费率（每币种）</b><small>负数表示返佣；BTC/ETH/XRP/DAI 默认 -1 bps，其余默认 -3 bps</small></div></div>
        <div className="fee-grid">
          {selectedSymbols.map(symbol => {
            const asset = symbol.replace('_JPY', '')
            return <label key={asset}><span>{asset}</span><div className="input-with-unit"><input type="number" min="-100" max="100" step="0.01" value={gmoMakerFees[asset] ?? defaultGmoMakerFee(asset)} onChange={e => setGmoMakerFees(current => ({ ...current, [asset]: Number(e.target.value) }))} /><i>bps</i></div></label>
          })}
        </div>
      </div>
      <label><span>单边最大挂单基准</span><div className="input-with-unit"><input type="number" step="0.0001" value={form.maxQuoteSize} onChange={e => set('maxQuoteSize', e.target.value)} /><i>AUTO</i></div><small>低于币种最小下单量时自动提升，超过最大量时自动收窄</small></label>
      <label><span>Delta 紧急停止阈值基准</span><div className="input-with-unit"><input type="number" step="0.0001" value={form.deltaLimit} onChange={e => set('deltaLimit', e.target.value)} /><i>AUTO</i></div><small>每个币种均独立计算，阈值不会低于该币种最小下单量</small></label>
      <label><span>最大对冲延迟</span><div className="input-with-unit"><input type="number" step="50" value={form.maxHedgeLatencyMs} onChange={e => set('maxHedgeLatencyMs', e.target.value)} /><i>ms</i></div></label>
      <div className="settings-section risk-limit-section">
        <div className="settings-title"><div><b>{state.mode === 'live' ? '实盘自动闸门' : 'Paper 运行闸门'}</b><small>{state.mode === 'live' ? '修改任一限额会先自动 Disarm' : 'Paper 配置更新不会中断模拟撮合'}</small></div></div>
        <div className="form-pair">
          <label><span>单笔上限 JPY</span><input className="secret-input" type="number" min="1" value={riskLimits.maxSingleOrderJpy} onChange={e => setRiskLimits(v => ({ ...v, maxSingleOrderJpy: Number(e.target.value) }))} /></label>
          <label><span>日成交额 JPY</span><input className="secret-input" type="number" min="1" value={riskLimits.maxDailyVolumeJpy} onChange={e => setRiskLimits(v => ({ ...v, maxDailyVolumeJpy: Number(e.target.value) }))} /></label>
          <label><span>日最大亏损 JPY</span><input className="secret-input" type="number" min="1" value={riskLimits.maxDailyLossJpy} onChange={e => setRiskLimits(v => ({ ...v, maxDailyLossJpy: Number(e.target.value) }))} /></label>
          <label><span>绝对 Delta</span><input className="secret-input" type="number" min="0" step="0.0001" value={riskLimits.maxAbsDelta} onChange={e => setRiskLimits(v => ({ ...v, maxAbsDelta: Number(e.target.value) }))} /></label>
          <label><span>连续对冲失败</span><input className="secret-input" type="number" min="1" value={riskLimits.maxHedgeFailures} onChange={e => setRiskLimits(v => ({ ...v, maxHedgeFailures: Number(e.target.value) }))} /></label>
          <label><span>对冲 P95 ms</span><input className="secret-input" type="number" min="1" value={riskLimits.maxHedgeP95Ms} onChange={e => setRiskLimits(v => ({ ...v, maxHedgeP95Ms: Number(e.target.value) }))} /></label>
          <label><span>Arm 有效期 秒</span><input className="secret-input" readOnly value={riskLimits.armTtlSec} /><i>按运行模式固定</i></label>
        </div>
      </div>
      {error && <div className="inline-error"><Warning size={18} weight="fill" />{error}</div>}
      <button className="primary full" onClick={save} disabled={saving || !selectedSymbols.length || (mode === 'live' && !confirmed)}>{saving ? (mode === 'live' ? '正在初始化多币种行情…' : '保存中…') : `保存并运行 ${selectedSymbols.length} 个币种`}</button>
    </aside>
  </div>
}

export function App() {
  const [state, setState] = useState<SystemState | null>(null)
  const [error, setError] = useState('')
  const [settings, setSettings] = useState(false)
  const [busy, setBusy] = useState('')
  const [selectedSymbol, setSelectedSymbol] = useState('')
  const [risk, setRisk] = useState<RiskStatus | null>(null)

  useEffect(() => {
    let stopped = false
    let controller: AbortController | undefined
    const connect = async () => {
      while (!stopped) {
        controller = new AbortController()
        try {
          const response = await authorizedFetch('/api/events', { signal: controller.signal })
          if (!response.ok || !response.body) {
            const data = await response.json().catch(() => ({}))
            throw new Error(data.detail ?? '实时连接鉴权失败')
          }
          const reader = response.body.getReader()
          const decoder = new TextDecoder()
          let buffered = ''
          while (!stopped) {
            const { value, done } = await reader.read()
            if (done) break
            buffered += decoder.decode(value, { stream: true })
            const frames = buffered.split('\n\n')
            buffered = frames.pop() ?? ''
            for (const frame of frames) {
              const payload = frame.split('\n').filter(line => line.startsWith('data:'))
                .map(line => line.slice(5).trimStart()).join('\n')
              if (payload) { setState(JSON.parse(payload)); setError('') }
            }
          }
        } catch (error) {
          if (!stopped && !(error instanceof DOMException && error.name === 'AbortError')) {
            setError(error instanceof Error ? error.message : '与交易服务的实时连接已断开，正在重连…')
          }
        }
        if (!stopped) await new Promise(resolve => window.setTimeout(resolve, 1500))
      }
    }
    void connect()
    return () => { stopped = true; controller?.abort() }
  }, [])

  useEffect(() => {
    let active = true
    const refresh = () => api('/api/risk').then(value => { if (active) setRisk(value) }).catch(() => undefined)
    refresh()
    const timer = window.setInterval(refresh, 5000)
    return () => { active = false; window.clearInterval(timer) }
  }, [])

  const activeSymbol = state && state.symbolStates[selectedSymbol] ? selectedSymbol : state?.activeSymbols[0]
  const viewState = useMemo(() => {
    if (!state || !activeSymbol) return state
    const runtime = state.symbolStates[activeSymbol]
    if (!runtime) return state
    return { ...state, ...runtime, metrics: { ...state.metrics,
      fillCount: runtime.fillCount, hedgeP95Ms: runtime.hedgeP95Ms } }
  }, [state, activeSymbol])
  const control = async (action: string) => {
    setBusy(action)
    try { await api('/api/control', { method: 'POST', body: JSON.stringify({ action }) }) }
    catch (e) { setError(e instanceof Error ? e.message : '控制失败') }
    finally { setBusy('') }
  }
  const toggleArm = async () => {
    setBusy('arm')
    try {
      let result
      if (risk?.armed) {
        result = await api('/api/risk/disarm', { method: 'POST' })
      } else {
        const phrase = window.prompt('输入实盘确认短语')
        if (!phrase) return
        result = await api('/api/risk/arm', { method: 'POST', body: JSON.stringify({ phrase }) })
      }
      setRisk(result)
    } catch (e) { setError(e instanceof Error ? e.message : '风控状态切换失败') }
    finally { setBusy('') }
  }

  if (!state || !viewState) return error ? <main className="fatal"><Warning size={32} weight="fill" /><h1>无法连接交易服务</h1><p>{error}</p><code>npm run dev</code></main> : <Skeleton />

  return <div className="app">
    <header className="topbar">
      <div className="brand"><span>JARB</span><small>MARKET MAKING / HEDGE</small></div>
      <nav>
        <span className={`mode ${state.mode}`}>{state.mode === 'paper' ? 'Paper 模式' : 'Live 模式'}</span>
        <span className={`live-pill ${state.running ? 'on' : ''}`}><i />{state.running ? '策略运行中' : '报价已暂停'}</span>
        <button className="icon-button" onClick={() => setSettings(true)} aria-label="打开策略参数"><SlidersHorizontal size={20} /></button>
      </nav>
    </header>

    <main className="shell">
      {error && <div className="banner"><Warning size={18} weight="fill" /><span>{error}</span><button onClick={() => setError('')}>关闭</button></div>}
      <div className="symbol-tabs" aria-label="选择币种视图">
        <span>并发策略</span>
        {state.activeSymbols.map(symbol => {
          const runtime = state.symbolStates[symbol]
          const disabled = state.disabledSymbols[symbol]
          return <button key={symbol} className={`${symbol === activeSymbol ? 'active' : ''} ${disabled ? 'unavailable' : ''}`} onClick={() => setSelectedSymbol(symbol)} title={disabled?.join('、')}>
            <i className={disabled ? 'exception' : runtime.reconciliation.status} />{runtime.instrument.baseAsset}<small>{disabled ? '底仓不足' : runtime.reconciliation.delta === 0 ? '已对冲' : `Δ ${decimal.format(runtime.reconciliation.delta)}`}</small>
          </button>
        })}
      </div>
      <section className="intro">
        <div><span className="eyebrow">LIQUIDITY OPERATIONS / {viewState.market.symbol}</span><h1>做市与即时对冲</h1><p>{state.mode === 'live' ? `正在并发运行 ${state.activeSymbols.length} 个 GMO 实时行情策略；当前查看 ${viewState.instrument.baseAsset}/JPY。` : `Paper 正通过完整引擎运行 ${state.activeSymbols.length} 个策略；当前查看 ${viewState.instrument.baseAsset}/JPY。`}</p></div>
        <div className="controls">
          {state.mode === 'paper' ? <div className={`online-note ${state.connection.status}`}><ShieldCheck size={17} />BitTrade 实时队列撮合</div> : <>
            <div className={`online-note ${state.connection.status}`}><ShieldCheck size={17} />{state.connection.status === 'error' ? 'Live 行情异常' : risk?.armed ? '实盘已 Arm' : `DISARMED${risk?.reason ? ` · ${risk.reason}` : ''}`}</div>
            <button className={risk?.armed ? 'kill active' : 'secondary'} onClick={toggleArm} disabled={!!busy || !risk?.recoveryComplete}>{risk?.armed ? 'Disarm 并撤单' : risk?.pendingArmActor ? '第二人复核 Arm' : 'Arm 实盘'}</button>
          </>}
          <button className="secondary" onClick={() => control(state.running ? 'pause' : 'resume')} disabled={!!busy}>{state.running ? <Pause size={18} weight="fill" /> : <Play size={18} weight="fill" />}{state.running ? '暂停报价' : '恢复报价'}</button>
          <button className={`kill ${state.killSwitch ? 'active' : ''}`} onClick={() => control(state.killSwitch ? 'reset-kill' : 'kill')} disabled={!!busy}><Siren size={18} weight="fill" />{state.killSwitch ? '解除急停' : '紧急停止'}</button>
        </div>
      </section>

      <section className="metrics-strip">
        <Metric label={`${viewState.instrument.baseAsset} 本日净收益`} value={`${viewState.pnl.net >= 0 ? '+' : ''}¥${jpy.format(viewState.pnl.net)}`} note={`价差 +¥${jpy.format(viewState.pnl.spread)} · 费用 −¥${jpy.format(viewState.pnl.hedgeCosts)}`} tone={viewState.pnl.net >= 0 ? 'positive' : 'negative'} />
        <Metric label="当前币种双边成交" value={`${viewState.metrics.fillCount}`} note={`全策略累计 ${state.metrics.fillCount} 笔`} />
        <Metric label="GMO 最优价差" value={`¥${price(viewState.market.ask - viewState.market.bid, viewState.instrument.priceTick)}`} note={`深度 ${decimal.format(viewState.market.bidSize + viewState.market.askSize)} ${viewState.instrument.baseAsset}`} />
        <Metric label="全策略对账异常" value={`${state.metrics.exceptionCount}`} note={viewState.reconciliation.status === 'matched' ? '当前币种无未处理差额' : '当前币种需要立即处理'} tone={state.metrics.exceptionCount ? 'negative' : ''} />
      </section>

      <Holdings state={viewState} />
      <div className="primary-grid"><MarketLadder state={viewState} /><Reconciliation state={viewState} /></div>
      <Trades state={viewState} />

      <section className="bottom-grid">
        <div className="pnl-section">
          <div className="section-head"><div><span className="eyebrow">P&amp;L BREAKDOWN</span><h2>收益构成</h2></div></div>
          <div className="pnl-equation">
            <div><span>价差收益</span><b>+¥{jpy.format(viewState.pnl.spread)}</b></div><i>+</i>
            <div><span>BitTrade Maker 费</span><b>{viewState.pnl.clientFees >= 0 ? '+' : ''}¥{jpy.format(viewState.pnl.clientFees)}</b></div><i>−</i>
            <div><span>GMO 成本</span><b>¥{jpy.format(viewState.pnl.hedgeCosts)}</b></div><i>=</i>
            <div className="net"><span>交易净收益</span><b>{viewState.pnl.net >= 0 ? '+' : ''}¥{jpy.format(viewState.pnl.net)}</b></div>
          </div>
        </div>
        <div className="audit-section">
          <div className="section-head"><div><span className="eyebrow">AUDIT TRAIL</span><h2>审计留痕</h2></div><button className="download" onClick={() => void downloadApi('/api/reconciliation/export')}><DownloadSimple size={17} />导出日结</button></div>
          <div className="event-list">{state.events.slice(0, 5).map(event => <div className={`event ${event.level}`} key={event.id}><i /> <span>{time(event.timestamp)}</span><b>{event.message}</b></div>)}</div>
        </div>
      </section>
    </main>

    <footer><span>并发币种：{state.activeSymbols.length} · 报价依据：GMO Coin</span><span>数据留存：JSONL · 各币种独立对账</span><button onClick={() => window.location.reload()}><ArrowClockwise size={15} />刷新连接</button></footer>
    {settings && <Settings state={state} risk={risk} onClose={() => setSettings(false)} onSaved={() => undefined} />}
  </div>
}
