import { useEffect, useMemo, useState } from 'react'
import {
  Pulse, ArrowClockwise, ArrowsLeftRight, CheckCircle, DownloadSimple, Gauge,
  Lightning, Pause, Play, ShieldCheck, Siren, SlidersHorizontal, Warning,
} from '@phosphor-icons/react'
import type { InstrumentRules, Side, SystemState } from './types'

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

async function api(path: string, init?: RequestInit) {
  const response = await fetch(path, { ...init, headers: { 'Content-Type': 'application/json', ...init?.headers } })
  const data = await response.json()
  if (!response.ok) throw new Error(data.error ?? data.detail ?? '请求失败')
  return data
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
  const mid = (state.market.ask + state.market.bid) / 2
  return <section className="market-panel panel" aria-labelledby="market-title">
    <div className="section-head">
      <div><span className="eyebrow">LIVE PRICE LADDER</span><h2 id="market-title">价格阶梯</h2></div>
      <div className={`feed-status ${state.connection.status === 'error' ? 'feed-error' : ''}`}><i /><span>{state.market.source === 'GMO' ? 'GMO 线上行情' : '模拟行情'}</span><b>{time(state.market.timestamp)}</b></div>
    </div>
    <div className="ladder">
      <div className="ladder-row client ask">
        <div><span>BitTrade Ask</span><small>客户买入价 · 可挂 {decimal.format(ask?.size ?? 0)} {state.instrument.baseAsset}</small></div>
        <strong>¥{price(ask?.price ?? 0, state.instrument.priceTick)}</strong>
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
        <div><span>BitTrade Bid</span><small>客户卖出价 · 可挂 {decimal.format(bid?.size ?? 0)} {state.instrument.baseAsset}</small></div>
        <strong>¥{price(bid?.price ?? 0, state.instrument.priceTick)}</strong>
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
      <div><span><ShieldCheck size={18} /> Maker 盈利下限</span><b>{(state.config.spreadBps - state.config.gmoFeeBps - state.config.expectedSlippageBps).toFixed(1)} bps</b></div>
      <div><span><Lightning size={18} /> Rust 核心 P99</span><b>{state.metrics.coreCalcP99Us ? `${state.metrics.coreCalcP99Us.toFixed(2)} µs` : '采样中'}</b></div>
      <div><span><Pulse size={18} /> 运行时长</span><b>{Math.floor(state.metrics.uptimeSec / 60)}m {state.metrics.uptimeSec % 60}s</b></div>
    </div>
  </section>
}

function Trades({ state }: { state: SystemState }) {
  return <section className="table-section" aria-labelledby="trade-title">
    <div className="section-head wide">
      <div><span className="eyebrow">MATCHED EXECUTIONS</span><h2 id="trade-title">双边成交明细</h2></div>
      <span className="muted">最近 {state.trades.length} 笔 · BitTrade 成交 → GMO 反向对冲</span>
    </div>
    {state.trades.length === 0 ? <div className="empty-state">
      <ArrowsLeftRight size={30} /><div><b>尚无双边成交</b><span>使用右上角“模拟成交”验证报价、对冲与对账闭环。</span></div>
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

function Settings({ state, onClose, onSaved }: { state: SystemState; onClose: () => void; onSaved: () => void }) {
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
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const set = (key: keyof typeof form, value: string) => setForm(v => ({ ...v, [key]: key === 'symbol' ? value : Number(value) }))
  const setSecret = (key: keyof typeof secrets, value: string) => setSecrets(v => ({ ...v, [key]: value }))
  useEffect(() => {
    let active = true
    api('/api/symbols').then(data => { if (active) setSymbols(data.symbols) })
      .catch(e => { if (active) setSymbolsError(e instanceof Error ? e.message : '币种加载失败') })
      .finally(() => { if (active) setSymbolsLoading(false) })
    return () => { active = false }
  }, [])
  const toggleSymbol = (symbol: string) => setSelectedSymbols(value => value.includes(symbol)
    ? value.filter(item => item !== symbol)
    : [...value, symbol])
  const save = async () => {
    setSaving(true); setError('')
    try {
      await api('/api/strategy', { method: 'PATCH', body: JSON.stringify({ ...form, symbols: selectedSymbols }) })
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
      <p className="drawer-intro">参数将同时作用于报价、风险闸门和审计记录。价差下限按 Maker 无手续费收益的保守情景校验。</p>
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
        <div className="settings-title"><div><b>运行模式</b><small>线上模式使用 GMO 实时行情</small></div><span className={`connection-dot ${state.connection.status}`} /> </div>
        <div className="mode-selector">
          <button className={mode === 'simulation' ? 'active' : ''} onClick={() => setMode('simulation')} type="button">模拟模式</button>
          <button className={mode === 'online' ? 'active online' : ''} onClick={() => setMode('online')} type="button">线上模式</button>
        </div>
        {mode === 'online' && <label className="confirm-row"><input type="checkbox" checked={confirmed} onChange={e => setConfirmed(e.target.checked)} /><span>我确认线上模式只读取实时行情，不会自动提交真实订单</span></label>}
      </div>

      <div className="settings-section credentials-section">
        <div className="settings-title"><div><b>GMO Coin API</b><small>{state.connection.gmoConfigured ? `已配置 ${state.connection.gmoKeyHint}` : '未配置 · 线上公开行情无需密钥'}</small></div>{state.connection.gmoConfigured && <button type="button" className="clear-button" onClick={() => setClearGmo(v => !v)}>{clearGmo ? '保留' : '清除'}</button>}</div>
        <label><span>API Key</span><input className="secret-input" type="password" autoComplete="new-password" placeholder={state.connection.gmoConfigured ? '留空则保留现有密钥' : '输入 GMO API Key'} value={secrets.gmoApiKey} onChange={e => setSecret('gmoApiKey', e.target.value)} /></label>
        <label><span>Secret Key</span><input className="secret-input" type="password" autoComplete="new-password" placeholder={state.connection.gmoConfigured ? '留空则保留现有密钥' : '输入 GMO Secret Key'} value={secrets.gmoSecretKey} onChange={e => setSecret('gmoSecretKey', e.target.value)} /></label>
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
        <label><span>GMO 手续费</span><div className="input-with-unit"><input type="number" step="0.1" value={form.gmoFeeBps} onChange={e => set('gmoFeeBps', e.target.value)} /><i>bps</i></div></label>
        <label><span>预期滑点</span><div className="input-with-unit"><input type="number" step="0.1" value={form.expectedSlippageBps} onChange={e => set('expectedSlippageBps', e.target.value)} /><i>bps</i></div></label>
      </div>
      <label><span>单边最大挂单基准</span><div className="input-with-unit"><input type="number" step="0.0001" value={form.maxQuoteSize} onChange={e => set('maxQuoteSize', e.target.value)} /><i>AUTO</i></div><small>低于币种最小下单量时自动提升，超过最大量时自动收窄</small></label>
      <label><span>Delta 紧急停止阈值基准</span><div className="input-with-unit"><input type="number" step="0.0001" value={form.deltaLimit} onChange={e => set('deltaLimit', e.target.value)} /><i>AUTO</i></div><small>每个币种均独立计算，阈值不会低于该币种最小下单量</small></label>
      <label><span>最大对冲延迟</span><div className="input-with-unit"><input type="number" step="50" value={form.maxHedgeLatencyMs} onChange={e => set('maxHedgeLatencyMs', e.target.value)} /><i>ms</i></div></label>
      {error && <div className="inline-error"><Warning size={18} weight="fill" />{error}</div>}
      <button className="primary full" onClick={save} disabled={saving || !selectedSymbols.length || (mode === 'online' && state.mode !== 'online' && !confirmed)}>{saving ? (mode === 'online' ? '正在初始化多币种行情…' : '保存中…') : `保存并运行 ${selectedSymbols.length} 个币种`}</button>
    </aside>
  </div>
}

export function App() {
  const [state, setState] = useState<SystemState | null>(null)
  const [error, setError] = useState('')
  const [settings, setSettings] = useState(false)
  const [busy, setBusy] = useState('')
  const [fillSide, setFillSide] = useState<Side>('SELL')
  const [selectedSymbol, setSelectedSymbol] = useState('')

  useEffect(() => {
    let source: EventSource | undefined
    const connect = () => {
      source = new EventSource('/api/events')
      source.onmessage = event => { setState(JSON.parse(event.data)); setError('') }
      source.onerror = () => setError('与交易服务的实时连接已断开，正在重连…')
    }
    connect()
    return () => source?.close()
  }, [])

  const activeSymbol = state && state.symbolStates[selectedSymbol] ? selectedSymbol : state?.activeSymbols[0]
  const viewState = useMemo(() => {
    if (!state || !activeSymbol) return state
    const runtime = state.symbolStates[activeSymbol]
    if (!runtime) return state
    const latencies = runtime.trades.map(trade => trade.latencyMs).sort((a, b) => a - b)
    return { ...state, ...runtime, metrics: { ...state.metrics, fillCount: runtime.trades.length,
      hedgeP95Ms: latencies.length ? latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * .95))] : 0 } }
  }, [state, activeSymbol])
  const quote = useMemo(() => viewState?.quotes.find(q => q.side === fillSide), [viewState, fillSide])
  const simulateSize = viewState && quote ? Math.min(quote.size, Math.max(viewState.instrument.minOrderSize, viewState.instrument.sizeStep)) : 0
  const control = async (action: string) => {
    setBusy(action)
    try { await api('/api/control', { method: 'POST', body: JSON.stringify({ action }) }) }
    catch (e) { setError(e instanceof Error ? e.message : '控制失败') }
    finally { setBusy('') }
  }
  const simulate = async () => {
    if (!quote) return
    setBusy('fill')
    try { await api('/api/sim/fill', { method: 'POST', body: JSON.stringify({ symbol: activeSymbol, side: fillSide, size: simulateSize, role: 'maker' }) }) }
    catch (e) { setError(e instanceof Error ? e.message : '模拟成交失败') }
    finally { setBusy('') }
  }

  if (!state || !viewState) return error ? <main className="fatal"><Warning size={32} weight="fill" /><h1>无法连接交易服务</h1><p>{error}</p><code>npm run dev</code></main> : <Skeleton />

  return <div className="app">
    <header className="topbar">
      <div className="brand"><span>JARB</span><small>MARKET MAKING / HEDGE</small></div>
      <nav>
        <span className={`mode ${state.mode}`}>{state.mode === 'simulation' ? '模拟模式' : '线上模式'}</span>
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
          return <button key={symbol} className={symbol === activeSymbol ? 'active' : ''} onClick={() => setSelectedSymbol(symbol)}>
            <i className={runtime.reconciliation.status} />{runtime.instrument.baseAsset}<small>{runtime.reconciliation.delta === 0 ? '已对冲' : `Δ ${decimal.format(runtime.reconciliation.delta)}`}</small>
          </button>
        })}
      </div>
      <section className="intro">
        <div><span className="eyebrow">LIQUIDITY OPERATIONS / {viewState.market.symbol}</span><h1>做市与即时对冲</h1><p>{state.mode === 'online' ? `正在并发运行 ${state.activeSymbols.length} 个 GMO 实时行情策略；当前查看 ${viewState.instrument.baseAsset}/JPY。` : `正在并发运行 ${state.activeSymbols.length} 个模拟策略；当前查看 ${viewState.instrument.baseAsset}/JPY。`}</p></div>
        <div className="controls">
          {state.mode === 'simulation' ? <div className="sim-control">
            <button onClick={() => setFillSide('SELL')} className={fillSide === 'SELL' ? 'active' : ''}>客户买入</button>
            <button onClick={() => setFillSide('BUY')} className={fillSide === 'BUY' ? 'active' : ''}>客户卖出</button>
            <button className="simulate" onClick={simulate} disabled={!state.running || busy === 'fill' || !simulateSize}><Lightning size={17} weight="fill" />{busy === 'fill' ? '对冲中…' : `模拟成交 ${decimal.format(simulateSize)} ${viewState.instrument.baseAsset}`}</button>
          </div> : <div className={`online-note ${state.connection.status}`}><ShieldCheck size={17} />{state.connection.status === 'error' ? '线上行情异常' : '线上行情 · 只读'}</div>}
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

      <div className="primary-grid"><MarketLadder state={viewState} /><Reconciliation state={viewState} /></div>
      <Trades state={viewState} />

      <section className="bottom-grid">
        <div className="pnl-section">
          <div className="section-head"><div><span className="eyebrow">P&amp;L BREAKDOWN</span><h2>收益构成</h2></div></div>
          <div className="pnl-equation">
            <div><span>价差收益</span><b>+¥{jpy.format(viewState.pnl.spread)}</b></div><i>+</i>
            <div><span>客户手续费</span><b>+¥{jpy.format(viewState.pnl.clientFees)}</b></div><i>−</i>
            <div><span>GMO 成本</span><b>¥{jpy.format(viewState.pnl.hedgeCosts)}</b></div><i>=</i>
            <div className="net"><span>交易净收益</span><b>{viewState.pnl.net >= 0 ? '+' : ''}¥{jpy.format(viewState.pnl.net)}</b></div>
          </div>
        </div>
        <div className="audit-section">
          <div className="section-head"><div><span className="eyebrow">AUDIT TRAIL</span><h2>审计留痕</h2></div><a className="download" href="/api/reconciliation/export"><DownloadSimple size={17} />导出日结</a></div>
          <div className="event-list">{state.events.slice(0, 5).map(event => <div className={`event ${event.level}`} key={event.id}><i /> <span>{time(event.timestamp)}</span><b>{event.message}</b></div>)}</div>
        </div>
      </section>
    </main>

    <footer><span>并发币种：{state.activeSymbols.length} · 报价依据：GMO Coin</span><span>数据留存：JSONL · 各币种独立对账</span><button onClick={() => window.location.reload()}><ArrowClockwise size={15} />刷新连接</button></footer>
    {settings && <Settings state={state} onClose={() => setSettings(false)} onSaved={() => undefined} />}
  </div>
}
