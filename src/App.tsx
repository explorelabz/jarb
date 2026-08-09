import { useEffect, useMemo, useState } from 'react'
import {
  Pulse, ArrowClockwise, ArrowsLeftRight, CheckCircle, DownloadSimple, Gauge,
  Lightning, Pause, Play, ShieldCheck, Siren, SlidersHorizontal, Warning,
} from '@phosphor-icons/react'
import type { Side, SystemState } from './types'

const jpy = new Intl.NumberFormat('ja-JP', { maximumFractionDigits: 0 })
const decimal = new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 8 })
const time = (value: string) => new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(new Date(value))
const signed = (value: number, suffix = '') => `${value > 0 ? '+' : ''}${decimal.format(value)}${suffix}`

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
      <div className="feed-status"><i /><span>GMO 行情</span><b>{time(state.market.timestamp)}</b></div>
    </div>
    <div className="ladder">
      <div className="ladder-row client ask">
        <div><span>BitTrade Ask</span><small>客户买入价 · 可挂 {decimal.format(ask?.size ?? 0)} BTC</small></div>
        <strong>¥{jpy.format(ask?.price ?? 0)}</strong>
      </div>
      <div className="spread-band"><span>卖出加价</span><b>+{state.config.spreadBps.toFixed(1)} bps</b></div>
      <div className="ladder-row external">
        <div><span>GMO Ask</span><small>对冲买入基准</small></div><strong>¥{jpy.format(state.market.ask)}</strong>
      </div>
      <div className="midline"><span>外部价差</span><b>¥{jpy.format(state.market.ask - state.market.bid)}</b><em>中间价 ¥{jpy.format(mid)}</em></div>
      <div className="ladder-row external">
        <div><span>GMO Bid</span><small>对冲卖出基准</small></div><strong>¥{jpy.format(state.market.bid)}</strong>
      </div>
      <div className="spread-band"><span>买入减价</span><b>−{state.config.spreadBps.toFixed(1)} bps</b></div>
      <div className="ladder-row client bid">
        <div><span>BitTrade Bid</span><small>客户卖出价 · 可挂 {decimal.format(bid?.size ?? 0)} BTC</small></div>
        <strong>¥{jpy.format(bid?.price ?? 0)}</strong>
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
      <span>实时 Delta</span><strong>{signed(state.reconciliation.delta)} <small>BTC</small></strong>
      <div className="limit"><i style={{ transform: `scaleX(${Math.max(0.006, limitRatio / 100)})` }} /><span>阈值 ±{state.config.deltaLimit} BTC</span></div>
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
      <thead><tr><th>时间 / 交易编号</th><th>客户方向</th><th className="num">数量 BTC</th><th className="num">BitTrade 成交</th><th className="num">GMO 对冲</th><th className="num">延迟</th><th className="num">净收益 JPY</th></tr></thead>
      <tbody>{state.trades.map(trade => <tr key={trade.id}>
        <td><b>{time(trade.timestamp)}</b><small>{trade.id}</small></td>
        <td><span className={`side ${trade.clientSide.toLowerCase()}`}>{trade.clientSide === 'SELL' ? '客户买入' : '客户卖出'}</span></td>
        <td className="num">{decimal.format(trade.size)}</td>
        <td className="num">¥{jpy.format(trade.clientPrice)}</td>
        <td className="num">¥{jpy.format(trade.hedgePrice)}</td>
        <td className="num">{trade.latencyMs} ms</td>
        <td className={`num pnl ${trade.netPnl >= 0 ? 'positive' : 'negative'}`}>{trade.netPnl >= 0 ? '+' : ''}¥{jpy.format(trade.netPnl)}</td>
      </tr>)}</tbody>
    </table></div>}
  </section>
}

function Settings({ state, onClose, onSaved }: { state: SystemState; onClose: () => void; onSaved: () => void }) {
  const [form, setForm] = useState(state.config)
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const set = (key: keyof typeof form, value: string) => setForm(v => ({ ...v, [key]: key === 'symbol' ? value : Number(value) }))
  const save = async () => {
    setSaving(true); setError('')
    try { await api('/api/strategy', { method: 'PATCH', body: JSON.stringify(form) }); onSaved(); onClose() }
    catch (e) { setError(e instanceof Error ? e.message : '保存失败') }
    finally { setSaving(false) }
  }
  return <div className="drawer-backdrop" onMouseDown={onClose} role="presentation">
    <aside className="drawer" onMouseDown={e => e.stopPropagation()} aria-label="策略参数">
      <div className="drawer-head"><div><span className="eyebrow">STRATEGY SETTINGS</span><h2>策略参数</h2></div><button className="text-button" onClick={onClose}>关闭</button></div>
      <p className="drawer-intro">参数将同时作用于报价、风险闸门和审计记录。价差下限按 Maker 无手续费收益的保守情景校验。</p>
      <label><span>BitTrade 加价幅度</span><div className="input-with-unit"><input type="number" step="0.1" value={form.spreadBps} onChange={e => set('spreadBps', e.target.value)} /><i>bps</i></div><small>必须高于手续费与预期滑点之和</small></label>
      <div className="form-pair">
        <label><span>GMO 手续费</span><div className="input-with-unit"><input type="number" step="0.1" value={form.gmoFeeBps} onChange={e => set('gmoFeeBps', e.target.value)} /><i>bps</i></div></label>
        <label><span>预期滑点</span><div className="input-with-unit"><input type="number" step="0.1" value={form.expectedSlippageBps} onChange={e => set('expectedSlippageBps', e.target.value)} /><i>bps</i></div></label>
      </div>
      <label><span>单边最大挂单</span><div className="input-with-unit"><input type="number" step="0.001" value={form.maxQuoteSize} onChange={e => set('maxQuoteSize', e.target.value)} /><i>BTC</i></div></label>
      <label><span>Delta 紧急停止阈值</span><div className="input-with-unit"><input type="number" step="0.001" value={form.deltaLimit} onChange={e => set('deltaLimit', e.target.value)} /><i>BTC</i></div></label>
      <label><span>最大对冲延迟</span><div className="input-with-unit"><input type="number" step="50" value={form.maxHedgeLatencyMs} onChange={e => set('maxHedgeLatencyMs', e.target.value)} /><i>ms</i></div></label>
      {error && <div className="inline-error"><Warning size={18} weight="fill" />{error}</div>}
      <button className="primary full" onClick={save} disabled={saving}>{saving ? '保存中…' : '保存并重新计算报价'}</button>
    </aside>
  </div>
}

export function App() {
  const [state, setState] = useState<SystemState | null>(null)
  const [error, setError] = useState('')
  const [settings, setSettings] = useState(false)
  const [busy, setBusy] = useState('')
  const [fillSide, setFillSide] = useState<Side>('SELL')

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

  const quote = useMemo(() => state?.quotes.find(q => q.side === fillSide), [state, fillSide])
  const control = async (action: string) => {
    setBusy(action)
    try { await api('/api/control', { method: 'POST', body: JSON.stringify({ action }) }) }
    catch (e) { setError(e instanceof Error ? e.message : '控制失败') }
    finally { setBusy('') }
  }
  const simulate = async () => {
    if (!quote) return
    setBusy('fill')
    try { await api('/api/sim/fill', { method: 'POST', body: JSON.stringify({ side: fillSide, size: Math.min(0.01, quote.size), role: 'maker' }) }) }
    catch (e) { setError(e instanceof Error ? e.message : '模拟成交失败') }
    finally { setBusy('') }
  }

  if (!state) return error ? <main className="fatal"><Warning size={32} weight="fill" /><h1>无法连接交易服务</h1><p>{error}</p><code>npm run dev</code></main> : <Skeleton />

  return <div className="app">
    <header className="topbar">
      <div className="brand"><span>BT</span><ArrowsLeftRight size={17} /><span>GMO</span><small>MARKET MAKING / HEDGE</small></div>
      <nav>
        <span className={`mode ${state.mode}`}>{state.mode === 'simulation' ? '模拟环境' : '实盘环境'}</span>
        <span className={`live-pill ${state.running ? 'on' : ''}`}><i />{state.running ? '策略运行中' : '报价已暂停'}</span>
        <button className="icon-button" onClick={() => setSettings(true)} aria-label="打开策略参数"><SlidersHorizontal size={20} /></button>
      </nav>
    </header>

    <main className="shell">
      {error && <div className="banner"><Warning size={18} weight="fill" /><span>{error}</span><button onClick={() => setError('')}>关闭</button></div>}
      <section className="intro">
        <div><span className="eyebrow">LIQUIDITY OPERATIONS / {state.market.symbol}</span><h1>做市与即时对冲</h1><p>以 GMO 深度为内侧基准，在 BitTrade 向外报价；客户成交后同量反向对冲。</p></div>
        <div className="controls">
          <div className="sim-control">
            <button onClick={() => setFillSide('SELL')} className={fillSide === 'SELL' ? 'active' : ''}>客户买入</button>
            <button onClick={() => setFillSide('BUY')} className={fillSide === 'BUY' ? 'active' : ''}>客户卖出</button>
            <button className="simulate" onClick={simulate} disabled={!state.running || busy === 'fill'}><Lightning size={17} weight="fill" />{busy === 'fill' ? '对冲中…' : '模拟成交 0.01 BTC'}</button>
          </div>
          <button className="secondary" onClick={() => control(state.running ? 'pause' : 'resume')} disabled={!!busy}>{state.running ? <Pause size={18} weight="fill" /> : <Play size={18} weight="fill" />}{state.running ? '暂停报价' : '恢复报价'}</button>
          <button className={`kill ${state.killSwitch ? 'active' : ''}`} onClick={() => control(state.killSwitch ? 'reset-kill' : 'kill')} disabled={!!busy}><Siren size={18} weight="fill" />{state.killSwitch ? '解除急停' : '紧急停止'}</button>
        </div>
      </section>

      <section className="metrics-strip">
        <Metric label="本日净收益" value={`${state.pnl.net >= 0 ? '+' : ''}¥${jpy.format(state.pnl.net)}`} note={`价差 +¥${jpy.format(state.pnl.spread)} · 费用 −¥${jpy.format(state.pnl.hedgeCosts)}`} tone={state.pnl.net >= 0 ? 'positive' : 'negative'} />
        <Metric label="双边成交" value={`${state.metrics.fillCount}`} note="全部已建立客户/对冲关联" />
        <Metric label="GMO 最优价差" value={`¥${jpy.format(state.market.ask - state.market.bid)}`} note={`深度 ${decimal.format(state.market.bidSize + state.market.askSize)} BTC`} />
        <Metric label="对账异常" value={`${state.metrics.exceptionCount}`} note={state.reconciliation.status === 'matched' ? '当前无未处理差额' : '需要立即处理'} tone={state.metrics.exceptionCount ? 'negative' : ''} />
      </section>

      <div className="primary-grid"><MarketLadder state={state} /><Reconciliation state={state} /></div>
      <Trades state={state} />

      <section className="bottom-grid">
        <div className="pnl-section">
          <div className="section-head"><div><span className="eyebrow">P&amp;L BREAKDOWN</span><h2>收益构成</h2></div></div>
          <div className="pnl-equation">
            <div><span>价差收益</span><b>+¥{jpy.format(state.pnl.spread)}</b></div><i>+</i>
            <div><span>客户手续费</span><b>+¥{jpy.format(state.pnl.clientFees)}</b></div><i>−</i>
            <div><span>GMO 成本</span><b>¥{jpy.format(state.pnl.hedgeCosts)}</b></div><i>=</i>
            <div className="net"><span>交易净收益</span><b>{state.pnl.net >= 0 ? '+' : ''}¥{jpy.format(state.pnl.net)}</b></div>
          </div>
        </div>
        <div className="audit-section">
          <div className="section-head"><div><span className="eyebrow">AUDIT TRAIL</span><h2>审计留痕</h2></div><a className="download" href="/api/reconciliation/export"><DownloadSimple size={17} />导出日结</a></div>
          <div className="event-list">{state.events.slice(0, 5).map(event => <div className={`event ${event.level}`} key={event.id}><i /> <span>{time(event.timestamp)}</span><b>{event.message}</b></div>)}</div>
        </div>
      </section>
    </main>

    <footer><span>报价依据：GMO Coin · 客户侧：BitTrade</span><span>数据留存：JSONL · 对账公式版本 v1.0</span><button onClick={() => window.location.reload()}><ArrowClockwise size={15} />刷新连接</button></footer>
    {settings && <Settings state={state} onClose={() => setSettings(false)} onSaved={() => undefined} />}
  </div>
}
