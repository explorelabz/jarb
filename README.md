# BitTrade × GMO 做市对冲系统

Python 异步交易编排 + Rust 核心计算模块。系统依据《流动性收益及对账逻辑》实现：以 GMO Coin 订单簿为对冲基准，在 BitTrade 向外加价挂出双边报价；客户成交后在 GMO 执行同数量反向交易，并持续计算收益、Delta 和审计记录。

## 架构

```text
GMO Public WS ──┐
REST 看门狗 5s ─┤
BalanceCache ────┼→ QuoteEngine → ExecutionGateway → BitTrade
RiskGate ────────┘                   │
                                    ↓
BitTrade 私有 WS → FillTracker → HedgeWorker → GMO FAK
       REST 5s 兜底       │             │
                          └──── SQLite WAL StateStore
```

组件之间使用事件总线通信，订单、成交、余额、审计和对冲意图先写入 SQLite WAL，再执行外部副作用。Python 负责网络 I/O 和业务编排，Rust 负责计算热路径。数量对账在 Rust 内部转换为 `1e-8` 定点整数，避免浮点累计造成伪差额。

## 已实现

- `BitTrade Ask = GMO Ask × (1 + spread)`；`BitTrade Bid = GMO Bid × (1 - spread)`。
- 挂单量不超过 GMO 最优价深度与策略上限。
- Maker 场景下，每个币种的价差必须高于 BitTrade Maker 费率 + GMO Taker 手续费 + 预期滑点；Maker 费率支持正成本或负返佣。
- 客户成交与 GMO 反向成交以 ID 关联，记录价格、数量、费用和延迟。
- `Σ BitTrade signed qty + Σ GMO signed qty = Delta`。
- Delta 阈值、暂停报价、紧急停止和对冲延迟监控。
- Rust 核心计算 P99 在线采样。
- JSONL 审计留痕和 JSON 日结导出。
- 模拟模式默认约每 2 秒轮换一个已启用币种和买卖方向，自动生成 BitTrade 模拟挂单成交、GMO 反向对冲、PnL、Delta、延迟与审计数据；暂停、Kill 或底仓不足时自动停止生成。
- GMO HMAC 与 BitTrade Signature V2 异步 API 适配器。
- 可从控制台切换模拟/线上模式；线上模式以 GMO Public WebSocket `orderbooks` 全量快照为主行情，最多 8 个订阅按 1.1 秒间隔串行发送以规避 `ERR-5003`。
- REST orderbook 保留为 5 秒看门狗；WebSocket 行情超过默认 800 ms 即停报价，陈旧或断线时切换 REST 并发送 Lark/飞书告警。
- 可在设置抽屉配置或清除 GMO、BitTrade API Key，状态与日志只显示脱敏提示。
- 币种选择来自 GMO 与 BitTrade 公共交易规则的实时交集，只保留双方均在线且开放 API 交易的 JPY 现货币种。
- 自动合并双方最小/最大下单量、数量步长与价格步长；Rust 核心支持小数日元报价。
- 最多可同时启用 8 个币种；每个币种独立维护行情、报价、仓位、Delta、成交与 P&amp;L，并在同一异步循环中并发刷新。
- 持久化订单状态机：`NEW → PLACING → OPEN/PARTIAL → CANCELING → CANCELED/FILLED`；网络结果不确定时进入 `UNKNOWN`，必须查询确认。
- 成交以数据库唯一键 `(order_id, trade_id)` 去重，并使用交易所累计成交量减去已记录累计量计算对冲差额。
- 对冲意图在 GMO 下单前落库；GMO FAK 下单后轮询 executions 与订单终态，成交确认超时会抛错并进入重试/升级，避免因结算延迟把空结果误判成零成交后立刻重复下单。
- 对冲延迟按“BitTrade 成交发生 → GMO executions 确认”记录，真实成交滑点写入审计，P95 直接进入自动 Disarm 风控。
- 按 endpoint 分组的令牌桶和优先队列保证“对冲 / kill / 撤单”优先于挂单和查询；请求获得令牌后并发派发，慢查询不会串行阻塞对冲；429 使用指数退避。
- 余额每 10 秒与两家交易所对齐，成交后保守地本地扣减；可挂量取策略上限、GMO 对侧深度、BitTrade 余额和 GMO 对冲余额的最小值，再乘 0.7 安全系数。
- 报价只有价格偏离、深度变化或剩余量达到阈值才重挂，并严格执行先撤单确认、后用新 `client_order_id` 挂单。撤单确认采用最长 10 秒退避轮询，`UNKNOWN` 会先主动查询对账再决定是否 Disarm。
- 报价前读取 BitTrade `/market/depth`；会穿越 BitTrade 当前最优价的方向直接跳过。post-only 拒单只放弃该侧并等待下轮重算，其他下单错误仍会 Disarm。
- GMO 对冲费率按基础币自动设置：BTC/ETH/XRP/DAI 为 5 bps，其余为 9 bps；每个币种单独执行盈利下限校验。默认 `SPREAD_BPS=25`。
- BitTrade 与 GMO 可分别配置 JPY 和每个基础币的策略底仓额度；任一币对的四项底仓或实际余额有一项为 0，整对禁止做市和对冲并撤销该币对挂单。
- 支持 Lark/飞书 Bot Webhook 底仓报警；相同故障默认 5 分钟内只推送一次，Webhook 仅保存在本机 SQLite，接口只返回脱敏提示。

## 环境准备

需要 Python 3.12、uv、Rust stable 和 Node.js 20+。

```bash
uv sync
uv run maturin develop --manifest-path rust-core/Cargo.toml --release
npm install
npm run dev
```

打开 <http://127.0.0.1:5173>。默认运行模拟模式，不需要 API Key。点击右上角设置可切换“线上模式”；切换时服务会先连接 GMO 公开行情，成功后才更新模式。

API Key 也可继续通过环境变量注入：

```dotenv
GMO_API_KEY=...
GMO_SECRET_KEY=...
BITTRADE_ACCESS_KEY=...
BITTRADE_SECRET_KEY=...
BITTRADE_ACCOUNT_ID=...
BITTRADE_MAKER_FEE_BPS=0
SPREAD_BPS=25
EXPECTED_SLIPPAGE_BPS=1.5
STALE_MARKET_MS=800
TRADING_MODE=online
ARM_CONFIRMATION_PHRASE="ARM JARB LIVE"
KILL_SENTINEL=data/KILL
```

通过界面填写的密钥只保存在 FastAPI 进程内存中，服务重启后会清除。完整密钥不会进入 SSE 状态、接口响应或审计日志；响应只包含“是否已配置”和 Key 末四位提示。

`BITTRADE_MAKER_FEE_BPS` 和设置界面的 Maker 费率使用 bps：正数表示交易成本，负数表示返佣，必须按账户实际 VIP 等级填写。GMO Taker 费率由币种自动决定，不能用一个全局值覆盖。`expectedSlippageBps` 应根据审计中的真实滑点分布定期校准（建议取包含行情漂移后的 P90）。订单数量与价格在适配器边界使用 `Decimal` 按共同 step/tick 对齐后再序列化，避免二进制浮点尾数进入交易所请求。

## 测试与性能基准

```bash
npm test
npm run bench
npm run build
```

基准测试报告 Rust 函数经 PyO3 跨语言调用后的吞吐和采样 P50/P99，因此包含 FFI 成本，比仅测试 Rust 内部函数更接近真实调用路径。

## 线上模式安全边界

`TRADING_MODE=online` 只连接真实行情和私有查询，进程每次启动都强制回到 `DISARMED`。启动恢复会校准交易所时间、核对本地与 BitTrade 未结订单、补拉成交并处理未决对冲；发现非本系统订单或无法确认的 GMO 对冲时不会开放 Arm。

操作员必须在页面输入 `ARM_CONFIRMATION_PHRASE` 二次确认，才会临时开放自动撤挂单；默认 60 分钟后自动 Disarm。Pause、Disarm、kill、行情超过 `staleMarketMs`、Delta/亏损/延迟/对冲失败超限，都会停止报价并执行 cancel-all 后轮询到未结订单为空。创建 `data/KILL` 可从进程外触发急停；移除文件并在 UI 解除 kill 后仍需重新恢复和 Arm。

这是会提交真实订单的交易软件。首次使用必须在隔离的小额账户验证 API 权限、最小下单量、手续费、余额字段和交易所维护时行为；SQLite 适用于单机运行，不支持两个实例同时管理同一账户。

连接配置接口：

- `GET /api/symbols`：计算两家交易所可对冲币种的实时交集，结果缓存 5 分钟；传 `refresh=true` 可强制刷新。
- `GET /api/connection`：返回模式、连接状态和脱敏后的配置状态。
- `PATCH /api/connection`：切换模式，增量设置或清除密钥；未提交的密钥字段会保留当前值。
- `GET /api/risk`：返回 Arm、恢复、kill 和自动 Disarm 原因。
- `POST /api/risk/arm`：提交确认短语与操作者，限时开放实盘执行。
- `POST /api/risk/disarm`：撤销实盘权限并撤销所有未结挂单。
- `GET /api/inventory`：返回两家交易所的多资产底仓额度、禁用币对与脱敏 Webhook 状态。
- `PATCH /api/inventory`：更新 BitTrade/GMO 底仓与 Lark Webhook；资产金额为 0 会立即禁用相关币对。

启用或停用币种时后端会再次校验交集，并拒绝停用存在未对冲 Delta 的币种。控制台顶部可在并发策略间切换观察视图，急停和暂停控制对全部币种生效；日结导出按币种分别包含成交、对冲、Delta 和 P&amp;L。

## 关键目录

- `rust-core/src/lib.rs`：Rust/PyO3 核心计算。
- `backend/core.py`：Python 到 Rust 的唯一计算边界。
- `backend/service.py`：组件生命周期、API 状态投影和多币种调度。
- `backend/engine/`：StateStore、RiskGate、QuoteEngine、ExecutionGateway、FillTracker、HedgeWorker、恢复与限频组件。
- `backend/adapters.py`：BitTrade/GMO 异步签名适配器。
- `backend/main.py`：FastAPI 接口与 SSE。
- `benchmarks/core_bench.py`：跨 FFI 性能基准。
- `src/`：React 交易控制台。

官方接口：

- BitTrade API: <https://api-doc.bittrade.co.jp/>
- GMO Coin API: <https://api.coin.z.com/docs/>
