# 做市对冲系统

Python 异步交易编排 + Rust 核心计算模块。系统依据《流动性收益及对账逻辑》实现：以 GMO Coin 订单簿为对冲基准，在 BitTrade 向外加价挂出双边报价；客户成交后在 GMO 执行同数量反向交易，并持续计算收益、Delta 和审计记录。

## 架构

```text
GMO Public WS ──┐
REST 看门狗 5s ─┤
BalanceCache ────┼→ QuoteEngine → ExecutionGateway → BitTrade
RiskGate ────────┘                   │
                                    ↓
BitTrade 私有 WS → FillTracker → HedgeWorker → GMO SOK(Post-Only) → 超时撤单 → 剩余量 GMO FAK
       REST 5s 兜底       │             │
                          └──── SQLite WAL StateStore
```

组件之间使用事件总线通信，订单、余额和审计先写入 SQLite WAL，再执行外部副作用；每笔成交与对应的反向对冲意图在同一个 SQLite 事务中提交。Python 负责网络 I/O 和业务编排，Rust 负责计算热路径。数量对账在 Rust 内部转换为 `1e-8` 定点整数，避免浮点累计造成伪差额。

## 已实现

- `BitTrade Ask = GMO Ask × (1 + spread)`；`BitTrade Bid = GMO Bid × (1 - spread)`。
- 挂单量使用 GMO `maxHedgeSlippageBps` 范围内的累计盘口深度，不再被 L1 单档卡死；容量仍受余额、策略与单笔风控上限约束。
- 每个币种的盈利下限使用 GMO SOK Maker 费率与 FAK Taker 费率按预计被动成交比例混合计算，并计入累计深度滑点；Maker 费率支持正成本或负返佣。
- 客户成交与 GMO 反向成交以 ID 关联，记录价格、数量、费用和延迟。
- `Σ BitTrade signed qty + Σ GMO signed qty = Delta`。
- Delta 阈值、暂停报价、紧急停止和对冲延迟监控。
- Rust 核心计算 P99 在线采样。
- JSONL 审计留痕和 JSON 日结导出。
- Paper 模式在交易所边界注入有状态的 FakeBitTrade / FakeGmo；行情、挂撤单、部分成交、FillTracker、HedgeWorker、风控与 SQLite 状态机和 Live 完全共用。
- GMO HMAC 与 BitTrade Signature V2 异步 API 适配器。
- 模式语义为 `paper/live`；切换后需重启服务。Live 以 GMO Public WebSocket `orderbooks` 全量快照为主行情，最多 8 个订阅按 1.1 秒间隔串行发送以规避 `ERR-5003`。
- REST orderbook 保留为 5 秒看门狗；WebSocket 行情超过默认 800 ms 即停报价，陈旧或断线时切换 REST 并发送 Lark/飞书告警。
- 可在设置抽屉配置或清除 GMO、BitTrade API Key，状态与日志只显示脱敏提示。
- 币种选择来自 GMO 与 BitTrade 公共交易规则的实时交集，只保留双方均在线且开放 API 交易的 JPY 现货币种。
- 自动合并双方最小/最大下单量、数量步长与价格步长；Rust 核心支持小数日元报价。
- 最多可同时启用 8 个币种；每个币种独立维护行情、报价、仓位、Delta、成交与 P&amp;L，并在同一异步循环中并发刷新。
- 持久化订单状态机：`NEW → PLACING → OPEN/PARTIAL → CANCELING → CANCELED/FILLED`；网络结果不确定时进入 `UNKNOWN`，必须查询确认。
- 成交以数据库唯一键 `(order_id, trade_id)` 去重，并使用交易所累计成交量减去已记录累计量计算对冲差额。
- 对冲意图在 GMO 下单前落库；先用最新 GMO 最优被动价提交 `LIMIT + SOK`，默认等待 800ms。未全成时先确认撤单/终态，再仅对剩余量提交 MARKET/FAK；两个订单号、部分成交进度和实际混合手续费均在关键边界持久化，避免重启后重复对冲。
- 对冲延迟按“BitTrade 成交发生 → GMO executions 确认”记录，真实成交滑点写入审计，P95 直接进入自动 Disarm 风控。默认限额为 2500ms；配置不得低于 `gmoPostOnlyTimeoutMs + 1200ms`，避免 800ms 被动等待本身必然触发旧的 1 秒阈值。
- 按 endpoint 分组的令牌桶和优先队列保证“对冲 / kill / 撤单”优先于挂单和查询；请求获得令牌后并发派发，慢查询不会串行阻塞对冲；429 使用指数退避。
- 余额每 10 秒与两家交易所对齐，成交后保守地本地扣减；可挂量取策略上限、GMO 对侧深度、BitTrade 余额和 GMO 对冲余额的最小值，再乘 0.7 安全系数。
- BitTrade 报价从自身多档盘口中寻找满足最低边际且前方累计量不超过 `queueBudget` 的价位，再前进一档进入队列；默认只有价格偏离 8bps、深度变化 60% 或剩余量低于 25% 才重挂。替换仍严格执行先撤单确认、后用新 `client_order_id` 挂单。
- 报价前读取 BitTrade `/market/depth`；会穿越 BitTrade 当前最优价的方向直接跳过。post-only 拒单只放弃该侧并等待下轮重算，其他下单错误仍会 Disarm。
- GMO FAK Taker 费率按基础币自动设置：BTC/ETH/XRP/DAI 为 5 bps，其余为 9 bps；SOK Maker 默认分别为 -1 bps / -3 bps。两者都可按币种覆盖，每个币种单独执行混合盈利下限校验。
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

打开 <http://127.0.0.1:5173>。默认运行 Paper 模式，不需要 API Key。Paper 会自动完成恢复、以 `ARM JARB PAPER` 走完整 Arm 门禁，然后由 Fake 交易所撮合真实引擎挂单。设置页可开关部分成交、dust、重复/乱序、撤单竞速、GMO 部分 FAK、成交延迟、post-only 拒单、429 与网络超时等故障注入。

控制面必须配置具名操作员 Token，除 `/api/health` 外的所有 API、SSE 和导出均要求 `Authorization: Bearer <token>`。控制台首次打开会提示输入 Token，Token 只保存在页面内存中、刷新即清除；不要把 Token 放进 URL：

```dotenv
JARB_OPERATOR_TOKENS="alice=<至少32字符的随机token>,bob=<另一个至少32字符的随机token>"
```

每个 Token 必须唯一绑定一个真实操作员。`REQUIRE_DUAL_ARM_APPROVAL=false` 时，当前登录操作员可独立执行包括 Arm、修改交易所凭据和修改风控限额在内的全部操作；actor 仍会写入审计。设为 `true` 才会启用双人确认，请求体里的自报姓名不会被接受。

Live 控制面不要直接暴露公网。后端应仅绑定 loopback，通过 SSH 隧道访问，或在 Nginx 前配置 mTLS。应用 CSP 只是纵深防御，不能替代网络隔离。

启用双人确认时，修改交易所凭据或风控限额的第一位操作员 PATCH 后会收到 HTTP 202 和 `approvalId`；第二位操作员从自己的浏览器或 CLI 调用 `POST /api/approvals/<approvalId>/approve`；随后任一参与者在五分钟内携带 `X-JARB-Approval: <approvalId>` 重放完全相同的 PATCH。单操作员模式下 PATCH 会直接执行，不产生审批 ID。

API Key 也可继续通过环境变量注入：

```dotenv
GMO_API_KEY=...
GMO_SECRET_KEY=...
BITTRADE_ACCESS_KEY=...
BITTRADE_SECRET_KEY=...
BITTRADE_ACCOUNT_ID=...
BITTRADE_MAKER_FEE_BPS=0
SPREAD_BPS=25
EXPECTED_SLIPPAGE_BPS=3
MAX_HEDGE_SLIPPAGE_BPS=3
EXPECTED_PASSIVE_FILL_RATIO=0.8
GMO_POST_ONLY_TIMEOUT_MS=800
MAX_HEDGE_LATENCY_MS=2500
MAX_HEDGE_P95_MS=2500
BITTRADE_QUEUE_BUDGET=0.05
STALE_MARKET_MS=3000
TRADING_MODE=live
ARM_CONFIRMATION_PHRASE="ARM JARB LIVE"
REQUIRE_DUAL_ARM_APPROVAL=false
KILL_SENTINEL=data/KILL
GMO_TAKER_FEE_BPS_OVERRIDES="BTC=5,ETH=5,XRP=5,DAI=5"
GMO_MAKER_FEE_BPS_OVERRIDES="BTC=-1,ETH=-1,XRP=-1,DAI=-1"
MAX_SINGLE_ORDER_JPY=250000
MAX_DAILY_VOLUME_JPY=5000000
MAX_DAILY_LOSS_JPY=100000
```

`ARM_CONFIRMATION_PHRASE` 没有代码内默认值；未显式配置时，服务会拒绝实盘 Arm。示例短语必须在部署前替换。

通过界面填写的密钥只保存在 FastAPI 进程内存中，服务重启后会清除。完整密钥不会进入 SSE 状态、接口响应或审计日志；响应只包含“是否已配置”和 Key 末四位提示。

`BITTRADE_MAKER_FEE_BPS` 和设置界面的 Maker 费率使用 bps：正数表示交易成本，负数表示返佣，必须按账户实际 VIP 等级填写。GMO Taker/Maker 费率可分别通过 `GMO_TAKER_FEE_BPS_OVERRIDES`、`GMO_MAKER_FEE_BPS_OVERRIDES` 或策略 API 按币种覆盖。`maxHedgeSlippageBps` 同时控制可计入容量的 GMO 累计深度，盈利校验会取它与 `expectedSlippageBps` 的较大值，避免扩大容量却漏算滑点。订单数量与价格在适配器边界使用 `Decimal` 按共同 step/tick 对齐后再序列化。

## 测试与性能基准

```bash
npm test
npm run bench
npm run build
```

基准测试报告 Rust 函数经 PyO3 跨语言调用后的吞吐和采样 P50/P99，因此包含 FFI 成本，比仅测试 Rust 内部函数更接近真实调用路径。

## Live 模式安全边界

`TRADING_MODE=live` 只连接真实行情和私有查询，进程每次启动都强制回到 `DISARMED`。启动恢复会校准交易所时间、核对本地与 BitTrade 未结订单、补拉成交并处理未决对冲；发现非本系统订单或无法确认的 GMO 对冲时不会开放 Arm。

默认要求两个不同的已认证操作员在 5 分钟内依次输入 `ARM_CONFIRMATION_PHRASE`，才会临时开放自动撤挂单；身份来自各自唯一的 `JARB_OPERATOR_TOKENS`，不信任请求体自报的 actor。默认 60 分钟后自动 Disarm。单笔、日成交、日亏损、Delta、对冲失败与延迟限额可通过 `/api/risk/limits` 或界面调整，修改时会先 Disarm。Pause、Disarm、kill、行情超过 `staleMarketMs` 或任一风险限额触发，都会停止报价并执行 cancel-all 后轮询到未结订单为空。创建 `data/KILL` 可从进程外触发急停；移除文件并在 UI 解除 kill 后仍需重新恢复和 Arm。

这是会提交真实订单的交易软件。首次使用必须在隔离的小额账户验证 API 权限、最小下单量、手续费、余额字段和交易所维护时行为；SQLite 适用于单机运行，不支持两个实例同时管理同一账户。

连接配置接口：

- `GET /api/symbols`：计算两家交易所可对冲币种的实时交集，结果缓存 5 分钟；传 `refresh=true` 可强制刷新。
- `GET /api/connection`：返回模式、连接状态和脱敏后的配置状态。
- `PATCH /api/connection`：增量设置或清除当前模式的密钥；Paper/Live 切换需修改 `TRADING_MODE` 后重启。
- `GET/PATCH /api/paper/scenarios`：查询或动态修改 Paper 撮合故障开关与概率。
- `POST /api/paper/fill`：向 FakeBitTrade 当前挂单注入一笔成交，仍通过 FillTracker 与 HedgeWorker 处理。
- `GET /api/risk`：返回 Arm、恢复、kill 和自动 Disarm 原因。
- `POST /api/risk/arm`：已认证操作员提交确认短语，限时开放实盘执行；操作者身份只取 Bearer Token。
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
