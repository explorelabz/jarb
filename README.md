# BitTrade × GMO 做市对冲系统

Python 异步交易编排 + Rust 核心计算模块。系统依据《流动性收益及对账逻辑》实现：以 GMO Coin 订单簿为对冲基准，在 BitTrade 向外加价挂出双边报价；客户成交后在 GMO 执行同数量反向交易，并持续计算收益、Delta 和审计记录。

## 架构

```text
React 交易台
      │ SSE / REST
FastAPI 异步服务
      ├── BitTrade / GMO 异步 HTTP 适配器
      ├── 策略状态机、风控、审计与日结
      └── PyO3 FFI
             └── Rust 原生核心
                 ├── 双边报价
                 ├── Maker 盈利下限
                 ├── 对冲方向
                 ├── 交易损益
                 └── 定点数量 Delta 对账
```

Python 负责网络 I/O 和业务编排，Rust 负责计算热路径。数量对账在 Rust 内部转换为 `1e-8` 定点整数，避免浮点累计造成伪差额。

## 已实现

- `BitTrade Ask = GMO Ask × (1 + spread)`；`BitTrade Bid = GMO Bid × (1 - spread)`。
- 挂单量不超过 GMO 最优价深度与策略上限。
- Maker 场景下，价差必须高于 GMO 手续费 + 预期滑点。
- 客户成交与 GMO 反向成交以 ID 关联，记录价格、数量、费用和延迟。
- `Σ BitTrade signed qty + Σ GMO signed qty = Delta`。
- Delta 阈值、暂停报价、紧急停止和对冲延迟监控。
- Rust 核心计算 P99 在线采样。
- JSONL 审计留痕和 JSON 日结导出。
- GMO HMAC 与 BitTrade Signature V2 异步 API 适配器。
- 可从控制台切换模拟/线上模式；线上模式使用 GMO 实时公开行情。
- 可在设置抽屉配置或清除 GMO、BitTrade API Key，状态与日志只显示脱敏提示。
- 币种选择来自 GMO 与 BitTrade 公共交易规则的实时交集，只保留双方均在线且开放 API 交易的 JPY 现货币种。
- 自动合并双方最小/最大下单量、数量步长与价格步长；Rust 核心支持小数日元报价。
- 最多可同时启用 8 个币种；每个币种独立维护行情、报价、仓位、Delta、成交与 P&amp;L，并在同一异步循环中并发刷新。

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
TRADING_MODE=online
```

通过界面填写的密钥只保存在 FastAPI 进程内存中，服务重启后会清除。完整密钥不会进入 SSE 状态、接口响应或审计日志；响应只包含“是否已配置”和 Key 末四位提示。

## 测试与性能基准

```bash
npm test
npm run bench
npm run build
```

基准测试报告 Rust 函数经 PyO3 跨语言调用后的吞吐和采样 P50/P99，因此包含 FFI 成本，比仅测试 Rust 内部函数更接近真实调用路径。

## 线上模式安全边界

“线上模式”目前只读取 GMO 实时公开行情并计算 BitTrade 参考报价，不会向 BitTrade 或 GMO 自动提交真实订单。API Key 配置用于后续私有接口能力，并仍由后端独占；切换线上模式时界面和接口都要求显式确认这一安全边界。

自动实盘撤挂单循环仍保持关闭。正式开放真实交易前必须完成小额账户验证：最小下单单位、价格步长、BitTrade 私有成交订阅、GMO 部分成交补单、断线重放、余额预检、限频、时钟同步、主备切换和不可篡改审计存储。

连接配置接口：

- `GET /api/symbols`：计算两家交易所可对冲币种的实时交集，结果缓存 5 分钟；传 `refresh=true` 可强制刷新。
- `GET /api/connection`：返回模式、连接状态和脱敏后的配置状态。
- `PATCH /api/connection`：切换模式，增量设置或清除密钥；未提交的密钥字段会保留当前值。

启用或停用币种时后端会再次校验交集，并拒绝停用存在未对冲 Delta 的币种。控制台顶部可在并发策略间切换观察视图，急停和暂停控制对全部币种生效；日结导出按币种分别包含成交、对冲、Delta 和 P&amp;L。

## 关键目录

- `rust-core/src/lib.rs`：Rust/PyO3 核心计算。
- `backend/core.py`：Python 到 Rust 的唯一计算边界。
- `backend/service.py`：异步策略状态机和风险控制。
- `backend/adapters.py`：BitTrade/GMO 异步签名适配器。
- `backend/main.py`：FastAPI 接口与 SSE。
- `benchmarks/core_bench.py`：跨 FFI 性能基准。
- `src/`：React 交易控制台。

官方接口：

- BitTrade API: <https://api-doc.bittrade.co.jp/>
- GMO Coin API: <https://api.coin.z.com/docs/>
