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

## 环境准备

需要 Python 3.12、uv、Rust stable 和 Node.js 20+。

```bash
uv sync
uv run maturin develop --manifest-path rust-core/Cargo.toml --release
npm install
npm run dev
```

打开 <http://127.0.0.1:5173>。默认只运行模拟环境，不需要 API Key。

## 测试与性能基准

```bash
npm test
npm run bench
npm run build
```

基准测试报告 Rust 函数经 PyO3 跨语言调用后的吞吐和采样 P50/P99，因此包含 FFI 成本，比仅测试 Rust 内部函数更接近真实调用路径。

## 实盘安全

代码默认拒绝进入实盘。即使配置 `TRADING_MODE=live`，仍必须同时设置：

```dotenv
LIVE_TRADING=true
ARM_LIVE_TRADING=I_UNDERSTAND
```

API 密钥只由 FastAPI 服务读取，绝不能暴露给前端。当前版本提供交易所签名适配器与完整模拟闭环，但自动实盘撤挂单循环仍保持关闭。上线前必须完成小额账户验证：最小下单单位、价格步长、BitTrade 私有成交订阅、GMO 部分成交补单、断线重放、余额预检、限频、时钟同步、主备切换和不可篡改审计存储。

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
