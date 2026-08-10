from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from uuid import uuid4

import httpx

from .adapters import BitTradeAdapter, GmoAdapter
from .audit_store import AuditStore
from .config import Credentials
from .core import make_quotes, matched_trade, opposite_side, reconcile, validate_config
from .engine.balance import BalanceCache
from .engine.alerting import LarkWebhookNotifier
from .engine.events import EventBus
from .engine.execution_gateway import ExecutionGateway
from .engine.fill_tracker import BitTradePrivateWS, BitTradeRestFillSource, FillTracker
from .engine.hedge_worker import GmoHedgeExecutor, HedgeWorker
from .engine.market_feed import MarketFeed
from .engine.quote_engine import QuoteEngine, WorkingQuote
from .engine.rate_limit import PriorityRateLimiter
from .engine.recovery import RecoveryCoordinator
from .engine.risk import RiskGate, RiskLimits, RiskSnapshot
from .engine.state_store import StateStore
from .models import (
    AuditEvent, ClientFill, ConnectionState, ConnectionUpdate, HedgeFill, InstrumentRules, MarketTop,
    MatchedTrade, Metrics, Pnl, SimulatedFillRequest, StrategyConfig, SymbolRuntime, SystemState, utc_now,
)


class TradingService:
    def __init__(self, config: StrategyConfig, mode: str = "simulation", credentials: Credentials | None = None,
                 gmo: GmoAdapter | None = None, bittrade: BitTradeAdapter | None = None,
                 db_path: Path | str = Path("data/jarb.db")):
        validate_config(config)
        credentials = credentials or Credentials()
        base_asset = config.symbol.removesuffix("_JPY")
        self.started_ns = time.monotonic_ns()
        self.clients: dict[str, list[ClientFill]] = {}
        self.hedges: dict[str, list[HedgeFill]] = {}
        self.matched_trades: dict[str, list[MatchedTrade]] = {}
        self.audit_store = AuditStore()
        self.state_store = StateStore(db_path)
        self.events = EventBus()
        self.rate_limiter = PriorityRateLimiter()
        self.balance_cache = BalanceCache()
        self.quote_engine = QuoteEngine()
        self.risk_gate = RiskGate(self.state_store, RiskLimits(
            max_abs_delta=config.deltaLimit, max_hedge_p95_ms=config.maxHedgeLatencyMs,
        ))
        self.lock = asyncio.Lock()
        self.subscribers: set[asyncio.Queue[str]] = set()
        self.task: asyncio.Task | None = None
        self.core_samples_us: list[float] = []
        self._symbol_cache: list[InstrumentRules] = []
        self._symbol_cache_at = 0.0
        self._http_client = httpx.AsyncClient(timeout=3.0) if gmo is None or bittrade is None else None
        self.gmo = gmo or GmoAdapter(credentials.gmo_api_key, credentials.gmo_secret_key, self._http_client)
        self.bittrade = bittrade or BitTradeAdapter(credentials.bittrade_access_key, credentials.bittrade_secret_key,
                                                    credentials.bittrade_account_id, self._http_client)
        self.notifier = LarkWebhookNotifier(self._http_client)
        self.inventory_allocations: dict[str, dict[str, Decimal]] = {
            "bittrade": {"JPY": Decimal("1000000"), base_asset: Decimal("1")},
            "gmo": {"JPY": Decimal("1000000"), base_asset: Decimal("1")},
        }
        self.balance_cache.configure_allocations(self.inventory_allocations)
        self.execution_gateway = ExecutionGateway(
            self.bittrade, self.state_store, self.risk_gate, self.rate_limiter,
        )
        self.market_feed = MarketFeed(self.gmo, self.events)
        self.fill_tracker: FillTracker | None = None
        self.rest_fill_source: BitTradeRestFillSource | None = None
        self.hedge_worker: HedgeWorker | None = None
        self._engine_ready = False
        self._working_quotes: dict[tuple[str, str], WorkingQuote] = {}
        instrument = InstrumentRules(symbol=config.symbol, baseAsset=base_asset, minOrderSize=.0001,
                                     maxOrderSize=5, sizeStep=.0001, priceTick=1)
        market = MarketTop(symbol=config.symbol, bid=17_482_140, ask=17_493_860, bidSize=0.4382,
                           askSize=0.3167, timestamp=utc_now(), source="SIM")
        runtime = SymbolRuntime(instrument=instrument, config=config, market=market, quotes=make_quotes(market, config),
                                reconciliation=reconcile(config.symbol, [], []))
        self.clients[config.symbol] = []
        self.hedges[config.symbol] = []
        self.matched_trades[config.symbol] = []
        self.state = SystemState(
            mode=mode, running=True, killSwitch=False, market=market, quotes=runtime.quotes,
            position=0, reconciliation=reconcile(config.symbol, [], []), pnl=Pnl(), metrics=Metrics(),
            trades=[], events=[], config=config, connection=self._connection_state("connecting" if mode == "online" else "simulation"),
            instrument=instrument, activeSymbols=[config.symbol], symbolStates={config.symbol: runtime},
        )

    async def start(self) -> None:
        if self.task is None:
            await self.state_store.initialize()
            self._engine_ready = True
            await self._load_inventory()
            await self.risk_gate.restore()
            await self.rate_limiter.start()
            if self.state.mode == "online":
                await self._start_live_components()
            await RecoveryCoordinator(
                self.state_store, self.risk_gate, gateway=self.execution_gateway,
                gmo=self.gmo if self.state.mode == "online" else None,
                bittrade=self.bittrade if self.state.mode == "online" and self._bittrade_configured() else None,
                cancel_existing=True,
                reconcile_fills=self._reconcile_unsettled if self.fill_tracker else None,
            ).run()
            if self.state.mode == "online":
                try:
                    await self._refresh_online_market()
                except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                    self.state.connection = self._connection_state("error", self._safe_error(exc))
            await self._record("info", "system.started", f"策略以{'线上' if self.state.mode == 'online' else '模拟'}模式启动")
            self.task = asyncio.create_task(self._run(), name="market-loop")

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        await self._stop_live_components()
        await self.rate_limiter.stop()
        if self._engine_ready:
            await self.state_store.close()
            self._engine_ready = False
        await self.notifier.close()
        if self._http_client is not None:
            await self._http_client.aclose()

    async def stream(self) -> AsyncIterator[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        self.subscribers.add(queue)
        try:
            yield self.state.model_dump_json()
            while True:
                yield await queue.get()
        finally:
            self.subscribers.discard(queue)

    async def configure(self, patch: dict) -> None:
        if self.state.mode == "online" and self._engine_ready and self.risk_gate.armed:
            await self.disarm("strategy configuration changed", "operator")
        requested_symbols = patch.pop("symbols", None)
        legacy_symbol = patch.pop("symbol", None)
        if requested_symbols is None and legacy_symbol is not None:
            requested_symbols = [legacy_symbol]
        target_symbols = self.state.activeSymbols if requested_symbols is None else list(dict.fromkeys(
            str(value).strip().upper() for value in requested_symbols if str(value).strip()
        ))
        if not target_symbols:
            raise ValueError("至少需要启用一个对冲币种")
        if len(target_symbols) > 8:
            raise ValueError("为控制行情接口频率，最多同时启用 8 个币种")

        try:
            common = await self.common_symbols() if requested_symbols is not None else [
                runtime.instrument for runtime in self.state.symbolStates.values()
            ]
        except (httpx.HTTPError, RuntimeError) as exc:
            raise ValueError(f"无法验证两家交易所的共同币种：{self._safe_error(exc)}") from exc
        rules = {item.symbol: item for item in common}
        unsupported = [symbol for symbol in target_symbols if symbol not in rules]
        if unsupported:
            raise ValueError(f"以下币种并非两家交易所共同可交易币种：{', '.join(unsupported)}")
        removed = set(self.state.activeSymbols) - set(target_symbols)
        exposed = [symbol for symbol in removed if self.state.symbolStates[symbol].reconciliation.delta != 0]
        if exposed:
            raise ValueError(f"以下币种仍有未对冲仓位，不能停用：{', '.join(exposed)}")

        template = self.state.config.model_copy(update=patch)
        validate_config(StrategyConfig.model_validate(template.model_dump()))
        new_symbols = [symbol for symbol in target_symbols if symbol not in self.state.symbolStates]
        try:
            markets = await asyncio.gather(*(self.gmo.ticker(rules[symbol].baseAsset) for symbol in new_symbols))
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            raise ValueError(f"无法初始化新增币种行情：{self._safe_error(exc)}") from exc
        market_by_symbol = dict(zip(new_symbols, markets, strict=True))

        async with self.lock:
            next_states: dict[str, SymbolRuntime] = {}
            for symbol in target_symbols:
                instrument = rules[symbol]
                runtime_config = StrategyConfig.model_validate({
                    **template.model_dump(),
                    "symbol": symbol,
                    "maxQuoteSize": min(instrument.maxOrderSize, max(template.maxQuoteSize, instrument.minOrderSize)),
                    "deltaLimit": max(template.deltaLimit, instrument.minOrderSize),
                })
                existing = self.state.symbolStates.get(symbol)
                if existing:
                    existing.instrument = instrument
                    existing.config = runtime_config
                    existing.quotes = self._timed_quotes(existing.market, runtime_config, instrument)
                    next_states[symbol] = existing
                else:
                    market = market_by_symbol[symbol].model_copy(update={"source": "GMO" if self.state.mode == "online" else "SIM"})
                    next_states[symbol] = SymbolRuntime(
                        instrument=instrument, config=runtime_config, market=market,
                        quotes=self._timed_quotes(market, runtime_config, instrument),
                        reconciliation=reconcile(symbol, [], []),
                    )
                    self.clients[symbol] = []
                    self.hedges[symbol] = []
                    self.matched_trades[symbol] = []
            for symbol in removed:
                self.clients.pop(symbol, None)
                self.hedges.pop(symbol, None)
                self.matched_trades.pop(symbol, None)
            for symbol in target_symbols:
                base = next_states[symbol].instrument.baseAsset
                self.inventory_allocations["bittrade"].setdefault(base, Decimal("0"))
                self.inventory_allocations["gmo"].setdefault(base, Decimal("0"))
            self.balance_cache.configure_allocations(self.inventory_allocations)
            self.state.activeSymbols = target_symbols
            self.state.symbolStates = next_states
            self.state.config = next_states[target_symbols[0]].config
            self._sync_primary()
        await self._record("info", "strategy.updated", f"多币种策略已更新：{', '.join(target_symbols)}",
                           {**patch, "symbols": target_symbols})
        if self._engine_ready:
            await self._persist_inventory()
            await self._recompute_inventory_status(notify=False)
        if self.state.mode == "online" and self._engine_ready:
            await self._stop_live_components()
            await self._start_live_components()
            await RecoveryCoordinator(
                self.state_store, self.risk_gate, gateway=self.execution_gateway,
                gmo=self.gmo, bittrade=self.bittrade if self._bittrade_configured() else None,
                cancel_existing=True,
                reconcile_fills=self._reconcile_unsettled if self.fill_tracker else None,
            ).run()
        self._publish()

    async def common_symbols(self, force: bool = False) -> list[InstrumentRules]:
        if not force and self._symbol_cache and time.monotonic() - self._symbol_cache_at < 300:
            return self._symbol_cache
        gmo_rows, bittrade_rows = await asyncio.gather(self.gmo.symbols(), self.bittrade.symbols())
        gmo_spot = {
            str(row.get("symbol", "")).upper(): row for row in gmo_rows
            if row.get("symbol") and "_" not in str(row["symbol"])
        }
        bittrade_jpy = {
            str(row.get("base-currency", "")).upper(): row for row in bittrade_rows
            if str(row.get("quote-currency", "")).lower() == "jpy"
            and row.get("state") == "online" and row.get("api-trading") == "enabled"
        }
        common: list[InstrumentRules] = []
        for base_asset in sorted(gmo_spot.keys() & bittrade_jpy.keys(), key=lambda value: (value != "BTC", value)):
            gmo_rule = gmo_spot[base_asset]
            bittrade_rule = bittrade_jpy[base_asset]
            amount_precision = int(bittrade_rule.get("amount-precision", 8))
            price_precision = int(bittrade_rule.get("price-precision", 0))
            min_size = max(float(gmo_rule["minOrderSize"]), float(bittrade_rule["limit-order-min-order-amt"]))
            max_size = min(float(gmo_rule["maxOrderSize"]), float(bittrade_rule["limit-order-max-order-amt"]))
            if min_size > max_size:
                continue
            common.append(InstrumentRules(
                symbol=f"{base_asset}_JPY", baseAsset=base_asset,
                minOrderSize=min_size, maxOrderSize=max_size,
                sizeStep=max(float(gmo_rule["sizeStep"]), 10 ** -amount_precision),
                priceTick=max(float(gmo_rule["tickSize"]), 10 ** -price_precision),
            ))
        if not common:
            raise RuntimeError("两家交易所当前没有可用于 API 对冲的共同 JPY 币种")
        self._symbol_cache = common
        self._symbol_cache_at = time.monotonic()
        return common

    async def configure_connection(self, update: ConnectionUpdate) -> None:
        changing_online = update.mode == "online" and self.state.mode != "online"
        if changing_online and not update.confirmOnline:
            raise ValueError("切换线上模式前必须确认真实账户安全提示")

        gmo_key = "" if update.clearGmoCredentials else self.gmo.api_key
        gmo_secret = "" if update.clearGmoCredentials else self.gmo.secret_key
        bittrade_key = "" if update.clearBittradeCredentials else self.bittrade.access_key
        bittrade_secret = "" if update.clearBittradeCredentials else self.bittrade.secret_key
        bittrade_account = "" if update.clearBittradeCredentials else self.bittrade.account_id
        if update.gmoApiKey is not None:
            gmo_key = update.gmoApiKey.strip()
        if update.gmoSecretKey is not None:
            gmo_secret = update.gmoSecretKey.strip()
        if update.bittradeAccessKey is not None:
            bittrade_key = update.bittradeAccessKey.strip()
        if update.bittradeSecretKey is not None:
            bittrade_secret = update.bittradeSecretKey.strip()
        if update.bittradeAccountId is not None:
            bittrade_account = update.bittradeAccountId.strip()
        self.gmo.set_credentials(gmo_key, gmo_secret)
        self.bittrade.set_credentials(bittrade_key, bittrade_secret, bittrade_account)

        if update.mode == "online":
            self.state.connection = self._connection_state("connecting")
            self._publish()
            try:
                await self._refresh_online_market()
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                message = self._safe_error(exc)
                self.state.connection = self._connection_state("error", message)
                self._publish()
                raise ValueError(f"无法连接 GMO 线上行情：{message}") from exc
            self.state.mode = "online"
            if self._engine_ready:
                await self._stop_live_components()
                await self._start_live_components()
                await RecoveryCoordinator(
                    self.state_store, self.risk_gate, gateway=self.execution_gateway,
                    gmo=self.gmo, bittrade=self.bittrade if self._bittrade_configured() else None,
                    cancel_existing=True,
                    reconcile_fills=self._reconcile_unsettled if self.fill_tracker else None,
                ).run()
            await self._record("warning", "connection.online", "已连接真实账户，当前保持 DISARMED")
        else:
            self.state.mode = "simulation"
            if self._engine_ready:
                await self.risk_gate.disarm("switched to simulation mode", "operator")
                await self._stop_live_components()
            for runtime in self.state.symbolStates.values():
                runtime.market = runtime.market.model_copy(update={"source": "SIM", "timestamp": utc_now()})
                runtime.quotes = self._timed_quotes(runtime.market, runtime.config, runtime.instrument)
            self.state.connection = self._connection_state("simulation")
            self._sync_primary()
            await self._record("info", "connection.simulation", "已切换至模拟行情")
        self._publish()

    def connection_summary(self) -> dict:
        return {"mode": self.state.mode, **self.state.connection.model_dump()}

    async def control(self, action: str) -> None:
        if action == "resume":
            if self.state.killSwitch:
                raise ValueError("紧急停止仍处于启用状态")
            self.state.running = True
        elif action == "pause":
            self.state.running = False
        elif action == "kill":
            self.state.killSwitch = True
            self.state.running = False
        elif action == "reset-kill":
            self.state.killSwitch = False
        else:
            raise ValueError("未知控制指令")
        if self._engine_ready:
            if action == "pause":
                await self.disarm("operator paused quoting", "operator")
            elif action == "kill":
                await self.risk_gate.kill("operator kill switch", "operator")
                await self._cancel_all_live()
            elif action == "reset-kill":
                await self.risk_gate.reset_kill("operator")
                await RecoveryCoordinator(
                    self.state_store, self.risk_gate, gateway=self.execution_gateway,
                    gmo=self.gmo if self.state.mode == "online" else None,
                    bittrade=self.bittrade if self.state.mode == "online" and self._bittrade_configured() else None,
                    cancel_existing=True,
                    reconcile_fills=self._reconcile_unsettled if self.fill_tracker else None,
                ).run()
        level = "critical" if action == "kill" else "warning"
        await self._record(level, f"risk.{action}", {"resume": "报价已恢复", "pause": "报价已暂停", "kill": "紧急停止已触发", "reset-kill": "紧急停止已解除"}[action])
        self._publish()

    async def simulate_fill(self, request: SimulatedFillRequest):
        symbol = (request.symbol or self.state.activeSymbols[0]).upper()
        runtime = self.state.symbolStates.get(symbol)
        if runtime is None:
            raise ValueError("所选币种未启用")
        blockers = self.balance_cache.pair_blockers(runtime.instrument.baseAsset, require_actual=False)
        if blockers:
            await self._disable_symbol(symbol, blockers)
            raise ValueError(f"{symbol} 底仓不完整，禁止交易：{', '.join(blockers)}")
        async with self.lock:
            if not self.state.running or self.state.killSwitch:
                raise ValueError("策略未运行，无法接受模拟成交")
            quote = next((q for q in runtime.quotes if q.side == request.side), None)
            if quote is None:
                raise ValueError("当前方向没有可用报价")
            if request.size < runtime.instrument.minOrderSize:
                raise ValueError(f"最小成交量为 {runtime.instrument.minOrderSize:g} {runtime.instrument.baseAsset}")
            size = self._floor_size(min(request.size, quote.size), runtime.instrument.sizeStep)
            if size < runtime.instrument.minOrderSize:
                raise ValueError("当前报价深度低于两家交易所的共同最小下单量")
            client = ClientFill(id=f"BT-{uuid4().hex[:8]}", orderId=f"Q-{uuid4().hex[:8]}", symbol=symbol,
                side=request.side, price=quote.price, size=size,
                fee=-quote.price * size * runtime.config.bittradeMakerFeeBps / 10_000 if request.role == "maker"
                    else -quote.price * size * 0.001,
                role=request.role, timestamp=utc_now())
            self.clients[symbol].append(client)
            runtime.position += (1 if client.side == "BUY" else -1) * size

            latency_ms = random.randint(74, 203)
            slippage = runtime.config.expectedSlippageBps / 10_000
            side = opposite_side(client.side)
            hedge_price = self._ceil_price(runtime.market.ask * (1 + slippage * random.random()), runtime.instrument.priceTick) \
                if side == "BUY" else self._floor_price(runtime.market.bid * (1 - slippage * random.random()), runtime.instrument.priceTick)
            hedge = HedgeFill(id=f"GMO-{uuid4().hex[:8]}", orderId=f"H-{uuid4().hex[:8]}", clientFillId=client.id,
                symbol=client.symbol, side=side, price=hedge_price, size=size,
                fee=hedge_price * size * runtime.config.gmoFeeBps / 10_000, latencyMs=latency_ms,
                timestamp=utc_now(), status="filled")
            self.hedges[symbol].append(hedge)
            runtime.position += (1 if hedge.side == "BUY" else -1) * size
            trade = matched_trade(client, hedge)
            self.matched_trades[symbol].append(trade)
            runtime.trades.insert(0, trade)
            runtime.trades = runtime.trades[:50]
            self._recalculate(symbol)
            self._sync_primary()
        await self._record("info", "client.fill", f"BitTrade 本公司{'买入' if client.side == 'BUY' else '卖出'} {size} {runtime.instrument.baseAsset}", {"clientFillId": client.id, "symbol": symbol})
        await self._record("info", "hedge.filled", f"GMO 反向对冲完成，延迟 {latency_ms} ms", {"hedgeFillId": hedge.id, "clientFillId": client.id})
        self._publish()
        return trade

    def export_reconciliation(self) -> dict:
        primary = self.state.symbolStates[self.state.activeSymbols[0]]
        return {"generatedAt": utc_now(), "scope": "daily", "core": "Rust/PyO3",
                "formula": "Σ(client signed quantity) + Σ(hedge signed quantity) = delta",
                "result": primary.reconciliation.model_dump(), "pnl": primary.pnl.model_dump(),
                "symbols": {symbol: {
                    "result": runtime.reconciliation.model_dump(),
                    "clientFills": [x.model_dump() for x in self.clients[symbol]],
                    "hedgeFills": [x.model_dump() for x in self.hedges[symbol]],
                    "matchedTrades": [x.model_dump() for x in self.matched_trades[symbol]],
                    "pnl": runtime.pnl.model_dump(),
                } for symbol, runtime in self.state.symbolStates.items()}}

    async def _run(self) -> None:
        while True:
            refresh_ms = min(runtime.config.quoteRefreshMs for runtime in self.state.symbolStates.values())
            await asyncio.sleep(refresh_ms / 1000)
            if self.state.mode == "online":
                try:
                    await self._refresh_online_market()
                except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                    message = self._safe_error(exc)
                    was_error = self.state.connection.lastError == message
                    self.state.connection = self._connection_state("error", message)
                    if not was_error:
                        await self._record("warning", "market.disconnected", f"GMO 行情连接异常：{message}")
            else:
                async with self.lock:
                    for runtime in self.state.symbolStates.values():
                        mid = (runtime.market.bid + runtime.market.ask) / 2
                        move = (random.random() - 0.5) * mid * .0003
                        half = max(runtime.instrument.priceTick, mid * (.0003 + random.random() * .00008))
                        bid = self._floor_price(mid + move - half, runtime.instrument.priceTick)
                        ask = self._ceil_price(mid + move + half, runtime.instrument.priceTick)
                        depth_floor = runtime.instrument.minOrderSize
                        depth_cap = max(depth_floor, min(runtime.instrument.maxOrderSize, runtime.config.maxQuoteSize * 4))
                        runtime.market = runtime.market.model_copy(update={"bid": bid, "ask": ask,
                            "bidSize": self._floor_size(depth_floor + random.random() * (depth_cap - depth_floor), runtime.instrument.sizeStep),
                            "askSize": self._floor_size(depth_floor + random.random() * (depth_cap - depth_floor), runtime.instrument.sizeStep),
                            "timestamp": utc_now()})
                        if self.state.running:
                            runtime.quotes = self._timed_quotes(runtime.market, runtime.config, runtime.instrument)
            async with self.lock:
                self.state.metrics.uptimeSec = int((time.monotonic_ns() - self.started_ns) / 1_000_000_000)
                self._sync_primary()
            await self._enforce_risk()
            await self._run_live_quotes()
            self._publish()

    async def _refresh_online_market(self) -> None:
        symbols = list(self.state.activeSymbols)
        market_by_symbol = await self.market_feed.refresh({
            symbol: self.state.symbolStates[symbol].instrument.baseAsset for symbol in symbols
        })
        async with self.lock:
            for symbol, market in market_by_symbol.items():
                runtime = self.state.symbolStates.get(symbol)
                if runtime is None:
                    continue
                runtime.market = market
                if self.state.running:
                    runtime.quotes = self._timed_quotes(market, runtime.config, runtime.instrument)
            self.state.connection = self._connection_state("connected")
            self._sync_primary()

    def _connection_state(self, status: str, error: str | None = None) -> ConnectionState:
        return ConnectionState(
            status=status,
            gmoConfigured=bool(self.gmo.api_key and self.gmo.secret_key),
            gmoKeyHint=self._key_hint(self.gmo.api_key),
            bittradeConfigured=bool(self.bittrade.access_key and self.bittrade.secret_key and self.bittrade.account_id),
            bittradeKeyHint=self._key_hint(self.bittrade.access_key),
            lastError=error,
        )

    @staticmethod
    def _key_hint(value: str) -> str | None:
        return f"••••{value[-4:]}" if value else None

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        message = str(exc).strip() or exc.__class__.__name__
        return message[:240]

    def _timed_quotes(self, market: MarketTop, config: StrategyConfig, instrument: InstrumentRules):
        start = time.perf_counter_ns()
        result = make_quotes(market, config, instrument.priceTick)
        self.core_samples_us.append((time.perf_counter_ns() - start) / 1_000)
        self.core_samples_us = self.core_samples_us[-2000:]
        ordered = sorted(self.core_samples_us)
        if ordered:
            self.state.metrics.coreCalcP99Us = ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))]
        normalized = []
        for quote in result:
            size = self._floor_size(min(quote.size, instrument.maxOrderSize), instrument.sizeStep)
            if size >= instrument.minOrderSize:
                normalized.append(quote.model_copy(update={"size": size}))
        return normalized

    @staticmethod
    def _floor_size(value: float, step: float) -> float:
        result = (Decimal(str(value)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step))
        return float(result)

    @staticmethod
    def _floor_price(value: float, tick: float) -> float:
        return TradingService._floor_size(value, tick)

    @staticmethod
    def _ceil_price(value: float, tick: float) -> float:
        floored = TradingService._floor_price(value, tick)
        return floored if floored >= value else float(Decimal(str(floored)) + Decimal(str(tick)))

    def _recalculate(self, symbol: str) -> None:
        runtime = self.state.symbolStates[symbol]
        full_history = self.matched_trades[symbol]
        runtime.reconciliation = reconcile(symbol, self.clients[symbol], self.hedges[symbol])
        runtime.pnl = Pnl(
            spread=sum(t.spreadPnl for t in full_history), clientFees=sum(t.clientFee for t in full_history),
            hedgeCosts=sum(t.hedgeCost for t in full_history), net=sum(t.netPnl for t in full_history),
        )
        symbol_latencies = sorted(t.latencyMs for t in full_history)
        runtime.fillCount = len(full_history)
        runtime.hedgeP95Ms = symbol_latencies[min(len(symbol_latencies) - 1, int(len(symbol_latencies) * .95))] if symbol_latencies else 0
        all_trades = [trade for item in self.matched_trades.values() for trade in item]
        latencies = sorted(t.latencyMs for t in all_trades)
        self.state.metrics.hedgeP95Ms = latencies[min(len(latencies) - 1, int(len(latencies) * .95))] if latencies else 0
        self.state.metrics.fillCount = len(all_trades)
        if abs(runtime.reconciliation.delta) > runtime.config.deltaLimit:
            self.state.metrics.exceptionCount += 1
            self.state.killSwitch = True
            self.state.running = False

    def _sync_primary(self) -> None:
        if not self.state.activeSymbols:
            return
        runtime = self.state.symbolStates[self.state.activeSymbols[0]]
        self.state.instrument = runtime.instrument
        self.state.config = runtime.config
        self.state.market = runtime.market
        self.state.quotes = runtime.quotes
        self.state.position = runtime.position
        self.state.reconciliation = runtime.reconciliation
        self.state.pnl = runtime.pnl
        self.state.trades = runtime.trades

    async def _record(self, level: str, event_type: str, message: str, metadata: dict | None = None) -> None:
        event = AuditEvent(id=str(uuid4()), timestamp=utc_now(), level=level, type=event_type, message=message, metadata=metadata)
        self.state.events.insert(0, event)
        self.state.events = self.state.events[:80]
        await self.audit_store.append(event)
        if self._engine_ready:
            await self.state_store.audit(event_type, level, message, metadata=metadata)

    def risk_status(self) -> dict:
        return {
            "armed": self.risk_gate.armed,
            "armedUntil": self.risk_gate.armed_until,
            "recoveryComplete": self.risk_gate.recovery_complete,
            "killed": self.risk_gate.killed,
            "reason": self.risk_gate.last_reason,
        }

    async def arm(self, phrase: str, actor: str) -> dict:
        if self.state.mode != "online":
            raise ValueError("只有线上模式可以 arm")
        if not self._bittrade_configured() or not self.gmo.api_key or not self.gmo.secret_key:
            raise ValueError("BitTrade 与 GMO 私有 API 凭据必须全部配置")
        stale = [
            runtime.instrument.symbol for runtime in self.state.symbolStates.values()
            if self._market_age_ms(runtime.market.timestamp) > runtime.config.staleMarketMs
        ]
        if stale:
            raise ValueError(f"行情已过期，不能 Arm：{', '.join(stale)}")
        await self._start_live_components()
        await self._refresh_balances()
        enabled = 0
        for symbol, runtime in self.state.symbolStates.items():
            blockers = self.balance_cache.pair_blockers(runtime.instrument.baseAsset, require_actual=True)
            if blockers:
                await self._disable_symbol(symbol, blockers)
            else:
                self.state.disabledSymbols.pop(symbol, None)
                enabled += 1
        if enabled == 0:
            raise ValueError("所有币对均因底仓或实际余额为 0 被禁用，不能 Arm")
        await self.risk_gate.arm(phrase, actor)
        self.state.running = True
        await self._record("critical", "risk.armed", "实盘下单权限已临时启用", {"actor": actor})
        self._publish()
        return self.risk_status()

    async def disarm(self, reason: str, actor: str = "operator") -> dict:
        was_armed = self.risk_gate.armed
        await self.risk_gate.disarm(reason, actor)
        if was_armed:
            await self._cancel_all_live()
        await self._record("warning", "risk.disarmed", reason, {"actor": actor})
        self._publish()
        return self.risk_status()

    async def _start_live_components(self) -> None:
        if self.fill_tracker is not None or not self._bittrade_configured():
            return
        if not self.gmo.api_key or not self.gmo.secret_key:
            return
        min_sizes = {
            symbol: Decimal(str(runtime.instrument.minOrderSize))
            for symbol, runtime in self.state.symbolStates.items()
        }
        size_steps = {
            symbol: Decimal(str(runtime.instrument.sizeStep))
            for symbol, runtime in self.state.symbolStates.items()
        }
        executor = GmoHedgeExecutor(self.gmo, self.rate_limiter, size_steps)
        self.hedge_worker = HedgeWorker(
            self.state_store, self.events, executor, self.risk_gate,
            min_sizes=min_sizes,
            delta_thresholds={
                symbol: Decimal(str(runtime.config.deltaLimit))
                for symbol, runtime in self.state.symbolStates.items()
            },
            resolver=executor.resolve,
            on_execution=self._apply_local_hedge_balance,
        )
        await self.hedge_worker.start()
        source = BitTradeRestFillSource(self.bittrade, self.state_store)
        self.rest_fill_source = source
        self.fill_tracker = FillTracker(
            self.state_store, self.events, rest_source=source, on_fill=self._apply_local_maker_balance,
        )
        websocket = BitTradePrivateWS(
            self.bittrade, list(self.state.activeSymbols),
            on_disconnect=self._ws_disconnected, on_reconnect=self._ws_reconnected,
        )
        await self.fill_tracker.start(websocket.stream())

    async def _stop_live_components(self) -> None:
        if self.fill_tracker:
            await self.fill_tracker.stop()
            self.fill_tracker = None
            self.rest_fill_source = None
        if self.hedge_worker:
            await self.hedge_worker.stop()
            self.hedge_worker = None

    async def _cancel_all_live(self) -> None:
        if self.state.mode != "online" or not self._bittrade_configured():
            return
        try:
            await self.execution_gateway.cancel_all()
            self._working_quotes.clear()
        except Exception as exc:
            await self._record("critical", "orders.cancel_all.failed", self._safe_error(exc))

    async def _reconcile_unsettled(self, orders: list[dict]) -> None:
        if self.fill_tracker is None or self.rest_fill_source is None:
            raise RuntimeError("fill tracker is not running")
        for event in await self.rest_fill_source.for_orders(orders):
            await self.fill_tracker.ingest(event)
        await self.state_store.set_state("last_processed_ts", int(time.time() * 1000))

    async def _ws_disconnected(self, exc: Exception) -> None:
        if self.risk_gate.armed:
            await self.disarm(f"BitTrade private WS disconnected: {self._safe_error(exc)}", "system")

    async def _ws_reconnected(self) -> None:
        await RecoveryCoordinator(
            self.state_store, self.risk_gate, gateway=self.execution_gateway,
            gmo=self.gmo, bittrade=self.bittrade, cancel_existing=True,
            reconcile_fills=self._reconcile_unsettled,
        ).run()

    async def _enforce_risk(self) -> None:
        if self.state.mode != "online":
            return
        ages = [self._market_age_ms(runtime.market.timestamp) for runtime in self.state.symbolStates.values()]
        max_age = max(ages, default=0)
        stale_limit = min(runtime.config.staleMarketMs for runtime in self.state.symbolStates.values())
        was_armed = self.risk_gate.armed
        had_arm_lease = self.risk_gate.armed_until > 0
        pending = await self.state_store.pending_hedge_exposure()
        day = datetime.now(timezone.utc).date().isoformat()
        daily_volume = await self.state_store.daily_fill_volume(day)
        daily_pnl = await self.state_store.daily_realized_pnl(
            day, maker_fee_bps=Decimal(str(self.state.config.bittradeMakerFeeBps)),
            hedge_fee_bps=Decimal(str(self.state.config.gmoFeeBps)),
        )
        hedge_failures, hedge_p95 = await self.state_store.hedge_health(day)
        allowed, reason = await self.risk_gate.evaluate(RiskSnapshot(
            market_age_ms=max_age, stale_market_ms=stale_limit,
            daily_pnl_jpy=float(daily_pnl),
            daily_volume_jpy=float(daily_volume),
            abs_delta=max(
                [abs(runtime.reconciliation.delta) for runtime in self.state.symbolStates.values()]
                + [float(abs(value)) for value in pending.values()], default=0,
            ),
            hedge_failures=hedge_failures,
            hedge_p95_ms=max(self.state.metrics.hedgeP95Ms, hedge_p95),
        ))
        if not allowed and reason == "market data stale":
            self.state.running = False
            for runtime in self.state.symbolStates.values():
                runtime.quotes = []
            if was_armed:
                await self._cancel_all_live()
        elif not allowed and await self.state_store.open_orders():
            await self._cancel_all_live()
        if not allowed and had_arm_lease:
            self.state.running = False
        if self.risk_gate.killed:
            self.state.killSwitch = True

    async def _refresh_balances(self) -> None:
        if not self._bittrade_configured() or not self.gmo.api_key or not self.gmo.secret_key:
            raise ValueError("私有 API 凭据未完整配置")
        bittrade_payload, gmo_payload = await asyncio.gather(
            self.bittrade.balances(), self.gmo.balances(),
        )
        bittrade_rows = bittrade_payload.get("data", [])
        gmo_rows = gmo_payload.get("data", [])
        updated_at = utc_now()
        for row in bittrade_rows:
            if row.get("type") not in (None, "trade"):
                continue
            asset = str(row.get("currency", row.get("symbol", ""))).upper()
            if not asset:
                continue
            available = Decimal(str(row.get("available", row.get("balance", "0"))))
            await self.balance_cache.update("bittrade", asset, available)
            await self.state_store.upsert_balance("bittrade", asset, available, Decimal("0"), updated_at)
        for row in gmo_rows:
            asset = str(row.get("symbol", row.get("currency", ""))).upper()
            if not asset:
                continue
            available = Decimal(str(row.get("available", row.get("amount", "0"))))
            await self.balance_cache.update("gmo", asset, available)
            await self.state_store.upsert_balance("gmo", asset, available, Decimal("0"), updated_at)
        required = {("bittrade", "JPY"), ("gmo", "JPY")}
        required.update((venue, runtime.instrument.baseAsset) for venue in ("bittrade", "gmo")
                        for runtime in self.state.symbolStates.values())
        missing = [f"{venue}:{asset}" for venue, asset in required
                   if not self.balance_cache.has(venue, asset)]
        if missing:
            raise RuntimeError(f"余额响应缺少：{', '.join(missing)}")

    async def _run_live_quotes(self) -> None:
        if self.state.mode != "online" or not self.risk_gate.armed or not self.state.running:
            return
        if self.balance_cache.stale():
            try:
                await self._refresh_balances()
            except Exception as exc:
                await self.disarm(f"balance refresh failed: {self._safe_error(exc)}", "system")
                return
        rows = await self.state_store.open_orders()
        uncertain = [row for row in rows if row["state"] not in ("OPEN", "PARTIAL")]
        if uncertain:
            await self.disarm(f"{len(uncertain)} orders require reconciliation", "system")
            return
        open_by_key = {(row["symbol"], row["side"]): row for row in rows}
        for symbol, runtime in self.state.symbolStates.items():
            blockers = self.balance_cache.pair_blockers(runtime.instrument.baseAsset, require_actual=True)
            if blockers:
                await self._disable_symbol(symbol, blockers)
                continue
            self.state.disabledSymbols.pop(symbol, None)
            targets = self._timed_quotes(runtime.market, runtime.config, runtime.instrument)
            adjusted = []
            for quote in targets:
                key = (symbol, quote.side)
                depth = Decimal(str(runtime.market.bidSize if quote.side == "BUY" else runtime.market.askSize))
                capacity = self.balance_cache.quote_capacity(
                    side=quote.side, base_asset=runtime.instrument.baseAsset,
                    price=Decimal(str(quote.price)),
                    strategy_limit=min(
                        Decimal(str(runtime.config.maxQuoteSize)),
                        Decimal(str(self.risk_gate.limits.max_single_order_jpy)) / Decimal(str(quote.price)),
                    ),
                    hedge_depth=depth,
                )
                size = Decimal(str(self._floor_size(float(capacity), runtime.instrument.sizeStep)))
                current = open_by_key.get(key)
                if size < Decimal(str(runtime.instrument.minOrderSize)):
                    if current:
                        await self.execution_gateway.cancel(current)
                        self._working_quotes.pop(key, None)
                    continue
                adjusted_quote = quote.model_copy(update={"size": float(size)})
                adjusted.append(adjusted_quote)
                cached = self._working_quotes.get(key)
                working = None
                if current:
                    working = WorkingQuote(
                        price=Decimal(current["price"]), original_qty=Decimal(current["qty"]),
                        remaining_qty=max(Decimal("0"), Decimal(current["qty"]) - Decimal(current["cumulative_filled"])),
                        reference_depth=cached.reference_depth if cached else depth,
                    )
                target_price = Decimal(str(adjusted_quote.price))
                if not self.quote_engine.should_requote(
                    working, target_price=target_price, target_qty=size, current_depth=depth,
                ):
                    continue
                snapshot = RiskSnapshot(
                    market_age_ms=self._market_age_ms(runtime.market.timestamp),
                    stale_market_ms=runtime.config.staleMarketMs,
                    daily_pnl_jpy=runtime.pnl.net,
                    abs_delta=abs(runtime.reconciliation.delta),
                    hedge_p95_ms=runtime.hedgeP95Ms,
                )
                try:
                    result = await self.execution_gateway.replace(
                        current, symbol=symbol, side=quote.side, qty=size, price=target_price,
                        size_step=Decimal(str(runtime.instrument.sizeStep)),
                        price_tick=Decimal(str(runtime.instrument.priceTick)), snapshot=snapshot,
                    )
                except Exception as exc:
                    await self.disarm(f"quote execution uncertain: {self._safe_error(exc)}", "system")
                    return
                if result["state"] == "OPEN":
                    self._working_quotes[key] = WorkingQuote(
                        price=target_price, original_qty=size, remaining_qty=size, reference_depth=depth,
                    )
                    open_by_key[key] = result
                else:
                    await self.disarm(f"quote entered {result['state']}; reconciliation required", "system")
                    return
            runtime.quotes = adjusted

    async def _apply_local_maker_balance(self, fill) -> None:
        base_asset = fill.symbol.removesuffix("_JPY")
        if fill.side == "BUY":
            asset, delta = "JPY", -(fill.incremental_qty * fill.price)
        else:
            asset, delta = base_asset, -fill.incremental_qty
        await self._apply_balance_delta("bittrade", asset, delta)

    async def _apply_local_hedge_balance(self, symbol: str, side: str, execution) -> None:
        base_asset = symbol.removesuffix("_JPY")
        if side == "BUY":
            asset, delta = "JPY", -execution.filled_notional
        else:
            asset, delta = base_asset, -execution.filled_qty
        await self._apply_balance_delta("gmo", asset, delta)

    async def _apply_balance_delta(self, venue: str, asset: str, delta: Decimal) -> None:
        await self.balance_cache.apply_local_delta(venue, asset, delta)
        await self.state_store.upsert_balance(
            venue, asset, self.balance_cache.available(venue, asset), Decimal("0"), utc_now(),
        )

    async def _load_inventory(self) -> None:
        saved = await self.state_store.get_state("inventory.allocations", None)
        if isinstance(saved, dict):
            self.inventory_allocations = {
                venue: {asset.upper(): Decimal(str(amount)) for asset, amount in saved.get(venue, {}).items()}
                for venue in ("bittrade", "gmo")
            }
        self.balance_cache.configure_allocations(self.inventory_allocations)
        webhook = await self.state_store.get_state("alerting.lark_webhook", "")
        if webhook:
            self.notifier.configure(str(webhook))
        await self._recompute_inventory_status(notify=False)

    async def configure_inventory(self, bittrade: dict[str, float], gmo: dict[str, float], *,
                                  webhook_url: str | None = None, clear_webhook: bool = False) -> dict:
        normalized: dict[str, dict[str, Decimal]] = {}
        for venue, values in (("bittrade", bittrade), ("gmo", gmo)):
            assets: dict[str, Decimal] = {}
            for asset, raw in values.items():
                code = asset.strip().upper()
                if not code or not code.replace("-", "").isalnum():
                    raise ValueError(f"无效资产代码：{asset}")
                amount = Decimal(str(raw))
                if not amount.is_finite() or amount < 0:
                    raise ValueError(f"{venue} {code} 底仓必须是非负数")
                assets[code] = amount
            normalized[venue] = assets
        if self.risk_gate.armed:
            await self.disarm("inventory configuration changed", "operator")
        self.inventory_allocations = normalized
        self.balance_cache.configure_allocations(normalized)
        if clear_webhook:
            self.notifier.configure("")
        elif webhook_url is not None and webhook_url.strip():
            self.notifier.configure(webhook_url)
        await self._persist_inventory()
        await self._recompute_inventory_status(notify=True)
        await self._record("warning", "inventory.updated", "双交易所底仓配置已更新")
        self._publish()
        return self.inventory_summary()

    def inventory_summary(self) -> dict:
        return {
            "bittrade": {asset: float(amount) for asset, amount in self.inventory_allocations["bittrade"].items()},
            "gmo": {asset: float(amount) for asset, amount in self.inventory_allocations["gmo"].items()},
            "webhookConfigured": bool(self.notifier.webhook_url),
            "webhookHint": f"••••{self.notifier.webhook_url[-8:]}" if self.notifier.webhook_url else None,
            "disabledSymbols": self.state.disabledSymbols,
        }

    async def _persist_inventory(self) -> None:
        if not self._engine_ready:
            return
        await self.state_store.set_state("inventory.allocations", {
            venue: {asset: str(amount) for asset, amount in assets.items()}
            for venue, assets in self.inventory_allocations.items()
        })
        await self.state_store.set_state("alerting.lark_webhook", self.notifier.webhook_url)

    async def _recompute_inventory_status(self, *, notify: bool) -> None:
        for symbol, runtime in self.state.symbolStates.items():
            blockers = self.balance_cache.pair_blockers(runtime.instrument.baseAsset, require_actual=False)
            if blockers:
                if notify:
                    await self._disable_symbol(symbol, blockers)
                else:
                    self.state.disabledSymbols[symbol] = blockers
                    runtime.quotes = []
            else:
                self.state.disabledSymbols.pop(symbol, None)

    async def _disable_symbol(self, symbol: str, blockers: list[str]) -> None:
        runtime = self.state.symbolStates.get(symbol)
        if runtime is not None:
            runtime.quotes = []
        self.state.disabledSymbols[symbol] = blockers
        if self._engine_ready:
            for order in [row for row in await self.state_store.open_orders() if row["symbol"] == symbol]:
                try:
                    await self.execution_gateway.cancel(order)
                except Exception as exc:
                    await self.state_store.audit("inventory.cancel.error", "critical", self._safe_error(exc))
            try:
                await self.notifier.send_once(
                    f"inventory:{symbol}:{'|'.join(blockers)}",
                    f"[JARB 底仓报警] {symbol} 已禁止做市和对冲。缺失/为零：{', '.join(blockers)}。"
                    "要求 BitTrade JPY、BitTrade 基础币、GMO JPY、GMO 基础币四项均大于 0。",
                )
            except Exception as exc:
                await self.state_store.audit("alert.webhook.failed", "warning", self._safe_error(exc))

    @staticmethod
    def _market_age_ms(timestamp: str) -> int:
        try:
            observed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return max(0, int((datetime.now(timezone.utc) - observed).total_seconds() * 1000))
        except ValueError:
            return 2 ** 31 - 1

    def _bittrade_configured(self) -> bool:
        return bool(self.bittrade.access_key and self.bittrade.secret_key and self.bittrade.account_id)

    def _publish(self) -> None:
        self._sync_primary()
        payload = self.state.model_dump_json()
        for queue in self.subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(payload)
