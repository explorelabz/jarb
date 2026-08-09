from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator
from decimal import Decimal, ROUND_DOWN
from uuid import uuid4

import httpx

from .adapters import BitTradeAdapter, GmoAdapter
from .audit_store import AuditStore
from .config import Credentials
from .core import make_quotes, matched_trade, opposite_side, reconcile, validate_config
from .models import (
    AuditEvent, ClientFill, ConnectionState, ConnectionUpdate, HedgeFill, InstrumentRules, MarketTop,
    Metrics, Pnl, SimulatedFillRequest, StrategyConfig, SymbolRuntime, SystemState, utc_now,
)


class TradingService:
    def __init__(self, config: StrategyConfig, mode: str = "simulation", credentials: Credentials | None = None,
                 gmo: GmoAdapter | None = None, bittrade: BitTradeAdapter | None = None):
        validate_config(config)
        credentials = credentials or Credentials()
        self.started_ns = time.monotonic_ns()
        self.clients: dict[str, list[ClientFill]] = {}
        self.hedges: dict[str, list[HedgeFill]] = {}
        self.store = AuditStore()
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
        base_asset = config.symbol.removesuffix("_JPY")
        instrument = InstrumentRules(symbol=config.symbol, baseAsset=base_asset, minOrderSize=.0001,
                                     maxOrderSize=5, sizeStep=.0001, priceTick=1)
        market = MarketTop(symbol=config.symbol, bid=17_482_140, ask=17_493_860, bidSize=0.4382,
                           askSize=0.3167, timestamp=utc_now(), source="SIM")
        runtime = SymbolRuntime(instrument=instrument, config=config, market=market, quotes=make_quotes(market, config),
                                reconciliation=reconcile(config.symbol, [], []))
        self.clients[config.symbol] = []
        self.hedges[config.symbol] = []
        self.state = SystemState(
            mode=mode, running=True, killSwitch=False, market=market, quotes=runtime.quotes,
            position=0, reconciliation=reconcile(config.symbol, [], []), pnl=Pnl(), metrics=Metrics(),
            trades=[], events=[], config=config, connection=self._connection_state("connecting" if mode == "online" else "simulation"),
            instrument=instrument, activeSymbols=[config.symbol], symbolStates={config.symbol: runtime},
        )

    async def start(self) -> None:
        if self.task is None:
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
            for symbol in removed:
                self.clients.pop(symbol, None)
                self.hedges.pop(symbol, None)
            self.state.activeSymbols = target_symbols
            self.state.symbolStates = next_states
            self.state.config = next_states[target_symbols[0]].config
            self._sync_primary()
        await self._record("info", "strategy.updated", f"多币种策略已更新：{', '.join(target_symbols)}",
                           {**patch, "symbols": target_symbols})
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
            raise ValueError("切换线上模式前必须确认只读行情安全提示")

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
            await self._record("warning", "connection.online", "已切换至 GMO 线上行情（只读，不会自动下单）")
        else:
            self.state.mode = "simulation"
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
        level = "critical" if action == "kill" else "warning"
        await self._record(level, f"risk.{action}", {"resume": "报价已恢复", "pause": "报价已暂停", "kill": "紧急停止已触发", "reset-kill": "紧急停止已解除"}[action])
        self._publish()

    async def simulate_fill(self, request: SimulatedFillRequest):
        symbol = (request.symbol or self.state.activeSymbols[0]).upper()
        runtime = self.state.symbolStates.get(symbol)
        if runtime is None:
            raise ValueError("所选币种未启用")
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
                side=request.side, price=quote.price, size=size, fee=quote.price * size * 0.001 if request.role == "taker" else 0,
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
            self._publish()

    async def _refresh_online_market(self) -> None:
        symbols = list(self.state.activeSymbols)
        markets = await asyncio.gather(*(self.gmo.ticker(self.state.symbolStates[symbol].instrument.baseAsset) for symbol in symbols))
        async with self.lock:
            for symbol, market in zip(symbols, markets, strict=True):
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
        runtime.reconciliation = reconcile(symbol, self.clients[symbol], self.hedges[symbol])
        runtime.pnl = Pnl(
            spread=sum(t.spreadPnl for t in runtime.trades), clientFees=sum(t.clientFee for t in runtime.trades),
            hedgeCosts=sum(t.hedgeCost for t in runtime.trades), net=sum(t.netPnl for t in runtime.trades),
        )
        all_trades = [trade for item in self.state.symbolStates.values() for trade in item.trades]
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
        await self.store.append(event)

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
