from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import asdict, replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from pathlib import Path
from uuid import uuid4

import httpx

from .adapters import BitTradeAdapter, GmoAdapter
from .audit_store import AuditStore
from .config import Credentials
from .core import make_quotes, matched_trade, reconcile, validate_config
from .engine.balance import BalanceCache
from .engine.alerting import LarkWebhookNotifier
from .engine.domain import EventType, FillDelta
from .engine.events import EventBus
from .engine.execution_gateway import ExecutionGateway
from .engine.fill_tracker import BitTradePrivateWS, BitTradeRestFillSource, FillTracker
from .engine.hedge_worker import GmoHedgeExecutor, HedgeExecution, HedgeWorker
from .engine.market_feed import GmoPublicWS, MarketFeed
from .engine.paper_exchange import FakeBitTrade, FakeGmo, PaperBroker, PaperScenarioConfig
from .engine.paper_matcher import (
    BitTradeDepthFeed, BitTradeTradeStream, PaperMatchingEngine, PublicTrade,
)
from .engine.quote_engine import QuoteEngine, WorkingQuote, target_price
from .engine.rate_limit import EndpointGroup, Priority, PriorityRateLimiter
from .engine.recovery import RecoveryCoordinator
from .engine.risk import RiskGate, RiskLimits, RiskSnapshot
from .engine.state_store import StateStore
from .models import (
    AssetHolding, AuditEvent, ClientFill, ConnectionState, ConnectionUpdate, HedgeFill, HoldingsState,
    InstrumentRules, MarketTop, MatchedTrade, Metrics, PaperFillRequest, PaperScenarioUpdate, Pnl, RiskLimitsUpdate,
    StrategyConfig, SymbolRuntime, SystemState, expected_gmo_fee_bps, gmo_maker_bps,
    gmo_taker_bps, utc_now,
)


# The public API accepts legacy online/simulation names, but service instances are
# canonicalized to live/paper before this mapping is consumed.
ARM_TTL_BY_MODE = {"live": 3_600, "online": 3_600, "paper": 86_400, "simulation": 0}
HEARTBEAT_INTERVAL_SEC = 600
ARM_EXPIRY_WARNING_SEC = 300
LEGACY_RANDOM_MATCH_FIELDS = {
    "autoMatch", "partialFills", "dustFills", "duplicateEvents", "outOfOrderEvents",
    "cancelAlreadyFilled", "cancelRaceFill", "autoMatchProbability", "dustProbability",
    "duplicateProbability", "outOfOrderProbability", "cancelRaceProbability",
    "gmoPostOnlyFillRatio",
}


class TradingService:
    def __init__(self, config: StrategyConfig, mode: str = "paper", credentials: Credentials | None = None,
                 gmo: GmoAdapter | None = None, bittrade: BitTradeAdapter | None = None,
                 db_path: Path | str = Path("data/jarb.db"),
                 risk_limits: RiskLimits | None = None,
                 gmo_fee_overrides: dict[str, float] | None = None,
                 gmo_maker_fee_overrides: dict[str, float] | None = None,
                 require_dual_arm_approval: bool = True,
                 market_stream_factory: Callable[[list[str], MarketFeed], Awaitable[None]] | None = None,
                 private_stream_factory: Callable[..., AsyncIterator] | None = None,
                 paper_scenarios: PaperScenarioConfig | None = None,
                 market_gmo: GmoAdapter | None = None,
                 market_bittrade: BitTradeAdapter | None = None,
                 bittrade_depth_factory: Callable[[list[str]], BitTradeDepthFeed] | None = None,
                 paper_trade_stream_factory: Callable[[list[str]], AsyncIterator[PublicTrade]] | None = None):
        mode = {"simulation": "paper", "online": "live"}.get(mode, mode)
        if mode not in {"paper", "live"}:
            raise ValueError("mode must be paper or live")
        self._gmo_fee_overrides = {
            str(asset).upper(): float(value) for asset, value in (gmo_fee_overrides or {}).items()
        }
        self._gmo_maker_fee_overrides = {
            str(asset).upper(): float(value)
            for asset, value in (gmo_maker_fee_overrides or {}).items()
        }
        base_asset = config.symbol.removesuffix("_JPY")
        config = StrategyConfig.model_validate({
            **config.model_dump(),
            "gmoFeeBps": gmo_taker_bps(base_asset, self._gmo_fee_overrides),
            "gmoMakerFeeBps": gmo_maker_bps(base_asset, self._gmo_maker_fee_overrides),
        })
        validate_config(config)
        credentials = credentials or Credentials()
        self.started_ns = time.monotonic_ns()
        self.clients: dict[str, list[ClientFill]] = {}
        self.hedges: dict[str, list[HedgeFill]] = {}
        self.matched_trades: dict[str, list[MatchedTrade]] = {}
        self.audit_store = AuditStore()
        self.state_store = StateStore(db_path, trading_mode=mode)
        self.events = EventBus()
        self.rate_limiter = PriorityRateLimiter()
        self.balance_cache = BalanceCache()
        self._position_baseline: dict[tuple[str, str], Decimal] = {}
        self._positions_updated_at: str | None = None
        self.quote_engine = QuoteEngine()
        effective_limits = risk_limits or RiskLimits(
            max_abs_delta=config.deltaLimit, max_hedge_p95_ms=config.maxHedgeLatencyMs,
        )
        effective_limits = replace(effective_limits, arm_ttl_sec=ARM_TTL_BY_MODE[mode])
        self.notifier = LarkWebhookNotifier()
        self.risk_gate = RiskGate(
            self.state_store, effective_limits,
            confirmation_phrase="ARM JARB PAPER" if mode == "paper" else None,
            require_dual_approval=False if mode == "paper" else require_dual_arm_approval,
            notifier=self.notifier,
        )
        self.lock = asyncio.Lock()
        self.subscribers: set[asyncio.Queue[str]] = set()
        self.task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._arm_expiry_warning_task: asyncio.Task | None = None
        self._arm_expiry_warning_for = 0.0
        self.core_samples_us: list[float] = []
        self._symbol_cache: list[InstrumentRules] = []
        self._symbol_cache_at = 0.0
        # Explicit execution adapters are generally test/dev seams and historically
        # supplied both public data and execution. Preserve that contract unless
        # callers explicitly provide separate market adapters.
        injected_execution_adapters = gmo is not None or bittrade is not None
        self.paper_broker: PaperBroker | None = None
        if mode == "paper" and gmo is None and bittrade is None:
            self.paper_broker = PaperBroker(paper_scenarios)
            gmo = FakeGmo(self.paper_broker)
            bittrade = FakeBitTrade(self.paper_broker)
            self.paper_broker.state_store = self.state_store
        self._http_client = httpx.AsyncClient(timeout=3.0) if gmo is None or bittrade is None else None
        self.gmo = gmo or GmoAdapter(credentials.gmo_api_key, credentials.gmo_secret_key, self._http_client)
        self.bittrade = bittrade or BitTradeAdapter(credentials.bittrade_access_key, credentials.bittrade_secret_key,
                                                    credentials.bittrade_account_id, self._http_client)
        self._market_http_client: httpx.AsyncClient | None = None
        if mode == "paper" and not injected_execution_adapters \
                and market_gmo is None and market_bittrade is None:
            # Paper uses public live books while all execution remains on FakeGmo/FakeBitTrade.
            self._market_http_client = httpx.AsyncClient(timeout=3.0)
            market_gmo = GmoAdapter(client=self._market_http_client)
            market_bittrade = BitTradeAdapter(client=self._market_http_client)
        self.market_gmo = market_gmo or self.gmo
        self.market_bittrade = market_bittrade or self.bittrade
        self._market_stream_factory = market_stream_factory or (
            (lambda bases, feed: self.market_gmo.market_stream(bases, feed)) if hasattr(self.market_gmo, "market_stream")
            else (lambda bases, feed: GmoPublicWS(bases, feed).run())
        )
        self._private_stream_factory = private_stream_factory or (
            (lambda adapter, symbols, **callbacks: adapter.stream()) if hasattr(self.bittrade, "stream")
            else (lambda adapter, symbols, **callbacks: BitTradePrivateWS(
                adapter, symbols, **callbacks,
            ).stream())
        )
        self._bittrade_depth_factory = bittrade_depth_factory or BitTradeDepthFeed
        self._paper_trade_stream_factory = paper_trade_stream_factory or (
            lambda symbols: BitTradeTradeStream(symbols).stream()
        )
        self.inventory_allocations: dict[str, dict[str, Decimal]] = {
            "bittrade": {"JPY": Decimal("1000000"), base_asset: Decimal("1")},
            "gmo": {"JPY": Decimal("1000000"), base_asset: Decimal("1")},
        }
        self.balance_cache.configure_allocations(self.inventory_allocations)
        self.execution_gateway = ExecutionGateway(
            self.bittrade, self.state_store, self.risk_gate, self.rate_limiter,
        )
        self.market_feed = MarketFeed(self.market_gmo, self.events)
        self.gmo_market_ws_task: asyncio.Task | None = None
        self.bittrade_depth_feed: BitTradeDepthFeed | None = None
        self.paper_engine: PaperMatchingEngine | None = None
        self.paper_trade_task: asyncio.Task | None = None
        self.paper_resync_task: asyncio.Task | None = None
        self._rest_market_fallback: set[str] = set()
        self._bittrade_depth_cache: dict[
            str, tuple[float, list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]
        ] = {}
        self._bittrade_depth_errors: set[str] = set()
        self.fill_tracker: FillTracker | None = None
        self.rest_fill_source: BitTradeRestFillSource | None = None
        self.hedge_worker: HedgeWorker | None = None
        self._engine_ready = False
        self._working_quotes: dict[tuple[str, str], WorkingQuote] = {}
        self._last_live_risk_snapshot = RiskSnapshot()
        self._paper_risk_observations = 0
        self._paper_risk_would_reject = 0
        self._paper_risk_current_reason: str | None = None
        self._projection_symbols: set[str] = set()
        self._projection_task: asyncio.Task | None = None
        instrument = InstrumentRules(symbol=config.symbol, baseAsset=base_asset, minOrderSize=.0001,
                                     maxOrderSize=5, sizeStep=.0001, priceTick=1)
        market = MarketTop(symbol=config.symbol, bid=17_482_140, ask=17_493_860, bidSize=0.4382,
                           askSize=0.3167, timestamp=utc_now(), source="GMO" if mode in {"paper", "live"} else "SIM")
        runtime = SymbolRuntime(instrument=instrument, config=config, market=market, quotes=make_quotes(market, config),
                                reconciliation=reconcile(config.symbol, [], []))
        self.clients[config.symbol] = []
        self.hedges[config.symbol] = []
        self.matched_trades[config.symbol] = []
        self.state = SystemState(
            mode=mode, running=True, killSwitch=False, market=market, quotes=runtime.quotes,
            position=0, reconciliation=reconcile(config.symbol, [], []), pnl=Pnl(), metrics=Metrics(),
            trades=[], events=[], config=config, connection=self._connection_state("connecting" if mode == "live" else "paper"),
            instrument=instrument, activeSymbols=[config.symbol], symbolStates={config.symbol: runtime},
        )

    async def start(self) -> None:
        if self.task is None:
            await self.state_store.initialize()
            self._engine_ready = True
            await self._load_runtime_settings()
            await self._load_inventory()
            if self.paper_broker is not None:
                await self.paper_broker.restore()
            self.state.holdings = self.holdings_snapshot()
            await self.risk_gate.restore()
            await self.rate_limiter.start()
            await self._start_live_components()
            await RecoveryCoordinator(
                self.state_store, self.risk_gate, gateway=self.execution_gateway,
                gmo=self.gmo,
                # Paper has no remote maker orders anymore; only durable local orders
                # are canceled through the queue matcher during recovery.
                bittrade=self.bittrade if self.places_real_orders and self._bittrade_configured() else None,
                cancel_existing=True,
                reconcile_fills=self._reconcile_unsettled if self.rest_fill_source else None,
            ).run()
            await self._refresh_live_projection()
            try:
                await self._refresh_online_market()
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                self.state.connection = self._connection_state("error", self._safe_error(exc))
            if self.state.mode == "paper" and self.risk_gate.recovery_complete and not self.risk_gate.killed:
                await self._refresh_balances()
                await self.risk_gate.arm("ARM JARB PAPER", "paper-engine")
                self.state.running = True
            await self._record("info", "system.started", f"策略以{'线上' if self.state.mode == 'live' else 'Paper'}模式启动")
            self.task = asyncio.create_task(self._run(), name="market-loop")
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="lark-heartbeat")
            self._arm_expiry_warning_task = asyncio.create_task(
                self._arm_expiry_warning_loop(), name="arm-expiry-warning",
            )

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        for attribute in ("_heartbeat_task", "_arm_expiry_warning_task"):
            task = getattr(self, attribute)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, attribute, None)
        if self._projection_task is not None:
            self._projection_task.cancel()
            try:
                await self._projection_task
            except asyncio.CancelledError:
                pass
            self._projection_task = None
        await self._stop_live_components()
        await self.rate_limiter.stop()
        if self._engine_ready:
            await self.state_store.close()
            self._engine_ready = False
        await self.notifier.close()
        if self._market_http_client is not None:
            await self._market_http_client.aclose()
        if self._http_client is not None:
            await self._http_client.aclose()

    @property
    def uses_live_market(self) -> bool:
        return self.state.mode in {"paper", "live"}

    @property
    def places_real_orders(self) -> bool:
        return self.state.mode == "live"

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
        if self.places_real_orders and self._engine_ready and self.risk_gate.armed:
            await self.disarm("strategy configuration changed", "operator")
        requested_symbols = patch.pop("symbols", None)
        legacy_symbol = patch.pop("symbol", None)
        fee_overrides_patch = patch.pop("gmoFeeBpsByAsset", None)
        maker_fee_overrides_patch = patch.pop("gmoMakerFeeBpsByAsset", None)
        patch.pop("gmoFeeBps", None)  # GMO taker fee is derived per base asset, never globally overridden.
        patch.pop("gmoMakerFeeBps", None)
        next_fee_overrides = dict(self._gmo_fee_overrides)
        if fee_overrides_patch is not None:
            if not isinstance(fee_overrides_patch, dict):
                raise ValueError("gmoFeeBpsByAsset 必须是币种到 bps 的映射")
            next_fee_overrides = {}
            for asset, value in fee_overrides_patch.items():
                normalized = str(asset).strip().upper()
                fee = float(value)
                if not normalized or fee < 0 or fee > 100:
                    raise ValueError("GMO Taker 费率必须在 0 到 100 bps 之间")
                next_fee_overrides[normalized] = fee
        next_maker_fee_overrides = dict(self._gmo_maker_fee_overrides)
        if maker_fee_overrides_patch is not None:
            if not isinstance(maker_fee_overrides_patch, dict):
                raise ValueError("gmoMakerFeeBpsByAsset 必须是币种到 bps 的映射")
            next_maker_fee_overrides = {}
            for asset, value in maker_fee_overrides_patch.items():
                normalized = str(asset).strip().upper()
                fee = float(value)
                if not normalized or fee < -100 or fee > 100:
                    raise ValueError("GMO Maker 费率必须在 -100 到 100 bps 之间")
                next_maker_fee_overrides[normalized] = fee
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
        runtime_configs: dict[str, StrategyConfig] = {}
        for symbol in target_symbols:
            instrument = rules[symbol]
            runtime_config = StrategyConfig.model_validate({
                **template.model_dump(),
                "symbol": symbol,
                "gmoFeeBps": gmo_taker_bps(instrument.baseAsset, next_fee_overrides),
                "gmoMakerFeeBps": gmo_maker_bps(instrument.baseAsset, next_maker_fee_overrides),
                "maxQuoteSize": min(instrument.maxOrderSize, max(template.maxQuoteSize, instrument.minOrderSize)),
                "deltaLimit": max(template.deltaLimit, instrument.minOrderSize),
            })
            validate_config(runtime_config)
            runtime_configs[symbol] = runtime_config
        new_symbols = [symbol for symbol in target_symbols if symbol not in self.state.symbolStates]
        try:
            markets = await asyncio.gather(*(self.market_gmo.ticker(rules[symbol].baseAsset) for symbol in new_symbols))
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            raise ValueError(f"无法初始化新增币种行情：{self._safe_error(exc)}") from exc
        market_by_symbol = dict(zip(new_symbols, markets, strict=True))

        async with self.lock:
            next_states: dict[str, SymbolRuntime] = {}
            for symbol in target_symbols:
                instrument = rules[symbol]
                runtime_config = runtime_configs[symbol]
                existing = self.state.symbolStates.get(symbol)
                if existing:
                    existing.instrument = instrument
                    existing.config = runtime_config
                    existing.quotes = self._timed_quotes(existing.market, runtime_config, instrument)
                    next_states[symbol] = existing
                else:
                    market = market_by_symbol[symbol].model_copy(
                        update={"source": "GMO" if self.uses_live_market else "SIM"},
                    )
                    if self.paper_broker is not None:
                        self.paper_broker.set_market(market)
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
            self._gmo_fee_overrides = next_fee_overrides
            self._gmo_maker_fee_overrides = next_maker_fee_overrides
            self.state.activeSymbols = target_symbols
            self.state.symbolStates = next_states
            self.state.config = next_states[target_symbols[0]].config
            self._sync_primary()
        await self._record("info", "strategy.updated", f"多币种策略已更新：{', '.join(target_symbols)}",
                           {**patch, "symbols": target_symbols})
        if self._engine_ready:
            await self._persist_inventory()
            await self.state_store.set_state("gmo.fee_overrides", self._gmo_fee_overrides)
            await self.state_store.set_state("gmo.maker_fee_overrides", self._gmo_maker_fee_overrides)
            await self._recompute_inventory_status(notify=False)
        if self._engine_ready:
            await self._stop_live_components()
            await self._start_live_components()
            if self.places_real_orders:
                await RecoveryCoordinator(
                    self.state_store, self.risk_gate, gateway=self.execution_gateway,
                    gmo=self.gmo, bittrade=self.bittrade if self._bittrade_configured() else None,
                    cancel_existing=True,
                    reconcile_fills=self._reconcile_unsettled if self.rest_fill_source else None,
                ).run()
        self._publish()

    async def common_symbols(self, force: bool = False) -> list[InstrumentRules]:
        if not force and self._symbol_cache and time.monotonic() - self._symbol_cache_at < 300:
            return self._symbol_cache
        gmo_rows, bittrade_rows = await asyncio.gather(
            self.market_gmo.symbols(), self.market_bittrade.symbols(),
        )
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
        if update.mode != self.state.mode:
            raise ValueError("Paper/Live 使用不同交易所边界，切换模式后请重启服务")
        if update.mode == "live" and not update.confirmOnline:
            raise ValueError("更新线上连接前必须确认真实账户安全提示")

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

        if update.mode == "live":
            self.state.connection = self._connection_state("connecting")
            self._publish()
            try:
                await self._refresh_online_market()
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                message = self._safe_error(exc)
                self.state.connection = self._connection_state("error", message)
                self._publish()
                raise ValueError(f"无法连接 GMO 线上行情：{message}") from exc
            await self.balance_cache.clear_balances()
            self._position_baseline.clear()
            self._positions_updated_at = None
            if self._engine_ready:
                await self._stop_live_components()
                await self._start_live_components()
                await RecoveryCoordinator(
                    self.state_store, self.risk_gate, gateway=self.execution_gateway,
                    gmo=self.gmo, bittrade=self.bittrade if self._bittrade_configured() else None,
                    cancel_existing=True,
                    reconcile_fills=self._reconcile_unsettled if self.rest_fill_source else None,
                ).run()
            await self._record("warning", "connection.live", "已连接真实账户，当前保持 DISARMED")
        else:
            self.state.connection = self._connection_state("paper")
            await self._record("info", "connection.paper", "Paper 交易所边界运行中")
        self._publish()

    def connection_summary(self) -> dict:
        return {"mode": self.state.mode, **self.state.connection.model_dump()}

    def paper_scenario_summary(self) -> dict:
        if self.paper_broker is None:
            raise ValueError("坏情况注入只在 Paper 模式可用")
        matching = self.paper_engine.stats() if self.paper_engine is not None else {
            "openOrders": 0, "throughFills": 0, "atLevelFills": 0,
            "throughQty": "0", "atLevelQty": "0", "throughRatio": 0.0,
            "publicTradesSeen": 0, "lastTradeTsMs": 0,
        }
        matching["publicDepth"] = self.bittrade_depth_feed.status() \
            if self.bittrade_depth_feed is not None and hasattr(self.bittrade_depth_feed, "status") else {}
        matching["risk"] = {
            "observations": self._paper_risk_observations,
            "wouldReject": self._paper_risk_would_reject,
            "currentReason": self._paper_risk_current_reason,
            "orders": self.execution_gateway.paper_risk_stats(),
        }
        return {
            **self.paper_broker.scenarios.model_dump(exclude=LEGACY_RANDOM_MATCH_FIELDS),
            "matching": matching,
        }

    async def configure_paper_scenarios(self, update: PaperScenarioUpdate) -> dict:
        if self.paper_broker is None:
            raise ValueError("坏情况注入只在 Paper 模式可用")
        result = self.paper_broker.configure(update.model_dump(exclude_none=True))
        if self._engine_ready:
            await self.state_store.set_state("paper.scenarios", result.model_dump())
        await self._record("warning", "paper.scenarios.updated", "Paper 撮合坏情况开关已更新", result.model_dump())
        return self.paper_scenario_summary()

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
                    gmo=self.gmo,
                    bittrade=self.bittrade if self.places_real_orders and self._bittrade_configured() else None,
                    cancel_existing=True,
                    reconcile_fills=self._reconcile_unsettled if self.rest_fill_source else None,
                ).run()
        level = "critical" if action == "kill" else "warning"
        await self._record(level, f"risk.{action}", {"resume": "报价已恢复", "pause": "报价已暂停", "kill": "紧急停止已触发", "reset-kill": "紧急停止已解除"}[action])
        self._publish()

    async def simulate_fill(self, request: PaperFillRequest):
        raise ValueError("Paper/Live 禁止手工注入成交；成交只能来自 BitTrade 公开成交流")

    def export_reconciliation(self) -> dict:
        primary = self.state.symbolStates[self.state.activeSymbols[0]]
        day = datetime.now(timezone.utc).date().isoformat()
        return {"generatedAt": utc_now(), "scope": "daily", "core": "Rust/PyO3",
                "formula": "Σ(client signed quantity) + Σ(hedge signed quantity) = delta",
                "result": primary.reconciliation.model_dump(), "pnl": primary.pnl.model_dump(),
                "symbols": {symbol: {
                "result": runtime.reconciliation.model_dump(),
                    "clientFills": [x.model_dump() for x in self.clients[symbol] if x.timestamp.startswith(day)],
                    "hedgeFills": [x.model_dump() for x in self.hedges[symbol] if x.timestamp.startswith(day)],
                    "matchedTrades": [x.model_dump() for x in self.matched_trades[symbol]],
                    "pnl": runtime.pnl.model_dump(),
                } for symbol, runtime in self.state.symbolStates.items()}}

    async def export_orders(self, trading_mode: str | None = None) -> list[dict]:
        trading_mode = {"simulation": "paper", "online": "live"}.get(trading_mode, trading_mode)
        if trading_mode not in (None, "paper", "live", "legacy_simulation"):
            raise ValueError("导出模式必须是 paper、live、legacy_simulation 或 all")
        if not self._engine_ready:
            raise RuntimeError("状态数据库尚未初始化")
        return await self.state_store.export_order_rows(trading_mode)

    async def _run(self) -> None:
        queue = self.events.open_queue(EventType.MARKET)
        loop = asyncio.get_running_loop()
        last_watchdog = loop.time()
        try:
            while True:
                try:
                    await asyncio.wait_for(queue.get(), timeout=1.0)
                    while not queue.empty():
                        queue.get_nowait()
                except TimeoutError:
                    pass
                recovered: list[str] = []
                async with self.lock:
                    for symbol, runtime in self.state.symbolStates.items():
                        latest = self.market_feed.latest.get(symbol)
                        if latest is None:
                            continue
                        runtime.market = latest
                        if self.paper_broker is not None:
                            self.paper_broker.set_market(latest)
                        if self.state.running:
                            runtime.quotes = self._timed_quotes(latest, runtime.config, runtime.instrument)
                        if self.market_feed.latest_transport.get(symbol) == "ws" \
                                and symbol in self._rest_market_fallback:
                            self._rest_market_fallback.discard(symbol)
                            recovered.append(symbol)
                    if self.market_feed.latest:
                        self.state.connection = self._connection_state("connected")
                for symbol in recovered:
                    await self._record("info", "market.ws.recovered", f"{symbol} 已恢复行情流")
                if loop.time() - last_watchdog >= 5.0:
                    last_watchdog = loop.time()
                    await self._market_watchdog()
                async with self.lock:
                    self.state.metrics.uptimeSec = int(
                        (time.monotonic_ns() - self.started_ns) / 1_000_000_000
                    )
                    self._sync_primary()
                await self._enforce_risk()
                await self._run_live_quotes()
                self._publish()
        finally:
            self.events.close_queue(EventType.MARKET, queue)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
            try:
                await self._send_heartbeat()
            except Exception as exc:
                if self._engine_ready:
                    await self.state_store.audit("alert.heartbeat.failed", "warning", self._safe_error(exc))

    async def _send_heartbeat(self) -> None:
        if not self.notifier.webhook_url or not self._engine_ready:
            return
        day = datetime.now(timezone.utc).date().isoformat()
        open_orders, fill_count = await asyncio.gather(
            self.state_store.open_orders(), self.state_store.daily_fill_count(day),
        )
        armed = "armed" if self.risk_gate.armed else "disarmed"
        message = (
            f"💓 JARB 心跳：{armed} / 挂单 {len(open_orders)} / "
            f"今日成交 {fill_count} 笔 / 模式 {self.state.mode}"
        )
        await self.notifier.send_once("heartbeat", message)

    async def _arm_expiry_warning_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            if self.state.mode != "live" or not self.risk_gate.armed:
                continue
            armed_until = self.risk_gate.armed_until
            remaining = armed_until - time.time()
            if 0 < remaining <= ARM_EXPIRY_WARNING_SEC and armed_until != self._arm_expiry_warning_for:
                try:
                    minutes = max(1, int((remaining + 59) // 60))
                    sent = await self.notifier.send_once(
                        f"risk:arm-expiry:{int(armed_until)}",
                        f"⚠️ JARB 实盘 Arm 将在约 {minutes} 分钟后到期；请检查后手动续期或 Disarm。",
                    )
                    if sent:
                        self._arm_expiry_warning_for = armed_until
                except Exception as exc:
                    if self._engine_ready:
                        await self.state_store.audit(
                            "alert.webhook.failed", "warning", f"arm expiry alert: {self._safe_error(exc)}",
                        )

    async def _market_watchdog(self) -> None:
        fallback: dict[str, str] = {}
        newly_stale: list[tuple[str, int]] = []
        for symbol, runtime in self.state.symbolStates.items():
            age = self.market_feed.age_ms(symbol)
            if age > runtime.config.staleMarketMs or symbol in self._rest_market_fallback:
                fallback[symbol] = runtime.instrument.baseAsset
                if symbol not in self._rest_market_fallback:
                    self._rest_market_fallback.add(symbol)
                    newly_stale.append((symbol, age))
        for symbol, age in newly_stale:
            message = f"{symbol} GMO WebSocket 行情陈旧（{age} ms），已切换 REST 看门狗"
            await self._record("warning", "market.ws.stale", message)
            try:
                await self.notifier.send_once(f"market:{symbol}", message)
            except Exception as exc:
                await self.state_store.audit("alert.webhook.failed", "warning", self._safe_error(exc))
        if not fallback:
            return
        try:
            await self.market_feed.refresh(fallback)
            self.state.connection = self._connection_state("connected")
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            message = self._safe_error(exc)
            was_error = self.state.connection.lastError == message
            self.state.connection = self._connection_state("error", message)
            if not was_error:
                await self._record("critical", "market.rest.failed", f"GMO REST 看门狗失败：{message}")

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
                if self.paper_broker is not None:
                    self.paper_broker.set_market(market)
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

    @staticmethod
    def _delta_headroom(side: str, delta: Decimal, limit: Decimal) -> Decimal:
        """Maximum one-sided fill that cannot breach the configured Delta boundary."""
        available = limit - delta if side == "BUY" else limit + delta
        return max(Decimal("0"), available)

    def _recalculate(self, symbol: str) -> None:
        runtime = self.state.symbolStates[symbol]
        full_history = self.matched_trades[symbol]
        runtime.reconciliation = reconcile(symbol, self.clients[symbol], self.hedges[symbol])
        runtime.position = runtime.reconciliation.delta
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
        self.state.metrics.exceptionCount = sum(
            1 for item in self.state.symbolStates.values() if item.reconciliation.status == "exception"
        )
        if abs(runtime.reconciliation.delta) > runtime.config.deltaLimit:
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
            "pendingArmActor": self.risk_gate.pending_arm_actor,
            "pendingArmUntil": self.risk_gate.pending_arm_until,
            "requiresDualApproval": self.risk_gate.require_dual_approval,
            "limits": self.risk_limits_summary(),
        }

    def risk_limits_summary(self) -> dict:
        limits = self.risk_gate.limits
        return {
            "maxSingleOrderJpy": limits.max_single_order_jpy,
            "maxDailyVolumeJpy": limits.max_daily_volume_jpy,
            "maxDailyLossJpy": limits.max_daily_loss_jpy,
            "maxAbsDelta": limits.max_abs_delta,
            "maxHedgeFailures": limits.max_hedge_failures,
            "maxHedgeP95Ms": limits.max_hedge_p95_ms,
            "armTtlSec": limits.arm_ttl_sec,
        }

    async def configure_risk_limits(self, update: RiskLimitsUpdate) -> dict:
        if self.state.mode == "live" and self.risk_gate.armed:
            await self.disarm("risk limits changed", "operator")
        values = update.model_dump(exclude_none=True)
        # Lease duration is dictated by the mode: one hour for live trading and
        # 24 hours for Paper runs. Keep accepting the UI's field for compatibility.
        values.pop("armTtlSec", None)
        minimum_p95 = max(
            runtime.config.gmoPostOnlyTimeoutMs for runtime in self.state.symbolStates.values()
        ) + 1200
        requested_p95 = values.get("maxHedgeP95Ms")
        if requested_p95 is not None and requested_p95 < minimum_p95:
            raise ValueError(
                f"maxHedgeP95Ms 必须至少为 {minimum_p95}ms "
                "（SOK 等待时间 + 撤单/FAK 确认余量）"
            )
        mapping = {
            "maxSingleOrderJpy": "max_single_order_jpy",
            "maxDailyVolumeJpy": "max_daily_volume_jpy",
            "maxDailyLossJpy": "max_daily_loss_jpy",
            "maxAbsDelta": "max_abs_delta",
            "maxHedgeFailures": "max_hedge_failures",
            "maxHedgeP95Ms": "max_hedge_p95_ms",
            "armTtlSec": "arm_ttl_sec",
        }
        self.risk_gate.limits = replace(
            self.risk_gate.limits, **{mapping[key]: value for key, value in values.items()},
        )
        if self._engine_ready:
            await self.state_store.set_state("risk.limits", asdict(self.risk_gate.limits))
        await self._record("warning", "risk.limits.updated", "实盘风控限额已更新", values)
        self._publish()
        return self.risk_limits_summary()

    async def _load_runtime_settings(self) -> None:
        saved_scenarios = await self.state_store.get_state("paper.scenarios", None)
        if self.paper_broker is not None and isinstance(saved_scenarios, dict):
            self.paper_broker.configure(saved_scenarios)
        saved_fees = await self.state_store.get_state("gmo.fee_overrides", None)
        saved_maker_fees = await self.state_store.get_state("gmo.maker_fee_overrides", None)
        if isinstance(saved_maker_fees, dict):
            self._gmo_maker_fee_overrides = {
                str(asset).upper(): float(value) for asset, value in saved_maker_fees.items()
            }
        if isinstance(saved_fees, dict):
            self._gmo_fee_overrides = {
                str(asset).upper(): float(value) for asset, value in saved_fees.items()
            }
        if isinstance(saved_fees, dict) or isinstance(saved_maker_fees, dict):
            for runtime in self.state.symbolStates.values():
                runtime.config = runtime.config.model_copy(update={
                    "gmoFeeBps": gmo_taker_bps(runtime.instrument.baseAsset, self._gmo_fee_overrides),
                    "gmoMakerFeeBps": gmo_maker_bps(
                        runtime.instrument.baseAsset, self._gmo_maker_fee_overrides,
                    ),
                })
                validate_config(runtime.config)
        saved_limits = await self.state_store.get_state("risk.limits", None)
        if isinstance(saved_limits, dict):
            allowed = set(asdict(self.risk_gate.limits))
            self.risk_gate.limits = replace(
                self.risk_gate.limits,
                **{key: value for key, value in saved_limits.items() if key in allowed and key != "arm_ttl_sec"},
            )
        # Do not inherit an arm lease from an older deployment or another mode.
        self.risk_gate.limits = replace(
            self.risk_gate.limits, arm_ttl_sec=ARM_TTL_BY_MODE[self.state.mode],
        )
        minimum_p95 = max(
            runtime.config.gmoPostOnlyTimeoutMs for runtime in self.state.symbolStates.values()
        ) + 1200
        if self.risk_gate.limits.max_hedge_p95_ms < minimum_p95:
            self.risk_gate.limits = replace(
                self.risk_gate.limits, max_hedge_p95_ms=max(2500, minimum_p95),
            )
            await self.state_store.set_state("risk.limits", asdict(self.risk_gate.limits))
        elif isinstance(saved_limits, dict) and saved_limits.get("arm_ttl_sec") != self.risk_gate.limits.arm_ttl_sec:
            await self.state_store.set_state("risk.limits", asdict(self.risk_gate.limits))
        self._sync_primary()

    async def arm(self, phrase: str, actor: str) -> dict:
        if not self._bittrade_configured() or not self.gmo.api_key or not self.gmo.secret_key:
            raise ValueError("BitTrade 与 GMO 私有 API 凭据必须全部配置")
        stale = [
            symbol for symbol, runtime in self.state.symbolStates.items()
            if self.market_feed.age_ms(symbol) > runtime.config.staleMarketMs
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
        armed = await self.risk_gate.arm(phrase, actor)
        if not armed:
            await self._record(
                "warning", "risk.arm.first_approval",
                "第一位操作员已确认，等待不同操作员在 5 分钟内复核", {"actor": actor},
            )
            self._publish()
            return self.risk_status()
        self.state.running = True
        await self._record(
            "critical", "risk.armed",
            f"{'实盘' if self.state.mode == 'live' else 'Paper'}下单权限已临时启用", {"actor": actor},
        )
        self._publish()
        return self.risk_status()

    async def disarm(self, reason: str, actor: str = "operator") -> dict:
        was_armed = self.risk_gate.armed
        await self.risk_gate.disarm(reason, actor)
        self.state.running = False
        if was_armed:
            await self._cancel_all_live()
        await self._record("warning", "risk.disarmed", reason, {"actor": actor})
        self._publish()
        return self.risk_status()

    async def _start_live_components(self) -> None:
        if self.gmo_market_ws_task is None:
            self.gmo_market_ws_task = asyncio.create_task(
                self._market_stream_factory(
                    [runtime.instrument.baseAsset for runtime in self.state.symbolStates.values()],
                    self.market_feed,
                ),
                name="market-stream",
            )
        if self.bittrade_depth_feed is None:
            self.bittrade_depth_feed = self._bittrade_depth_factory(list(self.state.activeSymbols))
            await self.bittrade_depth_feed.start()
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
        price_ticks = {
            symbol: Decimal(str(runtime.instrument.priceTick))
            for symbol, runtime in self.state.symbolStates.items()
        }
        executor = GmoHedgeExecutor(
            self.gmo, self.rate_limiter, size_steps,
            price_ticks=price_ticks,
            passive_price=self._gmo_passive_price,
            maker_fee_bps={
                symbol: Decimal(str(runtime.config.gmoMakerFeeBps))
                for symbol, runtime in self.state.symbolStates.items()
            },
            taker_fee_bps={
                symbol: Decimal(str(runtime.config.gmoFeeBps))
                for symbol, runtime in self.state.symbolStates.items()
            },
            passive_timeout_ms={
                symbol: runtime.config.gmoPostOnlyTimeoutMs
                for symbol, runtime in self.state.symbolStates.items()
            },
        )
        self.hedge_worker = HedgeWorker(
            self.state_store, self.events, executor, self.risk_gate,
            min_sizes=min_sizes,
            # Every executable incremental fill is hedged immediately. Only true dust below
            # the GMO minimum enters HedgeWorker's timed accumulation bucket.
            delta_thresholds=min_sizes,
            resolver=executor.resolve,
            on_execution=self._on_live_hedge_execution,
        )
        await self.hedge_worker.start()
        if self.state.mode == "paper":
            self.fill_tracker = FillTracker(
                self.state_store, self.events, on_fill=self._on_live_maker_fill,
            )
            await self.fill_tracker.start()
            saved_matching_stats = await self.state_store.get_state("paper.matching.stats", {})
            self.paper_engine = PaperMatchingEngine(
                self.fill_tracker, self.bittrade_depth_feed,
                maker_fee_bps=lambda symbol: Decimal(str(
                    self.state.symbolStates[symbol].config.bittradeMakerFeeBps
                )),
                initial_stats=saved_matching_stats if isinstance(saved_matching_stats, dict) else {},
                stats_callback=lambda stats: self.state_store.set_state("paper.matching.stats", stats),
            )
            self.execution_gateway.set_fill_reconciler(None)
            self.execution_gateway.set_paper_engine(self.paper_engine)
            self.paper_trade_task = asyncio.create_task(
                self._consume_public_trades(
                    self._paper_trade_stream_factory(list(self.state.activeSymbols)),
                ),
                name="bittrade-public-trades",
            )
            self.paper_resync_task = asyncio.create_task(
                self.paper_engine.resync_queue(), name="paper-queue-resync",
            )
        else:
            source = BitTradeRestFillSource(self.bittrade, self.state_store)
            self.rest_fill_source = source
            self.fill_tracker = FillTracker(
                self.state_store, self.events, rest_source=source, on_fill=self._on_live_maker_fill,
            )
            self.execution_gateway.set_fill_reconciler(self._reconcile_order_matches)
            stream = self._private_stream_factory(
                self.bittrade, list(self.state.activeSymbols),
                on_disconnect=self._ws_disconnected, on_reconnect=self._ws_reconnected,
            )
            await self.fill_tracker.start(stream)

    async def _consume_public_trades(self, stream: AsyncIterator[PublicTrade]) -> None:
        async for trade in stream:
            engine = self.paper_engine
            if engine is not None:
                await engine.on_trade(trade)

    def _gmo_passive_price(self, symbol: str, side: str) -> Decimal:
        runtime = self.state.symbolStates.get(symbol)
        market = self.market_feed.latest.get(symbol) or (runtime.market if runtime else None)
        if runtime is None or market is None:
            raise RuntimeError(f"{symbol} 没有可用于 SOK 对冲的 GMO 行情")
        if self.market_feed.latest.get(symbol) is not None \
                and self.market_feed.age_ms(symbol) > runtime.config.staleMarketMs:
            raise RuntimeError(f"{symbol} GMO 行情过期，拒绝提交 SOK 对冲")
        tick = Decimal(str(runtime.instrument.priceTick))
        raw = Decimal(str(market.bid if side == "BUY" else market.ask))
        rounding = ROUND_DOWN if side == "BUY" else ROUND_UP
        return (raw / tick).to_integral_value(rounding=rounding) * tick

    async def _stop_live_components(self) -> None:
        if self.gmo_market_ws_task:
            self.gmo_market_ws_task.cancel()
            try:
                await self.gmo_market_ws_task
            except asyncio.CancelledError:
                pass
            self.gmo_market_ws_task = None
        for attribute in ("paper_trade_task", "paper_resync_task"):
            task = getattr(self, attribute)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, attribute, None)
        if self.paper_engine is not None:
            await self.paper_engine.flush_stats()
        self.execution_gateway.set_paper_engine(None)
        self.paper_engine = None
        if self.bittrade_depth_feed is not None:
            await self.bittrade_depth_feed.stop()
            self.bittrade_depth_feed = None
        self._rest_market_fallback.clear()
        self._bittrade_depth_cache.clear()
        self._bittrade_depth_errors.clear()
        if self.fill_tracker:
            await self.fill_tracker.stop()
            self.fill_tracker = None
            self.rest_fill_source = None
            self.execution_gateway.set_fill_reconciler(None)
        if self.hedge_worker:
            await self.hedge_worker.stop()
            self.hedge_worker = None

    async def _cancel_all_live(self) -> None:
        if not self._bittrade_configured():
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
        for event in await self.rest_fill_source():
            await self.fill_tracker.ingest(event)
        await self.rest_fill_source.checkpoint()
        await self._refresh_live_projection()

    async def _reconcile_order_matches(self, order: dict) -> None:
        if self.fill_tracker is None or self.rest_fill_source is None:
            raise RuntimeError("fill tracker is not running")
        for event in await self.rest_fill_source.for_orders([order]):
            await self.fill_tracker.ingest(event)

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
        ages = [self.market_feed.age_ms(symbol) for symbol in self.state.symbolStates]
        max_age = max(ages, default=0)
        stale_limit = min(runtime.config.staleMarketMs for runtime in self.state.symbolStates.values())
        was_armed = self.risk_gate.armed
        had_arm_lease = self.risk_gate.armed_until > 0
        pending = await self.state_store.pending_hedge_exposure()
        day = datetime.now(timezone.utc).date().isoformat()
        daily_volume = await self.state_store.daily_fill_volume(day)
        daily_pnl = await self.state_store.daily_realized_pnl(
            day, maker_fee_bps=Decimal(str(self.state.config.bittradeMakerFeeBps)),
            hedge_fee_bps={
                symbol: Decimal(str(expected_gmo_fee_bps(runtime.config)))
                for symbol, runtime in self.state.symbolStates.items()
            },
        )
        hedge_failures, hedge_p95 = await self.state_store.hedge_health(day)
        snapshot = RiskSnapshot(
            market_age_ms=max_age, stale_market_ms=stale_limit,
            daily_pnl_jpy=float(daily_pnl),
            daily_volume_jpy=float(daily_volume),
            abs_delta=max(
                [abs(runtime.reconciliation.delta) for runtime in self.state.symbolStates.values()]
                + [float(abs(value)) for value in pending.values()], default=0,
            ),
            hedge_failures=hedge_failures,
            hedge_p95_ms=max(self.state.metrics.hedgeP95Ms, hedge_p95),
        )
        self._last_live_risk_snapshot = snapshot
        allowed, reason = await self.risk_gate.evaluate(
            snapshot, enforce=self.places_real_orders,
        )
        if not self.places_real_orders:
            self._paper_risk_observations += 1
            observed_reason = reason if not allowed else None
            if observed_reason is not None:
                self._paper_risk_would_reject += 1
            if observed_reason != self._paper_risk_current_reason:
                previous = self._paper_risk_current_reason
                self._paper_risk_current_reason = observed_reason
                if observed_reason is not None:
                    await self._record(
                        "warning", "risk.paper.would_reject",
                        f"Paper RiskGate 本轮若为实盘将拒绝：{observed_reason}",
                    )
                elif previous is not None:
                    await self._record(
                        "info", "risk.paper.recovered",
                        f"Paper RiskGate 观测恢复：{previous}",
                    )
            # Paper deliberately observes all live limits without stopping data
            # collection. Stale quotes are independently canceled and suppressed
            # by _run_live_quotes, which checks every symbol before placement.
            if self.risk_gate.killed:
                self.state.killSwitch = True
            return
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
            self._position_baseline.setdefault(("bittrade", asset), available)
            await self.balance_cache.update("bittrade", asset, available)
            await self.state_store.upsert_balance("bittrade", asset, available, Decimal("0"), updated_at)
        for row in gmo_rows:
            asset = str(row.get("symbol", row.get("currency", ""))).upper()
            if not asset:
                continue
            available = Decimal(str(row.get("available", row.get("amount", "0"))))
            self._position_baseline.setdefault(("gmo", asset), available)
            await self.balance_cache.update("gmo", asset, available)
            await self.state_store.upsert_balance("gmo", asset, available, Decimal("0"), updated_at)
        self._positions_updated_at = updated_at
        required = {("bittrade", "JPY"), ("gmo", "JPY")}
        required.update((venue, runtime.instrument.baseAsset) for venue in ("bittrade", "gmo")
                        for runtime in self.state.symbolStates.values())
        missing = [f"{venue}:{asset}" for venue, asset in required
                   if not self.balance_cache.has(venue, asset)]
        if missing:
            raise RuntimeError(f"余额响应缺少：{', '.join(missing)}")

    async def _run_live_quotes(self) -> None:
        if not self.risk_gate.armed or not self.state.running:
            return
        stale_symbols = [
            symbol for symbol, runtime in self.state.symbolStates.items()
            if self.market_feed.age_ms(symbol) > runtime.config.staleMarketMs
        ]
        if stale_symbols:
            if await self.state_store.open_orders():
                await self._cancel_all_live()
            for symbol in stale_symbols:
                self.state.symbolStates[symbol].quotes = []
            return
        if self.balance_cache.stale():
            try:
                await self._refresh_balances()
            except Exception as exc:
                await self.disarm(f"balance refresh failed: {self._safe_error(exc)}", "system")
                return
        rows = await self.state_store.open_orders()
        uncertain = [row for row in rows if row["state"] not in ("OPEN", "PARTIAL")]
        for row in uncertain:
            if row.get("exchange_order_id"):
                await self.execution_gateway.confirm(
                    row["client_order_id"], row["exchange_order_id"],
                )
        rows = await self.state_store.open_orders()
        uncertain = [row for row in rows if row["state"] not in ("OPEN", "PARTIAL")]
        if uncertain:
            await self.disarm(f"{len(uncertain)} orders require reconciliation", "system")
            return
        open_by_key = {(row["symbol"], row["side"]): row for row in rows}
        symbols = list(self.state.symbolStates)
        depth_results = await asyncio.gather(
            *(self._bittrade_book(symbol) for symbol in symbols), return_exceptions=True,
        )
        bittrade_depth: dict[
            str, tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]
        ] = {}
        for symbol, result in zip(symbols, depth_results, strict=True):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, Exception):
                if symbol not in self._bittrade_depth_errors:
                    self._bittrade_depth_errors.add(symbol)
                    await self._record(
                        "warning", "bittrade.depth.failed",
                        f"{symbol} 无法确认 BitTrade 盘口，本轮跳过报价：{self._safe_error(result)}",
                    )
                continue
            bittrade_depth[symbol] = result
            if symbol in self._bittrade_depth_errors:
                self._bittrade_depth_errors.discard(symbol)
                await self._record("info", "bittrade.depth.recovered", f"{symbol} BitTrade 盘口已恢复")
        for symbol, runtime in self.state.symbolStates.items():
            blockers = self.balance_cache.pair_blockers(runtime.instrument.baseAsset, require_actual=True)
            if blockers:
                await self._disable_symbol(symbol, blockers)
                continue
            self.state.disabledSymbols.pop(symbol, None)
            targets = self._timed_quotes(runtime.market, runtime.config, runtime.instrument)
            if symbol not in bittrade_depth:
                continue
            bittrade_bids, bittrade_asks = bittrade_depth[symbol]
            bittrade_best_bid, bittrade_best_ask = bittrade_bids[0][0], bittrade_asks[0][0]
            adjusted = []
            for quote in targets:
                key = (symbol, quote.side)
                depth = Decimal(str(quote.size))
                gmo_hedge_price = Decimal(str(
                    runtime.market.bid if quote.side == "BUY" else runtime.market.ask
                ))
                required_edge = Decimal(str(max(
                    runtime.config.spreadBps,
                    runtime.config.bittradeMakerFeeBps
                    + expected_gmo_fee_bps(runtime.config)
                    + max(runtime.config.expectedSlippageBps, runtime.config.maxHedgeSlippageBps),
                )))
                book_levels = bittrade_bids if quote.side == "BUY" else bittrade_asks
                selected_price = target_price(
                    book_levels, gmo_hedge_price, required_edge,
                    Decimal(str(runtime.config.queueBudget)),
                    Decimal(str(runtime.instrument.priceTick)), quote.side,
                    opposite_best=bittrade_best_bid if quote.side == "SELL" else bittrade_best_ask,
                )
                current = open_by_key.get(key)
                if selected_price is None:
                    if current:
                        await self.execution_gateway.cancel(current)
                        open_by_key.pop(key, None)
                    self._working_quotes.pop(key, None)
                    continue
                capacity = self.balance_cache.quote_capacity(
                    side=quote.side, base_asset=runtime.instrument.baseAsset,
                    price=selected_price,
                    strategy_limit=min(
                        Decimal(str(runtime.config.maxQuoteSize)),
                        Decimal(str(self.risk_gate.limits.max_single_order_jpy)) / selected_price,
                        self._delta_headroom(
                            quote.side,
                            Decimal(str(runtime.reconciliation.delta)),
                            min(
                                Decimal(str(runtime.config.deltaLimit)),
                                Decimal(str(self.risk_gate.limits.max_abs_delta)),
                            ),
                        ),
                    ),
                    hedge_depth=depth,
                )
                size = Decimal(str(self._floor_size(float(capacity), runtime.instrument.sizeStep)))
                if size < Decimal(str(runtime.instrument.minOrderSize)):
                    if current:
                        await self.execution_gateway.cancel(current)
                        open_by_key.pop(key, None)
                        self._working_quotes.pop(key, None)
                    continue
                adjusted_quote = quote.model_copy(update={"size": float(size), "price": float(selected_price)})
                target_order_price = selected_price
                would_cross = (quote.side == "SELL" and target_order_price <= bittrade_best_bid) \
                    or (quote.side == "BUY" and target_order_price >= bittrade_best_ask)
                if would_cross:
                    if current:
                        await self.execution_gateway.cancel(current)
                        open_by_key.pop(key, None)
                    self._working_quotes.pop(key, None)
                    continue
                adjusted.append(adjusted_quote)
                cached = self._working_quotes.get(key)
                working = None
                if current:
                    working = WorkingQuote(
                        price=Decimal(current["price"]), original_qty=Decimal(current["qty"]),
                        remaining_qty=max(Decimal("0"), Decimal(current["qty"]) - Decimal(current["cumulative_filled"])),
                        reference_depth=cached.reference_depth if cached else depth,
                    )
                if not self.quote_engine.should_requote(
                    working, target_price=target_order_price, target_qty=size, current_depth=depth,
                ):
                    continue
                snapshot = RiskSnapshot(
                    market_age_ms=self.market_feed.age_ms(symbol),
                    stale_market_ms=runtime.config.staleMarketMs,
                    daily_pnl_jpy=self._last_live_risk_snapshot.daily_pnl_jpy,
                    daily_volume_jpy=self._last_live_risk_snapshot.daily_volume_jpy,
                    abs_delta=max(abs(runtime.reconciliation.delta), self._last_live_risk_snapshot.abs_delta),
                    hedge_failures=self._last_live_risk_snapshot.hedge_failures,
                    hedge_p95_ms=max(runtime.hedgeP95Ms, self._last_live_risk_snapshot.hedge_p95_ms),
                )
                try:
                    result = await self.execution_gateway.replace(
                        current, symbol=symbol, side=quote.side, qty=size, price=target_order_price,
                        size_step=Decimal(str(runtime.instrument.sizeStep)),
                        price_tick=Decimal(str(runtime.instrument.priceTick)), snapshot=snapshot,
                    )
                except Exception as exc:
                    await self.disarm(f"quote execution uncertain: {self._safe_error(exc)}", "system")
                    return
                if result["state"] == "OPEN":
                    self._working_quotes[key] = WorkingQuote(
                        price=target_order_price, original_qty=size, remaining_qty=size, reference_depth=depth,
                    )
                    open_by_key[key] = result
                elif result["state"] == "FILLED":
                    self._working_quotes.pop(key, None)
                    open_by_key.pop(key, None)
                    continue
                elif result.get("last_error") in ("post_only_reject", "depth_unavailable"):
                    self._working_quotes.pop(key, None)
                    open_by_key.pop(key, None)
                    continue
                else:
                    await self.disarm(f"quote entered {result['state']}; reconciliation required", "system")
                    return
            runtime.quotes = adjusted

    async def _bittrade_book(self, symbol: str) -> tuple[
        list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]
    ]:
        if self.bittrade_depth_feed is None:
            raise RuntimeError("BitTrade WebSocket 深度流尚未启动")
        return self.bittrade_depth_feed.book(symbol)

    async def _on_live_maker_fill(self, fill: FillDelta) -> None:
        await self._apply_local_maker_balance(fill)
        self._schedule_projection(fill.symbol)

    async def _on_live_hedge_execution(self, symbol: str, side: str,
                                        execution: HedgeExecution) -> None:
        await self._apply_local_hedge_balance(symbol, side, execution)
        self._schedule_projection(symbol)

    def _schedule_projection(self, symbol: str) -> None:
        self._projection_symbols.add(symbol)
        if self._projection_task is None or self._projection_task.done():
            self._projection_task = asyncio.create_task(
                self._drain_projections(), name="state-projection",
            )

    async def _drain_projections(self) -> None:
        try:
            while self._projection_symbols:
                await asyncio.sleep(.05)
                symbols = tuple(self._projection_symbols)
                self._projection_symbols.clear()
                for symbol in symbols:
                    await self._refresh_live_projection(symbol)
                self._publish()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.state_store.audit("projection.error", "warning", self._safe_error(exc))

    async def _refresh_live_projection(self, symbol: str | None = None) -> None:
        if not self._engine_ready:
            return
        targets = [symbol] if symbol is not None else list(self.state.symbolStates)
        for target in targets:
            runtime = self.state.symbolStates.get(target)
            if runtime is None:
                continue
            rows = await self.state_store.trading_projection(target, trading_mode=self.state.mode)
            day = datetime.now(timezone.utc).date().isoformat()
            clients: list[ClientFill] = []
            hedges: list[HedgeFill] = []
            trades: list[MatchedTrade] = []
            size_step = Decimal(str(runtime.instrument.sizeStep))
            price_tick = Decimal(str(runtime.instrument.priceTick))
            for row in rows:
                qty = (Decimal(row["incremental_qty"]) / size_step).to_integral_value(
                    rounding=ROUND_DOWN,
                ) * size_step
                if qty <= 0:
                    continue
                price = (Decimal(row["price"]) / price_tick).to_integral_value(
                    rounding=ROUND_HALF_UP,
                ) * price_tick
                client_fee = -price * qty * Decimal(str(runtime.config.bittradeMakerFeeBps)) / Decimal("10000")
                client = ClientFill(
                    id=f"BT-{row['fill_id']}", orderId=str(row["order_id"]), symbol=row["symbol"],
                    side=row["side"], price=float(price), size=float(qty), fee=float(client_fee),
                    role="maker", timestamp=row["occurred_at"],
                )
                clients.append(client)
                filled_qty = (Decimal(row["filled_qty"] or "0") / size_step).to_integral_value(
                    rounding=ROUND_DOWN,
                ) * size_step
                if not row["hedge_id"] or filled_qty <= 0:
                    continue
                filled_notional = Decimal(row["filled_notional"] or "0")
                hedge_price = ((filled_notional / filled_qty) / price_tick).to_integral_value(
                    rounding=ROUND_HALF_UP,
                ) * price_tick
                hedge_fee = Decimal(str(row.get("fee_jpy") or "0"))
                if hedge_fee == 0:
                    hedge_fee = filled_notional * Decimal(str(expected_gmo_fee_bps(runtime.config))) \
                        / Decimal("10000")
                hedge = HedgeFill(
                    id=str(row["hedge_id"]), orderId=str(row["hedge_order_id"] or ""),
                    clientFillId=client.id, symbol=row["symbol"], side=row["hedge_side"],
                    price=float(hedge_price), size=float(filled_qty), fee=float(hedge_fee),
                    latencyMs=int(row["latency_ms"] or 0), timestamp=row["occurred_at"],
                    status="filled" if filled_qty >= qty else "partial",
                )
                hedges.append(hedge)
                matched_qty = min(qty, filled_qty)
                matched_client = client.model_copy(update={
                    "size": float(matched_qty),
                    "fee": float(-price * matched_qty * Decimal(str(runtime.config.bittradeMakerFeeBps))
                                 / Decimal("10000")),
                })
                matched_hedge = hedge.model_copy(update={
                    "size": float(matched_qty),
                    "fee": float(hedge_fee * matched_qty / filled_qty),
                })
                if row["occurred_at"].startswith(day):
                    trades.append(matched_trade(matched_client, matched_hedge))
            self.clients[target] = clients
            self.hedges[target] = hedges
            self.matched_trades[target] = trades
            runtime.trades = list(reversed(trades[-50:]))
            self._recalculate(target)
        pending_symbols = {intent.symbol for intent in await self.state_store.pending_hedges()}
        for target, runtime in self.state.symbolStates.items():
            if target in pending_symbols:
                runtime.reconciliation = runtime.reconciliation.model_copy(update={"status": "exception"})
        self.state.metrics.exceptionCount = sum(
            1 for runtime in self.state.symbolStates.values()
            if runtime.reconciliation.status == "exception"
        )
        if self.state.mode == "paper":
            await self._rebuild_paper_holdings_from_projection()
        self._sync_primary()

    async def _rebuild_paper_holdings_from_projection(self) -> None:
        """Project Paper venue balances from the same durable fills used by Delta/PnL.

        Fake venues update their authoritative balances before publishing a fill, while
        the service also applies an immediate local delta. A periodic balance snapshot
        can therefore race a projection callback. Rebuilding the Paper ledger from the
        durable fill/hedge projection keeps holdings, Delta, PnL, and exports on one
        accounting source without changing the live balance-cache behavior.
        """
        balances: dict[tuple[str, str], Decimal] = {
            (venue, asset): self._position_baseline.get((venue, asset), amount)
            for venue, assets in self.inventory_allocations.items()
            for asset, amount in assets.items()
        }

        def add(venue: str, asset: str, amount: Decimal) -> None:
            key = (venue, asset)
            opening = self._position_baseline.get(
                key, self.inventory_allocations.get(venue, {}).get(asset, Decimal("0")),
            )
            balances[key] = balances.get(key, opening) + amount

        for symbol in self.state.symbolStates:
            base_asset = symbol.removesuffix("_JPY")
            for fill in self.clients.get(symbol, []):
                qty = Decimal(str(fill.size))
                notional = Decimal(str(fill.price)) * qty
                maker_fee = Decimal(str(fill.fee))
                if fill.side == "BUY":
                    add("bittrade", base_asset, qty)
                    add("bittrade", "JPY", -notional + maker_fee)
                else:
                    add("bittrade", base_asset, -qty)
                    add("bittrade", "JPY", notional + maker_fee)
            for hedge in self.hedges.get(symbol, []):
                qty = Decimal(str(hedge.size))
                notional = Decimal(str(hedge.price)) * qty
                hedge_fee = Decimal(str(hedge.fee))
                if hedge.side == "BUY":
                    add("gmo", base_asset, qty)
                    add("gmo", "JPY", -(notional + hedge_fee))
                else:
                    add("gmo", base_asset, -qty)
                    add("gmo", "JPY", notional - hedge_fee)

        for (venue, asset), amount in balances.items():
            await self.balance_cache.update(venue, asset, amount)
        self._positions_updated_at = utc_now()
        self.state.holdings = self.holdings_snapshot()

    async def _apply_local_maker_balance(self, fill) -> None:
        if self.state.mode == "paper":
            # Paper holdings are rebuilt from durable fills/hedges so concurrent fake
            # exchange snapshots cannot double-apply the same balance movement.
            return
        base_asset = fill.symbol.removesuffix("_JPY")
        notional = fill.incremental_qty * fill.price
        fee = Decimal(str(fill.fee))
        if fill.side == "BUY":
            deltas = (("JPY", -(notional + fee)), (base_asset, fill.incremental_qty))
        else:
            deltas = ((base_asset, -fill.incremental_qty), ("JPY", notional - fee))
        for asset, delta in deltas:
            await self._apply_balance_delta("bittrade", asset, delta)

    async def _apply_local_hedge_balance(self, symbol: str, side: str, execution) -> None:
        base_asset = symbol.removesuffix("_JPY")
        runtime = self.state.symbolStates.get(symbol)
        if runtime is not None and execution.filled_qty > 0:
            reference = Decimal(str(runtime.market.ask if side == "BUY" else runtime.market.bid))
            actual = execution.filled_notional / execution.filled_qty
            if reference > 0:
                realized_bps = abs(actual - reference) / reference * Decimal("10000")
                await self._record(
                    "info", "hedge.slippage.realized",
                    f"{symbol} GMO 实际对冲滑点 {realized_bps:.3f} bps",
                    {
                        "symbol": symbol, "side": side, "orderId": execution.order_id,
                        "referencePrice": str(reference), "actualPrice": str(actual),
                        "realizedBps": str(realized_bps),
                        "submittedAt": execution.submitted_at, "confirmedAt": execution.confirmed_at,
                    },
                )
        if self.state.mode == "paper":
            return
        fee = execution.fee_jpy
        if side == "BUY":
            deltas = (("JPY", -(execution.filled_notional + fee)), (base_asset, execution.filled_qty))
        else:
            deltas = ((base_asset, -execution.filled_qty), ("JPY", execution.filled_notional - fee))
        for asset, delta in deltas:
            await self._apply_balance_delta("gmo", asset, delta)

    async def _apply_balance_delta(self, venue: str, asset: str, delta: Decimal) -> None:
        await self.balance_cache.apply_local_delta(venue, asset, delta)
        self._positions_updated_at = utc_now()
        self.state.holdings = self.holdings_snapshot()
        if self._engine_ready:
            await self.state_store.upsert_balance(
                venue, asset, self.balance_cache.available(venue, asset), Decimal("0"),
                self._positions_updated_at,
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
        if self.paper_broker is not None:
            self.paper_broker.set_allocations(self.inventory_allocations)
            await self._seed_paper_holdings(reset=True)
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
        if self.state.mode == "live" and self.risk_gate.armed:
            await self.disarm("inventory configuration changed", "operator")
        self.inventory_allocations = normalized
        self.balance_cache.configure_allocations(normalized)
        if self.paper_broker is not None:
            self.paper_broker.set_allocations(self.inventory_allocations)
            if self._engine_ready:
                await self.paper_broker.persist()
            await self._seed_paper_holdings(reset=True)
        if clear_webhook:
            self.notifier.configure("")
        elif webhook_url is not None and webhook_url.strip():
            self.notifier.configure(webhook_url)
        await self._persist_inventory()
        await self._recompute_inventory_status(notify=True)
        await self._record("warning", "inventory.updated", "双交易所底仓配置已更新")
        self._publish()
        return self.inventory_summary()

    async def _seed_paper_holdings(self, *, reset: bool) -> None:
        for venue, assets in self.inventory_allocations.items():
            for asset, amount in assets.items():
                key = (venue, asset)
                if reset or not self.balance_cache.has(venue, asset):
                    await self.balance_cache.update(venue, asset, amount)
                    self._position_baseline[key] = amount
        self._positions_updated_at = utc_now()

    def holdings_snapshot(self) -> HoldingsState:
        venues: dict[str, dict[str, AssetHolding]] = {"bittrade": {}, "gmo": {}}
        for venue in venues:
            assets = self.balance_cache.assets(venue)
            assets.update(self.inventory_allocations.get(venue, {}))
            for asset in sorted(assets, key=lambda value: (value != "JPY", value)):
                key = (venue, asset)
                has_balance = self.balance_cache.has(venue, asset)
                available = self.balance_cache.available(venue, asset) if has_balance else None
                opening = self._position_baseline.get(key)
                venues[venue][asset] = AssetHolding(
                    configured=float(self.balance_cache.allocation(venue, asset)),
                    opening=float(opening) if opening is not None else None,
                    available=float(available) if available is not None else None,
                    reserved=float(self.balance_cache.reserved(venue, asset)) if has_balance else 0,
                    change=float(available - opening) if available is not None and opening is not None else None,
                )
        source = "paper" if self.state.mode == "paper" else \
            "exchange" if any(self.balance_cache.has(venue, asset) for venue in venues for asset in venues[venue]) \
            else "configured"
        return HoldingsState(
            source=source, updatedAt=self._positions_updated_at,
            bittrade=venues["bittrade"], gmo=venues["gmo"],
        )

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

    def _bittrade_configured(self) -> bool:
        return bool(self.bittrade.access_key and self.bittrade.secret_key and self.bittrade.account_id)

    def _publish(self) -> None:
        self._sync_primary()
        self.state.holdings = self.holdings_snapshot()
        payload = self.state.model_dump_json()
        for queue in self.subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(payload)
