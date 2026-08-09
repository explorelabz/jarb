from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator
from uuid import uuid4

from .audit_store import AuditStore
from .core import make_quotes, matched_trade, opposite_side, reconcile, validate_config
from .models import (
    AuditEvent, ClientFill, HedgeFill, MarketTop, Metrics, Pnl, SimulatedFillRequest,
    StrategyConfig, SystemState, utc_now,
)


class TradingService:
    def __init__(self, config: StrategyConfig, mode: str = "simulation"):
        validate_config(config)
        self.started_ns = time.monotonic_ns()
        self.clients: list[ClientFill] = []
        self.hedges: list[HedgeFill] = []
        self.store = AuditStore()
        self.lock = asyncio.Lock()
        self.subscribers: set[asyncio.Queue[str]] = set()
        self.task: asyncio.Task | None = None
        self.core_samples_us: list[float] = []
        market = MarketTop(symbol=config.symbol, bid=17_482_140, ask=17_493_860, bidSize=0.4382,
                           askSize=0.3167, timestamp=utc_now(), source="SIM")
        self.state = SystemState(
            mode=mode, running=True, killSwitch=False, market=market, quotes=make_quotes(market, config),
            position=0, reconciliation=reconcile(config.symbol, [], []), pnl=Pnl(), metrics=Metrics(),
            trades=[], events=[], config=config,
        )

    async def start(self) -> None:
        if self.task is None:
            await self._record("info", "system.started", f"策略以{'实盘' if self.state.mode == 'live' else '模拟'}模式启动")
            self.task = asyncio.create_task(self._run(), name="market-loop")

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None

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
        config = self.state.config.model_copy(update=patch)
        config = StrategyConfig.model_validate(config.model_dump())
        validate_config(config)
        async with self.lock:
            self.state.config = config
            self.state.quotes = self._timed_quotes(self.state.market, config)
        await self._record("info", "strategy.updated", "报价与风险参数已更新", patch)
        self._publish()

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
        async with self.lock:
            if not self.state.running or self.state.killSwitch:
                raise ValueError("策略未运行，无法接受模拟成交")
            quote = next((q for q in self.state.quotes if q.side == request.side), None)
            if quote is None:
                raise ValueError("当前方向没有可用报价")
            size = min(max(request.size, 0.0001), quote.size)
            client = ClientFill(id=f"BT-{uuid4().hex[:8]}", orderId=f"Q-{uuid4().hex[:8]}", symbol=self.state.config.symbol,
                side=request.side, price=quote.price, size=size, fee=quote.price * size * 0.001 if request.role == "taker" else 0,
                role=request.role, timestamp=utc_now())
            self.clients.append(client)
            self.state.position += (1 if client.side == "BUY" else -1) * size

            latency_ms = random.randint(74, 203)
            slippage = self.state.config.expectedSlippageBps / 10_000
            side = opposite_side(client.side)
            hedge_price = round(self.state.market.ask * (1 + slippage * random.random())) if side == "BUY" else round(self.state.market.bid * (1 - slippage * random.random()))
            hedge = HedgeFill(id=f"GMO-{uuid4().hex[:8]}", orderId=f"H-{uuid4().hex[:8]}", clientFillId=client.id,
                symbol=client.symbol, side=side, price=hedge_price, size=size,
                fee=hedge_price * size * self.state.config.gmoFeeBps / 10_000, latencyMs=latency_ms,
                timestamp=utc_now(), status="filled")
            self.hedges.append(hedge)
            self.state.position += (1 if hedge.side == "BUY" else -1) * size
            trade = matched_trade(client, hedge)
            self.state.trades.insert(0, trade)
            self.state.trades = self.state.trades[:50]
            self._recalculate()
        await self._record("info", "client.fill", f"BitTrade 本公司{'买入' if client.side == 'BUY' else '卖出'} {size} BTC", {"clientFillId": client.id})
        await self._record("info", "hedge.filled", f"GMO 反向对冲完成，延迟 {latency_ms} ms", {"hedgeFillId": hedge.id, "clientFillId": client.id})
        self._publish()
        return trade

    def export_reconciliation(self) -> dict:
        return {"generatedAt": utc_now(), "scope": "daily", "core": "Rust/PyO3",
                "formula": "Σ(client signed quantity) + Σ(hedge signed quantity) = delta",
                "result": self.state.reconciliation.model_dump(), "clientFills": [x.model_dump() for x in self.clients],
                "hedgeFills": [x.model_dump() for x in self.hedges], "pnl": self.state.pnl.model_dump()}

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.state.config.quoteRefreshMs / 1000)
            async with self.lock:
                if self.state.mode == "simulation":
                    mid = (self.state.market.bid + self.state.market.ask) / 2
                    move = (random.random() - 0.5) * 4_800
                    half = 5_400 + random.random() * 900
                    self.state.market = self.state.market.model_copy(update={"bid": round(mid + move - half), "ask": round(mid + move + half),
                        "bidSize": round(0.08 + random.random() * 0.72, 4), "askSize": round(0.08 + random.random() * 0.72, 4), "timestamp": utc_now()})
                    if self.state.running:
                        self.state.quotes = self._timed_quotes(self.state.market, self.state.config)
                self.state.metrics.uptimeSec = int((time.monotonic_ns() - self.started_ns) / 1_000_000_000)
            self._publish()

    def _timed_quotes(self, market: MarketTop, config: StrategyConfig):
        start = time.perf_counter_ns()
        result = make_quotes(market, config)
        self.core_samples_us.append((time.perf_counter_ns() - start) / 1_000)
        self.core_samples_us = self.core_samples_us[-2000:]
        ordered = sorted(self.core_samples_us)
        if ordered:
            self.state.metrics.coreCalcP99Us = ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))]
        return result

    def _recalculate(self) -> None:
        self.state.reconciliation = reconcile(self.state.config.symbol, self.clients, self.hedges)
        self.state.pnl = Pnl(
            spread=sum(t.spreadPnl for t in self.state.trades), clientFees=sum(t.clientFee for t in self.state.trades),
            hedgeCosts=sum(t.hedgeCost for t in self.state.trades), net=sum(t.netPnl for t in self.state.trades),
        )
        latencies = sorted(t.latencyMs for t in self.state.trades)
        self.state.metrics.hedgeP95Ms = latencies[min(len(latencies) - 1, int(len(latencies) * .95))] if latencies else 0
        self.state.metrics.fillCount = len(self.state.trades)
        if abs(self.state.reconciliation.delta) > self.state.config.deltaLimit:
            self.state.metrics.exceptionCount += 1
            self.state.killSwitch = True
            self.state.running = False

    async def _record(self, level: str, event_type: str, message: str, metadata: dict | None = None) -> None:
        event = AuditEvent(id=str(uuid4()), timestamp=utc_now(), level=level, type=event_type, message=message, metadata=metadata)
        self.state.events.insert(0, event)
        self.state.events = self.state.events[:80]
        await self.store.append(event)

    def _publish(self) -> None:
        payload = self.state.model_dump_json()
        for queue in self.subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(payload)
