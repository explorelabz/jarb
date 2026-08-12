from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any

import httpx
from pydantic import BaseModel, Field

from ..adapters import DecimalQuote, ExchangeAPIError
from ..models import MarketTop, utc_now
from .fill_tracker import CumulativeFillEvent


PAPER_SYMBOLS: dict[str, dict[str, str | int]] = {
    "BTC": {"price": "17490000", "min": "0.0001", "max": "5", "step": "0.0001", "tick": "1", "precision": 4},
    "ETH": {"price": "520000", "min": "0.001", "max": "100", "step": "0.001", "tick": "1", "precision": 3},
    "XRP": {"price": "420", "min": "1", "max": "100000", "step": "1", "tick": "0.001", "precision": 0},
    "BCH": {"price": "86000", "min": "0.01", "max": "1000", "step": "0.01", "tick": "1", "precision": 2},
    "LTC": {"price": "18000", "min": "0.01", "max": "1000", "step": "0.01", "tick": "1", "precision": 2},
    "XLM": {"price": "58", "min": "1", "max": "100000", "step": "1", "tick": "0.001", "precision": 0},
    "BAT": {"price": "36", "min": "1", "max": "100000", "step": "1", "tick": "0.001", "precision": 0},
    "DOT": {"price": "720", "min": "0.1", "max": "10000", "step": "0.1", "tick": "0.001", "precision": 1},
}


class PaperScenarioConfig(BaseModel):
    """Fault-injection switches for paper trading. Safe defaults still exercise partial fills."""

    autoMatch: bool = True
    partialFills: bool = True
    dustFills: bool = False
    duplicateEvents: bool = False
    outOfOrderEvents: bool = False
    cancelAlreadyFilled: bool = False
    cancelRaceFill: bool = False
    gmoPartialFak: bool = False
    gmoPostOnlyFillDelayMs: int = Field(80, ge=0, le=10_000)
    # Keep the default Paper run stable enough for continuous observation. The
    # delayed-confirmation fault remains available as an explicit scenario.
    delayedExecutions: bool = False
    postOnlyReject: bool = True
    randomRateLimit: bool = False
    randomNetworkTimeout: bool = False
    autoMatchProbability: float = Field(.16, ge=0, le=1)
    dustProbability: float = Field(.15, ge=0, le=1)
    duplicateProbability: float = Field(.2, ge=0, le=1)
    outOfOrderProbability: float = Field(.2, ge=0, le=1)
    cancelRaceProbability: float = Field(.2, ge=0, le=1)
    gmoFillRatio: float = Field(.65, gt=0, le=1)
    executionDelayMinMs: int = Field(50, ge=0, le=10_000)
    executionDelayMaxMs: int = Field(300, ge=0, le=10_000)
    rateLimitProbability: float = Field(.02, ge=0, le=1)
    networkTimeoutProbability: float = Field(.01, ge=0, le=1)
    seed: int = 20260810


class PaperBroker:
    def __init__(self, scenarios: PaperScenarioConfig | None = None):
        self.scenarios = scenarios or PaperScenarioConfig()
        self.rng = random.Random(self.scenarios.seed)
        self.markets: dict[str, MarketTop] = {}
        self._live_market_updated_at: dict[str, float] = {}
        self.orders: dict[str, dict[str, Any]] = {}
        self.client_to_order: dict[str, str] = {}
        self.matches_by_order: dict[str, list[dict[str, Any]]] = {}
        self.recent: list[dict[str, Any]] = []
        self.fill_queue: asyncio.Queue[CumulativeFillEvent] = asyncio.Queue()
        self.gmo_orders: dict[str, dict[str, Any]] = {}
        self._order_seq = 0
        self._trade_seq = 0
        self._gmo_seq = 0
        self.balances: dict[str, dict[str, Decimal]] = {
            "bittrade": {"JPY": Decimal("1000000"), **{base: Decimal("10") for base in PAPER_SYMBOLS}},
            "gmo": {"JPY": Decimal("1000000"), **{base: Decimal("10") for base in PAPER_SYMBOLS}},
        }
        for base, rule in PAPER_SYMBOLS.items():
            self.markets[f"{base}_JPY"] = self._market(base, Decimal(str(rule["price"])))
        self.state_store = None

    async def restore(self) -> None:
        if self.state_store is None:
            return
        payload = await self.state_store.get_state("paper.exchange", None)
        if not isinstance(payload, dict):
            return
        self.orders = {key: dict(value) for key, value in payload.get("orders", {}).items()}
        self.client_to_order = dict(payload.get("clientToOrder", {}))
        self.matches_by_order = {
            key: [dict(row) for row in rows] for key, rows in payload.get("matchesByOrder", {}).items()
        }
        self.recent = [dict(row) for row in payload.get("recent", [])]
        self.gmo_orders = {
            key: {
                **value, "requested": Decimal(value["requested"]), "filled": Decimal(value["filled"]),
                "price": Decimal(value["price"]),
                "size_step": Decimal(value.get("size_step", "0.00000001")),
            }
            for key, value in payload.get("gmoOrders", {}).items()
        }
        self.balances = {
            venue: {asset: Decimal(amount) for asset, amount in assets.items()}
            for venue, assets in payload.get("balances", {}).items()
        } or self.balances
        self._order_seq = int(payload.get("orderSeq", self._order_seq))
        self._trade_seq = int(payload.get("tradeSeq", self._trade_seq))
        self._gmo_seq = int(payload.get("gmoSeq", self._gmo_seq))

    async def persist(self) -> None:
        if self.state_store is None:
            return
        recent = self.recent[-2_000:]
        retained_order_ids = {
            str(row["order-id"]) for row in recent if row.get("order-id") is not None
        }
        retained_order_ids.update(
            order_id for order_id, row in self.orders.items()
            if row.get("state") in ("submitted", "partial-filled")
        )
        orders = {
            order_id: row for order_id, row in self.orders.items() if order_id in retained_order_ids
        }
        retained_gmo_ids = list(self.gmo_orders)[-2_000:]
        await self.state_store.set_state("paper.exchange", {
            "orders": orders,
            "clientToOrder": {
                row["client-order-id"]: order_id for order_id, row in orders.items()
            },
            "matchesByOrder": {
                order_id: self.matches_by_order.get(order_id, []) for order_id in retained_order_ids
            },
            "recent": recent,
            "gmoOrders": {
                key: {
                    **row, "requested": str(row["requested"]), "filled": str(row["filled"]),
                    "price": str(row["price"]),
                    "size_step": str(row.get("size_step", "0.00000001")),
                }
                for key in retained_gmo_ids for row in (self.gmo_orders[key],)
            },
            "balances": {
                venue: {asset: str(amount) for asset, amount in assets.items()}
                for venue, assets in self.balances.items()
            },
            "orderSeq": self._order_seq, "tradeSeq": self._trade_seq, "gmoSeq": self._gmo_seq,
        })

    def configure(self, patch: dict[str, Any]) -> PaperScenarioConfig:
        values = self.scenarios.model_dump()
        values.update({key: value for key, value in patch.items() if value is not None})
        next_config = PaperScenarioConfig.model_validate(values)
        if next_config.executionDelayMinMs > next_config.executionDelayMaxMs:
            raise ValueError("executionDelayMinMs 不能大于 executionDelayMaxMs")
        if next_config.seed != self.scenarios.seed:
            self.rng.seed(next_config.seed)
        self.scenarios = next_config
        return next_config

    def set_allocations(self, allocations: dict[str, dict[str, Decimal]]) -> None:
        self.balances = {
            venue: {asset.upper(): Decimal(str(amount)) for asset, amount in assets.items()}
            for venue, assets in allocations.items()
        }

    def set_market(self, market: MarketTop) -> None:
        """Mirror an external market-data snapshot into the fake execution venues."""
        self.markets[market.symbol] = market
        self._live_market_updated_at[market.symbol] = time.monotonic()

    def execution_market(self, symbol: str, *, stale_after_sec: float = 3.0) -> MarketTop:
        updated_at = self._live_market_updated_at.get(symbol)
        if updated_at is None:
            raise RuntimeError(f"{symbol} 没有可用于 Paper 对冲的真实 GMO 盘口")
        age = time.monotonic() - updated_at
        if age > stale_after_sec:
            raise RuntimeError(f"{symbol} Paper 对冲的 GMO 盘口已过期（{age:.1f}s）")
        return self.markets[symbol]

    async def market_stream(self, bases: list[str], feed) -> None:
        selected = [base.upper() for base in bases]
        while True:
            for base in selected:
                symbol = f"{base}_JPY"
                current = self.markets[symbol]
                mid = Decimal(str((current.bid + current.ask) / 2))
                tick = Decimal(str(PAPER_SYMBOLS[base]["tick"]))
                movement = mid * Decimal(str(self.rng.uniform(-.00018, .00018)))
                next_mid = max(tick, mid + movement)
                market = self._market(base, next_mid)
                self.markets[symbol] = market
                await feed.update(market, transport="ws")
            await self.match_open_orders()
            await asyncio.sleep(.2)

    async def match_loop(self) -> None:
        """Keep fake fills running when market data comes from a real venue."""
        while True:
            await self.match_open_orders()
            await asyncio.sleep(.2)

    async def stream(self) -> AsyncIterator[CumulativeFillEvent]:
        while True:
            yield await self.fill_queue.get()

    async def match_open_orders(self) -> None:
        if not self.scenarios.autoMatch:
            return
        candidates = [row for row in self.orders.values() if row["state"] in ("submitted", "partial-filled")]
        self.rng.shuffle(candidates)
        for row in candidates:
            if self.rng.random() <= self.scenarios.autoMatchProbability:
                await self.fill_order(row["id"])

    async def fill_order(self, order_id: str, requested: Decimal | None = None) -> list[CumulativeFillEvent]:
        row = self.orders.get(str(order_id))
        if row is None:
            raise ValueError("没有找到可注入成交的 Paper 订单")
        total = Decimal(row["amount"])
        recorded = Decimal(row["field-amount"])
        remaining = max(Decimal("0"), total - recorded)
        if remaining <= 0:
            return []
        step = Decimal(str(PAPER_SYMBOLS[row["base"]]["step"]))
        qty = min(remaining, requested) if requested is not None else remaining
        if requested is None and self.scenarios.partialFills and remaining > step:
            parts_left = self.rng.randint(2, 5)
            qty = max(step, (remaining / parts_left / step).to_integral_value(rounding=ROUND_DOWN) * step)
        if self.scenarios.dustFills and self.rng.random() <= self.scenarios.dustProbability:
            qty = min(qty, step / Decimal("2"))
        qty = min(qty, remaining)
        if qty <= 0:
            return []
        event = await self._record_fill(row, qty)
        emitted = [event]
        await self.fill_queue.put(event)
        if self.scenarios.duplicateEvents and self.rng.random() <= self.scenarios.duplicateProbability:
            await self.fill_queue.put(event)
        if self.scenarios.outOfOrderEvents and self.rng.random() <= self.scenarios.outOfOrderProbability:
            older = max(Decimal("0"), event.cumulative_qty - qty)
            if older > 0:
                self._trade_seq += 1
                stale = CumulativeFillEvent(
                    event.client_order_id, event.order_id, f"PAPER-OOO-{self._trade_seq}",
                    event.symbol, event.side, older, event.price, Decimal("0"), event.occurred_at,
                )
                await self.fill_queue.put(stale)
                emitted.append(stale)
        return emitted

    async def _record_fill(self, row: dict[str, Any], qty: Decimal) -> CumulativeFillEvent:
        self._trade_seq += 1
        cumulative = Decimal(row["field-amount"]) + qty
        total = Decimal(row["amount"])
        row["field-amount"] = str(cumulative)
        row["state"] = "filled" if cumulative >= total else "partial-filled"
        now_ms = int(time.time() * 1000)
        match = {
            "trade-id": f"PAPER-T-{self._trade_seq}", "order-id": row["id"],
            "client-order-id": row["client-order-id"], "symbol": row["symbol"],
            "type": row["type"], "filled-amount": str(qty), "price": row["price"],
            "filled-fees": "0", "created-at": now_ms,
        }
        self.matches_by_order.setdefault(row["id"], []).append(match)
        self.recent.append(match)
        self._apply_bittrade_balance(row, qty, Decimal(row["price"]))
        await self.persist()
        return CumulativeFillEvent(
            client_order_id=row["client-order-id"], order_id=row["id"], trade_id=match["trade-id"],
            symbol=row["normalized-symbol"], side=row["side"], cumulative_qty=cumulative,
            price=Decimal(row["price"]), occurred_at=datetime.now(timezone.utc).isoformat(),
        )

    def _apply_bittrade_balance(self, order: dict[str, Any], qty: Decimal, price: Decimal) -> None:
        base = order["base"]
        if order["side"] == "BUY":
            self._add("bittrade", "JPY", -(qty * price))
            self._add("bittrade", base, qty)
        else:
            self._add("bittrade", base, -qty)
            self._add("bittrade", "JPY", qty * price)

    def apply_gmo_balance(self, base: str, side: str, qty: Decimal, notional: Decimal) -> None:
        if side == "BUY":
            self._add("gmo", "JPY", -notional)
            self._add("gmo", base, qty)
        else:
            self._add("gmo", base, -qty)
            self._add("gmo", "JPY", notional)

    def _add(self, venue: str, asset: str, delta: Decimal) -> None:
        self.balances.setdefault(venue, {})[asset] = self.balances.get(venue, {}).get(asset, Decimal("0")) + delta

    def maybe_fault(self) -> None:
        if self.scenarios.randomNetworkTimeout and self.rng.random() <= self.scenarios.networkTimeoutProbability:
            raise httpx.ReadTimeout("Paper exchange injected network timeout")
        if self.scenarios.randomRateLimit and self.rng.random() <= self.scenarios.rateLimitProbability:
            raise ExchangeAPIError("Paper", "too many requests", code="429")

    def _market(self, base: str, mid: Decimal) -> MarketTop:
        rule = PAPER_SYMBOLS[base]
        tick = Decimal(str(rule["tick"]))
        spread = max(tick, mid * Decimal("0.00012"))
        bid = ((mid - spread) / tick).to_integral_value(rounding=ROUND_DOWN) * tick
        ask = ((mid + spread) / tick).to_integral_value(rounding=ROUND_DOWN) * tick + tick
        step = Decimal(str(rule["step"]))
        depth = max(step, step * Decimal(str(self.rng.randint(2, 80))))
        level_gap = max(tick, (mid * Decimal("0.00004") / tick).to_integral_value(rounding=ROUND_DOWN) * tick)
        bid_levels: list[tuple[float, float]] = []
        ask_levels: list[tuple[float, float]] = []
        for index in range(10):
            multiplier = Decimal(str(1 + index))
            level_size = depth * Decimal(str(self.rng.uniform(.6, 1.8)))
            bid_levels.append((float(bid - level_gap * Decimal(index)), float(level_size * multiplier)))
            ask_levels.append((float(ask + level_gap * Decimal(index)), float(level_size * multiplier)))
        return MarketTop(
            symbol=f"{base}_JPY", bid=float(bid), ask=float(ask), bidSize=float(depth),
            askSize=ask_levels[0][1], bids=bid_levels, asks=ask_levels,
            timestamp=utc_now(), source="SIM",
        )


class FakeBitTrade:
    HOST = "paper.bittrade.local"

    def __init__(self, broker: PaperBroker):
        self.broker = broker
        self.access_key = "paper-access"
        self.secret_key = "paper-secret"
        self.account_id = "paper-account"
        self.time_offset_sec = 0.0

    async def place_quote(self, symbol: str, quote: DecimalQuote, client_order_id: str,
                          size_step: Decimal, price_tick: Decimal) -> dict:
        self.broker.maybe_fault()
        market = self.broker.markets[symbol]
        best_bid = Decimal(str(market.bid)) * Decimal("0.9998")
        best_ask = Decimal(str(market.ask)) * Decimal("1.0002")
        crosses = quote.side == "BUY" and quote.price >= best_ask or quote.side == "SELL" and quote.price <= best_bid
        if crosses and self.broker.scenarios.postOnlyReject:
            raise ExchangeAPIError("BitTrade", "post-only order would immediately match", code="order-value-error")
        self.broker._order_seq += 1
        order_id = f"PAPER-BT-{self.broker._order_seq}"
        base = symbol.removesuffix("_JPY")
        row = {
            "id": order_id, "client-order-id": client_order_id,
            "symbol": symbol.lower().replace("_", ""), "normalized-symbol": symbol, "base": base,
            "side": quote.side, "type": "buy-limit-maker" if quote.side == "BUY" else "sell-limit-maker",
            "amount": str(quote.size), "price": str(quote.price), "field-amount": "0",
            "state": "submitted", "created-at": int(time.time() * 1000),
        }
        self.broker.orders[order_id] = row
        self.broker.client_to_order[client_order_id] = order_id
        self.broker.matches_by_order[order_id] = []
        await self.broker.persist()
        return {"status": "ok", "data": order_id}

    async def cancel(self, order_id: str) -> dict:
        self.broker.maybe_fault()
        row = self.broker.orders[str(order_id)]
        should_race = self.broker.scenarios.cancelRaceFill and self.broker.rng.random() <= self.broker.scenarios.cancelRaceProbability
        if row["state"] == "filled" or self.broker.scenarios.cancelAlreadyFilled or should_race:
            if row["state"] != "filled":
                await self.broker.fill_order(str(order_id), Decimal(row["amount"]) - Decimal(row["field-amount"]))
            raise ExchangeAPIError("BitTrade", "order already filled", code="order-orderstate-error")
        row["state"] = "partial-canceled" if Decimal(row["field-amount"]) > 0 else "canceled"
        await self.broker.persist()
        return {"status": "ok", "data": order_id}

    async def batch_cancel(self, *, order_ids: list[str] | None = None,
                           client_order_ids: list[str] | None = None) -> dict:
        ids = order_ids or [self.broker.client_to_order[value] for value in client_order_ids or [] if value in self.broker.client_to_order]
        for order_id in ids:
            row = self.broker.orders.get(str(order_id))
            if row and row["state"] in ("submitted", "partial-filled"):
                row["state"] = "partial-canceled" if Decimal(row["field-amount"]) > 0 else "canceled"
        await self.broker.persist()
        return {"status": "ok", "data": {"success": ids}}

    async def cancel_all_open(self, symbols: list[str] | None = None) -> dict:
        allowed = set(symbols or [])
        for row in self.broker.orders.values():
            if row["state"] in ("submitted", "partial-filled") and (not allowed or row["normalized-symbol"] in allowed):
                row["state"] = "partial-canceled" if Decimal(row["field-amount"]) > 0 else "canceled"
        await self.broker.persist()
        return {"status": "ok", "data": {"success": True}}

    async def order(self, order_id: str) -> dict:
        self.broker.maybe_fault()
        return {"status": "ok", "data": dict(self.broker.orders[str(order_id)])}

    async def open_orders(self, symbol: str | None = None) -> dict:
        rows = [dict(row) for row in self.broker.orders.values() if row["state"] in ("submitted", "partial-filled")]
        if symbol:
            rows = [row for row in rows if row["normalized-symbol"] == symbol]
        return {"status": "ok", "data": rows}

    async def matches(self, order_id: str) -> dict:
        return {"status": "ok", "data": list(self.broker.matches_by_order.get(str(order_id), []))}

    async def recent_matches(self, symbol: str | None = None, *, start_time: str | None = None) -> dict:
        rows = list(self.broker.recent)
        if symbol:
            normalized = symbol.lower().replace("_", "")
            rows = [row for row in rows if row["symbol"] == normalized]
        if start_time:
            rows = [row for row in rows if int(row["created-at"]) >= int(start_time)]
        return {"status": "ok", "data": rows}

    async def depth(self, symbol: str) -> dict:
        self.broker.maybe_fault()
        market = self.broker.markets[symbol]
        bid = Decimal(str(market.bid)) * Decimal("0.9998")
        ask = Decimal(str(market.ask)) * Decimal("1.0002")
        gap = Decimal(str(market.bid)) * Decimal("0.0002")
        return {"status": "ok", "tick": {
            "bids": [[str(bid - gap * index), str(Decimal("0.0002") * (index + 1))]
                     for index in range(20)],
            "asks": [[str(ask + gap * index), str(Decimal("0.0002") * (index + 1))]
                     for index in range(20)],
        }}

    async def balances(self) -> dict:
        return {"status": "ok", "data": [
            {"currency": asset.lower(), "type": "trade", "balance": str(value), "available": str(value)}
            for asset, value in self.broker.balances["bittrade"].items()
        ]}

    async def symbols(self) -> list[dict]:
        return [{
            "base-currency": base.lower(), "quote-currency": "jpy", "state": "online",
            "api-trading": "enabled", "amount-precision": rule["precision"],
            "price-precision": max(0, -Decimal(str(rule["tick"])).as_tuple().exponent),
            "limit-order-min-order-amt": rule["min"], "limit-order-max-order-amt": rule["max"],
        } for base, rule in PAPER_SYMBOLS.items()]

    async def sync_time(self) -> float:
        self.time_offset_sec = 0.0
        return 0.0

    def set_credentials(self, access_key: str, secret_key: str, account_id: str) -> None:
        # Paper credentials deliberately remain non-empty so the real private-engine path starts.
        self.access_key = access_key or "paper-access"
        self.secret_key = secret_key or "paper-secret"
        self.account_id = account_id or "paper-account"

    def stream(self) -> AsyncIterator[CumulativeFillEvent]:
        return self.broker.stream()

    async def inject_fill(self, symbol: str, side: str, size: Decimal | None = None) -> list[CumulativeFillEvent]:
        candidates = [row for row in self.broker.orders.values() if row["normalized-symbol"] == symbol and row["side"] == side and row["state"] in ("submitted", "partial-filled")]
        if not candidates:
            raise ValueError(f"{symbol} {side} 当前没有可撮合的 Paper 挂单")
        return await self.broker.fill_order(candidates[0]["id"], size)


class FakeGmo:
    def __init__(self, broker: PaperBroker):
        self.broker = broker
        self.api_key = "paper-key"
        self.secret_key = "paper-secret"
        self.time_offset_ms = 0

    async def ticker(self, symbol: str = "BTC") -> MarketTop:
        self.broker.maybe_fault()
        return self.broker.markets[f"{symbol.removesuffix('_JPY').upper()}_JPY"]

    async def symbols(self) -> list[dict]:
        return [{
            "symbol": base, "minOrderSize": rule["min"], "maxOrderSize": rule["max"],
            "sizeStep": rule["step"], "tickSize": rule["tick"],
        } for base, rule in PAPER_SYMBOLS.items()]

    async def market_order(self, symbol: str, side: str, size: Decimal,
                           size_step: Decimal = Decimal("0.00000001")) -> dict:
        self.broker.maybe_fault()
        market = self.broker.execution_market(symbol)
        self.broker._gmo_seq += 1
        order_id = f"PAPER-GMO-{self.broker._gmo_seq}"
        requested = Decimal(str(size))
        ratio = Decimal(str(self.broker.scenarios.gmoFillRatio)) if self.broker.scenarios.gmoPartialFak else Decimal("1")
        target = (requested * ratio / size_step).to_integral_value(rounding=ROUND_DOWN) * size_step
        target = min(requested, max(Decimal("0"), target))
        raw_levels = market.asks if side == "BUY" else market.bids
        levels = [
            (Decimal(str(level[0])), Decimal(str(level[1]))) for level in raw_levels
            if len(level) >= 2 and Decimal(str(level[1])) > 0
        ]
        if not levels:
            fallback_price = market.ask if side == "BUY" else market.bid
            fallback_size = market.askSize if side == "BUY" else market.bidSize
            levels = [(Decimal(str(fallback_price)), Decimal(str(fallback_size)))]
        filled = Decimal("0")
        notional = Decimal("0")
        last_price = Decimal("0")
        for level_price, level_qty in levels:
            take = min(level_qty, target - filled)
            if take <= 0:
                break
            filled += take
            notional += take * level_price
            last_price = level_price
        aligned_filled = (filled / size_step).to_integral_value(rounding=ROUND_DOWN) * size_step
        if aligned_filled < filled:
            notional -= (filled - aligned_filled) * last_price
        filled = aligned_filled
        price = notional / filled if filled > 0 else Decimal("0")
        delay_ms = self.broker.rng.randint(
            self.broker.scenarios.executionDelayMinMs, self.broker.scenarios.executionDelayMaxMs,
        ) if self.broker.scenarios.delayedExecutions else 0
        self.broker.gmo_orders[order_id] = {
            "orderId": order_id, "symbol": symbol, "side": side, "requested": requested,
            "filled": filled, "price": price, "available_at": time.time() + delay_ms / 1000,
            "partial": filled < requested, "timeInForce": "FAK", "executionType": "MARKET",
            "canceled": False,
            "evaluated": True,
        }
        self.broker.apply_gmo_balance(symbol.removesuffix("_JPY"), side, filled, filled * price)
        await self.broker.persist()
        return {"status": 0, "data": {"orderId": order_id}}

    async def post_only_order(self, symbol: str, side: str, size: Decimal, price: Decimal,
                              size_step: Decimal, price_tick: Decimal) -> dict:
        self.broker.maybe_fault()
        self.broker.execution_market(symbol)
        self.broker._gmo_seq += 1
        order_id = f"PAPER-GMO-{self.broker._gmo_seq}"
        requested = Decimal(str(size))
        limit_price = Decimal(str(price))
        self.broker.gmo_orders[order_id] = {
            "orderId": order_id, "symbol": symbol, "side": side, "requested": requested,
            "filled": Decimal("0"), "price": limit_price,
            "available_at": time.time() + self.broker.scenarios.gmoPostOnlyFillDelayMs / 1000,
            "partial": True, "timeInForce": "SOK", "executionType": "LIMIT",
            "canceled": False, "evaluated": False, "size_step": size_step,
        }
        await self.broker.persist()
        return {"status": 0, "data": {"orderId": order_id}}

    def _evaluate_post_only(self, row: dict[str, Any]) -> bool:
        if row.get("evaluated") or time.time() < row["available_at"]:
            return False
        market = self.broker.execution_market(row["symbol"])
        limit_price = Decimal(str(row["price"]))
        raw_levels = market.asks if row["side"] == "BUY" else market.bids
        levels = [
            (Decimal(str(level[0])), Decimal(str(level[1]))) for level in raw_levels
            if len(level) >= 2 and Decimal(str(level[1])) > 0
        ]
        if not levels:
            fallback_price = market.ask if row["side"] == "BUY" else market.bid
            fallback_size = market.askSize if row["side"] == "BUY" else market.bidSize
            levels = [(Decimal(str(fallback_price)), Decimal(str(fallback_size)))]
        if row["side"] == "BUY":
            touched = [(price, qty) for price, qty in levels if price <= limit_price]
        else:
            touched = [(price, qty) for price, qty in levels if price >= limit_price]
        requested = Decimal(str(row["requested"]))
        step = Decimal(str(row.get("size_step", "0.00000001")))
        filled = min(requested, sum((qty for _, qty in touched), Decimal("0")))
        filled = (filled / step).to_integral_value(rounding=ROUND_DOWN) * step
        notional = filled * limit_price
        row["filled"] = filled
        row["partial"] = filled < requested
        row["evaluated"] = True
        if filled > 0:
            self.broker.apply_gmo_balance(
                row["symbol"].removesuffix("_JPY"), row["side"], filled, notional,
            )
        return True

    async def cancel_order(self, order_id: str) -> dict:
        row = self.broker.gmo_orders[str(order_id)]
        row["canceled"] = True
        await self.broker.persist()
        return {"status": 0, "data": str(order_id)}

    async def executions(self, order_id: str) -> dict:
        self.broker.maybe_fault()
        row = self.broker.gmo_orders[str(order_id)]
        if time.time() < row["available_at"]:
            return {"status": 0, "data": []}
        if self._evaluate_post_only(row):
            await self.broker.persist()
        return {"status": 0, "data": [{
            "orderId": order_id, "size": str(row["filled"]), "price": str(row["price"]),
        }] if row["filled"] > 0 else []}

    async def order(self, order_id: str) -> dict:
        row = self.broker.gmo_orders[str(order_id)]
        if self._evaluate_post_only(row):
            await self.broker.persist()
        if row.get("canceled"):
            status = "CANCELED"
        elif time.time() < row["available_at"]:
            status = "ORDERED"
        elif row.get("timeInForce") == "SOK" and row["partial"]:
            status = "ORDERED"
        else:
            status = "CANCELED" if row["partial"] else "EXECUTED"
        return {"status": 0, "data": {"list": [{
            "orderId": order_id, "status": status, "executedSize": str(row["filled"]),
            "timeInForce": row.get("timeInForce", "FAK"),
            "executionType": row.get("executionType", "MARKET"), "price": str(row["price"]),
        }]}}

    async def balances(self) -> dict:
        return {"status": 0, "data": [
            {"symbol": asset, "amount": str(value), "available": str(value)}
            for asset, value in self.broker.balances["gmo"].items()
        ]}

    async def sync_time(self) -> int:
        self.time_offset_ms = 0
        return 0

    def set_credentials(self, api_key: str, secret_key: str) -> None:
        self.api_key = api_key or "paper-key"
        self.secret_key = secret_key or "paper-secret"

    async def market_stream(self, bases: list[str], feed) -> None:
        await self.broker.market_stream(bases, feed)
