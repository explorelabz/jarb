from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from .domain import (
    HEDGE_TRANSITIONS, ORDER_TRANSITIONS, FillDelta, HedgeIntent, HedgeStatus, OrderState,
)


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    exchange_order_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
    qty TEXT NOT NULL,
    price TEXT NOT NULL,
    state TEXT NOT NULL,
    cumulative_filled TEXT NOT NULL DEFAULT '0',
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS orders_state_idx ON orders(state, symbol);

CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
    cumulative_qty TEXT NOT NULL,
    incremental_qty TEXT NOT NULL,
    price TEXT NOT NULL,
    fee TEXT NOT NULL DEFAULT '0',
    occurred_at TEXT NOT NULL,
    UNIQUE(order_id, trade_id),
    FOREIGN KEY(client_order_id) REFERENCES orders(client_order_id)
);

CREATE TABLE IF NOT EXISTS hedge_intents (
    id TEXT PRIMARY KEY,
    client_fill_id INTEGER NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
    qty TEXT NOT NULL,
    filled_qty TEXT NOT NULL DEFAULT '0',
    filled_notional TEXT NOT NULL DEFAULT '0',
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    exchange_order_id TEXT,
    last_error TEXT,
    source_fill_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(client_fill_id) REFERENCES fills(id)
);
CREATE INDEX IF NOT EXISTS hedge_status_idx ON hedge_intents(status, symbol, side);

CREATE TABLE IF NOT EXISTS balances (
    venue TEXT NOT NULL,
    asset TEXT NOT NULL,
    available TEXT NOT NULL,
    reserved TEXT NOT NULL DEFAULT '0',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(venue, asset)
);

CREATE TABLE IF NOT EXISTS engine_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    level TEXT NOT NULL,
    actor TEXT,
    message TEXT NOT NULL,
    metadata TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class StateStore:
    """SQLite WAL store. Every state transition and fill delta is committed before side effects."""

    def __init__(self, path: Path | str = Path("data/jarb.db")):
        self.path = Path(path)
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if self._connection is None:
                self._connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
                self._connection.row_factory = sqlite3.Row
                self._connection.executescript(SCHEMA)
                columns = {
                    row["name"] for row in self._connection.execute("PRAGMA table_info(hedge_intents)")
                }
                if "filled_notional" not in columns:
                    self._connection.execute(
                        "ALTER TABLE hedge_intents ADD COLUMN filled_notional TEXT NOT NULL DEFAULT '0'"
                    )
                if "latency_ms" not in columns:
                    self._connection.execute(
                        "ALTER TABLE hedge_intents ADD COLUMN latency_ms INTEGER NOT NULL DEFAULT 0"
                    )
                if "source_fill_at" not in columns:
                    self._connection.execute(
                        "ALTER TABLE hedge_intents ADD COLUMN source_fill_at TEXT"
                    )
                    self._connection.execute(
                        "UPDATE hedge_intents SET source_fill_at=created_at WHERE source_fill_at IS NULL"
                    )

    async def close(self) -> None:
        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def _db(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("StateStore is not initialized")
        return self._connection

    async def create_order(self, client_order_id: str, symbol: str, side: str, qty: Decimal, price: Decimal) -> dict:
        return await asyncio.to_thread(self._create_order_sync, client_order_id, symbol, side, qty, price)

    def _create_order_sync(self, client_order_id: str, symbol: str, side: str, qty: Decimal, price: Decimal) -> dict:
        with self._lock:
            db = self._db()
            db.execute(
                "INSERT OR IGNORE INTO orders(client_order_id,symbol,side,qty,price,state) VALUES(?,?,?,?,?,?)",
                (client_order_id, symbol, side, str(qty), str(price), OrderState.NEW),
            )
            return dict(db.execute("SELECT * FROM orders WHERE client_order_id=?", (client_order_id,)).fetchone())

    async def transition_order(self, client_order_id: str, target: OrderState, *, exchange_order_id: str | None = None,
                               error: str | None = None) -> dict:
        return await asyncio.to_thread(self._transition_order_sync, client_order_id, target, exchange_order_id, error)

    def _transition_order_sync(self, client_order_id: str, target: OrderState, exchange_order_id: str | None,
                               error: str | None) -> dict:
        with self._lock:
            db = self._db()
            row = db.execute("SELECT * FROM orders WHERE client_order_id=?", (client_order_id,)).fetchone()
            if row is None:
                raise KeyError(client_order_id)
            current = OrderState(row["state"])
            if target != current and target not in ORDER_TRANSITIONS[current]:
                raise ValueError(f"invalid order transition {current} -> {target}")
            db.execute(
                "UPDATE orders SET state=?,exchange_order_id=COALESCE(?,exchange_order_id),last_error=?,updated_at=CURRENT_TIMESTAMP WHERE client_order_id=?",
                (target, exchange_order_id, error, client_order_id),
            )
            return dict(db.execute("SELECT * FROM orders WHERE client_order_id=?", (client_order_id,)).fetchone())

    async def record_cumulative_fill(self, *, client_order_id: str, order_id: str, trade_id: str,
                                     symbol: str, side: str,
                                     cumulative_qty: Decimal, price: Decimal, fee: Decimal,
                                     occurred_at: str) -> FillDelta | None:
        return await asyncio.to_thread(
            self._record_cumulative_fill_sync, client_order_id, order_id, trade_id, symbol, side,
            cumulative_qty, price, fee, occurred_at,
        )

    def _record_cumulative_fill_sync(self, client_order_id: str, order_id: str, trade_id: str,
                                     symbol: str, side: str,
                                     cumulative_qty: Decimal, price: Decimal, fee: Decimal,
                                     occurred_at: str) -> FillDelta | None:
        with self._lock:
            db = self._db()
            db.execute("BEGIN IMMEDIATE")
            try:
                duplicate = db.execute("SELECT id FROM fills WHERE order_id=? AND trade_id=?", (order_id, trade_id)).fetchone()
                if duplicate:
                    db.execute("COMMIT")
                    return None
                order = db.execute(
                    "SELECT cumulative_filled,qty FROM orders WHERE client_order_id=?", (client_order_id,),
                ).fetchone()
                if order is None:
                    raise KeyError(client_order_id)
                recorded = Decimal(order["cumulative_filled"])
                incremental = max(Decimal("0"), cumulative_qty - recorded)
                cursor = db.execute(
                    "INSERT INTO fills(client_order_id,order_id,trade_id,symbol,side,cumulative_qty,incremental_qty,price,fee,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (client_order_id, order_id, trade_id, symbol, side, str(cumulative_qty),
                     str(incremental), str(price), str(fee), occurred_at),
                )
                new_cumulative = max(recorded, cumulative_qty)
                total = Decimal(order["qty"])
                next_state = OrderState.FILLED if new_cumulative >= total else OrderState.PARTIAL
                db.execute(
                    "UPDATE orders SET cumulative_filled=?,state=?,updated_at=CURRENT_TIMESTAMP WHERE client_order_id=?",
                    (str(new_cumulative), next_state, client_order_id),
                )
                db.execute("COMMIT")
                if incremental == 0:
                    return None
                return FillDelta(cursor.lastrowid, client_order_id, order_id, trade_id, symbol, side,
                                 cumulative_qty, incremental, price, fee, occurred_at)
            except Exception:
                db.execute("ROLLBACK")
                raise

    async def create_hedge_intent(self, fill: FillDelta, hedge_side: str) -> HedgeIntent:
        return await asyncio.to_thread(self._create_hedge_intent_sync, fill, hedge_side)

    def _create_hedge_intent_sync(self, fill: FillDelta, hedge_side: str) -> HedgeIntent:
        with self._lock:
            db = self._db()
            intent_id = f"H-{uuid4().hex}"
            db.execute(
                "INSERT OR IGNORE INTO hedge_intents(id,client_fill_id,symbol,side,qty,status,source_fill_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (intent_id, fill.fill_id, fill.symbol, hedge_side, str(fill.incremental_qty),
                 HedgeStatus.PENDING, fill.occurred_at),
            )
            row = db.execute("SELECT * FROM hedge_intents WHERE client_fill_id=?", (fill.fill_id,)).fetchone()
            return self._intent(row)

    async def transition_hedge(self, intent_id: str, target: HedgeStatus, *, filled_qty: Decimal | None = None,
                               filled_notional: Decimal | None = None,
                               latency_ms: int | None = None,
                               exchange_order_id: str | None = None, error: str | None = None) -> HedgeIntent:
        return await asyncio.to_thread(
            self._transition_hedge_sync, intent_id, target, filled_qty, filled_notional,
            latency_ms, exchange_order_id, error,
        )

    def _transition_hedge_sync(self, intent_id: str, target: HedgeStatus, filled_qty: Decimal | None,
                               filled_notional: Decimal | None, latency_ms: int | None,
                               exchange_order_id: str | None, error: str | None) -> HedgeIntent:
        with self._lock:
            db = self._db()
            row = db.execute("SELECT * FROM hedge_intents WHERE id=?", (intent_id,)).fetchone()
            if row is None:
                raise KeyError(intent_id)
            current = HedgeStatus(row["status"])
            if target != current and target not in HEDGE_TRANSITIONS[current]:
                raise ValueError(f"invalid hedge transition {current} -> {target}")
            attempts = row["attempts"] + (1 if target == HedgeStatus.HEDGING and target != current else 0)
            db.execute(
                "UPDATE hedge_intents SET status=?,filled_qty=COALESCE(?,filled_qty),filled_notional=COALESCE(?,filled_notional),latency_ms=COALESCE(?,latency_ms),exchange_order_id=COALESCE(?,exchange_order_id),attempts=?,last_error=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (target, str(filled_qty) if filled_qty is not None else None,
                 str(filled_notional) if filled_notional is not None else None,
                 latency_ms, exchange_order_id, attempts, error, intent_id),
            )
            return self._intent(db.execute("SELECT * FROM hedge_intents WHERE id=?", (intent_id,)).fetchone())

    async def pending_hedges(self) -> list[HedgeIntent]:
        return await asyncio.to_thread(self._pending_hedges_sync)

    def _pending_hedges_sync(self) -> list[HedgeIntent]:
        with self._lock:
            rows = self._db().execute(
                "SELECT * FROM hedge_intents WHERE status IN (?,?,?) ORDER BY created_at,id",
                (HedgeStatus.PENDING, HedgeStatus.RETRY, HedgeStatus.HEDGING),
            ).fetchall()
            return [self._intent(row) for row in rows]

    async def pending_hedge_exposure(self) -> dict[str, Decimal]:
        return await asyncio.to_thread(self._pending_hedge_exposure_sync)

    def _pending_hedge_exposure_sync(self) -> dict[str, Decimal]:
        with self._lock:
            rows = self._db().execute(
                "SELECT symbol,side,qty,filled_qty FROM hedge_intents WHERE status IN (?,?,?)",
                (HedgeStatus.PENDING, HedgeStatus.RETRY, HedgeStatus.HEDGING),
            ).fetchall()
        result: dict[str, Decimal] = {}
        for row in rows:
            signed = (Decimal(row["qty"]) - Decimal(row["filled_qty"])) * (
                Decimal("1") if row["side"] == "BUY" else Decimal("-1")
            )
            result[row["symbol"]] = result.get(row["symbol"], Decimal("0")) + signed
        return result

    async def escalated_hedges(self) -> list[HedgeIntent]:
        return await asyncio.to_thread(self._escalated_hedges_sync)

    def _escalated_hedges_sync(self) -> list[HedgeIntent]:
        with self._lock:
            rows = self._db().execute(
                "SELECT * FROM hedge_intents WHERE status=? ORDER BY created_at,id",
                (HedgeStatus.ESCALATE,),
            ).fetchall()
        return [self._intent(row) for row in rows]

    async def hedge_health(self, day_prefix: str) -> tuple[int, int]:
        return await asyncio.to_thread(self._hedge_health_sync, day_prefix)

    def _hedge_health_sync(self, day_prefix: str) -> tuple[int, int]:
        with self._lock:
            rows = self._db().execute(
                "SELECT status,latency_ms FROM hedge_intents WHERE created_at LIKE ?",
                (f"{day_prefix}%",),
            ).fetchall()
        failures = sum(1 for row in rows if row["status"] in (HedgeStatus.RETRY, HedgeStatus.ESCALATE))
        latencies = sorted(row["latency_ms"] for row in rows if row["latency_ms"] > 0)
        p95 = latencies[min(len(latencies) - 1, int(len(latencies) * .95))] if latencies else 0
        return failures, p95

    async def daily_fill_volume(self, day_prefix: str) -> Decimal:
        return await asyncio.to_thread(self._daily_fill_volume_sync, day_prefix)

    def _daily_fill_volume_sync(self, day_prefix: str) -> Decimal:
        with self._lock:
            rows = self._db().execute(
                "SELECT incremental_qty,price FROM fills WHERE occurred_at LIKE ?",
                (f"{day_prefix}%",),
            ).fetchall()
        return sum((Decimal(row["incremental_qty"]) * Decimal(row["price"]) for row in rows), Decimal("0"))

    async def daily_realized_pnl(self, day_prefix: str, *, maker_fee_bps: Decimal,
                                 hedge_fee_bps: Decimal | dict[str, Decimal]) -> Decimal:
        return await asyncio.to_thread(
            self._daily_realized_pnl_sync, day_prefix, maker_fee_bps, hedge_fee_bps,
        )

    def _daily_realized_pnl_sync(self, day_prefix: str, maker_fee_bps: Decimal,
                                 hedge_fee_bps: Decimal | dict[str, Decimal]) -> Decimal:
        with self._lock:
            rows = self._db().execute(
                "SELECT f.symbol,f.side,f.incremental_qty,f.price,h.filled_qty,h.filled_notional "
                "FROM fills f JOIN hedge_intents h ON h.client_fill_id=f.id "
                "WHERE f.occurred_at LIKE ? AND h.filled_qty > '0'",
                (f"{day_prefix}%",),
            ).fetchall()
        pnl = Decimal("0")
        for row in rows:
            fill_qty = Decimal(row["incremental_qty"])
            hedged_qty = min(fill_qty, Decimal(row["filled_qty"]))
            if fill_qty <= 0 or hedged_qty <= 0:
                continue
            client_notional = Decimal(row["price"]) * hedged_qty
            hedge_notional = Decimal(row["filled_notional"])
            spread = hedge_notional - client_notional if row["side"] == "BUY" else client_notional - hedge_notional
            symbol_hedge_fee = hedge_fee_bps.get(row["symbol"], Decimal("9")) \
                if isinstance(hedge_fee_bps, dict) else hedge_fee_bps
            pnl += spread - client_notional * maker_fee_bps / Decimal("10000") \
                - hedge_notional * symbol_hedge_fee / Decimal("10000")
        return pnl

    async def open_orders(self) -> list[dict]:
        return await asyncio.to_thread(self._open_orders_sync)

    def _open_orders_sync(self) -> list[dict]:
        states = (OrderState.PLACING, OrderState.OPEN, OrderState.PARTIAL, OrderState.CANCELING, OrderState.UNKNOWN)
        with self._lock:
            marks = ",".join("?" for _ in states)
            return [dict(row) for row in self._db().execute(f"SELECT * FROM orders WHERE state IN ({marks})", states).fetchall()]

    async def orders_for_fill_reconciliation(self, limit: int = 1000) -> list[dict]:
        return await asyncio.to_thread(self._orders_for_fill_reconciliation_sync, limit)

    def _orders_for_fill_reconciliation_sync(self, limit: int) -> list[dict]:
        with self._lock:
            rows = self._db().execute(
                "SELECT * FROM orders WHERE exchange_order_id IS NOT NULL AND state NOT IN (?,?) "
                "ORDER BY updated_at DESC LIMIT ?",
                (OrderState.NEW, OrderState.FAILED, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    async def set_state(self, key: str, value: Any) -> None:
        await asyncio.to_thread(self._set_state_sync, key, value)

    def _set_state_sync(self, key: str, value: Any) -> None:
        with self._lock:
            self._db().execute(
                "INSERT INTO engine_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=CURRENT_TIMESTAMP",
                (key, json.dumps(value, separators=(",", ":"))),
            )

    async def upsert_balance(self, venue: str, asset: str, available: Decimal,
                             reserved: Decimal, updated_at: str) -> None:
        await asyncio.to_thread(
            self._upsert_balance_sync, venue, asset, available, reserved, updated_at,
        )

    def _upsert_balance_sync(self, venue: str, asset: str, available: Decimal,
                             reserved: Decimal, updated_at: str) -> None:
        with self._lock:
            self._db().execute(
                "INSERT INTO balances(venue,asset,available,reserved,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(venue,asset) DO UPDATE SET available=excluded.available,reserved=excluded.reserved,updated_at=excluded.updated_at",
                (venue, asset, str(available), str(reserved), updated_at),
            )

    async def get_state(self, key: str, default: Any = None) -> Any:
        return await asyncio.to_thread(self._get_state_sync, key, default)

    def _get_state_sync(self, key: str, default: Any) -> Any:
        with self._lock:
            row = self._db().execute("SELECT value FROM engine_state WHERE key=?", (key,)).fetchone()
            return default if row is None else json.loads(row["value"])

    async def next_sequence(self, key: str) -> int:
        return await asyncio.to_thread(self._next_sequence_sync, key)

    def _next_sequence_sync(self, key: str) -> int:
        with self._lock:
            db = self._db()
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute("SELECT value FROM engine_state WHERE key=?", (key,)).fetchone()
                value = (json.loads(row["value"]) if row else 0) + 1
                db.execute(
                    "INSERT INTO engine_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=CURRENT_TIMESTAMP",
                    (key, json.dumps(value)),
                )
                db.execute("COMMIT")
                return value
            except Exception:
                db.execute("ROLLBACK")
                raise

    async def audit(self, event_type: str, level: str, message: str, *, actor: str | None = None,
                    metadata: dict | None = None) -> None:
        await asyncio.to_thread(self._audit_sync, event_type, level, message, actor, metadata)

    def _audit_sync(self, event_type: str, level: str, message: str, actor: str | None,
                    metadata: dict | None) -> None:
        with self._lock:
            self._db().execute(
                "INSERT INTO audit_events(id,event_type,level,actor,message,metadata) VALUES(?,?,?,?,?,?)",
                (str(uuid4()), event_type, level, actor, message, json.dumps(metadata) if metadata else None),
            )

    @staticmethod
    def _intent(row: sqlite3.Row) -> HedgeIntent:
        return HedgeIntent(
            id=row["id"], client_fill_id=row["client_fill_id"], symbol=row["symbol"], side=row["side"],
            qty=Decimal(row["qty"]), filled_qty=Decimal(row["filled_qty"]),
            filled_notional=Decimal(row["filled_notional"]), status=HedgeStatus(row["status"]),
            attempts=row["attempts"], latency_ms=row["latency_ms"], created_at=row["created_at"],
            source_fill_at=row["source_fill_at"] or row["created_at"],
            exchange_order_id=row["exchange_order_id"],
        )
