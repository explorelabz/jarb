from __future__ import annotations

import json
import csv
import io
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .config import (
    credentials, gmo_fee_overrides, gmo_maker_fee_overrides, requested_mode,
    require_dual_arm_approval, risk_limits,
    strategy_config,
)
from .models import (
    ArmRequest, ConnectionUpdate, ControlRequest, InventoryUpdate, PaperFillRequest,
    PaperScenarioUpdate, RiskLimitsUpdate,
)
from .service import TradingService

service = TradingService(
    strategy_config, requested_mode, credentials, risk_limits=risk_limits,
    gmo_fee_overrides=gmo_fee_overrides,
    gmo_maker_fee_overrides=gmo_maker_fee_overrides,
    require_dual_arm_approval=require_dual_arm_approval,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await service.start()
    yield
    await service.stop()


app = FastAPI(title="BitTrade/GMO Market Maker", version="0.2.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"], allow_methods=["GET", "POST", "PATCH"], allow_headers=["*"])


@app.get("/api/state")
async def state():
    return service.state


@app.get("/api/events")
async def events():
    async def generate():
        async for payload in service.stream():
            yield f"data: {payload}\n\n"
    return StreamingResponse(generate(), media_type="text/event-stream", headers={"Cache-Control": "no-cache, no-transform"})


@app.patch("/api/strategy")
async def update_strategy(patch: dict):
    try:
        await service.configure(patch)
        return service.state
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/symbols")
async def symbols(refresh: bool = False):
    try:
        rows = await service.common_symbols(force=refresh)
        return {"symbols": rows, "selected": service.state.activeSymbols}
    except (httpx.HTTPError, RuntimeError, ValueError) as exc:
        raise HTTPException(502, f"无法获取两家交易所共同币种：{exc}") from exc


@app.get("/api/connection")
async def connection():
    return service.connection_summary()


@app.patch("/api/connection")
async def update_connection(update: ConnectionUpdate):
    try:
        await service.configure_connection(update)
        return service.connection_summary()
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/control")
async def control(request: ControlRequest):
    try:
        await service.control(request.action)
        return service.state
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/risk")
async def risk_status():
    return service.risk_status()


@app.get("/api/risk/limits")
async def risk_limits_config():
    return service.risk_limits_summary()


@app.patch("/api/risk/limits")
async def update_risk_limits(update: RiskLimitsUpdate):
    return await service.configure_risk_limits(update)


@app.post("/api/risk/arm")
async def arm(request: ArmRequest):
    try:
        return await service.arm(request.phrase, request.actor)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/risk/disarm")
async def disarm():
    return await service.disarm("operator requested disarm", "operator")


@app.get("/api/inventory")
async def inventory():
    return service.inventory_summary()


@app.patch("/api/inventory")
async def update_inventory(update: InventoryUpdate):
    try:
        return await service.configure_inventory(
            update.bittrade, update.gmo,
            webhook_url=update.webhookUrl, clear_webhook=update.clearWebhook,
        )
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/paper/fill", status_code=202)
@app.post("/api/sim/fill", status_code=202, deprecated=True)
async def simulate_fill(request: PaperFillRequest):
    if service.state.mode != "paper":
        raise HTTPException(400, "该端点只允许 Paper 模式使用")
    try:
        return await service.simulate_fill(request)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/paper/scenarios")
async def paper_scenarios():
    try:
        return service.paper_scenario_summary()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.patch("/api/paper/scenarios")
async def update_paper_scenarios(update: PaperScenarioUpdate):
    try:
        return await service.configure_paper_scenarios(update)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/reconciliation/export")
async def export_reconciliation():
    return JSONResponse(service.export_reconciliation(), headers={"Content-Disposition": "attachment; filename=reconciliation.json"})


@app.get("/api/orders/export")
async def export_orders(format: str = "csv", mode: str = "all"):
    try:
        rows = await service.export_orders(None if mode == "all" else mode)
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc)) from exc
    if format == "json":
        return JSONResponse(
            {"generatedAt": service.state.reconciliation.checkedAt, "mode": mode, "orders": rows},
            headers={"Content-Disposition": f"attachment; filename=jarb-orders-{mode}.json"},
        )
    if format != "csv":
        raise HTTPException(400, "导出格式必须是 csv 或 json")
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return Response(
        content="\ufeff" + output.getvalue(), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=jarb-orders-{mode}.csv"},
    )


@app.get("/api/health")
async def health():
    return {"ok": True, "mode": service.state.mode, "connection": service.state.connection.status,
            "activeSymbols": service.state.activeSymbols, "runtime": "Python", "core": "Rust/PyO3"}
