from __future__ import annotations

import json
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from .config import credentials, requested_mode, strategy_config
from .models import ArmRequest, ConnectionUpdate, ControlRequest, InventoryUpdate, SimulatedFillRequest
from .service import TradingService

service = TradingService(strategy_config, requested_mode, credentials)


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


@app.post("/api/sim/fill", status_code=201)
async def simulate_fill(request: SimulatedFillRequest):
    if service.state.mode != "simulation":
        raise HTTPException(400, "该端点只允许模拟模式使用")
    try:
        return await service.simulate_fill(request)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/reconciliation/export")
async def export_reconciliation():
    return JSONResponse(service.export_reconciliation(), headers={"Content-Disposition": "attachment; filename=reconciliation.json"})


@app.get("/api/health")
async def health():
    return {"ok": True, "mode": service.state.mode, "connection": service.state.connection.status,
            "activeSymbols": service.state.activeSymbols, "runtime": "Python", "core": "Rust/PyO3"}
