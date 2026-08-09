from __future__ import annotations

import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from .config import live_armed, requested_mode, strategy_config
from .models import ControlRequest, SimulatedFillRequest
from .service import TradingService

if requested_mode == "live" and not live_armed:
    raise RuntimeError("Refusing live mode: set LIVE_TRADING=true and ARM_LIVE_TRADING=I_UNDERSTAND")

service = TradingService(strategy_config, requested_mode)


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


@app.post("/api/control")
async def control(request: ControlRequest):
    try:
        await service.control(request.action)
        return service.state
    except ValueError as exc:
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
    return {"ok": True, "mode": service.state.mode, "liveArmed": live_armed, "runtime": "Python", "core": "Rust/PyO3"}
