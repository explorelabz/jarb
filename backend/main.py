from __future__ import annotations

import json
import csv
import io
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .auth import authenticate_request, current_operator, sensitive_approvals

from .config import (
    credentials, gmo_fee_overrides, gmo_maker_fee_overrides, requested_mode,
    require_dual_arm_approval, risk_limits,
    strategy_config,
)
from .core import core_runtime
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


app = FastAPI(
    title="BitTrade/GMO Market Maker", version="0.2.0", lifespan=lifespan,
    dependencies=[Depends(authenticate_request)],
)
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"], allow_methods=["GET", "POST", "PATCH"], allow_headers=["*"])


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
        "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


async def sensitive_change(
    action: str, payload: bytes, actor: str, approval_id: str | None,
) -> JSONResponse | None:
    if not approval_id:
        approval = sensitive_approvals.begin(action, payload, actor)
        await service.state_store.audit(
            "sensitive.approval.requested", "critical",
            f"two-person approval requested for {action}", actor=actor,
            metadata={"approvalId": approval.id, "expiresAt": approval.expires_at},
        )
        return JSONResponse(
            status_code=202,
            content={
                "approvalRequired": True, "approvalId": approval.id,
                "expiresAt": approval.expires_at,
                "message": "请由另一位操作员从独立浏览器或 CLI 复核，再携带审批 ID 重试原请求",
            },
        )
    try:
        approval = sensitive_approvals.consume(approval_id, action, payload, actor)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    await service.state_store.audit(
        "sensitive.approval.consumed", "critical",
        f"two-person approval consumed for {action}", actor=actor,
        metadata={
            "approvalId": approval.id, "firstActor": approval.first_actor,
            "secondActor": approval.second_actor,
        },
    )
    return None


@app.get("/api/state")
async def state():
    return {
        **service.state.model_dump(),
        "runtime": {"language": "Python", "core": core_runtime()},
    }


@app.get("/api/events")
async def events():
    async def generate():
        async for payload in service.stream():
            yield f"data: {payload}\n\n"
    return StreamingResponse(generate(), media_type="text/event-stream", headers={"Cache-Control": "no-cache, no-transform"})


@app.patch("/api/strategy")
async def update_strategy(patch: dict, actor: str = Depends(current_operator)):
    try:
        await service.configure(patch, actor=actor)
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
async def update_connection(
    update: ConnectionUpdate, actor: str = Depends(current_operator),
    approval_id: str | None = Header(default=None, alias="X-JARB-Approval"),
):
    try:
        pending = await sensitive_change(
            "connection.update", update.model_dump_json(exclude_unset=True).encode(),
            actor, approval_id,
        )
        if pending is not None:
            return pending
        await service.configure_connection(update, actor=actor)
        return service.connection_summary()
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/control")
async def control(request: ControlRequest, actor: str = Depends(current_operator)):
    try:
        await service.control(request.action, actor=actor)
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
async def update_risk_limits(
    update: RiskLimitsUpdate, actor: str = Depends(current_operator),
    approval_id: str | None = Header(default=None, alias="X-JARB-Approval"),
):
    pending = await sensitive_change(
        "risk.limits.update", update.model_dump_json(exclude_unset=True).encode(),
        actor, approval_id,
    )
    if pending is not None:
        return pending
    return await service.configure_risk_limits(update, actor=actor)


@app.post("/api/approvals/{approval_id}/approve")
async def approve_sensitive_change(
    approval_id: str, actor: str = Depends(current_operator),
):
    try:
        approval = sensitive_approvals.approve(approval_id, actor)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    await service.state_store.audit(
        "sensitive.approval.second", "critical",
        f"second operator approved {approval.action}", actor=actor,
        metadata={"approvalId": approval.id, "firstActor": approval.first_actor},
    )
    return {"approved": True, "approvalId": approval.id, "action": approval.action}


@app.post("/api/risk/arm")
async def arm(request: ArmRequest, actor: str = Depends(current_operator)):
    try:
        return await service.arm(request.phrase, actor)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/risk/disarm")
async def disarm(actor: str = Depends(current_operator)):
    return await service.disarm("operator requested disarm", actor)


@app.get("/api/inventory")
async def inventory():
    return service.inventory_summary()


@app.patch("/api/inventory")
async def update_inventory(update: InventoryUpdate, actor: str = Depends(current_operator)):
    try:
        return await service.configure_inventory(
            update.bittrade, update.gmo,
            webhook_url=update.webhookUrl, clear_webhook=update.clearWebhook,
            actor=actor,
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
async def update_paper_scenarios(
    update: PaperScenarioUpdate, actor: str = Depends(current_operator),
):
    try:
        return await service.configure_paper_scenarios(update, actor=actor)
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
    return {"ok": True}
