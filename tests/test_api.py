from __future__ import annotations

import asyncio
import sqlite3
import httpx
import pytest

import backend.main as main
from backend.engine.risk import RiskGate
from backend.engine.state_store import StateStore
from backend.main import app


ALICE_TOKEN = "alice-token-0123456789abcdef0123456789abcdef"
BOB_TOKEN = "bob-token-0123456789abcdef0123456789abcdef"
AUTH = {"Authorization": f"Bearer {ALICE_TOKEN}"}
BOB_AUTH = {"Authorization": f"Bearer {BOB_TOKEN}"}


@pytest.mark.asyncio
async def test_health_and_paper_mode_exposes_live_market_source(monkeypatch):
    monkeypatch.setenv("JARB_OPERATOR_TOKENS", f"alice={ALICE_TOKEN},bob={BOB_TOKEN}")
    monkeypatch.setattr(main, "require_dual_arm_approval", True)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            health = (await client.get("/api/health")).json()
            assert health == {"ok": True}

            async def approved_connection(payload):
                requested = await client.patch("/api/connection", headers=AUTH, json=payload)
                assert requested.status_code == 202
                approval_id = requested.json()["approvalId"]
                approved = await client.post(
                    f"/api/approvals/{approval_id}/approve", headers=BOB_AUTH,
                )
                assert approved.status_code == 200
                return await client.patch(
                    "/api/connection", headers={**AUTH, "X-JARB-Approval": approval_id},
                    json=payload,
                )

            configured = await approved_connection({
                "mode": "paper",
                "gmoApiKey": "gmo-public-1234",
                "gmoSecretKey": "gmo-secret-value",
                "bittradeAccessKey": "bittrade-public-5678",
                "bittradeSecretKey": "bittrade-secret-value",
                "bittradeAccountId": "account-1",
            })
            assert configured.status_code == 200
            assert configured.json()["gmoKeyHint"] == "••••1234"
            assert configured.json()["bittradeKeyHint"] == "••••5678"
            assert "gmo-secret-value" not in configured.text
            assert "bittrade-secret-value" not in (await client.get("/api/state", headers=AUTH)).text

            state = (await client.get("/api/state", headers=AUTH)).json()
            assert state["mode"] == "paper"
            assert state["runtime"]["core"] in ("Rust/PyO3", "Python/Decimal fallback")
            assert state["market"]["source"] == "GMO"
            manual_fill = await client.post("/api/paper/fill", headers=AUTH, json={
                "symbol": "BTC_JPY", "side": "BUY", "size": .001, "role": "maker",
            })
            assert manual_fill.status_code == 400
            assert "禁止手工注入成交" in manual_fill.text
            order_csv = await client.get("/api/orders/export?format=csv&mode=paper", headers=AUTH)
            assert order_csv.status_code == 200
            assert "trading_mode,client_order_id" in order_csv.text
            assert "attachment; filename=jarb-orders-paper.csv" in order_csv.headers["content-disposition"]

            limits_payload = {"maxSingleOrderJpy": 240000}
            limits_request = await client.patch(
                "/api/risk/limits", headers=AUTH, json=limits_payload,
            )
            assert limits_request.status_code == 202
            limits_approval = limits_request.json()["approvalId"]
            assert (await client.post(
                f"/api/approvals/{limits_approval}/approve", headers=AUTH,
            )).status_code == 409
            assert (await client.post(
                f"/api/approvals/{limits_approval}/approve", headers=BOB_AUTH,
            )).status_code == 200
            limits_updated = await client.patch(
                "/api/risk/limits",
                headers={**AUTH, "X-JARB-Approval": limits_approval},
                json=limits_payload,
            )
            assert limits_updated.status_code == 200
            assert limits_updated.json()["maxSingleOrderJpy"] == 240000

            scenarios_updated = await client.patch(
                "/api/paper/scenarios", headers=AUTH, json={"seed": 13},
            )
            assert scenarios_updated.status_code == 200
            with sqlite3.connect(main.service.state_store.path) as db:
                assert db.execute(
                    "SELECT actor FROM audit_events WHERE event_type='risk.limits.updated' "
                    "ORDER BY created_at DESC LIMIT 1"
                ).fetchone()[0] == "alice"
                assert db.execute(
                    "SELECT actor FROM audit_events WHERE event_type='paper.scenarios.updated' "
                    "ORDER BY created_at DESC LIMIT 1"
                ).fetchone()[0] == "alice"

            cleared = await approved_connection({
                "mode": "paper", "clearGmoCredentials": True, "clearBittradeCredentials": True,
            })
            assert cleared.json()["gmoConfigured"] is True
            assert cleared.json()["bittradeConfigured"] is True
            monkeypatch.setattr(main, "require_dual_arm_approval", False)
            updated = await client.patch(
                "/api/risk/limits", headers=AUTH,
                json={"maxSingleOrderJpy": 230000},
            )
            assert updated.status_code == 200
            assert updated.json()["maxSingleOrderJpy"] == 230000
            assert "approvalId" not in updated.json()
            disabled = await client.post(
                "/api/approvals/not-required/approve", headers=AUTH,
            )
            assert disabled.status_code == 409
            assert "单操作员模式" in disabled.text


@pytest.mark.asyncio
async def test_control_plane_fails_closed_and_dual_arm_uses_token_identity(tmp_path, monkeypatch):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        monkeypatch.delenv("JARB_OPERATOR_TOKENS", raising=False)
        assert (await client.get("/api/state")).status_code == 503
        assert (await client.get("/api/health")).status_code == 200

        monkeypatch.setenv("JARB_OPERATOR_TOKENS", f"alice={ALICE_TOKEN},bob={BOB_TOKEN}")
        assert (await client.get("/api/state")).status_code == 401
        assert (await client.get(
            "/api/state", headers={"Authorization": "Bearer invalid"},
        )).status_code == 401

        store = StateStore(tmp_path / "auth-risk.db")
        await store.initialize()
        gate = RiskGate(
            store, confirmation_phrase="ARM", kill_sentinel=tmp_path / "KILL",
            require_dual_approval=True,
        )
        await gate.restore()
        await gate.mark_recovery_complete()
        actors: list[str] = []

        async def authenticated_arm(phrase: str, actor: str) -> dict:
            actors.append(actor)
            armed = await gate.arm(phrase, actor)
            return {"armed": armed, "pendingArmActor": gate.pending_arm_actor}

        monkeypatch.setattr(main.service, "arm", authenticated_arm)
        spoof = await client.post(
            "/api/risk/arm", headers=AUTH, json={"phrase": "ARM", "actor": "bob"},
        )
        assert spoof.status_code == 422
        assert actors == []

        first = await client.post("/api/risk/arm", headers=AUTH, json={"phrase": "ARM"})
        assert first.status_code == 200 and first.json()["pendingArmActor"] == "alice"
        same_identity = await client.post("/api/risk/arm", headers=AUTH, json={"phrase": "ARM"})
        assert same_identity.status_code == 400
        second = await client.post(
            "/api/risk/arm", headers={"Authorization": f"Bearer {BOB_TOKEN}"}, json={"phrase": "ARM"},
        )
        assert second.status_code == 200 and second.json()["armed"] is True
        assert actors == ["alice", "alice", "bob"]
        await store.close()
