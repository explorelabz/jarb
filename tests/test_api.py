from __future__ import annotations

import asyncio
import httpx
import pytest

import backend.main as main
from backend.engine.risk import RiskGate
from backend.engine.state_store import StateStore
from backend.main import app


ALICE_TOKEN = "alice-token-0123456789abcdef0123456789abcdef"
BOB_TOKEN = "bob-token-0123456789abcdef0123456789abcdef"
AUTH = {"Authorization": f"Bearer {ALICE_TOKEN}"}


@pytest.mark.asyncio
async def test_health_and_paper_mode_exposes_live_market_source(monkeypatch):
    monkeypatch.setenv("JARB_OPERATOR_TOKENS", f"alice={ALICE_TOKEN},bob={BOB_TOKEN}")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            health = (await client.get("/api/health")).json()
            assert health["runtime"] == "Python"
            assert health["core"] == "Rust/PyO3"
            missing_confirmation = await client.patch(
                "/api/connection", json={"mode": "live"}, headers=AUTH,
            )
            assert missing_confirmation.status_code == 400

            configured = await client.patch("/api/connection", headers=AUTH, json={
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

            cleared = await client.patch("/api/connection", headers=AUTH, json={
                "mode": "paper", "clearGmoCredentials": True, "clearBittradeCredentials": True,
            })
            assert cleared.json()["gmoConfigured"] is True
            assert cleared.json()["bittradeConfigured"] is True


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
