from __future__ import annotations

import httpx
import pytest

from backend.main import app


@pytest.mark.asyncio
async def test_health_and_simulated_hedge_close_delta():
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            health = (await client.get("/api/health")).json()
            assert health["runtime"] == "Python"
            assert health["core"] == "Rust/PyO3"
            missing_confirmation = await client.patch("/api/connection", json={"mode": "online"})
            assert missing_confirmation.status_code == 400

            configured = await client.patch("/api/connection", json={
                "mode": "simulation",
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
            assert "bittrade-secret-value" not in (await client.get("/api/state")).text

            trade = await client.post("/api/sim/fill", json={"side": "SELL", "size": .01, "role": "maker"})
            assert trade.status_code == 201
            exported = (await client.get("/api/reconciliation/export")).json()
            assert exported["result"]["delta"] == 0
            assert exported["result"]["status"] == "matched"

            cleared = await client.patch("/api/connection", json={
                "mode": "simulation", "clearGmoCredentials": True, "clearBittradeCredentials": True,
            })
            assert cleared.json()["gmoConfigured"] is False
            assert cleared.json()["bittradeConfigured"] is False
