from __future__ import annotations

import asyncio
import httpx
import pytest

from backend.main import app


@pytest.mark.asyncio
async def test_health_and_paper_mode_exposes_live_market_source():
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            health = (await client.get("/api/health")).json()
            assert health["runtime"] == "Python"
            assert health["core"] == "Rust/PyO3"
            missing_confirmation = await client.patch("/api/connection", json={"mode": "live"})
            assert missing_confirmation.status_code == 400

            configured = await client.patch("/api/connection", json={
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
            assert "bittrade-secret-value" not in (await client.get("/api/state")).text

            state = (await client.get("/api/state")).json()
            assert state["mode"] == "paper"
            assert state["market"]["source"] == "GMO"
            manual_fill = await client.post("/api/paper/fill", json={
                "symbol": "BTC_JPY", "side": "BUY", "size": .001, "role": "maker",
            })
            assert manual_fill.status_code == 400
            assert "禁止手工注入成交" in manual_fill.text
            order_csv = await client.get("/api/orders/export?format=csv&mode=paper")
            assert order_csv.status_code == 200
            assert "trading_mode,client_order_id" in order_csv.text
            assert "attachment; filename=jarb-orders-paper.csv" in order_csv.headers["content-disposition"]

            cleared = await client.patch("/api/connection", json={
                "mode": "paper", "clearGmoCredentials": True, "clearBittradeCredentials": True,
            })
            assert cleared.json()["gmoConfigured"] is True
            assert cleared.json()["bittradeConfigured"] is True
