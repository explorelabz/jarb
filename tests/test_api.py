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
            trade = await client.post("/api/sim/fill", json={"side": "SELL", "size": .01, "role": "maker"})
            assert trade.status_code == 201
            exported = (await client.get("/api/reconciliation/export")).json()
            assert exported["result"]["delta"] == 0
            assert exported["result"]["status"] == "matched"
