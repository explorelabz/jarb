from __future__ import annotations

import asyncio
import time
from urllib.parse import urlparse

import httpx


class LarkWebhookNotifier:
    def __init__(self, client: httpx.AsyncClient | None = None, *, cooldown_sec: float = 300):
        self.client = client or httpx.AsyncClient(timeout=5.0)
        self._owns_client = client is None
        self.cooldown_sec = cooldown_sec
        self.webhook_url = ""
        self._last_sent: dict[str, float] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def validate_url(value: str) -> str:
        url = value.strip()
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in {"open.larksuite.com", "open.feishu.cn"}:
            raise ValueError("Webhook 必须是 Lark/飞书官方 HTTPS 地址")
        if not parsed.path.startswith("/open-apis/bot/v2/hook/"):
            raise ValueError("Webhook 路径格式不正确")
        return url

    def configure(self, value: str) -> None:
        self.webhook_url = self.validate_url(value) if value else ""

    async def send(self, message: str) -> bool:
        """Send a non-deduplicated safety alert."""
        if not self.webhook_url:
            return False
        async with self._lock:
            await self._deliver(message)
            return True

    async def send_once(self, key: str, message: str) -> bool:
        if not self.webhook_url:
            return False
        async with self._lock:
            now = time.monotonic()
            if now - self._last_sent.get(key, float("-inf")) < self.cooldown_sec:
                return False
            await self._deliver(message)
            self._last_sent[key] = now
            return True

    async def _deliver(self, message: str) -> None:
        response = await self.client.post(
            self.webhook_url,
            json={"msg_type": "text", "content": {"text": message}},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code", payload.get("StatusCode", 0)) != 0:
            raise RuntimeError(f"Lark webhook rejected alert: {payload.get('msg', payload.get('StatusMessage', 'unknown'))}")

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()
