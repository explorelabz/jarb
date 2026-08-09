from __future__ import annotations

import asyncio
from pathlib import Path

from .models import AuditEvent


class AuditStore:
    def __init__(self, path: Path = Path("data/audit.jsonl")):
        self.path = path
        self._lock = asyncio.Lock()

    async def append(self, event: AuditEvent) -> None:
        line = event.model_dump_json() + "\n"
        async with self._lock:
            await asyncio.to_thread(self._append_sync, line)

    def _append_sync(self, line: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
