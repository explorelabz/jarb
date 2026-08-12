from __future__ import annotations

import hmac
import hashlib
import os
import re
import time
from dataclasses import dataclass
from uuid import uuid4

from fastapi import HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


_ACTOR_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")
_bearer = HTTPBearer(auto_error=False)


def operator_tokens(value: str | None = None) -> dict[str, str]:
    """Parse actor-bound bearer tokens without exposing them in API state."""
    raw = os.getenv("JARB_OPERATOR_TOKENS", "") if value is None else value
    result: dict[str, str] = {}
    for entry in raw.split(","):
        if not entry.strip():
            continue
        actor, separator, token = entry.partition("=")
        actor = actor.strip()
        token = token.strip()
        if not separator or not _ACTOR_RE.fullmatch(actor) or len(token) < 32:
            raise ValueError(
                "JARB_OPERATOR_TOKENS 必须使用 actor=token 格式，actor 仅含字母数字/_.@-，token 至少 32 字符"
            )
        if actor in result or any(hmac.compare_digest(token, saved) for saved in result.values()):
            raise ValueError("JARB_OPERATOR_TOKENS 的 actor 和 token 必须唯一")
        result[actor] = token
    return result


async def authenticate_request(request: Request) -> str:
    # Keep the liveness probe usable without granting access to trading state.
    if request.url.path == "/api/health":
        request.state.operator = "health-check"
        return request.state.operator
    try:
        configured = operator_tokens()
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not configured:
        raise HTTPException(
            status_code=503,
            detail="控制面鉴权未配置；请设置 JARB_OPERATOR_TOKENS",
        )
    credentials: HTTPAuthorizationCredentials | None = await _bearer(request)
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=401, detail="需要操作员 Bearer Token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    actor = next((
        name for name, token in configured.items()
        if hmac.compare_digest(credentials.credentials, token)
    ), None)
    if actor is None:
        raise HTTPException(
            status_code=401, detail="操作员 Token 无效",
            headers={"WWW-Authenticate": "Bearer"},
        )
    request.state.operator = actor
    return actor


def current_operator(request: Request) -> str:
    actor = getattr(request.state, "operator", None)
    if not actor or actor == "health-check":
        raise HTTPException(status_code=401, detail="未认证的操作员")
    return str(actor)


@dataclass
class SensitiveApproval:
    id: str
    action: str
    digest: str
    first_actor: str
    expires_at: float
    second_actor: str | None = None
    consumed: bool = False


class SensitiveApprovalGate:
    """Short-lived two-person approval without retaining sensitive request bodies."""

    def __init__(self, ttl_sec: int = 300):
        self.ttl_sec = ttl_sec
        self._items: dict[str, SensitiveApproval] = {}

    @staticmethod
    def digest(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def begin(self, action: str, payload: bytes, actor: str) -> SensitiveApproval:
        now = time.time()
        self._purge(now)
        approval = SensitiveApproval(
            id=f"AP-{uuid4().hex}", action=action, digest=self.digest(payload),
            first_actor=actor, expires_at=now + self.ttl_sec,
        )
        self._items[approval.id] = approval
        return approval

    def approve(self, approval_id: str, actor: str) -> SensitiveApproval:
        approval = self._get(approval_id)
        if hmac.compare_digest(actor, approval.first_actor):
            raise ValueError("敏感变更必须由另一位操作员复核")
        approval.second_actor = actor
        return approval

    def consume(
        self, approval_id: str, action: str, payload: bytes, actor: str,
    ) -> SensitiveApproval:
        approval = self._get(approval_id)
        if approval.consumed:
            raise ValueError("敏感变更审批已使用")
        if approval.second_actor is None:
            raise ValueError("敏感变更仍在等待第二位操作员复核")
        if approval.action != action or not hmac.compare_digest(approval.digest, self.digest(payload)):
            raise ValueError("敏感变更审批与本次请求不匹配")
        if actor not in (approval.first_actor, approval.second_actor):
            raise ValueError("只有参与审批的操作员可以执行敏感变更")
        approval.consumed = True
        return approval

    def _get(self, approval_id: str) -> SensitiveApproval:
        now = time.time()
        self._purge(now)
        approval = self._items.get(approval_id)
        if approval is None or approval.expires_at <= now:
            raise ValueError("敏感变更审批不存在或已过期")
        return approval

    def _purge(self, now: float) -> None:
        self._items = {
            key: value for key, value in self._items.items()
            if value.expires_at > now and not value.consumed
        }


sensitive_approvals = SensitiveApprovalGate()
