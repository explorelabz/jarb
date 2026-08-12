from __future__ import annotations

import hmac
import os
import re

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
