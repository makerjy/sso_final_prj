from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token

from app.services.runtime.user_scope import normalize_user_id


_CURRENT_USER_ID: ContextVar[str] = ContextVar("current_user_id", default="")


def get_request_user_id() -> str:
    return normalize_user_id(_CURRENT_USER_ID.get(""))


def set_request_user(user_id: str | None) -> Token[str]:
    normalized = normalize_user_id(user_id)
    return _CURRENT_USER_ID.set(normalized)


def reset_request_user(token: Token[str] | None) -> None:
    if token is None:
        return
    try:
        _CURRENT_USER_ID.reset(token)
    except Exception:
        pass


@contextmanager
def use_request_user(user_id: str | None):
    token = set_request_user(user_id)
    try:
        yield get_request_user_id()
    finally:
        reset_request_user(token)

