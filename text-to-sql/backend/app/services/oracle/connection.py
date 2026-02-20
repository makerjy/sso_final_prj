from __future__ import annotations

import os
import threading
from typing import Any
from pathlib import Path

from fastapi import HTTPException

from app.core.config import get_settings
from app.services.runtime.request_context import get_request_user_id
from app.services.runtime.settings_store import load_connection_settings
from app.services.runtime.user_scope import normalize_user_id

try:
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    oracledb = None


_POOLS: dict[str, Any] = {}
_POOL_LOCK = threading.Lock()
_CLIENT_INIT = False
_CLIENT_LOCK = threading.Lock()
_SSL_RETRY_ERROR_MARKERS = (
    "ORA-28759",
    "ORA-288",
)


def _pool_create_error(exc: Exception) -> HTTPException:
    detail = str(exc).strip()
    if "ORA-01017" in detail.upper():
        return HTTPException(
            status_code=503,
            detail=(
                "Oracle authentication failed (ORA-01017). "
                "Check username/password and save connection settings again."
            ),
        )
    return HTTPException(status_code=503, detail=f"Oracle pool create failed: {detail or exc}")


def _has_client_lib(lib_path: Path) -> bool:
    return any(
        next(lib_path.glob(pattern), None) is not None
        for pattern in ("libclntsh.so*", "oci.dll", "libclntsh.dylib")
    )


def _candidate_client_dirs() -> list[Path]:
    candidates: list[Path] = []
    env_dir = os.getenv("ORACLE_LIB_DIR", "").strip()
    if env_dir:
        candidates.append(Path(env_dir))

    here = Path(__file__).resolve()
    repo_root = here.parents[5] if len(here.parents) > 5 else here.parents[-1]
    text_to_sql_root = here.parents[4] if len(
        here.parents) > 4 else here.parents[-1]
    for rel in (
        Path("oracle/instantclient_23_26"),
        Path("query-visualization/oracle/instantclient_23_26"),
        Path("text-to-sql/oracle/instantclient_23_26"),
    ):
        candidates.append(repo_root / rel)
        candidates.append(text_to_sql_root / rel)

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _require_oracledb() -> Any:
    if oracledb is None:
        raise HTTPException(
            status_code=500, detail="oracledb library is not installed")
    return oracledb


def _init_oracle_client() -> None:
    global _CLIENT_INIT
    if _CLIENT_INIT:
        return
    lib = _require_oracledb()
    config_dir = os.getenv("ORACLE_TNS_ADMIN", "").strip()
    explicit_lib_dir = bool(os.getenv("ORACLE_LIB_DIR", "").strip())
    selected_path: Path | None = None
    for candidate in _candidate_client_dirs():
        if not candidate.exists():
            continue
        if _has_client_lib(candidate):
            selected_path = candidate
            break
    if selected_path is None:
        # Keep thin mode if no Instant Client is available.
        return
    lib_dir = str(selected_path)
    if not config_dir:
        candidate_tns = selected_path / "network" / "admin"
        if candidate_tns.exists():
            config_dir = str(candidate_tns)
    with _CLIENT_LOCK:
        if _CLIENT_INIT:
            return
        try:
            if config_dir:
                lib.init_oracle_client(lib_dir=lib_dir, config_dir=config_dir)
            else:
                lib.init_oracle_client(lib_dir=lib_dir)
        except Exception as exc:  # pragma: no cover - depends on client install
            if explicit_lib_dir:
                raise HTTPException(
                    status_code=500,
                    detail=f"Oracle client init failed. Check ORACLE_LIB_DIR/ORACLE_TNS_ADMIN. {exc}",
                ) from exc
            # Auto-detected client init failed: fall back to thin mode.
            return
        _CLIENT_INIT = True


def _resolve_user_id(user_id: str | None = None) -> str:
    explicit = normalize_user_id(user_id)
    if explicit:
        return explicit
    return normalize_user_id(get_request_user_id())


def _pool_key(user_id: str | None = None) -> str:
    resolved_user = _resolve_user_id(user_id)
    if resolved_user:
        return f"user::{resolved_user}"
    return "__global__"


def _close_pool(pool: Any) -> None:
    try:
        pool.close()
    except Exception:
        pass


def get_pool(user_id: str | None = None):
    key = _pool_key(user_id)
    with _POOL_LOCK:
        pool = _POOLS.get(key)
        if pool is not None:
            return pool

    settings = get_settings()
    resolved_user = _resolve_user_id(user_id)
    overrides = load_connection_settings(
        resolved_user or None,
        include_global_fallback=not bool(resolved_user),
    )
    host = str(overrides.get("host") or "").strip()
    port = str(overrides.get("port") or "").strip()
    database = str(overrides.get("database") or "").strip()
    ssl_mode = str(overrides.get("sslMode") or "").strip().lower()
    dsn_override = str(overrides.get("dsn") or "").strip()
    dsn = settings.oracle_dsn
    tcp_fallback_dsn = ""
    if dsn_override:
        dsn = dsn_override
    elif host and port and database:
        if ssl_mode in {"require", "verify-ca", "verify-full"}:
            dsn = f"tcps://{host}:{port}/{database}"
            tcp_fallback_dsn = f"{host}:{port}/{database}"
        else:
            dsn = f"{host}:{port}/{database}"
            tcp_fallback_dsn = dsn
    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="Oracle connection is not configured. Save connection settings first.",
        )
    _init_oracle_client()
    lib = _require_oracledb()
    with _POOL_LOCK:
        existing = _POOLS.get(key)
        if existing is not None:
            return existing

        username = str(overrides.get("username") or settings.oracle_user or "").strip()
        password_value = overrides.get("password")
        if password_value is None:
            password_value = settings.oracle_password
        password = str(password_value or "")
        if password != password.strip():
            password = password.strip()
        if not username:
            raise HTTPException(
                status_code=503,
                detail="Oracle username is not configured. Save connection settings first.",
            )
        if not password:
            raise HTTPException(
                status_code=503,
                detail="Oracle password is empty. Save connection settings with a valid password.",
            )
        pool_kwargs = {
            "user": username,
            "password": password,
            "dsn": dsn,
            "min": settings.oracle_pool_min,
            "max": settings.oracle_pool_max,
            "increment": settings.oracle_pool_inc,
            "timeout": settings.oracle_pool_timeout_sec,
        }
        try:
            created = lib.create_pool(**pool_kwargs)
        except Exception as exc:
            # When sslMode=require is set without a wallet/tns config,
            # Oracle connections can fail during SSL negotiation; retry
            # with plain TCP once for best-effort compatibility.
            upper_msg = str(exc).upper()
            if (
                str(dsn).lower().startswith("tcps://")
                and tcp_fallback_dsn
                and ssl_mode == "require"
                and any(marker in upper_msg for marker in _SSL_RETRY_ERROR_MARKERS)
                and not dsn_override
            ):
                try:
                    pool_kwargs["dsn"] = tcp_fallback_dsn
                    created = lib.create_pool(**pool_kwargs)
                except Exception as retry_exc:
                    raise _pool_create_error(retry_exc) from retry_exc
            else:
                raise _pool_create_error(exc) from exc
        _POOLS[key] = created
        return created


def reset_pool(user_id: str | None = None) -> None:
    key = _pool_key(user_id)
    with _POOL_LOCK:
        pool = _POOLS.pop(key, None)
        if pool is not None:
            _close_pool(pool)
            return

        # Preserve legacy behavior for no-user contexts.
        if key == "__global__":
            pools = list(_POOLS.values())
            _POOLS.clear()
            for item in pools:
                _close_pool(item)


def acquire_connection(user_id: str | None = None):
    pool = get_pool(user_id)
    try:
        return pool.acquire()
    except Exception as exc:  # pragma: no cover - depends on driver
        message = str(exc)
        upper = message.upper()
        recoverable = any(
            marker in upper
            for marker in (
                "DPY-4011",
                "DPY-6005",
                "DPI-1080",
                "CONNECTION WAS CLOSED",
                "CONNECTION RESET",
                "EOF OCCURRED",
            )
        )
        if recoverable:
            # Recover stale/disconnected pools once before failing the request.
            reset_pool(user_id)
            try:
                return get_pool(user_id).acquire()
            except Exception as retry_exc:
                raise HTTPException(
                    status_code=503,
                    detail=f"Oracle pool unavailable: {retry_exc}",
                ) from retry_exc
        raise HTTPException(
            status_code=503, detail=f"Oracle pool unavailable: {exc}") from exc


def pool_status(user_id: str | None = None) -> dict[str, Any]:
    pool = get_pool(user_id)
    try:
        conn = pool.acquire()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dual")
        cur.fetchone()
        cur.close()
        conn.close()
    except Exception as exc:  # pragma: no cover - depends on driver
        raise HTTPException(
            status_code=503, detail=f"Oracle connection check failed: {exc}") from exc
    return {
        "open": True,
        "busy": getattr(pool, "busy", None),
        "open_connections": getattr(pool, "open", None),
        "max": getattr(pool, "max", None),
    }
