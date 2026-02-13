from __future__ import annotations

import os
import threading
from typing import Any

from fastapi import HTTPException

from app.core.config import get_settings
from app.services.runtime.settings_store import load_connection_settings

try:
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    oracledb = None


_POOL = None
_POOL_LOCK = threading.Lock()
_CLIENT_INIT = False
_CLIENT_LOCK = threading.Lock()


def _require_oracledb() -> Any:
    if oracledb is None:
        raise HTTPException(status_code=500, detail="oracledb library is not installed")
    return oracledb


def _init_oracle_client() -> None:
    global _CLIENT_INIT
    if _CLIENT_INIT:
        return
    lib = _require_oracledb()
    lib_dir = os.getenv("ORACLE_LIB_DIR", "").strip()
    config_dir = os.getenv("ORACLE_TNS_ADMIN", "").strip()
    if not lib_dir:
        _CLIENT_INIT = True
        return
    with _CLIENT_LOCK:
        if _CLIENT_INIT:
            return
        try:
            if config_dir:
                lib.init_oracle_client(lib_dir=lib_dir, config_dir=config_dir)
            else:
                lib.init_oracle_client(lib_dir=lib_dir)
        except Exception as exc:  # pragma: no cover - depends on client install
            raise HTTPException(
                status_code=500,
                detail=f"Oracle client init failed. Check ORACLE_LIB_DIR/ORACLE_TNS_ADMIN. {exc}",
            ) from exc
        _CLIENT_INIT = True


def get_pool():
    global _POOL
    if _POOL is not None:
        return _POOL
    settings = get_settings()
    overrides = load_connection_settings()
    host = str(overrides.get("host") or "").strip()
    port = str(overrides.get("port") or "").strip()
    database = str(overrides.get("database") or "").strip()
    dsn_override = str(overrides.get("dsn") or "").strip()
    dsn = settings.oracle_dsn
    if dsn_override:
        dsn = dsn_override
    elif host and port and database:
        dsn = f"{host}:{port}/{database}"
    if not dsn:
        raise HTTPException(status_code=500, detail="ORACLE_DSN is not configured")
    _init_oracle_client()
    lib = _require_oracledb()
    with _POOL_LOCK:
        if _POOL is None:
            _POOL = lib.create_pool(
                user=(overrides.get("username") or settings.oracle_user),
                password=(overrides.get("password") or settings.oracle_password),
                dsn=dsn,
                min=settings.oracle_pool_min,
                max=settings.oracle_pool_max,
                increment=settings.oracle_pool_inc,
                timeout=settings.oracle_pool_timeout_sec,
            )
    return _POOL


def reset_pool() -> None:
    global _POOL
    with _POOL_LOCK:
        if _POOL is not None:
            try:
                _POOL.close()
            except Exception:
                pass
        _POOL = None


def acquire_connection():
    pool = get_pool()
    try:
        return pool.acquire()
    except Exception as exc:  # pragma: no cover - depends on driver
        raise HTTPException(status_code=503, detail=f"Oracle pool unavailable: {exc}") from exc


def pool_status() -> dict[str, Any]:
    pool = get_pool()
    try:
        conn = pool.acquire()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dual")
        cur.fetchone()
        cur.close()
        conn.close()
    except Exception as exc:  # pragma: no cover - depends on driver
        raise HTTPException(status_code=503, detail=f"Oracle connection check failed: {exc}") from exc
    return {
        "open": True,
        "busy": getattr(pool, "busy", None),
        "open_connections": getattr(pool, "open", None),
        "max": getattr(pool, "max", None),
    }
