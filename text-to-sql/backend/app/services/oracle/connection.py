from __future__ import annotations

import os
import threading
from typing import Any
from pathlib import Path

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


def get_pool():
    global _POOL
    if _POOL is not None:
        return _POOL
    settings = get_settings()
    overrides = load_connection_settings()
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
            status_code=500, detail="ORACLE_DSN is not configured")
    _init_oracle_client()
    lib = _require_oracledb()
    with _POOL_LOCK:
        if _POOL is None:
            username = (overrides.get("username") or settings.oracle_user)
            password = (overrides.get("password") or settings.oracle_password)
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
                _POOL = lib.create_pool(**pool_kwargs)
            except Exception as exc:
                # When sslMode=require is set without a wallet/tns config,
                # Oracle thick mode can fail with ORA-28759.
                if (
                    str(dsn).lower().startswith("tcps://")
                    and tcp_fallback_dsn
                    and "ORA-28759" in str(exc).upper()
                    and not dsn_override
                ):
                    try:
                        pool_kwargs["dsn"] = tcp_fallback_dsn
                        _POOL = lib.create_pool(**pool_kwargs)
                    except Exception as retry_exc:
                        raise HTTPException(
                            status_code=503,
                            detail=f"Oracle pool create failed: {retry_exc}",
                        ) from retry_exc
                else:
                    raise HTTPException(
                        status_code=503,
                        detail=f"Oracle pool create failed: {exc}",
                    ) from exc
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
            reset_pool()
            try:
                return get_pool().acquire()
            except Exception as retry_exc:
                raise HTTPException(
                    status_code=503,
                    detail=f"Oracle pool unavailable: {retry_exc}",
                ) from retry_exc
        raise HTTPException(
            status_code=503, detail=f"Oracle pool unavailable: {exc}") from exc


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
        raise HTTPException(
            status_code=503, detail=f"Oracle connection check failed: {exc}") from exc
    return {
        "open": True,
        "busy": getattr(pool, "busy", None),
        "open_connections": getattr(pool, "open", None),
        "max": getattr(pool, "max", None),
    }
