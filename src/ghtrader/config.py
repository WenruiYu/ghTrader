"""
Configuration: centralized settings and credential management.

Loads configuration from:
1. Environment variables
2. .env file (if present, via python-dotenv)
3. runs/control/accounts.env (if present; dashboard-managed; loaded after .env)

Usage:
    from ghtrader.config import load_config, get_tqsdk_auth
    
    # At CLI startup
    load_config()
    
    # When needing TqSdk auth
    auth = get_tqsdk_auth()
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger()

# Flag to track if config has been loaded
_config_loaded = False


def find_dotenv() -> Path | None:
    """Find the .env file, searching up from current directory."""
    # First check workspace root
    workspace = Path(__file__).parent.parent.parent.parent
    env_file = workspace / ".env"
    if env_file.exists():
        return env_file
    
    # Check current directory and parents
    current = Path.cwd()
    for _ in range(10):  # Max 10 levels up
        env_file = current / ".env"
        if env_file.exists():
            return env_file
        if current.parent == current:
            break
        current = current.parent
    
    return None


def load_config() -> None:
    """
    Load configuration from .env file if present.
    
    Should be called once at CLI startup.
    """
    global _config_loaded
    if _config_loaded:
        return

    # Tests should control environment explicitly. Avoid auto-loading a local `.env`
    # (which may exist on developer machines and would make tests flaky).
    if os.environ.get("PYTEST_CURRENT_TEST") or str(os.environ.get("GHTRADER_DISABLE_DOTENV", "")).lower() in {"1", "true", "yes"}:
        log.debug("config.skip_dotenv", reason="pytest_or_disabled")
        _config_loaded = True
        return
    
    try:
        from dotenv import load_dotenv

        env_file = find_dotenv()
        if env_file:
            load_dotenv(env_file, override=False)
            log.debug("config.loaded_dotenv", path=str(env_file))
        else:
            log.debug("config.no_dotenv_found")

        runs_dir = os.environ.get("GHTRADER_RUNS_DIR", "runs")
        accounts_env = Path(runs_dir) / "control" / "accounts.env"
        if accounts_env.exists():
            # Dashboard-managed broker account profiles (contains secrets).
            # Loaded after `.env` and may override account keys.
            load_dotenv(accounts_env, override=True)
            log.debug("config.loaded_accounts_env", path=str(accounts_env))
        else:
            log.debug("config.no_accounts_env", path=str(accounts_env))
    except ImportError:
        log.debug("config.dotenv_not_installed")
    
    _config_loaded = True


def get_env(key: str, default: str | None = None, required: bool = False) -> str | None:
    """Get an environment variable with optional default and required check."""
    value = os.environ.get(key, default)
    if required and not value:
        raise RuntimeError(
            f"Required environment variable {key} is not set. "
            f"Please set it in your .env file or environment."
        )
    return value


def env_int(key: str, default: int) -> int:
    """Read an integer environment variable with safe fallback."""
    load_config()
    try:
        return int(get_env(key, str(int(default))) or int(default))
    except Exception:
        return int(default)


def env_float(key: str, default: float) -> float:
    """Read a float environment variable with safe fallback."""
    load_config()
    try:
        return float(get_env(key, str(float(default))) or float(default))
    except Exception:
        return float(default)


def env_bool(key: str, default: bool) -> bool:
    """Read a boolean environment variable with common truthy values."""
    load_config()
    raw = str(get_env(key, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def get_qdb_redis_config() -> dict[str, Any]:
    """
    Canonical Redis config for QuestDB read-path cache/health checks.

    This is intentionally a small dict to keep call sites lightweight.
    """
    load_config()
    host = str(get_env("GHTRADER_QDB_REDIS_HOST", "127.0.0.1") or "127.0.0.1")
    port = max(1, env_int("GHTRADER_QDB_REDIS_PORT", 6379))
    db = max(0, env_int("GHTRADER_QDB_REDIS_DB", 0))
    timeout_s = max(0.1, env_float("GHTRADER_QDB_REDIS_TIMEOUT_S", 0.2))
    enabled = env_bool("GHTRADER_QDB_REDIS_CACHE_ENABLED", True)
    ttl_s = max(1, env_int("GHTRADER_QDB_REDIS_TTL_S", 300))
    return {
        "enabled": bool(enabled),
        "host": host,
        "port": int(port),
        "db": int(db),
        "timeout_s": float(timeout_s),
        "ttl_s": int(ttl_s),
    }


def l5_start_env_key(*, variety: str | None = None) -> str:
    v = str(variety or "").strip().lower()
    if not v:
        return "GHTRADER_L5_START_DATE"
    v_norm = "".join(ch for ch in v if ch.isalnum())
    if not v_norm:
        return "GHTRADER_L5_START_DATE"
    return f"GHTRADER_L5_START_DATE_{v_norm.upper()}"


def get_l5_start_date_with_source(*, variety: str | None = None) -> tuple[date, str]:
    """
    Resolve L5 start date for a variety.

    Resolution order:
    1) GHTRADER_L5_START_DATE_<VAR>
    2) GHTRADER_L5_START_DATE (legacy fallback)
    """
    load_config()
    key_var = l5_start_env_key(variety=variety)
    keys = [key_var] if key_var != "GHTRADER_L5_START_DATE" else []
    keys.append("GHTRADER_L5_START_DATE")

    for key in keys:
        raw = str(get_env(key, "") or "").strip()
        if not raw:
            continue
        try:
            return date.fromisoformat(raw[:10]), key
        except Exception as e:
            raise RuntimeError(f"Invalid {key}: {raw}") from e

    if key_var != "GHTRADER_L5_START_DATE":
        raise RuntimeError(
            f"Required environment variable {key_var} is not set "
            f"(and legacy fallback GHTRADER_L5_START_DATE is also missing)."
        )
    raise RuntimeError("Required environment variable GHTRADER_L5_START_DATE is not set.")


def get_l5_start_date(*, variety: str | None = None) -> date:
    d, _key = get_l5_start_date_with_source(variety=variety)
    return d


def get_tqsdk_auth() -> Any:
    """
    Get TqSdk authentication object.
    
    Reads credentials from environment variables (which may have been loaded from .env).
    
    Returns:
        TqAuth instance
    
    Raises:
        RuntimeError: If credentials are not configured
    """
    # Ensure config is loaded
    load_config()

    try:
        from tqsdk import TqAuth  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e
    
    user = get_env("TQSDK_USER", required=True)
    password = get_env("TQSDK_PASSWORD", required=True)
    
    return TqAuth(user, password)


def is_live_enabled() -> bool:
    """Check if live trading is enabled (DANGEROUS)."""
    load_config()
    return get_env("GHTRADER_LIVE_ENABLED", "false").lower() == "true"


def get_data_dir() -> Path:
    """Get the data directory path."""
    load_config()
    path_str = get_env("GHTRADER_DATA_DIR", "data")
    return Path(path_str)


def get_artifacts_dir() -> Path:
    """Get the artifacts directory path."""
    load_config()
    path_str = get_env("GHTRADER_ARTIFACTS_DIR", "artifacts")
    return Path(path_str)


def get_runs_dir() -> Path:
    """Get the runs directory path."""
    load_config()
    path_str = get_env("GHTRADER_RUNS_DIR", "runs")
    return Path(path_str)


# ---------------------------------------------------------------------------
# QuestDB (canonical tick DB) connection defaults
# ---------------------------------------------------------------------------


def get_questdb_host() -> str:
    load_config()
    return str(get_env("GHTRADER_QUESTDB_HOST", "127.0.0.1") or "127.0.0.1")


def get_questdb_ilp_port() -> int:
    load_config()
    try:
        return int(get_env("GHTRADER_QUESTDB_ILP_PORT", "9009") or 9009)
    except Exception:
        return 9009


def get_questdb_pg_port() -> int:
    load_config()
    try:
        return int(get_env("GHTRADER_QUESTDB_PG_PORT", "8812") or 8812)
    except Exception:
        return 8812


def get_questdb_pg_user() -> str:
    load_config()
    return str(get_env("GHTRADER_QUESTDB_PG_USER", "admin") or "admin")


def get_questdb_pg_password() -> str:
    load_config()
    return str(get_env("GHTRADER_QUESTDB_PG_PASSWORD", "quest") or "quest")


def get_questdb_pg_dbname() -> str:
    load_config()
    return str(get_env("GHTRADER_QUESTDB_PG_DBNAME", "qdb") or "qdb")


def get_questdb_metrics_port() -> int:
    load_config()
    try:
        return int(get_env("GHTRADER_QUESTDB_METRICS_PORT", "9003") or 9003)
    except Exception:
        return 9003
