"""
Configuration: centralized settings and credential management.

Loads configuration from:
1. Environment variables
2. .env file (if present, via python-dotenv)

Usage:
    from ghtrader.config import load_config, get_tqsdk_auth
    
    # At CLI startup
    load_config()
    
    # When needing TqSdk auth
    auth = get_tqsdk_auth()
"""

from __future__ import annotations

import os
import sys
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
    
    try:
        from dotenv import load_dotenv
        
        env_file = find_dotenv()
        if env_file:
            load_dotenv(env_file, override=False)
            log.debug("config.loaded_dotenv", path=str(env_file))
        else:
            log.debug("config.no_dotenv_found")
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
    
    # Add vendored tqsdk to path if needed
    tqsdk_path = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
    if tqsdk_path.exists() and str(tqsdk_path) not in sys.path:
        sys.path.insert(0, str(tqsdk_path))
    
    from tqsdk import TqAuth
    
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


def get_lake_version() -> str:
    """
    Return the active lake version identifier.

    - v1: data/lake (legacy)
    - v2: data/lake_v2 (trading-day partitions; preferred)
    """
    load_config()
    v = str(get_env("GHTRADER_LAKE_VERSION", "v1") or "v1").strip().lower()
    if v in {"v1", "v2"}:
        return v
    raise RuntimeError(f"Invalid GHTRADER_LAKE_VERSION={v!r} (expected 'v1' or 'v2')")
