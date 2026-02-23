from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

from ghtrader.config_service.schema import detect_legacy_managed_env, parse_bool
from ghtrader.config_service.store import ConfigRevision, ConfigStore


def _resolve_runs_dir(runs_dir: Path | None = None) -> Path:
    if runs_dir is not None:
        return Path(runs_dir)
    from ghtrader.config import get_runs_dir

    return Path(get_runs_dir())


class ConfigResolver:
    def __init__(
        self,
        *,
        runs_dir: Path | None = None,
        fallback_env: bool = True,
        cache_ttl_s: float = 1.0,
    ) -> None:
        self.runs_dir = _resolve_runs_dir(runs_dir)
        self.store = ConfigStore(self.runs_dir / "control" / "config.db")
        self.fallback_env = bool(fallback_env)
        self.cache_ttl_s = max(0.1, float(cache_ttl_s))
        self._cache_loaded_at = 0.0
        self._cached_revision = 0
        self._cached_hash = ""
        self._cached_snapshot: dict[str, str] = {}
        self.refresh(force=True)

    def refresh(self, *, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._cache_loaded_at) < self.cache_ttl_s:
            return
        rev, snap, snap_hash = self.store.get_latest_snapshot()
        self._cached_revision = int(rev)
        self._cached_hash = str(snap_hash)
        self._cached_snapshot = dict(snap)
        self._cache_loaded_at = now

    @property
    def revision(self) -> int:
        self.refresh()
        return int(self._cached_revision)

    @property
    def snapshot_hash(self) -> str:
        self.refresh()
        return str(self._cached_hash)

    def snapshot(self) -> dict[str, str]:
        self.refresh()
        return dict(self._cached_snapshot)

    def has_key(self, key: str) -> bool:
        self.refresh()
        k = str(key or "").strip()
        if not k:
            return False
        if k in self._cached_snapshot:
            return True
        if self.fallback_env and (k in os.environ):
            return True
        return False

    def get_raw_with_source(self, key: str, default: str | None = None) -> tuple[str | None, str]:
        self.refresh()
        k = str(key or "").strip()
        if not k:
            return default, "default"
        if k in self._cached_snapshot:
            return str(self._cached_snapshot.get(k, "")), f"config:{k}"
        if self.fallback_env and (k in os.environ):
            return str(os.environ.get(k, "") or ""), f"env:{k}"
        return default, "default"

    def get_str(self, key: str, default: str = "") -> str:
        raw, _src = self.get_raw_with_source(key, default)
        return str(raw if raw is not None else default)

    def get_bool(self, key: str, default: bool = False) -> bool:
        raw, _src = self.get_raw_with_source(key, None)
        if raw is None or str(raw).strip() == "":
            return bool(default)
        return bool(parse_bool(raw, default))

    def get_int(
        self,
        key: str,
        default: int = 0,
        *,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> int:
        raw, _src = self.get_raw_with_source(key, None)
        if raw is None or str(raw).strip() == "":
            out = int(default)
        else:
            try:
                out = int(str(raw).strip())
            except Exception:
                out = int(default)
        if min_value is not None:
            out = max(int(min_value), int(out))
        if max_value is not None:
            out = min(int(max_value), int(out))
        return int(out)

    def get_float(
        self,
        key: str,
        default: float = 0.0,
        *,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> float:
        raw, _src = self.get_raw_with_source(key, None)
        if raw is None or str(raw).strip() == "":
            out = float(default)
        else:
            try:
                out = float(str(raw).strip())
            except Exception:
                out = float(default)
        if min_value is not None:
            out = max(float(min_value), float(out))
        if max_value is not None:
            out = min(float(max_value), float(out))
        return float(out)

    def get_optional_int(
        self,
        key: str,
        *,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> tuple[int | None, str]:
        raw, src = self.get_raw_with_source(key, None)
        if raw is None or str(raw).strip() == "":
            return None, src
        try:
            out = int(str(raw).strip())
        except Exception:
            return None, src
        if min_value is not None:
            out = max(int(min_value), int(out))
        if max_value is not None:
            out = min(int(max_value), int(out))
        return int(out), src

    def get_optional_float(
        self,
        key: str,
        *,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> tuple[float | None, str]:
        raw, src = self.get_raw_with_source(key, None)
        if raw is None or str(raw).strip() == "":
            return None, src
        try:
            out = float(str(raw).strip())
        except Exception:
            return None, src
        if min_value is not None:
            out = max(float(min_value), float(out))
        if max_value is not None:
            out = min(float(max_value), float(out))
        return float(out), src

    def resolve_float_from_keys(
        self,
        *,
        keys: list[str],
        default: float | None,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> tuple[float | None, str]:
        for key in keys:
            val, src = self.get_optional_float(key, min_value=min_value, max_value=max_value)
            if val is not None:
                return float(val), src
        if default is None:
            return None, "unset"
        out = float(default)
        if min_value is not None:
            out = max(float(min_value), out)
        if max_value is not None:
            out = min(float(max_value), out)
        return float(out), "default"

    def resolve_int_from_keys(
        self,
        *,
        keys: list[str],
        default: int,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> tuple[int, str]:
        for key in keys:
            val, src = self.get_optional_int(key, min_value=min_value, max_value=max_value)
            if val is not None:
                return int(val), src
        out = int(default)
        if min_value is not None:
            out = max(int(min_value), int(out))
        if max_value is not None:
            out = min(int(max_value), int(out))
        return int(out), "default"

    def resolve_bool_from_keys(
        self,
        *,
        keys: list[str],
        default: bool,
    ) -> tuple[bool, str]:
        for key in keys:
            raw, src = self.get_raw_with_source(key, None)
            if raw is None or str(raw).strip() == "":
                continue
            return bool(parse_bool(raw, default)), src
        return bool(default), "default"

    def set_values(
        self,
        *,
        values: dict[str, Any],
        actor: str,
        reason: str,
        action: str = "set",
    ) -> ConfigRevision:
        rev = self.store.set_values(values=values, actor=actor, reason=reason, action=action)
        self.refresh(force=True)
        return rev

    def rollback(self, *, revision: int, actor: str, reason: str) -> ConfigRevision:
        rev = self.store.rollback(revision=int(revision), actor=actor, reason=reason)
        self.refresh(force=True)
        return rev

    def list_revisions(self, *, limit: int = 50) -> list[ConfigRevision]:
        return self.store.list_revisions(limit=limit)

    def export_snapshot_payload(self) -> dict[str, Any]:
        self.refresh()
        return {
            "config_revision": int(self._cached_revision),
            "config_hash": str(self._cached_hash),
            "config_values": dict(self._cached_snapshot),
        }


_RESOLVER_CACHE: dict[str, ConfigResolver] = {}


def get_config_resolver(*, runs_dir: Path | None = None, refresh: bool = False) -> ConfigResolver:
    rd = _resolve_runs_dir(runs_dir)
    key = str(rd.resolve())
    resolver = _RESOLVER_CACHE.get(key)
    if resolver is None:
        resolver = ConfigResolver(runs_dir=rd)
        _RESOLVER_CACHE[key] = resolver
    elif refresh:
        resolver.refresh(force=True)
    return resolver


def _is_config_migration_command(argv: list[str]) -> bool:
    items = [str(x or "").strip() for x in (argv or []) if str(x or "").strip()]
    for i, tok in enumerate(items):
        if tok == "config" and i + 1 < len(items) and items[i + 1] in {"migrate-env", "migrate_env"}:
            return True
    return False


def enforce_no_legacy_env(
    *,
    strict: bool = True,
    allow_in_tests: bool = True,
    argv: list[str] | None = None,
) -> list[str]:
    if parse_bool(os.environ.get("GHTRADER_CONFIG_ALLOW_LEGACY_ENV", ""), False):
        return []
    if allow_in_tests and os.environ.get("PYTEST_CURRENT_TEST"):
        return []
    argv_now = list(argv) if argv is not None else list(sys.argv)
    if _is_config_migration_command(argv_now):
        return []

    found = detect_legacy_managed_env(dict(os.environ))
    if found and strict:
        sample = ", ".join(found[:12])
        more = "" if len(found) <= 12 else f" (+{len(found) - 12} more)"
        raise RuntimeError(
            "Deprecated business env variables detected. "
            "Use `ghtrader config migrate-env` to import into Config Service and remove them from env files. "
            f"Found: {sample}{more}"
        )
    return found

