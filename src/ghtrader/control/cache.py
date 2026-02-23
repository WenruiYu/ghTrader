from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CacheEntry:
    payload: Any
    updated_at: float
    key: str


class TTLCacheSlot:
    """
    Small thread-safe cache slot with TTL and optional key scoping.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entry: CacheEntry | None = None

    def get(self, *, ttl_s: float, key: str = "", now: float | None = None) -> Any | None:
        ts = float(time.time() if now is None else now)
        with self._lock:
            ent = self._entry
            if ent is None:
                return None
            if key and ent.key != str(key):
                return None
            if (ts - float(ent.updated_at)) > float(max(0.0, ttl_s)):
                return None
            return ent.payload

    def set(self, payload: Any, *, key: str = "", now: float | None = None) -> None:
        ts = float(time.time() if now is None else now)
        with self._lock:
            self._entry = CacheEntry(payload=payload, updated_at=ts, key=str(key))

    def clear(self) -> None:
        with self._lock:
            self._entry = None


class TTLCacheMap:
    """
    Small thread-safe map cache with per-entry timestamps.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, CacheEntry] = {}

    def get(self, key: str, *, ttl_s: float, now: float | None = None) -> Any | None:
        kk = str(key)
        ts = float(time.time() if now is None else now)
        with self._lock:
            ent = self._entries.get(kk)
            if ent is None:
                return None
            if (ts - float(ent.updated_at)) > float(max(0.0, ttl_s)):
                self._entries.pop(kk, None)
                return None
            return ent.payload

    def set(self, key: str, payload: Any, *, now: float | None = None) -> None:
        kk = str(key)
        ts = float(time.time() if now is None else now)
        with self._lock:
            self._entries[kk] = CacheEntry(payload=payload, updated_at=ts, key=kk)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

