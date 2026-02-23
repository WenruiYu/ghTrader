from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import math
import threading
from typing import Any


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    qq = min(1.0, max(0.0, float(q)))
    pos = qq * (len(sorted_values) - 1)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return float(sorted_values[lower])
    frac = pos - lower
    lo = float(sorted_values[lower])
    hi = float(sorted_values[upper])
    return lo + (hi - lo) * frac


@dataclass
class _MetricWindow:
    latencies_ms: deque[float] = field(default_factory=deque)
    total: int = 0
    errors: int = 0


class ObservabilityStore:
    """
    In-memory rolling observability store for latency/error-rate snapshots.
    """

    def __init__(self, *, max_samples: int = 2048) -> None:
        self.max_samples = max(64, int(max_samples))
        self._lock = threading.Lock()
        self._metrics: dict[str, _MetricWindow] = {}

    def observe(self, *, metric: str, latency_s: float | None = None, ok: bool = True) -> None:
        name = str(metric or "").strip() or "unknown"
        with self._lock:
            win = self._metrics.get(name)
            if win is None:
                win = _MetricWindow(latencies_ms=deque(maxlen=self.max_samples))
                self._metrics[name] = win
            win.total += 1
            if not bool(ok):
                win.errors += 1
            if latency_s is not None:
                ms = max(0.0, float(latency_s) * 1000.0)
                win.latencies_ms.append(ms)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            out: dict[str, Any] = {}
            for name, win in self._metrics.items():
                vals = list(win.latencies_ms)
                vals.sort()
                n = int(win.total)
                e = int(win.errors)
                out[name] = {
                    "count": n,
                    "error_count": e,
                    "error_rate": (float(e) / float(n)) if n > 0 else 0.0,
                    "samples": int(len(vals)),
                    "p50_ms": _percentile(vals, 0.50),
                    "p95_ms": _percentile(vals, 0.95),
                    "p99_ms": _percentile(vals, 0.99),
                    "max_ms": (float(vals[-1]) if vals else 0.0),
                }
            return out

    def clear(self) -> None:
        with self._lock:
            self._metrics.clear()


_GLOBAL_LOCK = threading.Lock()
_GLOBAL_STORES: dict[str, ObservabilityStore] = {}


def get_store(domain: str, *, max_samples: int = 2048) -> ObservabilityStore:
    key = str(domain or "").strip() or "default"
    with _GLOBAL_LOCK:
        store = _GLOBAL_STORES.get(key)
        if store is None:
            store = ObservabilityStore(max_samples=max_samples)
            _GLOBAL_STORES[key] = store
        return store


def snapshot_all() -> dict[str, Any]:
    with _GLOBAL_LOCK:
        names = list(_GLOBAL_STORES.keys())
    out: dict[str, Any] = {}
    for name in names:
        out[name] = get_store(name).snapshot()
    return out

