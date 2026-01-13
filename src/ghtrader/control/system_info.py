from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DiskUsage:
    total_gb: float
    used_gb: float
    free_gb: float


def disk_usage(path: Path) -> DiskUsage:
    """
    Return disk usage for the filesystem containing `path`.

    This must be resilient to non-existent paths (fresh checkout where `artifacts/` hasn't been created yet).
    In that case we probe parent directories until we find an existing path.
    """
    try:
        p = Path(path)
        while not p.exists() and p != p.parent:
            p = p.parent
        du = shutil.disk_usage(p)
        gb = 1024**3
        return DiskUsage(total_gb=du.total / gb, used_gb=du.used / gb, free_gb=du.free / gb)
    except Exception:
        nan = float("nan")
        return DiskUsage(total_gb=nan, used_gb=nan, free_gb=nan)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _human_bytes(n: int | None) -> str:
    if n is None:
        return "n/a"
    x = float(n)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if x < 1024.0 or unit == "PiB":
            if unit == "B":
                return f"{int(x)} {unit}"
            return f"{x:.2f} {unit}"
        x /= 1024.0
    return f"{x:.2f} PiB"


def cpu_mem_info() -> dict[str, Any]:
    try:
        import psutil

        return {
            # Use interval=None for a non-blocking sample (first call may be 0.0).
            "cpu_percent": psutil.cpu_percent(interval=None),
            "mem_percent": psutil.virtual_memory().percent,
        }
    except Exception:
        return {"cpu_percent": None, "mem_percent": None}


def gpu_info() -> str | None:
    """
    Best-effort GPU summary via nvidia-smi if available.
    """
    # Resolve executable in a way that's robust to a restricted PATH for the dashboard process.
    candidates: list[str] = []
    try:
        p = shutil.which("nvidia-smi")
        if p:
            candidates.append(p)
    except Exception:
        pass

    for fp in ["/usr/bin/nvidia-smi", "/bin/nvidia-smi", "/usr/local/bin/nvidia-smi"]:
        try:
            if Path(fp).exists():
                candidates.append(fp)
        except Exception:
            continue

    # Deduplicate while preserving order.
    exe: str | None = None
    seen: set[str] = set()
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        exe = c
        break

    if not exe:
        return "nvidia-smi not found (PATH may be restricted for the dashboard process)"

    # Try the structured query first (compact output).
    query_cmd = [
        exe,
        "--query-gpu=name,memory.total,memory.used,utilization.gpu",
        "--format=csv,noheader",
    ]
    try:
        res = subprocess.run(query_cmd, capture_output=True, text=True, timeout=5)
        out = (res.stdout or "").strip()
        if res.returncode == 0 and out:
            return out
        err = (res.stderr or "").strip()
    except Exception as e:
        err = str(e)

    # Fallback: plain `nvidia-smi` (more verbose, but widely supported).
    try:
        res2 = subprocess.run([exe], capture_output=True, text=True, timeout=5)
        out2 = (res2.stdout or "").strip()
        if res2.returncode == 0 and out2:
            return out2
        err2 = (res2.stderr or "").strip()
        return f"nvidia-smi failed (path={exe}, rc={res2.returncode}): {err2 or err or 'unknown error'}"
    except Exception as e:
        return f"nvidia-smi failed (path={exe}): {err or str(e)}"


# -----------------------------------------------------------------------------
# Cached system snapshot (for dashboard /system auto-refresh)
# -----------------------------------------------------------------------------

_CACHE_LOCK = threading.Lock()

_FAST_TTL_SEC = 3.0
_GPU_TTL_SEC = 30.0
_DIR_TTL_SEC = 300.0
_DU_TIMEOUT_SEC = 8.0

_fast_key: tuple[str, str, str] | None = None
_fast_at: float = 0.0
_fast_payload: dict[str, Any] | None = None

_gpu_at: float = 0.0
_gpu_payload: dict[str, Any] | None = None
_gpu_thread_running: bool = False

_dir_key: tuple[str, str, str] | None = None
_dir_at: float = 0.0
_dir_payload: dict[str, Any] | None = None
_dir_thread_running: bool = False


def _fs_item(label: str, p: Path) -> dict[str, Any]:
    exists = bool(p.exists())
    du = disk_usage(p)
    used_pct = None
    try:
        if du.total_gb and du.total_gb == du.total_gb and du.total_gb > 0:
            used_pct = float(du.used_gb) / float(du.total_gb) * 100.0
    except Exception:
        used_pct = None
    return {
        "key": label,
        "path": str(p),
        "exists": exists,
        "fs": {
            "total_gb": du.total_gb,
            "used_gb": du.used_gb,
            "free_gb": du.free_gb,
            "used_pct": used_pct,
        },
        "dir": None,
    }


def _run_du_bytes(path: Path, *, timeout_s: float) -> tuple[int | None, str | None]:
    if not path.exists():
        return None, "missing"
    last_err = "unknown error"
    # Prefer GNU du -sb (bytes), fall back to -s -B1.
    for cmd in (["du", "-sb", str(path)], ["du", "-s", "-B1", str(path)]):
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
            out = (res.stdout or "").strip()
            err = (res.stderr or "").strip()
            if res.returncode != 0:
                last_err = err or f"rc={res.returncode}"
                continue
            if not out:
                last_err = err or "empty output"
                continue
            first = out.split()[0]
            return int(first), None
        except FileNotFoundError:
            return None, "du not found"
        except subprocess.TimeoutExpired:
            return None, f"timeout after {timeout_s:.1f}s"
        except Exception as e:
            last_err = str(e)
            continue
    return None, last_err  # type: ignore[possibly-undefined]


def _compute_dir_sizes(data_dir: Path, runs_dir: Path, artifacts_dir: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, p in [("data", data_dir), ("runs", runs_dir), ("artifacts", artifacts_dir)]:
        b, err = _run_du_bytes(p, timeout_s=_DU_TIMEOUT_SEC)
        out[key] = {
            "bytes": b,
            "human": _human_bytes(b),
            "error": err or "",
        }
    return out


def _maybe_start_gpu_refresh(*, force: bool) -> None:
    global _gpu_thread_running
    with _CACHE_LOCK:
        if _gpu_thread_running:
            return
        now = time.time()
        if (not force) and _gpu_payload is not None and (now - _gpu_at) <= _GPU_TTL_SEC:
            return
        _gpu_thread_running = True

    def _worker() -> None:
        global _gpu_payload, _gpu_at, _gpu_thread_running
        try:
            info = gpu_info() or ""
            payload = {"info": info, "updated_at": _now_utc_iso()}
            with _CACHE_LOCK:
                _gpu_payload = payload
                _gpu_at = time.time()
        finally:
            with _CACHE_LOCK:
                _gpu_thread_running = False

    t = threading.Thread(target=_worker, name="ghtrader-gpu-refresh", daemon=True)
    t.start()


def _maybe_start_dir_refresh(*, key: tuple[str, str, str], data_dir: Path, runs_dir: Path, artifacts_dir: Path, force: bool) -> None:
    global _dir_thread_running, _dir_key
    with _CACHE_LOCK:
        if _dir_thread_running:
            return
        now = time.time()
        if (not force) and _dir_payload is not None and _dir_key == key and (now - _dir_at) <= _DIR_TTL_SEC:
            return
        _dir_thread_running = True
        _dir_key = key

    def _worker() -> None:
        global _dir_payload, _dir_at, _dir_thread_running, _dir_key
        try:
            sizes = _compute_dir_sizes(data_dir, runs_dir, artifacts_dir)
            payload = {"sizes": sizes, "updated_at": _now_utc_iso()}
            with _CACHE_LOCK:
                _dir_payload = payload
                _dir_at = time.time()
        finally:
            with _CACHE_LOCK:
                _dir_thread_running = False

    t = threading.Thread(target=_worker, name="ghtrader-dirsize-refresh", daemon=True)
    t.start()


def system_snapshot(
    *,
    data_dir: Path,
    runs_dir: Path,
    artifacts_dir: Path,
    include_dir_sizes: bool = False,
    refresh: str = "none",
) -> dict[str, Any]:
    """
    Return a JSON-serializable snapshot for the dashboard System page.

    Notes:
    - CPU/mem + filesystem totals are computed synchronously (fast).
    - GPU and directory sizes are refreshed in background threads and served from cache.
    """
    refresh = str(refresh or "none").strip().lower()
    if refresh not in {"none", "fast", "dir"}:
        refresh = "none"

    global _fast_payload, _fast_key, _fast_at

    key = (str(data_dir), str(runs_dir), str(artifacts_dir))
    now = time.time()

    # Fast metrics cache (CPU/mem + filesystem totals)
    need_fast = False
    with _CACHE_LOCK:
        if _fast_payload is None or _fast_key != key or refresh in {"fast", "dir"} or (now - _fast_at) > _FAST_TTL_SEC:
            need_fast = True

    if need_fast:
        cpu_mem = cpu_mem_info()
        load = None
        try:
            la = os.getloadavg()
            load = {"load1": float(la[0]), "load5": float(la[1]), "load15": float(la[2])}
        except Exception:
            load = {"load1": None, "load5": None, "load15": None}

        disks = [
            _fs_item("data", data_dir),
            _fs_item("runs", runs_dir),
            _fs_item("artifacts", artifacts_dir),
        ]

        payload = {
            "updated_at": _now_utc_iso(),
            "cpu_mem": cpu_mem,
            "load": load,
            "disks": disks,
        }
        with _CACHE_LOCK:
            _fast_payload = payload
            _fast_key = key
            _fast_at = time.time()

    # GPU refresh: always background; force on refresh=fast
    _maybe_start_gpu_refresh(force=(refresh == "fast"))

    # Dir sizes: lazy/cached. Only start an expensive refresh when explicitly requested.
    if include_dir_sizes and refresh == "dir":
        _maybe_start_dir_refresh(
            key=key,
            data_dir=data_dir,
            runs_dir=runs_dir,
            artifacts_dir=artifacts_dir,
            force=True,
        )

    # Build response from caches
    with _CACHE_LOCK:
        fast = dict(_fast_payload or {})
        gpu_payload = dict(_gpu_payload or {})
        gpu_running = bool(_gpu_thread_running)
        dir_payload = dict(_dir_payload or {})
        dir_running = bool(_dir_thread_running)
        dir_key = _dir_key

    disks_out = list(fast.get("disks") or [])
    if include_dir_sizes and dir_payload and dir_key == key:
        sizes = (dir_payload.get("sizes") or {}) if isinstance(dir_payload.get("sizes"), dict) else {}
        for d in disks_out:
            k = d.get("key")
            if k in sizes:
                d["dir"] = sizes[k]

    return {
        "ts": _now_utc_iso(),
        "fast_updated_at": fast.get("updated_at", ""),
        "cpu_mem": fast.get("cpu_mem", {"cpu_percent": None, "mem_percent": None}),
        "load": fast.get("load", {"load1": None, "load5": None, "load15": None}),
        "disks": disks_out,
        "gpu": {
            "info": gpu_payload.get("info", "Loading GPU info...") if (gpu_payload.get("info") or gpu_running) else "n/a",
            "updated_at": gpu_payload.get("updated_at", ""),
            "status": "running" if gpu_running else ("ready" if gpu_payload.get("info") else "idle"),
        },
        "dir_sizes": {
            "updated_at": dir_payload.get("updated_at", ""),
            "status": "running"
            if dir_running
            else ("ready" if (include_dir_sizes and dir_payload and dir_key == key) else "idle"),
        },
    }

