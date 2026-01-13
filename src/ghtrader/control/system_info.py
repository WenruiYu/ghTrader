from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DiskUsage:
    total_gb: float
    used_gb: float
    free_gb: float


def disk_usage(path: Path) -> DiskUsage:
    du = shutil.disk_usage(path)
    gb = 1024**3
    return DiskUsage(total_gb=du.total / gb, used_gb=du.used / gb, free_gb=du.free / gb)


def cpu_mem_info() -> dict[str, Any]:
    try:
        import psutil

        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "mem_percent": psutil.virtual_memory().percent,
        }
    except Exception:
        return {"cpu_percent": None, "mem_percent": None}


def gpu_info() -> str | None:
    """
    Best-effort GPU summary via nvidia-smi if available.
    """
    try:
        res = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total,memory.used,utilization.gpu", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if res.returncode != 0:
            return None
        return res.stdout.strip()
    except Exception:
        return None

