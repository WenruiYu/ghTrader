"""
Distributed training utilities (torchrun/DDP).

Design goals:
- Safe no-op when not launched with torchrun (WORLD_SIZE=1).
- Minimal surface area: rank helpers, device selection, init, seeding.
- Keep model code simple and ensure only rank0 writes artifacts.
"""

from __future__ import annotations

import os
import random
from datetime import timedelta

import numpy as np
import torch


def _get_env_int(key: str, default: int) -> int:
    val = os.environ.get(key)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception as e:
        raise ValueError(f"Invalid integer env var {key}={val!r}") from e


def get_world_size() -> int:
    """World size set by torchrun; defaults to 1."""
    return _get_env_int("WORLD_SIZE", 1)


def get_rank() -> int:
    """Global rank set by torchrun; defaults to 0."""
    return _get_env_int("RANK", 0)


def get_local_rank() -> int:
    """Local rank (GPU index) set by torchrun; defaults to 0."""
    return _get_env_int("LOCAL_RANK", 0)


def is_distributed() -> bool:
    """True when launched under torchrun with WORLD_SIZE > 1."""
    return get_world_size() > 1


def is_rank0() -> bool:
    """True when this process is the primary rank."""
    return get_rank() == 0


def get_device() -> torch.device:
    """
    Return the preferred device for this process.

    - If CUDA is available and torchrun is used: cuda:{LOCAL_RANK}
    - If CUDA is available and not distributed: cuda:0
    - Else: cpu
    """
    if torch.cuda.is_available():
        if is_distributed():
            return torch.device(f"cuda:{get_local_rank()}")
        return torch.device("cuda:0")
    return torch.device("cpu")


def setup_distributed(backend: str | None = None, timeout_sec: int = 1800) -> bool:
    """
    Initialize torch.distributed if running under torchrun.

    Returns:
        True if distributed is (or becomes) initialized, else False.
    """
    if not is_distributed():
        return False

    import torch.distributed as dist

    if dist.is_initialized():
        return True

    if backend is None:
        backend = "nccl" if torch.cuda.is_available() else "gloo"

    # torchrun provides MASTER_ADDR/MASTER_PORT/RANK/WORLD_SIZE.
    dist.init_process_group(backend=backend, timeout=timedelta(seconds=timeout_sec))

    # Bind this rank to its GPU (important for NCCL correctness).
    if torch.cuda.is_available():
        torch.cuda.set_device(get_local_rank())

    return True


def barrier() -> None:
    """Barrier if distributed is initialized; otherwise no-op."""
    if not is_distributed():
        return
    import torch.distributed as dist

    if dist.is_initialized():
        dist.barrier()


def seed_everything(seed: int, deterministic: bool = False) -> None:
    """
    Seed Python/NumPy/PyTorch RNGs for reproducibility.

    Note: full determinism on GPU may reduce performance.
    """
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)

    if deterministic:
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False

