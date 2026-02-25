from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ghtrader.control.jobs import python_module_argv
from ghtrader.control.views_helpers import safe_int as _safe_int


def has_active_ddp_training(*, store: Any) -> bool:
    active = []
    try:
        active = store.list_active_jobs() + store.list_unstarted_queued_jobs(limit=2000)
    except Exception:
        return False
    for j in active:
        cmd = [str(x) for x in (j.command or [])]
        title = str(j.title or "").lower()
        if not cmd and not title:
            continue
        # torchrun --nproc_per_node>1 ... ghtrader.cli train
        if cmd and ("torchrun" in Path(cmd[0]).name.lower() or any("torchrun" in p.lower() for p in cmd)):
            if "ghtrader.cli" in cmd and "train" in cmd:
                for p in cmd:
                    if p.startswith("--nproc_per_node="):
                        n = _safe_int(p.split("=", 1)[1], 1, min_value=1)
                        if n > 1:
                            return True
        # fallback parser for training jobs without explicit torchrun marker in title.
        if title.startswith("train ") and "mode=torchrun-ddp-" in title:
            return True
    return False


def build_train_job_argv(
    *,
    model: str,
    symbol: str,
    data_dir: str,
    artifacts_dir: str,
    horizon: str,
    epochs: str,
    batch_size: str,
    seq_len: str,
    lr: str,
    gpus: int,
    ddp_requested: bool,
    num_workers: int,
    prefetch_factor: int,
    pin_memory: str,
    cuda_device_count: Callable[[], int],
    resolve_torchrun_path: Callable[[], str | None],
) -> tuple[list[str], dict[str, Any]]:
    deep_models = {"deeplob", "transformer", "tcn", "tlob", "ssm"}
    model_norm = str(model).strip().lower()
    requested_gpus = _safe_int(gpus, 1, min_value=1, max_value=8)
    cuda_count = max(0, int(cuda_device_count()))

    effective_gpus = requested_gpus
    use_ddp = bool(ddp_requested) and model_norm in deep_models and requested_gpus > 1
    launch_mode = "single-process"
    notes: list[str] = []

    if use_ddp and cuda_count > 0 and effective_gpus > cuda_count:
        effective_gpus = max(1, min(8, int(cuda_count)))
        notes.append(f"gpu_clamped_to_{effective_gpus}")
    if use_ddp and effective_gpus <= 1:
        use_ddp = False
        notes.append("ddp_disabled_insufficient_gpu")

    torchrun_path = resolve_torchrun_path() if use_ddp else None
    cli_gpus = effective_gpus
    if use_ddp and not torchrun_path:
        # Health check failed: keep training alive via single-process fallback.
        use_ddp = False
        cli_gpus = 1
        launch_mode = "fallback-single-no-torchrun"
        notes.append("ddp_disabled_no_torchrun")
    elif use_ddp and torchrun_path:
        launch_mode = f"torchrun-ddp-{effective_gpus}"
        notes.append("ddp_enabled")
    else:
        if model_norm not in deep_models and requested_gpus > 1:
            cli_gpus = 1
            notes.append("non_deep_model_forces_single_gpu")
        launch_mode = "single-process"

    cli_args = [
        "train",
        "--model",
        model,
        "--symbol",
        symbol,
        "--data-dir",
        data_dir,
        "--artifacts-dir",
        artifacts_dir,
        "--horizon",
        horizon,
        "--gpus",
        str(cli_gpus),
        "--epochs",
        epochs,
        "--batch-size",
        batch_size,
        "--seq-len",
        seq_len,
        "--lr",
        lr,
        "--num-workers",
        str(max(0, int(num_workers))),
        "--prefetch-factor",
        str(max(1, int(prefetch_factor))),
        "--pin-memory",
        (str(pin_memory).strip().lower() if str(pin_memory).strip() else "auto"),
        "--ddp" if use_ddp else "--no-ddp",
    ]

    if use_ddp and torchrun_path:
        argv = [
            str(torchrun_path),
            "--standalone",
            "--nnodes=1",
            f"--nproc_per_node={effective_gpus}",
            "-m",
            "ghtrader.cli",
            *cli_args,
        ]
    else:
        argv = python_module_argv("ghtrader.cli", *cli_args)

    return (
        argv,
        {
            "mode": launch_mode,
            "requested_gpus": requested_gpus,
            "effective_gpus": int(effective_gpus if use_ddp else cli_gpus),
            "cuda_count": int(cuda_count),
            "notes": notes,
        },
    )
