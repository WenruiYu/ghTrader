from __future__ import annotations

import importlib
import sys


def test_views_training_helper_bindings_come_from_dedicated_module() -> None:
    views_mod = importlib.import_module("ghtrader.control.views")
    assert getattr(getattr(views_mod, "_build_train_job_argv_impl"), "__module__", "") == "ghtrader.control.views_training_helpers"
    assert getattr(getattr(views_mod, "_has_active_ddp_training_impl"), "__module__", "") == "ghtrader.control.views_training_helpers"


def test_build_train_job_argv_falls_back_without_torchrun() -> None:
    helper_mod = importlib.import_module("ghtrader.control.views_training_helpers")
    argv, meta = helper_mod.build_train_job_argv(
        model="deeplob",
        symbol="KQ.m@SHFE.cu",
        data_dir="data",
        artifacts_dir="artifacts",
        horizon="50",
        epochs="50",
        batch_size="256",
        seq_len="100",
        lr="0.001",
        gpus=4,
        ddp_requested=True,
        num_workers=0,
        prefetch_factor=2,
        pin_memory="auto",
        cuda_device_count=lambda: 8,
        resolve_torchrun_path=lambda: None,
    )
    assert argv[0] == sys.executable
    assert "--no-ddp" in argv
    assert str(meta.get("mode") or "") == "fallback-single-no-torchrun"


def test_has_active_ddp_training_detects_torchrun_jobs() -> None:
    helper_mod = importlib.import_module("ghtrader.control.views_training_helpers")

    class _Job:
        def __init__(self) -> None:
            self.command = [
                "torchrun",
                "--standalone",
                "--nnodes=1",
                "--nproc_per_node=4",
                "-m",
                "ghtrader.cli",
                "train",
            ]
            self.title = "train deeplob KQ.m@SHFE.cu mode=torchrun-ddp-4 gpus=4"

    class _Store:
        def list_active_jobs(self):  # type: ignore[no-untyped-def]
            return [_Job()]

        def list_unstarted_queued_jobs(self, limit: int = 2000):  # type: ignore[no-untyped-def]
            _ = limit
            return []

    assert helper_mod.has_active_ddp_training(store=_Store()) is True
