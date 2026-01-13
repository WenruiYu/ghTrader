from __future__ import annotations

import importlib
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_explorer_page_exists(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/explorer")
    assert r.status_code == 200
    assert "SQL Explorer" in r.text


def test_system_page_handles_missing_artifacts_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runs_dir = tmp_path / "runs"
    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    runs_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    # Intentionally do NOT create artifacts_dir.

    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(artifacts_dir))

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/system")
    assert r.status_code == 200
    assert f"{artifacts_dir} (missing)" in r.text


def test_api_system_snapshot_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    # Avoid starting a slow GPU probe thread in this basic shape test.
    import ghtrader.control.system_info as si

    monkeypatch.setattr(si, "gpu_info", lambda: "GPU")

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    r = client.get("/api/system")
    assert r.status_code == 200
    j = r.json()
    assert "cpu_mem" in j
    assert "disks" in j
    assert "gpu" in j
    keys = {d["key"] for d in j["disks"]}
    assert keys == {"data", "runs", "artifacts"}


def test_api_system_gpu_refresh_background_fallback_to_plain_nvidia_smi(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    No real GPU required: simulate query form failing but plain `nvidia-smi` succeeding.
    GPU refresh runs in a background thread, so we poll until it completes.
    """
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    import ghtrader.control.system_info as si

    # Ensure gpu_info picks an executable via which() without needing a real file.
    monkeypatch.setattr(si.shutil, "which", lambda _: "/bin/nvidia-smi")

    calls: list[list[str]] = []

    class R:
        def __init__(self, rc: int, out: str = "", err: str = ""):
            self.returncode = rc
            self.stdout = out
            self.stderr = err

    def fake_run(cmd, capture_output=True, text=True, timeout=5):
        calls.append(list(cmd))
        if "--query-gpu" in " ".join(cmd):
            return R(1, "", "unsupported query")
        return R(0, "NVIDIA-SMI OK", "")

    monkeypatch.setattr(si.subprocess, "run", fake_run)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    # Trigger a forced refresh (starts background refresh).
    client.get("/api/system?refresh=fast")

    t0 = time.time()
    while time.time() - t0 < 2.0:
        j = client.get("/api/system").json()
        if j.get("gpu", {}).get("status") == "ready" and "NVIDIA-SMI OK" in str(j.get("gpu", {}).get("info", "")):
            break
        time.sleep(0.02)

    j = client.get("/api/system").json()
    assert "NVIDIA-SMI OK" in str(j.get("gpu", {}).get("info", ""))
    assert any("--query-gpu" in " ".join(c) for c in calls)


def test_api_system_dir_sizes_refresh_background(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Directory sizes are refreshed on-demand in a background thread.
    """
    runs_dir = tmp_path / "runs"
    data_dir = tmp_path / "data"
    artifacts_dir = tmp_path / "artifacts"
    runs_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(runs_dir))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(artifacts_dir))

    import ghtrader.control.system_info as si

    # Avoid GPU subprocess usage in this test.
    monkeypatch.setattr(si, "gpu_info", lambda: "GPU")

    class R:
        def __init__(self, rc: int, out: str = "", err: str = ""):
            self.returncode = rc
            self.stdout = out
            self.stderr = err

    def fake_run(cmd, capture_output=True, text=True, timeout=8):
        # du -sb <path>
        if isinstance(cmd, list) and cmd and cmd[0] == "du":
            p = str(cmd[-1])
            # Different bytes per dir to prove wiring.
            if p.endswith("/data"):
                return R(0, "1024\t" + p + "\n", "")
            if p.endswith("/runs"):
                return R(0, "2048\t" + p + "\n", "")
            if p.endswith("/artifacts"):
                return R(0, "4096\t" + p + "\n", "")
            return R(0, "0\t" + p + "\n", "")
        return R(0, "", "")

    monkeypatch.setattr(si.subprocess, "run", fake_run)

    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    client = TestClient(mod.app)

    client.get("/api/system?include_dir_sizes=true&refresh=dir")
    t0 = time.time()
    while time.time() - t0 < 2.0:
        j = client.get("/api/system?include_dir_sizes=true").json()
        if j.get("dir_sizes", {}).get("status") == "ready":
            break
        time.sleep(0.02)

    j = client.get("/api/system?include_dir_sizes=true").json()
    disks = {d["key"]: d for d in j["disks"]}
    assert disks["data"]["dir"]["bytes"] == 1024
    assert disks["runs"]["dir"]["bytes"] == 2048
    assert disks["artifacts"]["dir"]["bytes"] == 4096

