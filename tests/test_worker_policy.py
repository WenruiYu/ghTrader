from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_config(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DISABLE_DOTENV", "true")


def test_check_workers_clamps_to_caps(monkeypatch) -> None:
    from ghtrader.util.worker_policy import resolve_worker_count

    monkeypatch.setenv("GHTRADER_WORKERS_GLOBAL_MAX", "200")
    monkeypatch.setenv("GHTRADER_DIAGNOSE_MAX_WORKERS", "96")
    monkeypatch.setenv("GHTRADER_QDB_PG_NET_CONNECTION_LIMIT", "64")
    monkeypatch.setenv("GHTRADER_QDB_CONN_RESERVE", "10")

    # auto: cpu//4 = 32, cap min(global 200, diag 96, qdb 54) => 32
    assert resolve_worker_count(kind="check", requested=0, cpu_count=128) == 32

    # requested should still clamp to diag/qdb cap
    assert resolve_worker_count(kind="check", requested=200, cpu_count=128) == 54


def test_download_workers_auto(monkeypatch) -> None:
    from ghtrader.util.worker_policy import resolve_worker_count

    monkeypatch.setenv("GHTRADER_WORKERS_GLOBAL_MAX", "64")
    monkeypatch.setenv("GHTRADER_DOWNLOAD_MAX_WORKERS", "16")

    # auto: max(2, cpu//32=4) => 4, cap=16
    assert resolve_worker_count(kind="download", requested=0, cpu_count=128) == 4


def test_download_workers_respect_qdb_budget(monkeypatch) -> None:
    from ghtrader.util.worker_policy import resolve_worker_count

    monkeypatch.setenv("GHTRADER_WORKERS_GLOBAL_MAX", "128")
    monkeypatch.setenv("GHTRADER_DOWNLOAD_MAX_WORKERS", "64")
    monkeypatch.setenv("GHTRADER_DOWNLOAD_QDB_CONN_BUDGET", "10")

    # requested should clamp to download-side QuestDB budget
    assert resolve_worker_count(kind="download", requested=32, cpu_count=256) == 10


def test_query_workers_clamp_to_qdb_cap(monkeypatch) -> None:
    from ghtrader.util.worker_policy import resolve_worker_count

    monkeypatch.setenv("GHTRADER_WORKERS_GLOBAL_MAX", "256")
    monkeypatch.setenv("GHTRADER_QUERY_MAX_WORKERS", "128")
    monkeypatch.setenv("GHTRADER_QDB_PG_NET_CONNECTION_LIMIT", "40")
    monkeypatch.setenv("GHTRADER_QDB_CONN_RESERVE", "8")

    # qdb cap = 32 should dominate explicit request
    assert resolve_worker_count(kind="query", requested=80, cpu_count=256) == 32


def test_index_bootstrap_workers_cpu_cap(monkeypatch) -> None:
    from ghtrader.util.worker_policy import resolve_worker_count

    monkeypatch.setenv("GHTRADER_WORKERS_GLOBAL_MAX", "128")
    monkeypatch.setenv("GHTRADER_INDEX_BOOTSTRAP_WORKERS", "96")

    # auto: cpu//4=16, cap=min(128,96,cpu=64)=64 => 16
    assert resolve_worker_count(kind="index_bootstrap", requested=None, cpu_count=64) == 16
