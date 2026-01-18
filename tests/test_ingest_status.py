from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest


def test_parse_ingest_command_download_variants(tmp_path: Path) -> None:
    from ghtrader.control.ingest_status import parse_ingest_command

    cmd1 = [
        "ghtrader",
        "download",
        "--symbol",
        "SHFE.cu2602",
        "--start",
        "2025-02-17",
        "--end",
        "2025-02-20",
        "--data-dir",
        str(tmp_path / "data"),
    ]
    out1 = parse_ingest_command(cmd1)
    assert out1["kind"] == "download"
    assert out1["args"]["symbol"] == "SHFE.cu2602"
    assert out1["args"]["start_date"] == "2025-02-17"
    assert out1["args"]["end_date"] == "2025-02-20"

    cmd2 = [
        "/usr/bin/python3",
        "-m",
        "ghtrader.cli",
        "download",
        "--symbol",
        "SHFE.cu2602",
        "--start",
        "2025-02-17",
        "--end",
        "2025-02-20",
        "--data-dir",
        str(tmp_path / "data"),
    ]
    out2 = parse_ingest_command(cmd2)
    assert out2["kind"] == "download"
    assert out2["args"]["symbol"] == "SHFE.cu2602"

    cmd3 = cmd2 + ["--dataset-version", "v2"]
    out3 = parse_ingest_command(cmd3)
    assert out3["kind"] == "download"
    assert out3["args"]["dataset_version"] == "v2"


def test_parse_ingest_command_download_contract_range(tmp_path: Path) -> None:
    from ghtrader.control.ingest_status import parse_ingest_command

    cmd = [
        "/usr/bin/python3",
        "-m",
        "ghtrader.cli",
        "download-contract-range",
        "--exchange",
        "SHFE",
        "--var",
        "cu",
        "--start-contract",
        "1601",
        "--end-contract",
        "auto",
        "--start-date",
        "2015-01-01",
        "--end-date",
        "2015-01-08",
        "--data-dir",
        str(tmp_path / "data"),
        "--chunk-days",
        "5",
    ]
    out = parse_ingest_command(cmd)
    assert out["kind"] == "download_contract_range"
    assert out["args"]["exchange"] == "SHFE"
    assert out["args"]["var"] == "cu"
    assert out["args"]["start_contract"] == "1601"
    assert out["args"]["end_contract"] == "auto"
    assert out["args"]["start_date"] == "2015-01-01"
    assert out["args"]["end_date"] == "2015-01-08"

    cmd2 = cmd + ["--dataset-version", "v2"]
    out2 = parse_ingest_command(cmd2)
    assert out2["args"]["dataset_version"] == "v2"


def test_parse_log_tail_extracts_current_symbol_and_chunk() -> None:
    from ghtrader.control.ingest_status import parse_ingest_log_tail

    log_text = "\n".join(
        [
            "2026-01-12 22:00:24 -     INFO - 通知 : 与 wss://api.shinnytech.com/t/nfmd/front/mobile 的网络连接已建立",
            "2026-01-12T14:51:39.011945Z [warning  ] tq_ingest.empty_chunk          chunk_start=2016-02-08 symbol=SHFE.cu1701",
            "2026-01-12T15:00:00.000000Z [info     ] tq_ingest.contract_download     symbol=SHFE.cu2602 start=2025-02-17 end=2026-01-13",
            "2026-01-12T15:00:01.000000Z [info     ] tq_ingest.chunk_download        symbol=SHFE.cu2602 chunk_start=2025-02-17 chunk_end=2025-02-21 chunk_idx=3",
        ]
    )
    out = parse_ingest_log_tail(log_text)
    assert out.get("current_symbol") == "SHFE.cu2602"
    assert out.get("chunk_idx") == 3
    assert out.get("chunk_start") == "2025-02-17"
    assert out.get("chunk_end") == "2025-02-21"


def test_compute_range_progress_without_questdb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that without QuestDB, progress reports 0 days done.
    
    QuestDB is now the canonical source for coverage data. Without QuestDB,
    the function gracefully returns empty coverage.
    """
    from ghtrader.control.ingest_status import compute_download_contract_range_status

    # Force QuestDB to be unreachable in this unit test.
    monkeypatch.setenv("GHTRADER_QUESTDB_PG_PORT", "1")

    data_dir = tmp_path / "data"

    # Window: 2025-01-02..2025-01-07 (weekday fallback calendar => 4 trading days)
    d0 = date(2025, 1, 2)
    d1 = date(2025, 1, 7)

    status = compute_download_contract_range_status(
        exchange="SHFE",
        var="cu",
        start_contract="2501",
        end_contract="2502",
        start_date=d0,
        end_date=d1,
        data_dir=data_dir,
        log_hint={},
    )
    assert status["kind"] == "download_contract_range"
    assert status["summary"]["contracts_total"] == 2
    assert status["summary"]["days_expected_total"] == 8
    # Without QuestDB, days_done is 0
    assert status["summary"]["days_done_total"] == 0
    assert status["summary"]["pct"] == 0.0


def test_compute_range_progress_v2_without_questdb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that without QuestDB, progress reports 0 days done for v2.
    
    QuestDB is now the canonical source for coverage data.
    """
    from ghtrader.control.ingest_status import compute_download_contract_range_status

    # Force QuestDB to be unreachable in this unit test.
    monkeypatch.setenv("GHTRADER_QUESTDB_PG_PORT", "1")

    data_dir = tmp_path / "data"
    d0 = date(2025, 1, 2)
    d1 = date(2025, 1, 7)

    status = compute_download_contract_range_status(
        exchange="SHFE",
        var="cu",
        start_contract="2501",
        end_contract="2502",
        start_date=d0,
        end_date=d1,
        data_dir=data_dir,
        log_hint={},
        dataset_version="v2",
    )
    assert status["kind"] == "download_contract_range"
    assert status["dataset_version"] == "v2"
    assert status["summary"]["days_expected_total"] == 8
    # Without QuestDB, days_done is 0
    assert status["summary"]["days_done_total"] == 0

