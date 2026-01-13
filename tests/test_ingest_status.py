from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd


def _write_calendar_cache(data_dir: Path, days: list[date]) -> None:
    p = data_dir / "akshare" / "calendar" / "calendar.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"date": pd.to_datetime([d.isoformat() for d in days])})
    df.to_parquet(p, index=False)


def _write_active_ranges_cache(data_dir: Path, *, exchange: str, var: str, rows: list[dict]) -> None:
    root = data_dir / "akshare" / "active_ranges" / f"market={exchange.upper()}" / f"var={var.lower()}"
    root.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(root / "active_ranges.parquet", index=False)
    (root / "manifest.json").write_text(
        json.dumps(
            {"exchange": exchange.upper(), "var": var.lower(), "scanned_start": "2000-01-01", "scanned_end": "2100-01-01"},
            indent=2,
        )
    )


def _touch_tick_day(data_dir: Path, symbol: str, day: date) -> None:
    d = data_dir / "lake" / "ticks" / f"symbol={symbol}" / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    # Presence of the date dir indicates a downloaded day; parquet files may be multiple parts.
    (d / "part-test.parquet").write_bytes(b"PAR1")


def _write_no_data_dates(data_dir: Path, symbol: str, days: list[date]) -> None:
    p = data_dir / "lake" / "ticks" / f"symbol={symbol}" / "_no_data_dates.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps([d.isoformat() for d in days], indent=2))


def _touch_tick_day_v2(data_dir: Path, symbol: str, day: date) -> None:
    d = data_dir / "lake_v2" / "ticks" / f"symbol={symbol}" / f"date={day.isoformat()}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "part-test.parquet").write_bytes(b"PAR1")


def _write_no_data_dates_v2(data_dir: Path, symbol: str, days: list[date]) -> None:
    p = data_dir / "lake_v2" / "ticks" / f"symbol={symbol}" / "_no_data_dates.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps([d.isoformat() for d in days], indent=2))


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

    cmd3 = cmd2 + ["--lake-version", "v2"]
    out3 = parse_ingest_command(cmd3)
    assert out3["kind"] == "download"
    assert out3["args"]["lake_version"] == "v2"


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
        "--data-dir",
        str(tmp_path / "data"),
        "--chunk-days",
        "5",
        "--no-refresh-akshare",
    ]
    out = parse_ingest_command(cmd)
    assert out["kind"] == "download_contract_range"
    assert out["args"]["exchange"] == "SHFE"
    assert out["args"]["var"] == "cu"
    assert out["args"]["start_contract"] == "1601"
    assert out["args"]["end_contract"] == "auto"

    cmd2 = cmd + ["--lake-version", "v2"]
    out2 = parse_ingest_command(cmd2)
    assert out2["args"]["lake_version"] == "v2"


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


def test_compute_range_progress_from_lake_and_no_data(tmp_path: Path) -> None:
    from ghtrader.control.ingest_status import compute_download_contract_range_status

    data_dir = tmp_path / "data"
    _write_calendar_cache(
        data_dir,
        days=[
            # 2025-01 range
            date(2025, 1, 2),
            date(2025, 1, 3),
            date(2025, 1, 6),
            date(2025, 1, 7),
            # 2025-02 range
            date(2025, 2, 3),
            date(2025, 2, 4),
            date(2025, 2, 5),
        ],
    )

    _write_active_ranges_cache(
        data_dir,
        exchange="SHFE",
        var="cu",
        rows=[
            {"symbol": "CU2501", "first_active": "2025-01-02", "last_active": "2025-01-07"},
            {"symbol": "CU2502", "first_active": "2025-02-03", "last_active": "2025-02-05"},
        ],
    )

    # CU2501 expected trading days: 4 (02,03,06,07)
    # downloaded: 2, no-data: 1 => done 3/4
    sym1 = "SHFE.cu2501"
    _touch_tick_day(data_dir, sym1, date(2025, 1, 2))
    _touch_tick_day(data_dir, sym1, date(2025, 1, 3))
    _write_no_data_dates(data_dir, sym1, [date(2025, 1, 6)])

    # CU2502 expected: 3 (03,04,05)
    # downloaded: 3 => done 3/3
    sym2 = "SHFE.cu2502"
    _touch_tick_day(data_dir, sym2, date(2025, 2, 3))
    _touch_tick_day(data_dir, sym2, date(2025, 2, 4))
    _touch_tick_day(data_dir, sym2, date(2025, 2, 5))

    status = compute_download_contract_range_status(
        exchange="SHFE",
        var="cu",
        start_contract="2501",
        end_contract="2502",
        data_dir=data_dir,
        log_hint={},
    )
    assert status["kind"] == "download_contract_range"
    assert status["summary"]["contracts_total"] == 2
    assert status["summary"]["days_expected_total"] == 7
    assert status["summary"]["days_done_total"] == 6
    assert 0.84 < status["summary"]["pct"] < 0.86


def test_compute_range_progress_v2_scans_lake_v2(tmp_path: Path) -> None:
    from ghtrader.control.ingest_status import compute_download_contract_range_status

    data_dir = tmp_path / "data"
    _write_calendar_cache(
        data_dir,
        days=[
            date(2025, 1, 2),
            date(2025, 1, 3),
            date(2025, 1, 6),
            date(2025, 1, 7),
            date(2025, 2, 3),
            date(2025, 2, 4),
            date(2025, 2, 5),
        ],
    )
    _write_active_ranges_cache(
        data_dir,
        exchange="SHFE",
        var="cu",
        rows=[
            {"symbol": "CU2501", "first_active": "2025-01-02", "last_active": "2025-01-07"},
            {"symbol": "CU2502", "first_active": "2025-02-03", "last_active": "2025-02-05"},
        ],
    )

    sym1 = "SHFE.cu2501"
    _touch_tick_day_v2(data_dir, sym1, date(2025, 1, 2))
    _touch_tick_day_v2(data_dir, sym1, date(2025, 1, 3))
    _write_no_data_dates_v2(data_dir, sym1, [date(2025, 1, 6)])

    sym2 = "SHFE.cu2502"
    _touch_tick_day_v2(data_dir, sym2, date(2025, 2, 3))
    _touch_tick_day_v2(data_dir, sym2, date(2025, 2, 4))
    _touch_tick_day_v2(data_dir, sym2, date(2025, 2, 5))

    status = compute_download_contract_range_status(
        exchange="SHFE",
        var="cu",
        start_contract="2501",
        end_contract="2502",
        data_dir=data_dir,
        log_hint={},
        lake_version="v2",
    )
    assert status["kind"] == "download_contract_range"
    assert status["lake_version"] == "v2"
    assert status["summary"]["days_expected_total"] == 7
    assert status["summary"]["days_done_total"] == 6

