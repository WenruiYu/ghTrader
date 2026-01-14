from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from ghtrader.config import (
    get_data_dir,
    get_questdb_host,
    get_questdb_pg_dbname,
    get_questdb_pg_password,
    get_questdb_pg_port,
    get_questdb_pg_user,
)
from ghtrader.main_depth import MainDepthMaterializationResult, materialize_main_with_depth
from ghtrader.questdb_queries import QuestDBQueryConfig, query_contract_coverage

log = structlog.get_logger()


@dataclass(frozen=True)
class MainL5BuildResult:
    derived_symbol: str
    lake_version: str
    schedule_full_path: Path
    schedule_l5_path: Path
    l5_start_date: date
    materialization: MainDepthMaterializationResult


def _roll_schedule_path(data_dir: Path, var: str) -> Path:
    return data_dir / "rolls" / "shfe_main_schedule" / f"var={str(var).lower().strip()}" / "schedule.parquet"


def _roll_schedule_l5_path(data_dir: Path, var: str) -> Path:
    return data_dir / "rolls" / "shfe_main_schedule" / f"var={str(var).lower().strip()}" / "schedule_l5.parquet"


def _recompute_segment_id(schedule: pd.DataFrame) -> pd.DataFrame:
    s = schedule.sort_values("date").reset_index(drop=True).copy()
    seg_ids: list[int] = []
    seg = 0
    prev: str | None = None
    for mc in s["main_contract"].astype(str).tolist():
        if prev is None:
            seg = 0
        elif mc != prev:
            seg += 1
        seg_ids.append(int(seg))
        prev = mc
    s["segment_id"] = seg_ids
    return s


def _parse_day(x: Any) -> date | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return pd.to_datetime(s, errors="coerce").date()  # type: ignore[no-any-return]
    except Exception:
        return None


def _row_has_l5_for_contract_day(
    *,
    contract: str,
    day: date,
    cov: dict[str, dict[str, Any]],
) -> bool:
    c = cov.get(contract) or {}
    d0 = _parse_day(c.get("first_l5_day"))
    d1 = _parse_day(c.get("last_l5_day"))
    if d0 is None or d1 is None:
        return False
    return d0 <= day <= d1


def build_main_l5_l5_era_only(
    *,
    var: str,
    derived_symbol: str,
    schedule_path: Path | None = None,
    data_dir: Path | None = None,
    overwrite: bool = False,
    lake_version: str = "v2",
    questdb_table: str | None = None,
) -> MainL5BuildResult:
    """
    Build derived main_l5 ticks starting from the earliest date where the schedule's
    main contract has true L5 available (QuestDB-backed coverage).
    """
    if data_dir is None:
        data_dir = get_data_dir()

    _ = lake_version  # v2-only
    lv = "v2"

    schedule_full_path = schedule_path or _roll_schedule_path(data_dir, var)
    if not schedule_full_path.exists():
        raise FileNotFoundError(str(schedule_full_path))

    schedule = pd.read_parquet(schedule_full_path)
    if schedule.empty:
        raise ValueError("Schedule is empty")
    if "date" not in schedule.columns or "main_contract" not in schedule.columns:
        raise ValueError(f"Schedule missing required columns: {list(schedule.columns)}")

    schedule = schedule.copy()
    schedule["date"] = pd.to_datetime(schedule["date"], errors="coerce").dt.date
    schedule["main_contract"] = schedule["main_contract"].astype(str)
    schedule = schedule.dropna(subset=["date", "main_contract"]).sort_values("date").reset_index(drop=True)
    if schedule.empty:
        raise ValueError("Schedule has no usable rows after normalization")

    underlyings = sorted(set([c for c in schedule["main_contract"].astype(str).tolist() if c]))

    # Query QuestDB coverage to detect true-L5 era.
    cfg = QuestDBQueryConfig(
        host=get_questdb_host(),
        pg_port=int(get_questdb_pg_port()),
        pg_user=str(get_questdb_pg_user()),
        pg_password=str(get_questdb_pg_password()),
        pg_dbname=str(get_questdb_pg_dbname()),
    )
    tbl = str(questdb_table).strip() if questdb_table is not None and str(questdb_table).strip() else "ghtrader_ticks_raw_v2"
    cov = query_contract_coverage(cfg=cfg, table=tbl, symbols=underlyings, lake_version="v2", ticks_lake="raw")

    keep_mask: list[bool] = []
    for _, row in schedule.iterrows():
        d: date = row["date"]
        c: str = str(row["main_contract"])
        keep_mask.append(_row_has_l5_for_contract_day(contract=c, day=d, cov=cov))
    schedule_l5 = schedule[pd.Series(keep_mask)].copy()
    if schedule_l5.empty:
        raise RuntimeError("No schedule rows have true L5 coverage (QuestDB). Is L5 data synced to QuestDB?")

    schedule_l5 = schedule_l5.sort_values("date").reset_index(drop=True)
    l5_start_date: date = schedule_l5["date"].iloc[0]

    # Recompute segment ids for the effective schedule (L5 era only).
    schedule_l5 = _recompute_segment_id(schedule_l5)

    schedule_l5_path = _roll_schedule_l5_path(data_dir, var)
    schedule_l5_path.parent.mkdir(parents=True, exist_ok=True)
    schedule_l5.to_parquet(schedule_l5_path, index=False)
    try:
        manifest = {
            "created_at": datetime.now().isoformat(),
            "variety": str(var).lower().strip(),
            "derived_symbol": str(derived_symbol),
            "schedule_full_path": str(schedule_full_path),
            "schedule_l5_path": str(schedule_l5_path),
            "l5_start_date": l5_start_date.isoformat(),
            "rows": int(len(schedule_l5)),
            "questdb": {"table": tbl},
        }
        (schedule_l5_path.parent / "schedule_l5_manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    except Exception:
        pass

    # Materialize derived ticks using the effective L5-era schedule.
    mat = materialize_main_with_depth(
        derived_symbol=str(derived_symbol),
        schedule_path=schedule_l5_path,
        data_dir=data_dir,
        overwrite=bool(overwrite),
        lake_version=lv,  # type: ignore[arg-type]
    )

    return MainL5BuildResult(
        derived_symbol=str(derived_symbol),
        lake_version=lv,
        schedule_full_path=schedule_full_path,
        schedule_l5_path=schedule_l5_path,
        l5_start_date=l5_start_date,
        materialization=mat,
    )

