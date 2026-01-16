from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import structlog

from ghtrader.main_depth import (
    MainDepthQuestDBResult,
    materialize_main_with_depth,
)
from ghtrader.questdb_index import INDEX_TABLE_V2, ensure_index_tables, query_contract_coverage_from_index
from ghtrader.questdb_client import make_questdb_query_config_from_env
from ghtrader.questdb_main_schedule import fetch_schedule

log = structlog.get_logger()


@dataclass(frozen=True)
class MainL5BuildResult:
    derived_symbol: str
    lake_version: str
    exchange: str
    variety: str
    schedule_hash: str
    l5_start_date: date
    schedule_rows_used: int
    materialization: MainDepthQuestDBResult


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
    exchange: str = "SHFE",
    overwrite: bool = False,
) -> MainL5BuildResult:
    """
    Build derived main_l5 ticks starting from the earliest date where the schedule's
    main contract has true L5 available (QuestDB-backed coverage).

    Args:
        var: Variety code (e.g. 'cu', 'au', 'ag')
        derived_symbol: continuous-style symbol name (e.g. 'KQ.m@SHFE.cu')
        overwrite: if True, re-process days that already exist
    """
    lv = "v2"
    ex = str(exchange).upper().strip()
    var_l = str(var).lower().strip()

    # Load canonical schedule from QuestDB.
    cfg = make_questdb_query_config_from_env()
    schedule = fetch_schedule(cfg=cfg, exchange=ex, variety=var_l, start_day=None, end_day=None, connect_timeout_s=2)
    if schedule.empty:
        raise FileNotFoundError(f"No schedule rows found in QuestDB for {ex}.{var_l}. Run `ghtrader main-schedule --var {var_l} ...` first.")

    # Extract schedule_hash (expected stable across rows for a schedule build).
    schedule_hash = ""
    try:
        schedule_hash = str(schedule["schedule_hash"].dropna().astype(str).iloc[0])
    except Exception:
        schedule_hash = ""
    if not schedule_hash:
        raise RuntimeError(
            f"Schedule rows for {ex}.{var_l} are missing schedule_hash. "
            "Re-run `ghtrader main-schedule` to (re)populate `ghtrader_main_schedule_v2`."
        )

    underlyings = sorted(set([c for c in schedule["main_contract"].astype(str).tolist() if c]))

    # Query QuestDB coverage to detect true-L5 era.
    ensure_index_tables(cfg=cfg, index_table=INDEX_TABLE_V2, connect_timeout_s=2)
    cov = query_contract_coverage_from_index(cfg=cfg, symbols=underlyings, lake_version="v2", ticks_lake="raw", index_table=INDEX_TABLE_V2)

    keep_mask: list[bool] = []
    for _, row in schedule.iterrows():
        d: date = row["trading_day"]
        c: str = str(row["main_contract"])
        keep_mask.append(_row_has_l5_for_contract_day(contract=c, day=d, cov=cov))
    schedule_l5 = schedule[pd.Series(keep_mask)].copy()
    if schedule_l5.empty:
        raise RuntimeError("No schedule rows have true L5 coverage (QuestDB index). Is L5 data ingested and the index populated?")

    schedule_l5 = schedule_l5.sort_values("trading_day").reset_index(drop=True)
    l5_start_date: date = schedule_l5["trading_day"].iloc[0]

    # Materialize derived ticks to QuestDB (ghtrader_ticks_main_l5_v2)
    mat = materialize_main_with_depth(
        derived_symbol=str(derived_symbol),
        schedule=schedule_l5,
        schedule_hash=str(schedule_hash),
        lake_version=lv,
        overwrite=bool(overwrite),
    )

    return MainL5BuildResult(
        derived_symbol=str(derived_symbol),
        lake_version=lv,
        exchange=ex,
        variety=var_l,
        schedule_hash=str(schedule_hash),
        l5_start_date=l5_start_date,
        schedule_rows_used=int(len(schedule_l5)),
        materialization=mat,
    )

