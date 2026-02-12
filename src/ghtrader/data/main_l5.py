"""
Schedule-driven main_l5 builder (QuestDB-only, no raw ticks).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import json
import pandas as pd
import structlog

log = structlog.get_logger()


@dataclass(frozen=True)
class MainL5BuildResult:
    derived_symbol: str
    schedule_hash: str
    rows_total: int
    days_total: int
    segments: list[dict[str, Any]]


def _schedule_hash_from_rows(schedule: pd.DataFrame) -> str:
    try:
        v = str(schedule["schedule_hash"].dropna().astype(str).iloc[0])
        return v
    except Exception:
        return ""


def _main_l5_update_report_dir(runs_dir: Path) -> Path:
    return runs_dir / "control" / "reports" / "main_l5_update"


def _write_main_l5_update_report(
    *,
    runs_dir: Path,
    derived_symbol: str,
    schedule_hash: str,
    exchange: str,
    variety: str,
    update_mode: bool,
    full_rebuild: bool,
    schedule: pd.DataFrame,
    covered_days: set[date],
    coverage_bounds: dict[str, Any] | None,
) -> Path | None:
    try:
        expected_days = sorted({d for d in schedule["trading_day"].tolist() if isinstance(d, date)})
    except Exception:
        expected_days = []
    missing_days = [d for d in expected_days if d not in covered_days]

    missing_by_segment: list[dict[str, Any]] = []
    try:
        for seg_id, g in schedule.groupby("segment_id", sort=True):
            seg_days = [d for d in g["trading_day"].tolist() if isinstance(d, date)]
            seg_missing = [d for d in seg_days if d not in covered_days]
            missing_by_segment.append(
                {
                    "segment_id": int(seg_id),
                    "underlying_contract": str(g["main_contract"].iloc[0]),
                    "missing_days": int(len(seg_missing)),
                    "total_days": int(len(seg_days)),
                }
            )
    except Exception:
        missing_by_segment = []

    bounds = coverage_bounds or {}
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "exchange": str(exchange),
        "variety": str(variety),
        "derived_symbol": str(derived_symbol),
        "schedule_hash": str(schedule_hash),
        "update_mode": bool(update_mode),
        "full_rebuild": bool(full_rebuild),
        "expected_days": int(len(expected_days)),
        "covered_days": int(len(covered_days)),
        "missing_days": int(len(missing_days)),
        "covered_first_day": bounds.get("first_day"),
        "covered_last_day": bounds.get("last_day"),
        "covered_last_ts": bounds.get("last_ts"),
        "missing_by_segment": missing_by_segment,
    }

    try:
        out_dir = _main_l5_update_report_dir(Path(runs_dir))
        out_dir.mkdir(parents=True, exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        out_path = out_dir / f"main_l5_update_{derived_symbol.replace('@', '_').replace('.', '_')}_{run_id}.json"
        out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        return out_path
    except Exception:
        return None


def _ensure_main_l5_table(table: str = "ghtrader_ticks_main_l5_v2") -> None:
    from ghtrader.config import (
        get_questdb_host,
        get_questdb_ilp_port,
        get_questdb_pg_dbname,
        get_questdb_pg_password,
        get_questdb_pg_port,
        get_questdb_pg_user,
    )
    from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend

    cfg = ServingDBConfig(
        backend="questdb",
        host=str(get_questdb_host()),
        questdb_ilp_port=int(get_questdb_ilp_port()),
        questdb_pg_port=int(get_questdb_pg_port()),
        questdb_pg_user=str(get_questdb_pg_user()),
        questdb_pg_password=str(get_questdb_pg_password()),
        questdb_pg_dbname=str(get_questdb_pg_dbname()),
    )
    backend = make_serving_backend(cfg)
    backend.ensure_table(table=str(table), include_segment_metadata=True)


def _clear_main_l5_symbol(*, cfg: Any, symbol: str) -> int:
    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES
    from ghtrader.questdb.client import connect_pg

    sym = str(symbol).strip()
    if not sym:
        return 0
    _ensure_main_l5_table()
    base_cols = ["symbol", "ts", "datetime_ns", "trading_day", "row_hash"]
    tick_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]
    prov_cols = ["dataset_version", "ticks_kind", "underlying_contract", "segment_id", "schedule_hash"]
    cols = base_cols + tick_cols + prov_cols
    cols_sql = ", ".join(cols)

    where = "symbol = %s"
    count_sql = f"SELECT count() FROM ghtrader_ticks_main_l5_v2 WHERE {where}"
    delete_sql = f"DELETE FROM ghtrader_ticks_main_l5_v2 WHERE {where}"
    tmp_table = "ghtrader_ticks_main_l5_v2_clean"
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            try:
                cur.execute(count_sql, [sym])
                (count,) = cur.fetchone() or (0,)
            except Exception:
                count = 0
            try:
                cur.execute(delete_sql, [sym])
                return int(count)
            except Exception as e:
                log.warning("main_l5.delete_failed", symbol=sym, error=str(e))
                cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
                _ensure_main_l5_table(table=tmp_table)
                insert_sql = (
                    f"INSERT INTO {tmp_table} ({cols_sql}) "
                    f"SELECT {cols_sql} FROM ghtrader_ticks_main_l5_v2 WHERE symbol <> %s"
                )
                cur.execute(insert_sql, [sym])
                cur.execute("DROP TABLE ghtrader_ticks_main_l5_v2")
                cur.execute(f"RENAME TABLE {tmp_table} TO ghtrader_ticks_main_l5_v2")
                return int(count)


def _purge_main_l5_l1(*, cfg: Any, symbol: str) -> int:
    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES
    from ghtrader.questdb.client import connect_pg
    from ghtrader.util.l5_detection import l5_sql_condition

    sym = str(symbol).strip()
    if not sym:
        return 0
    _ensure_main_l5_table()
    base_cols = ["symbol", "ts", "datetime_ns", "trading_day", "row_hash"]
    tick_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]
    prov_cols = ["dataset_version", "ticks_kind", "underlying_contract", "segment_id", "schedule_hash"]
    cols = base_cols + tick_cols + prov_cols
    cols_sql = ", ".join(cols)

    l5_cond = l5_sql_condition()
    where = f"symbol = %s AND NOT ({l5_cond})"
    count_sql = f"SELECT count() FROM ghtrader_ticks_main_l5_v2 WHERE {where}"
    delete_sql = f"DELETE FROM ghtrader_ticks_main_l5_v2 WHERE {where}"
    tmp_table = "ghtrader_ticks_main_l5_v2_clean"
    with connect_pg(cfg, connect_timeout_s=2) as conn:
        try:
            conn.autocommit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        with conn.cursor() as cur:
            try:
                cur.execute(count_sql, [sym])
                (count,) = cur.fetchone() or (0,)
            except Exception:
                count = 0
            try:
                cur.execute(delete_sql, [sym])
                return int(count)
            except Exception as e:
                log.warning("main_l5.purge_failed", symbol=sym, error=str(e))
                cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
                _ensure_main_l5_table(table=tmp_table)
                insert_sql = (
                    f"INSERT INTO {tmp_table} ({cols_sql}) "
                    f"SELECT {cols_sql} FROM ghtrader_ticks_main_l5_v2 "
                    f"WHERE NOT (symbol = %s AND NOT ({l5_cond}))"
                )
                cur.execute(insert_sql, [sym])
                cur.execute("DROP TABLE ghtrader_ticks_main_l5_v2")
                cur.execute(f"RENAME TABLE {tmp_table} TO ghtrader_ticks_main_l5_v2")
                return int(count)




def build_main_l5(
    *,
    var: str,
    derived_symbol: str,
    exchange: str = "SHFE",
    data_dir: str = "data",
    update_mode: bool = False,
) -> MainL5BuildResult:
    """
    Build `main_l5` directly from schedule + TqSdk per-day L5 downloads.
    """
    from ghtrader.config import get_runs_dir
    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.main_schedule import fetch_schedule, trim_main_schedule_before
    from ghtrader.questdb.queries import (
        list_schedule_hashes_for_symbol,
        list_trading_days_for_symbol,
        query_symbol_day_bounds,
    )
    from ghtrader.tq.ingest import download_main_l5_for_days
    from ghtrader.tq.runtime import trading_day_from_ts_ns

    ex = str(exchange).upper().strip()
    var_l = str(var).lower().strip()
    ds = str(derived_symbol).strip() or f"KQ.m@{ex}.{var_l}"

    cfg = make_questdb_query_config_from_env()
    schedule = fetch_schedule(cfg=cfg, exchange=ex, variety=var_l, start_day=None, end_day=None, connect_timeout_s=2)
    if schedule.empty:
        raise FileNotFoundError(f"No schedule rows found in QuestDB for {ex}.{var_l}. Run `ghtrader main-schedule` first.")

    schedule = schedule.dropna(subset=["trading_day", "main_contract"]).sort_values("trading_day").reset_index(drop=True)
    if "segment_id" not in schedule.columns:
        seg_ids: list[int] = []
        seg = 0
        prev: str | None = None
        for mc in schedule["main_contract"].astype(str).tolist():
            if prev is None:
                seg = 0
            elif mc != prev:
                seg += 1
            seg_ids.append(int(seg))
            prev = mc
        schedule["segment_id"] = seg_ids

    schedule_hash = _schedule_hash_from_rows(schedule)
    if not schedule_hash:
        raise RuntimeError(
            f"Schedule rows for {ex}.{var_l} are missing schedule_hash. "
            "Re-run `ghtrader main-schedule` to populate `ghtrader_main_schedule_v2`."
        )

    schedule_days = [d for d in schedule["trading_day"].tolist() if isinstance(d, date)]
    schedule_start = min(schedule_days) if schedule_days else None
    schedule_end = max(schedule_days) if schedule_days else None

    full_rebuild = not bool(update_mode)
    existing_days: set[date] = set()
    existing_hashes: set[str] = set()
    if update_mode and schedule_start and schedule_end:
        try:
            existing_hashes = list_schedule_hashes_for_symbol(
                cfg=cfg,
                table="ghtrader_ticks_main_l5_v2",
                symbol=ds,
                dataset_version="v2",
                ticks_kind="main_l5",
            )
        except Exception:
            existing_hashes = set()
        if existing_hashes and existing_hashes != {str(schedule_hash)}:
            full_rebuild = True
            log.warning(
                "main_l5.schedule_hash_mismatch",
                derived_symbol=ds,
                schedule_hash=str(schedule_hash),
                existing_hashes=sorted(existing_hashes),
            )
        else:
            try:
                existing_days = set(
                    list_trading_days_for_symbol(
                        cfg=cfg,
                        table="ghtrader_ticks_main_l5_v2",
                        symbol=ds,
                        start_day=schedule_start,
                        end_day=schedule_end,
                        dataset_version="v2",
                        ticks_kind="main_l5",
                        l5_only=True,
                    )
                )
            except Exception:
                existing_days = set()

    if update_mode and not full_rebuild and not existing_hashes and existing_days:
        full_rebuild = True
        existing_days = set()
        log.warning(
            "main_l5.schedule_hash_missing",
            derived_symbol=ds,
            schedule_hash=str(schedule_hash),
        )

    if full_rebuild:
        cleared = _clear_main_l5_symbol(cfg=cfg, symbol=ds)
        if int(cleared or 0) > 0:
            log.info("main_l5.cleared", derived_symbol=ds, rows_deleted=int(cleared))

    try:
        segments_total = int(schedule["segment_id"].nunique())
    except Exception:
        segments_total = 0
    log.info(
        "main_l5.build_start",
        exchange=ex,
        var=var_l,
        derived_symbol=ds,
        schedule_hash=str(schedule_hash),
        segments_total=int(segments_total),
        days_total=int(len(schedule)),
        update_mode=bool(update_mode),
        full_rebuild=bool(full_rebuild),
        existing_days=int(len(existing_days)),
    )

    segments_out: list[dict[str, Any]] = []
    rows_total = 0
    days_total = 0

    seg_done = 0
    for seg_id, g in schedule.groupby("segment_id", sort=True):
        contract = str(g["main_contract"].iloc[0]).strip()
        days = [d for d in g["trading_day"].tolist() if isinstance(d, date)]
        if not full_rebuild:
            days = [d for d in days if d not in existing_days]
        if not contract or not days:
            continue

        seg_done += 1
        log.info(
            "main_l5.segment_start",
            segment_id=int(seg_id),
            segments_done=int(seg_done),
            segments_total=int(segments_total),
            underlying_contract=contract,
            days=int(len(days)),
        )
        res = download_main_l5_for_days(
            underlying_symbol=contract,
            derived_symbol=ds,
            trading_days=sorted(set(days)),
            segment_id=int(seg_id),
            schedule_hash=str(schedule_hash),
            data_dir=Path(str(data_dir)),
            dataset_version="v2",
        )
        log.info(
            "main_l5.segment_done",
            segment_id=int(seg_id),
            underlying_contract=contract,
            days=int(len(days)),
            rows_total=int(res.get("rows_total") or 0),
        )
        rows_total += int(res.get("rows_total") or 0)
        days_total += int(len(days))
        segments_out.append(
            {
                "segment_id": int(seg_id),
                "underlying_contract": contract,
                "days": int(len(days)),
                "rows_total": int(res.get("rows_total") or 0),
            }
        )

    purged = _purge_main_l5_l1(cfg=cfg, symbol=ds)
    if int(purged or 0) > 0:
        log.info("main_l5.l1_purged", derived_symbol=ds, rows_deleted=int(purged))

    coverage_bounds: dict[str, Any] = {}
    try:
        coverage = query_symbol_day_bounds(
            cfg=cfg,
            table="ghtrader_ticks_main_l5_v2",
            symbols=[ds],
            dataset_version="v2",
            ticks_kind="main_l5",
            l5_only=True,
        )
        coverage_bounds = dict(coverage.get(ds) or {})
    except Exception:
        coverage_bounds = {}

    aligned_start: date | None = None
    try:
        first_ns = coverage_bounds.get("first_ns")
        if first_ns is not None:
            aligned_start = trading_day_from_ts_ns(int(first_ns), data_dir=Path(str(data_dir)))
    except Exception:
        aligned_start = None

    if aligned_start and schedule_start and aligned_start > schedule_start:
        try:
            trimmed = trim_main_schedule_before(
                cfg=cfg,
                exchange=ex,
                variety=var_l,
                start_day=aligned_start,
                table="ghtrader_main_schedule_v2",
                connect_timeout_s=2,
            )
            log.info(
                "main_schedule.aligned",
                exchange=ex,
                var=var_l,
                aligned_start=aligned_start.isoformat(),
                rows_deleted=int(trimmed or 0),
            )
            schedule = schedule[schedule["trading_day"] >= aligned_start].reset_index(drop=True)
            schedule_days = [d for d in schedule["trading_day"].tolist() if isinstance(d, date)]
            schedule_start = min(schedule_days) if schedule_days else schedule_start
            schedule_end = max(schedule_days) if schedule_days else schedule_end
        except Exception as e:
            log.warning(
                "main_schedule.align_failed",
                exchange=ex,
                var=var_l,
                aligned_start=aligned_start.isoformat(),
                error=str(e),
            )

    covered_days: set[date] = set()
    if schedule_start and schedule_end:
        try:
            covered_days = set(
                list_trading_days_for_symbol(
                    cfg=cfg,
                    table="ghtrader_ticks_main_l5_v2",
                    symbol=ds,
                    start_day=schedule_start,
                    end_day=schedule_end,
                    dataset_version="v2",
                    ticks_kind="main_l5",
                    l5_only=True,
                )
            )
        except Exception:
            covered_days = set()

    report_path = _write_main_l5_update_report(
        runs_dir=get_runs_dir(),
        derived_symbol=ds,
        schedule_hash=str(schedule_hash),
        exchange=ex,
        variety=var_l,
        update_mode=bool(update_mode),
        full_rebuild=bool(full_rebuild),
        schedule=schedule,
        covered_days=covered_days,
        coverage_bounds=coverage_bounds,
    )
    if coverage_bounds:
        log.info(
            "main_l5.coverage",
            derived_symbol=ds,
            covered_first_day=coverage_bounds.get("first_day"),
            covered_last_day=coverage_bounds.get("last_day"),
            covered_days=int(len(covered_days)),
            covered_last_ts=coverage_bounds.get("last_ts"),
        )

    log.info(
        "main_l5.build_done",
        derived_symbol=ds,
        schedule_hash=str(schedule_hash),
        rows_total=int(rows_total),
        days_total=int(days_total),
        segments=int(len(segments_out)),
        report_path=(str(report_path) if report_path else ""),
    )

    return MainL5BuildResult(
        derived_symbol=ds,
        schedule_hash=str(schedule_hash),
        rows_total=int(rows_total),
        days_total=int(days_total),
        segments=segments_out,
    )
