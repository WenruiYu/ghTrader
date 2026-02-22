"""
Schedule-driven main_l5 builder (QuestDB-only, no raw ticks).
"""

from __future__ import annotations

import os
import re
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


def _job_progress_from_env(*, runs_dir: Path, total_phases: int, message: str) -> Any | None:
    job_id = str(os.environ.get("GHTRADER_JOB_ID", "") or "").strip()
    if not job_id:
        return None
    try:
        from ghtrader.control.progress import JobProgress

        progress = JobProgress(job_id=job_id, runs_dir=Path(runs_dir))
        progress.start(total_phases=max(1, int(total_phases)), message=str(message))
        return progress
    except Exception:
        return None


def _progress_update(progress: Any | None, **kwargs: Any) -> None:
    if progress is None:
        return
    try:
        progress.update(**kwargs)
    except Exception:
        pass


def _progress_finish(progress: Any | None, *, message: str) -> None:
    if progress is None:
        return
    try:
        progress.finish(message=str(message))
    except Exception:
        pass


def _schedule_hash_from_rows(schedule: pd.DataFrame) -> str:
    try:
        v = str(schedule["schedule_hash"].dropna().astype(str).iloc[0])
        return v
    except Exception:
        return ""


def _resolve_missing_days_tolerance(*, variety: str) -> tuple[int, str]:
    var_key = re.sub(r"[^A-Za-z0-9]+", "", str(variety or "").strip()).upper()
    keys = []
    if var_key:
        keys.append(f"GHTRADER_MAIN_L5_MISSING_DAYS_TOLERANCE_{var_key}")
    keys.append("GHTRADER_MAIN_L5_MISSING_DAYS_TOLERANCE")
    for key in keys:
        raw = str(os.environ.get(key, "") or "").strip()
        if not raw:
            continue
        try:
            return max(0, int(raw)), f"env:{key}"
        except Exception:
            continue
    return 0, "default"


def _classify_missing_days(
    *,
    missing_days: list[date],
    row_counts: dict[str, int] | None,
) -> tuple[list[date], list[date]]:
    rc = row_counts or {}
    provider_missing: list[date] = []
    engineering_missing: list[date] = []
    for d in missing_days:
        key = d.isoformat()
        if key in rc:
            try:
                if int(rc.get(key, 0)) <= 0:
                    provider_missing.append(d)
                    continue
            except Exception:
                pass
        engineering_missing.append(d)
    return provider_missing, engineering_missing


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
    row_counts: dict[str, int] | None = None,
    missing_days_tolerance: int = 0,
    missing_days_tolerance_source: str = "default",
) -> Path | None:
    try:
        expected_days = sorted({d for d in schedule["trading_day"].tolist() if isinstance(d, date)})
    except Exception:
        expected_days = []
    missing_days = [d for d in expected_days if d not in covered_days]
    provider_missing_days, engineering_missing_days = _classify_missing_days(
        missing_days=missing_days,
        row_counts=row_counts,
    )
    tolerated_provider_missing_days = max(0, min(int(missing_days_tolerance), int(len(provider_missing_days))))
    provider_missing_days_excess = max(0, int(len(provider_missing_days)) - int(tolerated_provider_missing_days))

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
        "provider_missing_days": int(len(provider_missing_days)),
        "engineering_missing_days": int(len(engineering_missing_days)),
        "provider_missing_days_sample": [d.isoformat() for d in provider_missing_days[:8]],
        "engineering_missing_days_sample": [d.isoformat() for d in engineering_missing_days[:8]],
        "missing_days_tolerance": int(max(0, int(missing_days_tolerance))),
        "missing_days_tolerance_source": str(missing_days_tolerance_source or "default"),
        "tolerated_provider_missing_days": int(tolerated_provider_missing_days),
        "provider_missing_days_excess": int(provider_missing_days_excess),
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
    from ghtrader.questdb.row_cleanup import replace_table_delete_where

    sym = str(symbol).strip()
    if not sym:
        return 0
    base_cols = ["symbol", "ts", "datetime_ns", "trading_day", "row_hash"]
    tick_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]
    prov_cols = ["dataset_version", "ticks_kind", "underlying_contract", "segment_id", "schedule_hash"]
    cols = base_cols + tick_cols + prov_cols
    where = "symbol = %s"
    return int(
        replace_table_delete_where(
            cfg=cfg,
            table="ghtrader_ticks_main_l5_v2",
            columns=cols,
            delete_where_sql=where,
            delete_params=[sym],
            ensure_table=_ensure_main_l5_table,
            connect_timeout_s=2,
        )
    )


def _purge_main_l5_l1(*, cfg: Any, symbol: str) -> int:
    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES
    from ghtrader.questdb.row_cleanup import replace_table_delete_where
    from ghtrader.util.l5_detection import l5_sql_condition

    sym = str(symbol).strip()
    if not sym:
        return 0
    base_cols = ["symbol", "ts", "datetime_ns", "trading_day", "row_hash"]
    tick_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]
    prov_cols = ["dataset_version", "ticks_kind", "underlying_contract", "segment_id", "schedule_hash"]
    cols = base_cols + tick_cols + prov_cols
    l5_cond = l5_sql_condition()
    where = f"symbol = %s AND NOT ({l5_cond})"
    return int(
        replace_table_delete_where(
            cfg=cfg,
            table="ghtrader_ticks_main_l5_v2",
            columns=cols,
            delete_where_sql=where,
            delete_params=[sym],
            ensure_table=_ensure_main_l5_table,
            connect_timeout_s=2,
        )
    )




def build_main_l5(
    *,
    var: str,
    derived_symbol: str,
    exchange: str = "SHFE",
    data_dir: str = "data",
    update_mode: bool = False,
    enforce_health: bool = False,
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

    runs_dir = get_runs_dir()
    ex = str(exchange).upper().strip()
    var_l = str(var).lower().strip()
    ds = str(derived_symbol).strip() or f"KQ.m@{ex}.{var_l}"
    progress = _job_progress_from_env(
        runs_dir=Path(runs_dir),
        total_phases=4,
        message=f"Initializing main_l5 build for {ds}",
    )
    _progress_update(
        progress,
        phase="prepare",
        phase_idx=0,
        total_phases=4,
        step="load_schedule",
        step_idx=0,
        total_steps=3,
        message=f"Loading schedule for {ex}.{var_l}",
    )

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
    _progress_update(
        progress,
        phase="prepare",
        phase_idx=0,
        total_phases=4,
        step="plan_segments",
        step_idx=1,
        total_steps=3,
        message=f"Planning segments for {ds}",
    )

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

    segment_jobs: list[tuple[int, str, list[date]]] = []
    for seg_id, g in schedule.groupby("segment_id", sort=True):
        contract = str(g["main_contract"].iloc[0]).strip()
        days = [d for d in g["trading_day"].tolist() if isinstance(d, date)]
        if not full_rebuild:
            days = [d for d in days if d not in existing_days]
        if not contract or not days:
            continue
        segment_jobs.append((int(seg_id), contract, sorted(set(days))))

    try:
        from ghtrader.util.worker_policy import resolve_worker_count

        requested_total_workers: int | None = None
        raw_total_workers = str(os.environ.get("GHTRADER_MAIN_L5_TOTAL_WORKERS", "") or "").strip()
        if raw_total_workers:
            try:
                requested_total_workers = int(raw_total_workers)
            except Exception:
                requested_total_workers = None
        total_worker_budget = int(resolve_worker_count(kind="download", requested=requested_total_workers))
        # Containerized hosts can report very large CPU counts (e.g. 512). Keep
        # a conservative default budget unless the operator overrides explicitly.
        if requested_total_workers is None:
            total_worker_budget = min(int(total_worker_budget), 8)

        requested_segment_workers: int | None = None
        raw_segment_workers = str(os.environ.get("GHTRADER_MAIN_L5_SEGMENT_WORKERS", "") or "").strip()
        if raw_segment_workers:
            try:
                requested_segment_workers = int(raw_segment_workers)
            except Exception:
                requested_segment_workers = None

        if requested_segment_workers is None:
            segment_workers = min(max(1, int(total_worker_budget)), max(1, len(segment_jobs)))
        else:
            segment_workers = min(
                max(1, int(requested_segment_workers)),
                max(1, int(total_worker_budget)),
                max(1, len(segment_jobs)),
            )
    except Exception:
        total_worker_budget = 1
        requested_total_workers = None
        requested_segment_workers = None
        segment_workers = 1

    segment_workers = max(1, min(int(segment_workers), max(1, len(segment_jobs))))
    total_worker_budget = max(1, int(total_worker_budget))
    per_segment_download_workers = max(1, int(total_worker_budget // max(1, segment_workers)))
    log.info(
        "main_l5.segment_worker_policy",
        requested_segment=(
            int(requested_segment_workers)
            if "requested_segment_workers" in locals() and requested_segment_workers is not None
            else None
        ),
        requested_total=(
            int(requested_total_workers)
            if "requested_total_workers" in locals() and requested_total_workers is not None
            else None
        ),
        total_budget=int(total_worker_budget),
        selected_segment_workers=int(segment_workers),
        per_segment_download_workers=int(per_segment_download_workers),
        segments=int(len(segment_jobs)),
    )
    _progress_update(
        progress,
        phase="prepare",
        phase_idx=0,
        total_phases=4,
        step="worker_policy",
        step_idx=2,
        total_steps=3,
        message=(
            f"Prepared {len(segment_jobs)} segments, "
            f"segment_workers={segment_workers}, per_segment_workers={per_segment_download_workers}"
        ),
    )

    def _run_segment(*, seg_id: int, contract: str, days: list[date], seg_idx: int, seg_total: int) -> dict[str, Any]:
        log.info(
            "main_l5.segment_start",
            segment_id=int(seg_id),
            segments_done=int(seg_idx),
            segments_total=int(seg_total),
            underlying_contract=contract,
            days=int(len(days)),
        )
        res = download_main_l5_for_days(
            underlying_symbol=contract,
            derived_symbol=ds,
            trading_days=days,
            segment_id=int(seg_id),
            schedule_hash=str(schedule_hash),
            data_dir=Path(str(data_dir)),
            exchange=ex,
            variety=var_l,
            dataset_version="v2",
            write_manifest_file=False,
            workers=int(per_segment_download_workers),
        )
        log.info(
            "main_l5.segment_done",
            segment_id=int(seg_id),
            underlying_contract=contract,
            days=int(len(days)),
            rows_total=int(res.get("rows_total") or 0),
        )
        out_row_counts = res.get("row_counts")
        row_counts = out_row_counts if isinstance(out_row_counts, dict) else {}
        return {
            "segment_id": int(seg_id),
            "underlying_contract": contract,
            "days": int(len(days)),
            "rows_total": int(res.get("rows_total") or 0),
            "row_counts": row_counts,
        }

    segment_results: list[dict[str, Any]] = []
    segments_total = max(1, int(len(segment_jobs)))
    segments_done = 0
    _progress_update(
        progress,
        phase="download",
        phase_idx=1,
        total_phases=4,
        step="segments",
        step_idx=0,
        total_steps=segments_total,
        total_items=segments_total,
        item_idx=0,
        message=f"Downloading segments (0/{len(segment_jobs)})",
    )
    if segment_jobs and segment_workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=segment_workers) as executor:
            futures = {
                executor.submit(
                    _run_segment,
                    seg_id=int(seg_id),
                    contract=str(contract),
                    days=list(days),
                    seg_idx=int(idx),
                    seg_total=int(len(segment_jobs)),
                ): int(seg_id)
                for idx, (seg_id, contract, days) in enumerate(segment_jobs, start=1)
            }
            for fut in as_completed(futures):
                seg_res = fut.result()
                segment_results.append(seg_res)
                segments_done += 1
                seg_id_done = int(seg_res.get("segment_id") or 0)
                seg_contract = str(seg_res.get("underlying_contract") or "")
                seg_rows = int(seg_res.get("rows_total") or 0)
                _progress_update(
                    progress,
                    phase="download",
                    phase_idx=1,
                    total_phases=4,
                    step="segments",
                    step_idx=min(segments_done, segments_total),
                    total_steps=segments_total,
                    total_items=segments_total,
                    item=f"segment={seg_id_done} contract={seg_contract}",
                    item_idx=min(segments_done, segments_total),
                    message=f"Downloaded {segments_done}/{len(segment_jobs)} segments, rows={seg_rows}",
                )
                log.info(
                    "main_l5.progress",
                    derived_symbol=ds,
                    segments_done=int(segments_done),
                    segments_total=int(len(segment_jobs)),
                    segment_id=int(seg_id_done),
                    rows_total=int(seg_rows),
                )
    else:
        for idx, (seg_id, contract, days) in enumerate(segment_jobs, start=1):
            seg_res = _run_segment(
                seg_id=int(seg_id),
                contract=str(contract),
                days=list(days),
                seg_idx=int(idx),
                seg_total=int(len(segment_jobs)),
            )
            segment_results.append(seg_res)
            segments_done += 1
            seg_id_done = int(seg_res.get("segment_id") or 0)
            seg_contract = str(seg_res.get("underlying_contract") or "")
            seg_rows = int(seg_res.get("rows_total") or 0)
            _progress_update(
                progress,
                phase="download",
                phase_idx=1,
                total_phases=4,
                step="segments",
                step_idx=min(segments_done, segments_total),
                total_steps=segments_total,
                total_items=segments_total,
                item=f"segment={seg_id_done} contract={seg_contract}",
                item_idx=min(segments_done, segments_total),
                message=f"Downloaded {segments_done}/{len(segment_jobs)} segments, rows={seg_rows}",
            )
            log.info(
                "main_l5.progress",
                derived_symbol=ds,
                segments_done=int(segments_done),
                segments_total=int(len(segment_jobs)),
                segment_id=int(seg_id_done),
                rows_total=int(seg_rows),
            )

    segment_results = sorted(segment_results, key=lambda x: int(x.get("segment_id") or 0))
    row_counts_agg: dict[str, int] = {}
    for seg in segment_results:
        rows_total += int(seg.get("rows_total") or 0)
        days_total += int(seg.get("days") or 0)
        segments_out.append(
            {
                "segment_id": int(seg.get("segment_id") or 0),
                "underlying_contract": str(seg.get("underlying_contract") or ""),
                "days": int(seg.get("days") or 0),
                "rows_total": int(seg.get("rows_total") or 0),
            }
        )
        seg_row_counts = seg.get("row_counts")
        if isinstance(seg_row_counts, dict):
            for day_iso, cnt in seg_row_counts.items():
                day_key = str(day_iso)
                try:
                    row_counts_agg[day_key] = int(row_counts_agg.get(day_key, 0)) + int(cnt or 0)
                except Exception:
                    row_counts_agg[day_key] = int(row_counts_agg.get(day_key, 0))

    # Keep one manifest per main_l5 build run so downstream validation reads a consistent day map.
    _progress_update(
        progress,
        phase="finalize",
        phase_idx=2,
        total_phases=4,
        step="manifest",
        step_idx=0,
        total_steps=2,
        message="Writing aggregated manifest",
    )
    if segment_jobs:
        try:
            from ghtrader.data.manifest import write_manifest

            all_days = sorted({d for _, _, days in segment_jobs for d in days})
            if all_days:
                manifest_path = write_manifest(
                    data_dir=Path(str(data_dir)),
                    symbols=[ds],
                    start_date=min(all_days),
                    end_date=max(all_days),
                    source="tq_main_l5",
                    row_counts=row_counts_agg,
                )
                log.info("main_l5.manifest_written", derived_symbol=ds, path=str(manifest_path), days=int(len(all_days)))
        except Exception as e:
            log.warning("main_l5.manifest_write_failed", derived_symbol=ds, error=str(e))

    _progress_update(
        progress,
        phase="finalize",
        phase_idx=2,
        total_phases=4,
        step="cleanup_l1",
        step_idx=1,
        total_steps=2,
        message="Purging any residual L1-only rows",
    )
    purged = _purge_main_l5_l1(cfg=cfg, symbol=ds)
    if int(purged or 0) > 0:
        log.info("main_l5.l1_purged", derived_symbol=ds, rows_deleted=int(purged))

    _progress_update(
        progress,
        phase="health",
        phase_idx=3,
        total_phases=4,
        step="coverage_check",
        step_idx=0,
        total_steps=1,
        message="Running post-build health checks",
    )
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

    expected_days = sorted({d for d in schedule["trading_day"].tolist() if isinstance(d, date)})
    missing_days = sorted([d for d in expected_days if d not in covered_days])
    missing_days_tolerance, missing_days_tolerance_source = _resolve_missing_days_tolerance(variety=var_l)
    provider_missing_days, engineering_missing_days = _classify_missing_days(
        missing_days=missing_days,
        row_counts=row_counts_agg,
    )
    tolerated_provider_missing_days = min(int(missing_days_tolerance), int(len(provider_missing_days)))
    provider_missing_days_excess = max(0, int(len(provider_missing_days)) - int(tolerated_provider_missing_days))

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
        row_counts=row_counts_agg,
        missing_days_tolerance=int(missing_days_tolerance),
        missing_days_tolerance_source=str(missing_days_tolerance_source),
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

    hashes_after: set[str] = set()
    health_errors: list[str] = []
    try:
        hashes_after = set(
            list_schedule_hashes_for_symbol(
                cfg=cfg,
                table="ghtrader_ticks_main_l5_v2",
                symbol=ds,
                dataset_version="v2",
                ticks_kind="main_l5",
            )
        )
    except Exception as e:
        health_errors.append(f"failed to query persisted schedule_hashes: {e}")
    if engineering_missing_days:
        health_errors.append(
            "engineering_missing_days="
            + str(len(engineering_missing_days))
            + " sample="
            + str([d.isoformat() for d in engineering_missing_days[:8]])
        )
    if provider_missing_days_excess > 0:
        health_errors.append(
            "provider_missing_days="
            + str(len(provider_missing_days))
            + " tolerance="
            + str(int(missing_days_tolerance))
            + " excess="
            + str(int(provider_missing_days_excess))
            + " sample="
            + str([d.isoformat() for d in provider_missing_days[:8]])
        )
    if hashes_after and hashes_after != {str(schedule_hash)}:
        health_errors.append(
            f"persisted_hashes={sorted(hashes_after)} expected={[str(schedule_hash)]}"
        )
    log.info(
        "main_l5.health",
        derived_symbol=ds,
        expected_days=int(len(expected_days)),
        covered_days=int(len(covered_days)),
        missing_days=int(len(missing_days)),
        provider_missing_days=int(len(provider_missing_days)),
        engineering_missing_days=int(len(engineering_missing_days)),
        missing_days_tolerance=int(missing_days_tolerance),
        missing_days_tolerance_source=str(missing_days_tolerance_source),
        provider_missing_days_excess=int(provider_missing_days_excess),
        persisted_hashes=sorted(hashes_after),
        ok=bool(len(health_errors) == 0),
    )
    if bool(enforce_health) and health_errors:
        raise RuntimeError("main_l5 health check failed: " + "; ".join(health_errors))

    log.info(
        "main_l5.build_done",
        derived_symbol=ds,
        schedule_hash=str(schedule_hash),
        rows_total=int(rows_total),
        days_total=int(days_total),
        segments=int(len(segments_out)),
        report_path=(str(report_path) if report_path else ""),
    )
    _progress_finish(
        progress,
        message=(
            f"Complete: symbol={ds}, rows={int(rows_total)}, "
            f"days={int(days_total)}, segments={int(len(segments_out))}"
        ),
    )

    return MainL5BuildResult(
        derived_symbol=ds,
        schedule_hash=str(schedule_hash),
        rows_total=int(rows_total),
        days_total=int(days_total),
        segments=segments_out,
    )
