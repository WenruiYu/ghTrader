"""
Main-contract roll schedule builder (TqSdk main mapping only), QuestDB-backed.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import structlog

from ghtrader.config import get_data_dir, get_runs_dir
from ghtrader.data.trading_calendar import get_trading_days
from ghtrader.questdb.main_schedule import (
    MAIN_SCHEDULE_TABLE_V2,
    MainScheduleRow,
    clear_main_schedule_rows,
    fetch_schedule,
    fetch_main_schedule_state,
    ensure_main_schedule_table,
    upsert_main_schedule_rows,
)

log = structlog.get_logger()


@dataclass(frozen=True)
class MainScheduleResult:
    schedule: pd.DataFrame
    schedule_hash: str
    exchange: str
    variety: str
    questdb_table: str


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


def _stable_hash_df(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _schedule_report_dir(runs_dir: Path) -> Path:
    return runs_dir / "control" / "reports" / "main_schedule"


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _assert_main_schedule_health(
    *,
    cfg: Any,
    exchange: str,
    variety: str,
    expected_schedule: pd.DataFrame,
    expected_hash: str,
    table: str = MAIN_SCHEDULE_TABLE_V2,
) -> None:
    persisted = fetch_schedule(
        cfg=cfg,
        exchange=exchange,
        variety=variety,
        start_day=None,
        end_day=None,
        table=table,
        connect_timeout_s=2,
    )
    persisted = persisted.dropna(subset=["trading_day", "main_contract"]).sort_values("trading_day").reset_index(drop=True)
    expected = expected_schedule[["date", "main_contract", "segment_id"]].dropna(subset=["date", "main_contract"]).sort_values("date").reset_index(drop=True)

    expected_rows = int(len(expected))
    actual_rows = int(len(persisted))
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"main_schedule health check failed: rows mismatch (expected={expected_rows}, actual={actual_rows})"
        )

    expected_days = [d for d in expected["date"].tolist() if isinstance(d, date)]
    actual_days = [d for d in persisted["trading_day"].tolist() if isinstance(d, date)]
    if expected_days != actual_days:
        raise RuntimeError("main_schedule health check failed: trading_day coverage mismatch")

    expected_contracts = expected["main_contract"].astype(str).tolist()
    actual_contracts = persisted["main_contract"].astype(str).tolist()
    if expected_contracts != actual_contracts:
        raise RuntimeError("main_schedule health check failed: main_contract sequence mismatch")

    expected_segments = pd.to_numeric(expected["segment_id"], errors="coerce").fillna(-1).astype("int64").tolist()
    actual_segments = pd.to_numeric(persisted["segment_id"], errors="coerce").fillna(-1).astype("int64").tolist()
    if expected_segments != actual_segments:
        raise RuntimeError("main_schedule health check failed: segment_id sequence mismatch")

    hashes = {str(h).strip() for h in persisted.get("schedule_hash", pd.Series(dtype=str)).dropna().astype(str).tolist() if str(h).strip()}
    if hashes != {str(expected_hash)}:
        raise RuntimeError(
            f"main_schedule health check failed: schedule_hash mismatch (expected={expected_hash}, actual={sorted(hashes)})"
        )


def normalize_contract_symbol(raw_symbol: str, *, exchange: str = "SHFE") -> str:
    """
    Normalize contract symbol to EXCHANGE.varYYMM.
    """
    s = str(raw_symbol or "").strip()
    if not s:
        return ""
    if "." in s:
        head, tail = s.split(".", 1)
        if head.upper() in {"SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX"}:
            contract = re.sub(r"[^A-Za-z0-9]", "", tail).lower()
            return f"{head.upper()}.{contract}"
    ex = str(exchange).upper().strip()
    s2 = re.sub(r"[^A-Za-z0-9]", "", s).lower()
    return f"{ex}.{s2}"


def build_daily_schedule_from_events(
    *,
    events: list[dict[str, Any]],
    trading_days: list[date],
    exchange: str = "SHFE",
) -> pd.DataFrame:
    """
    Expand main-contract change events into a daily schedule.
    """
    if not events:
        raise ValueError("events is empty")
    if not trading_days:
        raise ValueError("trading_days is empty")

    ev_rows: list[tuple[date, str]] = []
    for ev in events:
        td = ev.get("trading_day")
        sym = ev.get("underlying_symbol")
        if isinstance(td, date) and str(sym or "").strip():
            ev_rows.append((td, normalize_contract_symbol(str(sym), exchange=exchange)))
    if not ev_rows:
        raise ValueError("events contain no usable rows")

    ev_map: dict[date, str] = {}
    for td, sym in sorted(ev_rows, key=lambda r: r[0]):
        ev_map[td] = sym

    rows: list[dict[str, Any]] = []
    last_sym: str | None = None
    for day in sorted(set(trading_days)):
        if day in ev_map:
            last_sym = ev_map[day]
        if last_sym:
            rows.append({"date": day, "main_contract": str(last_sym)})

    schedule = pd.DataFrame(rows)
    if schedule.empty:
        raise ValueError("Computed empty schedule from events")

    schedule = schedule.sort_values("date").reset_index(drop=True)
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
    return schedule


def build_tqsdk_main_schedule(
    *,
    var: str,
    data_dir: Path | None = None,
    runs_dir: Path | None = None,
    dataset_version: str = "v2",
    exchange: str = "SHFE",
    enforce_health: bool = False,
) -> MainScheduleResult:
    """
    Build and persist a main schedule using TqSdk main mapping (KQ.m@...).
    """
    if data_dir is None:
        data_dir = get_data_dir()
    if runs_dir is None:
        runs_dir = get_runs_dir()

    ex = str(exchange).upper().strip()
    var_l = str(var).lower().strip()

    from ghtrader.config import get_l5_start_date_with_source
    from ghtrader.data.trading_calendar import latest_trading_day
    from ghtrader.questdb.client import make_questdb_query_config_from_env

    resolved_start, resolved_start_key = get_l5_start_date_with_source(variety=var_l)
    resolved_end = latest_trading_day(data_dir=Path(data_dir), refresh=False, allow_download=True)
    if resolved_end < resolved_start:
        raise ValueError(f"latest trading day is before {resolved_start_key}")
    log.info(
        "main_schedule.build_start",
        exchange=ex,
        var=var_l,
        start_key=resolved_start_key,
        start=resolved_start.isoformat(),
        end=resolved_end.isoformat(),
    )
    progress = _job_progress_from_env(
        runs_dir=Path(runs_dir),
        total_phases=3,
        message=f"Initializing main_schedule build for {ex}.{var_l}",
    )
    _progress_update(
        progress,
        phase="prepare",
        phase_idx=0,
        total_phases=3,
        step="load_state",
        step_idx=0,
        total_steps=2,
        message=f"Loading existing schedule state for {ex}.{var_l}",
    )

    cfg = make_questdb_query_config_from_env()
    ensure_main_schedule_table(cfg=cfg, table=MAIN_SCHEDULE_TABLE_V2, connect_timeout_s=2)
    state = fetch_main_schedule_state(
        cfg=cfg,
        exchange=ex,
        variety=var_l,
        table=MAIN_SCHEDULE_TABLE_V2,
        connect_timeout_s=2,
    )
    state_hashes = {str(h) for h in set(state.get("schedule_hashes") or set()) if str(h).strip()}

    trading_days = get_trading_days(market=ex, start=resolved_start, end=resolved_end, data_dir=Path(data_dir), refresh=False)
    _progress_update(
        progress,
        phase="prepare",
        phase_idx=0,
        total_phases=3,
        step="plan_window",
        step_idx=1,
        total_steps=2,
        message=f"Trading days in window: {len(trading_days)}",
    )
    if (
        _env_bool("GHTRADER_MAIN_SCHEDULE_FAST_NOOP", True)
        and trading_days
        and state.get("first_day") == resolved_start
        and state.get("last_day") == resolved_end
        and int(state.get("n_days") or 0) == int(len(trading_days))
        and len(state_hashes) == 1
    ):
        try:
            existing = fetch_schedule(
                cfg=cfg,
                exchange=ex,
                variety=var_l,
                start_day=resolved_start,
                end_day=resolved_end,
                table=MAIN_SCHEDULE_TABLE_V2,
                connect_timeout_s=2,
            )
            existing = existing.dropna(subset=["trading_day", "main_contract"]).sort_values("trading_day").reset_index(drop=True)
            expected_days = sorted(set(trading_days))
            existing_days = [d for d in existing["trading_day"].tolist() if isinstance(d, date)]
            if existing_days == expected_days and int(len(existing)) == int(len(expected_days)):
                schedule_hash = next(iter(state_hashes))
                schedule = pd.DataFrame(
                    {
                        "date": existing["trading_day"],
                        "main_contract": existing["main_contract"].astype(str),
                        "segment_id": pd.to_numeric(existing["segment_id"], errors="coerce").fillna(0).astype("int64"),
                    }
                )
                log.info(
                    "main_schedule.fast_noop",
                    exchange=ex,
                    var=var_l,
                    start=resolved_start.isoformat(),
                    end=resolved_end.isoformat(),
                    rows=int(len(schedule)),
                    schedule_hash=str(schedule_hash),
                )
                if bool(enforce_health):
                    _assert_main_schedule_health(
                        cfg=cfg,
                        exchange=ex,
                        variety=var_l,
                        expected_schedule=schedule,
                        expected_hash=str(schedule_hash),
                        table=MAIN_SCHEDULE_TABLE_V2,
                    )
                _progress_finish(
                    progress,
                    message=(
                        f"Fast no-op complete: {ex}.{var_l}, rows={int(len(schedule))}, "
                        f"schedule_hash={str(schedule_hash)}"
                    ),
                )
                return MainScheduleResult(
                    schedule=schedule,
                    schedule_hash=str(schedule_hash),
                    exchange=ex,
                    variety=var_l,
                    questdb_table=MAIN_SCHEDULE_TABLE_V2,
                )
        except Exception as e:
            log.warning(
                "main_schedule.fast_noop_probe_failed",
                exchange=ex,
                var=var_l,
                error=str(e),
            )

    _progress_update(
        progress,
        phase="extract",
        phase_idx=1,
        total_phases=3,
        step="tqsdk_events",
        step_idx=0,
        total_steps=2,
        message=f"Extracting TqSdk main-contract events for {ex}.{var_l}",
    )
    from ghtrader.tq.main_schedule import extract_main_schedule_events

    events = extract_main_schedule_events(
        exchange=ex,
        variety=var_l,
        start=resolved_start,
        end=resolved_end,
        data_dir=Path(data_dir),
    )
    if not events:
        raise RuntimeError(
            f"No underlying_symbol events for KQ.m@{ex}.{var_l} "
            f"{resolved_start.isoformat()}->{resolved_end.isoformat()}"
        )

    log.info(
        "main_schedule.events_loaded",
        exchange=ex,
        var=var_l,
        events=int(len(events)),
        trading_days=int(len(trading_days)),
    )
    _progress_update(
        progress,
        phase="extract",
        phase_idx=1,
        total_phases=3,
        step="build_daily_schedule",
        step_idx=1,
        total_steps=2,
        message=f"Building daily schedule from {len(events)} events",
    )
    schedule = build_daily_schedule_from_events(events=events, trading_days=trading_days, exchange=ex)
    try:
        segments_total = int(schedule["segment_id"].nunique())
    except Exception:
        segments_total = 0
    log.info(
        "main_schedule.schedule_built",
        exchange=ex,
        var=var_l,
        rows=int(len(schedule)),
        segments=int(segments_total),
    )

    schedule_hash = _stable_hash_df(schedule[["date", "main_contract", "segment_id"]])
    if (
        state.get("first_day") == resolved_start
        and state.get("last_day") == resolved_end
        and int(state.get("n_days") or 0) == int(len(schedule))
        and state_hashes == {str(schedule_hash)}
    ):
        log.info(
            "main_schedule.up_to_date",
            exchange=ex,
            var=var_l,
            start=resolved_start.isoformat(),
            end=resolved_end.isoformat(),
            rows=int(len(schedule)),
            schedule_hash=str(schedule_hash),
        )
        if bool(enforce_health):
            _assert_main_schedule_health(
                cfg=cfg,
                exchange=ex,
                variety=var_l,
                expected_schedule=schedule,
                expected_hash=str(schedule_hash),
                table=MAIN_SCHEDULE_TABLE_V2,
            )
        _progress_finish(
            progress,
            message=(
                f"Up-to-date: {ex}.{var_l}, rows={int(len(schedule))}, "
                f"schedule_hash={str(schedule_hash)}"
            ),
        )
        return MainScheduleResult(
            schedule=schedule,
            schedule_hash=schedule_hash,
            exchange=ex,
            variety=var_l,
            questdb_table=MAIN_SCHEDULE_TABLE_V2,
        )
    _progress_update(
        progress,
        phase="persist",
        phase_idx=2,
        total_phases=3,
        step="clear_existing",
        step_idx=0,
        total_steps=3,
        message=f"Clearing old rows for {ex}.{var_l}",
    )
    cleared = clear_main_schedule_rows(
        cfg=cfg,
        exchange=ex,
        variety=var_l,
        table=MAIN_SCHEDULE_TABLE_V2,
        connect_timeout_s=2,
    )
    if int(cleared or 0) > 0:
        log.info(
            "main_schedule.cleared",
            exchange=ex,
            var=var_l,
            rows_deleted=int(cleared),
        )
    _progress_update(
        progress,
        phase="persist",
        phase_idx=2,
        total_phases=3,
        step="upsert_rows",
        step_idx=1,
        total_steps=3,
        message=f"Writing {int(len(schedule))} schedule rows",
    )
    rows = [
        MainScheduleRow(
            exchange=ex,
            variety=var_l,
            trading_day=str(d.isoformat()),
            main_contract=str(mc),
            segment_id=int(seg),
            schedule_hash=str(schedule_hash),
        )
        for d, mc, seg in schedule[["date", "main_contract", "segment_id"]].itertuples(index=False, name=None)
        if d is not None and str(mc).strip()
    ]
    n_upsert = upsert_main_schedule_rows(
        cfg=cfg,
        exchange=ex,
        variety=var_l,
        schedule_hash=str(schedule_hash),
        rows=rows,
        table=MAIN_SCHEDULE_TABLE_V2,
        connect_timeout_s=2,
    )
    if bool(enforce_health):
        _assert_main_schedule_health(
            cfg=cfg,
            exchange=ex,
            variety=var_l,
            expected_schedule=schedule,
            expected_hash=str(schedule_hash),
            table=MAIN_SCHEDULE_TABLE_V2,
        )

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "exchange": ex,
        "variety": var_l,
        "start_date": resolved_start.isoformat(),
        "start_env_key": str(resolved_start_key),
        "end_date": resolved_end.isoformat(),
        "rows": int(len(schedule)),
        "schedule_hash": schedule_hash,
        "distinct_main_contracts": sorted([c for c in schedule["main_contract"].astype(str).unique().tolist() if c]),
        "source": "tqsdk",
        "events": int(len(events)),
        "questdb": {
            "schedule_table": MAIN_SCHEDULE_TABLE_V2,
            "schedule_rows_upserted": int(n_upsert),
            "dataset_version": str(dataset_version),
        },
    }
    report_path: Path | None = None
    try:
        out_dir = _schedule_report_dir(Path(runs_dir))
        out_dir.mkdir(parents=True, exist_ok=True)
        run_id = uuid4().hex[:12]
        report_path = out_dir / f"main_schedule_exchange={report['exchange']}_var={report['variety']}_{run_id}.json"
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    except Exception:
        report_path = None
    _progress_update(
        progress,
        phase="persist",
        phase_idx=2,
        total_phases=3,
        step="write_report",
        step_idx=2,
        total_steps=3,
        message="Writing main_schedule report",
    )

    log.info(
        "main_schedule.tqsdk_built",
        exchange=report["exchange"],
        var=report["variety"],
        start=resolved_start.isoformat(),
        end=resolved_end.isoformat(),
        rows=int(len(schedule)),
        schedule_hash=schedule_hash,
        schedule_table=MAIN_SCHEDULE_TABLE_V2,
        schedule_rows_upserted=int(n_upsert),
        report_path=(str(report_path) if report_path else ""),
    )
    _progress_finish(
        progress,
        message=(
            f"Complete: {ex}.{var_l}, rows={int(len(schedule))}, "
            f"schedule_hash={str(schedule_hash)}"
        ),
    )

    return MainScheduleResult(
        schedule=schedule,
        schedule_hash=schedule_hash,
        exchange=ex,
        variety=var_l,
        questdb_table=MAIN_SCHEDULE_TABLE_V2,
    )


def build_main_schedule(
    *,
    var: str,
    data_dir: Path | None = None,
    exchange: str = "SHFE",
    enforce_health: bool = False,
) -> MainScheduleResult:
    return build_tqsdk_main_schedule(
        var=var,
        data_dir=data_dir,
        exchange=exchange,
        dataset_version="v2",
        enforce_health=bool(enforce_health),
    )


