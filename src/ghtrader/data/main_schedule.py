"""
Main-contract roll schedule builder (TqSdk main mapping only), QuestDB-backed.
"""

from __future__ import annotations

import hashlib
import json
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


def _stable_hash_df(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _schedule_report_dir(runs_dir: Path) -> Path:
    return runs_dir / "control" / "reports" / "main_schedule"


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
) -> MainScheduleResult:
    """
    Build and persist a main schedule using TqSdk main mapping (KQ.m@...).
    """
    if data_dir is None:
        data_dir = get_data_dir()
    if runs_dir is None:
        runs_dir = get_runs_dir()
    from ghtrader.config import get_l5_start_date
    from ghtrader.data.trading_calendar import latest_trading_day

    resolved_start = get_l5_start_date()
    resolved_end = latest_trading_day(data_dir=Path(data_dir), refresh=False, allow_download=True)
    if resolved_end < resolved_start:
        raise ValueError("latest trading day is before GHTRADER_L5_START_DATE")
    log.info(
        "main_schedule.build_start",
        exchange=str(exchange).upper().strip(),
        var=str(var).lower().strip(),
        start=resolved_start.isoformat(),
        end=resolved_end.isoformat(),
    )

    from ghtrader.tq.main_schedule import extract_main_schedule_events

    events = extract_main_schedule_events(
        exchange=str(exchange).upper().strip(),
        variety=str(var).lower().strip(),
        start=resolved_start,
        end=resolved_end,
        data_dir=Path(data_dir),
    )
    if not events:
        raise RuntimeError(f"No underlying_symbol events for KQ.m@{exchange}.{var} {start}->{end}")

    trading_days = get_trading_days(market=str(exchange).upper().strip(), start=resolved_start, end=resolved_end, data_dir=Path(data_dir), refresh=False)
    log.info(
        "main_schedule.events_loaded",
        exchange=str(exchange).upper().strip(),
        var=str(var).lower().strip(),
        events=int(len(events)),
        trading_days=int(len(trading_days)),
    )
    schedule = build_daily_schedule_from_events(events=events, trading_days=trading_days, exchange=str(exchange).upper().strip())
    try:
        segments_total = int(schedule["segment_id"].nunique())
    except Exception:
        segments_total = 0
    log.info(
        "main_schedule.schedule_built",
        exchange=str(exchange).upper().strip(),
        var=str(var).lower().strip(),
        rows=int(len(schedule)),
        segments=int(segments_total),
    )

    schedule_hash = _stable_hash_df(schedule[["date", "main_contract", "segment_id"]])

    # Persist schedule to QuestDB (canonical).
    from ghtrader.questdb.client import make_questdb_query_config_from_env

    cfg = make_questdb_query_config_from_env()
    ensure_main_schedule_table(cfg=cfg, table=MAIN_SCHEDULE_TABLE_V2, connect_timeout_s=2)
    state = fetch_main_schedule_state(
        cfg=cfg,
        exchange=str(exchange).upper().strip(),
        variety=str(var).lower().strip(),
        table=MAIN_SCHEDULE_TABLE_V2,
        connect_timeout_s=2,
    )
    if (
        state.get("first_day") == resolved_start
        and state.get("last_day") == resolved_end
        and int(state.get("n_days") or 0) == int(len(schedule))
        and set(state.get("schedule_hashes") or set()) == {str(schedule_hash)}
    ):
        log.info(
            "main_schedule.up_to_date",
            exchange=str(exchange).upper().strip(),
            var=str(var).lower().strip(),
            start=resolved_start.isoformat(),
            end=resolved_end.isoformat(),
            rows=int(len(schedule)),
            schedule_hash=str(schedule_hash),
        )
        return MainScheduleResult(
            schedule=schedule,
            schedule_hash=schedule_hash,
            exchange=str(exchange).upper().strip(),
            variety=str(var).lower().strip(),
            questdb_table=MAIN_SCHEDULE_TABLE_V2,
        )
    cleared = clear_main_schedule_rows(
        cfg=cfg,
        exchange=str(exchange).upper().strip(),
        variety=str(var).lower().strip(),
        table=MAIN_SCHEDULE_TABLE_V2,
        connect_timeout_s=2,
    )
    if int(cleared or 0) > 0:
        log.info(
            "main_schedule.cleared",
            exchange=str(exchange).upper().strip(),
            var=str(var).lower().strip(),
            rows_deleted=int(cleared),
        )
    rows = [
        MainScheduleRow(
            exchange=str(exchange).upper().strip(),
            variety=str(var).lower().strip(),
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
        exchange=str(exchange).upper().strip(),
        variety=str(var).lower().strip(),
        schedule_hash=str(schedule_hash),
        rows=rows,
        table=MAIN_SCHEDULE_TABLE_V2,
        connect_timeout_s=2,
    )

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "exchange": str(exchange).upper().strip(),
        "variety": str(var).lower().strip(),
        "start_date": resolved_start.isoformat(),
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

    return MainScheduleResult(
        schedule=schedule,
        schedule_hash=schedule_hash,
        exchange=str(exchange).upper().strip(),
        variety=str(var).lower().strip(),
        questdb_table=MAIN_SCHEDULE_TABLE_V2,
    )


def build_main_schedule(
    *,
    var: str,
    data_dir: Path | None = None,
    exchange: str = "SHFE",
) -> MainScheduleResult:
    return build_tqsdk_main_schedule(
        var=var,
        data_dir=data_dir,
        exchange=exchange,
        dataset_version="v2",
    )


