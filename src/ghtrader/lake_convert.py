from __future__ import annotations

import hashlib
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import structlog

from ghtrader.lake import LakeVersion, TICK_ARROW_SCHEMA, TICK_COLUMN_NAMES, ticks_root_dir, ticks_symbol_dir, write_ticks_partition
from ghtrader.trading_calendar import get_trading_calendar

log = structlog.get_logger()


@dataclass(frozen=True)
class LakeConvertStats:
    symbols: int
    source_files: int
    output_groups: int
    written_files: int
    skipped_files: int
    rows_written: int


def _iter_v1_symbols(data_dir: Path) -> list[str]:
    root = ticks_root_dir(data_dir, "raw", lake_version="v1")
    if not root.exists():
        return []
    out: list[str] = []
    for d in sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith("symbol=")], key=lambda p: p.name):
        sym = d.name.split("=", 1)[1]
        if sym:
            out.append(sym)
    return out


def _iter_v1_date_dirs(data_dir: Path, symbol: str) -> list[tuple[date, Path]]:
    root = ticks_symbol_dir(data_dir, symbol, ticks_lake="raw", lake_version="v1")
    if not root.exists():
        return []
    out: list[tuple[date, Path]] = []
    for d in sorted([p for p in root.iterdir() if p.is_dir() and p.name.startswith("date=")], key=lambda p: p.name):
        try:
            dt = date.fromisoformat(d.name.split("=", 1)[1])
        except Exception:
            continue
        out.append((dt, d))
    return out


def _next_trading_day(d: date, cal: list[date]) -> date:
    if cal:
        j = bisect_right(cal, d)
        if j < len(cal):
            return cal[j]
    return d + timedelta(days=1)


def _compute_v2_dates(datetime_ns: pd.Series, *, cal: list[date]) -> pd.Series:
    """
    Compute lake_v2 trading-day partition date for each tick timestamp.

    Matches the logic used in tq_ingest download_historical_ticks():
    - start from calendar date of the tick
    - for hour >= 18, map to next trading day using the cached calendar when available
    """
    dt_series = pd.to_datetime(datetime_ns, unit="ns")
    cal_dates = dt_series.dt.date
    mask = dt_series.dt.hour >= 18
    if not mask.any():
        return cal_dates

    uniq = sorted({d for d in cal_dates[mask].tolist() if isinstance(d, date)})
    next_map = {d: _next_trading_day(d, cal) for d in uniq}
    out = cal_dates.copy()
    out.loc[mask] = cal_dates.loc[mask].map(next_map)  # type: ignore[assignment]
    return out


def _stable_part_id(*, rel_source: str, v2_date: date) -> str:
    """
    Deterministic part id for idempotent conversion.

    Use >=64-bit prefix to make collisions vanishingly unlikely.
    """
    payload = f"{rel_source}|{v2_date.isoformat()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def convert_lake_v1_to_v2(
    *,
    data_dir: Path,
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    dry_run: bool = False,
    copy_no_data: bool = False,
) -> LakeConvertStats:
    """
    Offline conversion from lake_v1 raw ticks to lake_v2 raw ticks.

    This does NOT re-download any data. It reads existing parquet partitions from:
      - v1: data/lake/ticks/symbol=.../date=.../*.parquet
    and writes v2 partitions under:
      - v2: data/lake_v2/ticks/symbol=.../date=.../part-<stable>.parquet

    The conversion is idempotent and resumable by using deterministic output file ids.
    """
    lv_src: LakeVersion = "v1"
    lv_dst: LakeVersion = "v2"

    syms = list(symbols) if symbols else _iter_v1_symbols(data_dir)
    syms = [s for s in syms if s]
    if not syms:
        log.warning("lake_convert.no_symbols", data_dir=str(data_dir))
        return LakeConvertStats(symbols=0, source_files=0, output_groups=0, written_files=0, skipped_files=0, rows_written=0)

    cal = get_trading_calendar(data_dir=data_dir, refresh=False)
    if not cal:
        log.warning("lake_convert.calendar_missing", msg="Trading calendar cache missing; using +1 day fallback for night session mapping.")

    src_files = 0
    out_groups = 0
    written = 0
    skipped = 0
    rows_written = 0

    for sym in syms:
        date_dirs = _iter_v1_date_dirs(data_dir, sym)
        if start is not None:
            date_dirs = [(d, p) for d, p in date_dirs if d >= start]
        if end is not None:
            date_dirs = [(d, p) for d, p in date_dirs if d <= end]

        if not date_dirs:
            continue

        log.info("lake_convert.symbol_start", symbol=sym, n_days=len(date_dirs), dry_run=bool(dry_run))
        sym_src_root = ticks_symbol_dir(data_dir, sym, ticks_lake="raw", lake_version=lv_src)

        for dt, ddir in date_dirs:
            parts = sorted(ddir.glob("*.parquet"))
            if not parts:
                continue

            for p in parts:
                src_files += 1

                try:
                    table = pq.read_table(p, schema=TICK_ARROW_SCHEMA)
                except Exception as e:
                    log.warning("lake_convert.read_failed", symbol=sym, path=str(p), error=str(e))
                    continue

                if table.num_rows == 0:
                    continue

                df = table.to_pandas()
                if "datetime" not in df.columns:
                    continue

                # Compute v2 trading-day date per row and split.
                v2_dates = _compute_v2_dates(df["datetime"], cal=cal)
                df["_v2_date"] = v2_dates

                rel_source = str(p.relative_to(sym_src_root))
                for v2_dt, g in df.groupby("_v2_date"):
                    if not isinstance(v2_dt, date):
                        try:
                            v2_dt = date.fromisoformat(str(v2_dt))
                        except Exception:
                            continue

                    out_groups += 1
                    part_id = _stable_part_id(rel_source=rel_source, v2_date=v2_dt)
                    out_path = (
                        ticks_symbol_dir(data_dir, sym, ticks_lake="raw", lake_version=lv_dst)
                        / f"date={v2_dt.isoformat()}"
                        / f"part-{part_id}.parquet"
                    )
                    if out_path.exists():
                        skipped += 1
                        continue

                    if dry_run:
                        written += 1
                        rows_written += int(len(g))
                        continue

                    # Drop temp columns and write.
                    g2 = g.drop(columns=["_v2_date"], errors="ignore")
                    # Ensure canonical columns (writer enforces types/order).
                    g2 = g2[[c for c in TICK_COLUMN_NAMES if c in g2.columns]]

                    try:
                        write_ticks_partition(
                            g2,
                            data_dir=data_dir,
                            symbol=sym,
                            dt=v2_dt,
                            part_id=part_id,
                            lake_version=lv_dst,
                        )
                        written += 1
                        rows_written += int(len(g2))
                    except FileExistsError:
                        skipped += 1
                    except Exception as e:
                        log.warning("lake_convert.write_failed", symbol=sym, v2_date=v2_dt.isoformat(), error=str(e))

        # Optional: copy no-data dates marker (best-effort; operator convenience only).
        if copy_no_data:
            try:
                src_no_data = ticks_symbol_dir(data_dir, sym, ticks_lake="raw", lake_version=lv_src) / "_no_data_dates.json"
                dst_no_data = ticks_symbol_dir(data_dir, sym, ticks_lake="raw", lake_version=lv_dst) / "_no_data_dates.json"
                if src_no_data.exists() and not dst_no_data.exists():
                    dst_no_data.parent.mkdir(parents=True, exist_ok=True)
                    dst_no_data.write_text(src_no_data.read_text())
            except Exception:
                pass

    log.info(
        "lake_convert.done",
        symbols=len(syms),
        source_files=src_files,
        output_groups=out_groups,
        written_files=written,
        skipped_files=skipped,
        rows_written=rows_written,
        dry_run=bool(dry_run),
    )
    return LakeConvertStats(
        symbols=len(syms),
        source_files=src_files,
        output_groups=out_groups,
        written_files=written,
        skipped_files=skipped,
        rows_written=rows_written,
    )

