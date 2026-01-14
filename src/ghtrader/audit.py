from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ghtrader.integrity import (
    parquet_num_rows,
    read_sha256_sidecar,
    sha256_file,
    write_json_atomic,
)
from ghtrader.lake import LakeVersion, TICK_ARROW_SCHEMA, lake_root_dir

log = structlog.get_logger()


Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class Finding:
    severity: Severity
    code: str
    message: str
    path: str | None = None
    extra: dict[str, Any] | None = None


def _parse_exchange_var(symbol: str) -> tuple[str | None, str | None]:
    """
    Best-effort parse:
      "SHFE.cu2602" -> ("SHFE", "cu")
    """
    s = str(symbol).strip()
    if "." not in s:
        return None, None
    ex, tail = s.split(".", 1)
    ex = ex.upper().strip() or None
    var = ""
    for ch in tail:
        if ch.isalpha():
            var += ch
        else:
            break
    var = var.lower().strip() or None
    return ex, var


def _last_trading_day_leq(cal: list[date], today: date) -> date:
    if not cal:
        d = today
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d
    # calendar sorted
    lo = 0
    hi = len(cal)
    while lo < hi:
        mid = (lo + hi) // 2
        if cal[mid] <= today:
            lo = mid + 1
        else:
            hi = mid
    idx = lo - 1
    return cal[idx] if idx >= 0 else today


def _count_trading_days_between(*, cal: list[date], start: date, end: date) -> int:
    if end < start:
        return 0
    if cal:
        from bisect import bisect_left, bisect_right

        i0 = bisect_left(cal, start)
        i1 = bisect_right(cal, end)
        return int(max(0, i1 - i0))
    # weekday fallback
    cur = start
    n = 0
    while cur <= end:
        if cur.weekday() < 5:
            n += 1
        cur += timedelta(days=1)
    return int(n)


def _iter_local_tick_symbols(*, data_dir: Path, lake_version: LakeVersion) -> set[str]:
    """
    List symbols present on disk under: data/lake_v2/ticks/symbol=...
    """
    out: set[str] = set()
    root = lake_root_dir(data_dir, lake_version) / "ticks"
    try:
        if not root.exists():
            return out
        for p in root.iterdir():
            if not p.is_dir() or not p.name.startswith("symbol="):
                continue
            sym = p.name.split("=", 1)[-1].strip()
            if sym:
                out.add(sym)
    except Exception:
        return out
    return out


def audit_completeness(
    *,
    data_dir: Path,
    runs_dir: Path,
    lake_version: LakeVersion,
    symbols: list[str] | None = None,
    exchange: str | None = None,
    var: str | None = None,
    refresh_catalog: bool = False,
) -> list[Finding]:
    """
    Completeness audit (best-effort):
    - Uses TqSdk catalog metadata (open_date / expire_datetime) when available.
    - Computes expected trading days and compares local Parquet mirror vs expected.
    - When QuestDB is reachable, also checks coarse DB coverage (first/last day + tick_days).
    """
    from ghtrader.lake import list_available_dates
    from ghtrader.tq_ingest import _load_no_data_dates  # type: ignore
    from ghtrader.tqsdk_catalog import get_contract_catalog
    from ghtrader.trading_calendar import get_trading_calendar

    ex_filter = str(exchange).upper().strip() if exchange else None
    v_filter = str(var).lower().strip() if var else None

    cal = get_trading_calendar(data_dir=data_dir, refresh=False)
    today = datetime.now(timezone.utc).date()
    today_trading = _last_trading_day_leq(cal, today)

    # Determine symbols to check.
    only = {str(s).strip() for s in (symbols or []) if str(s).strip()} or None
    local_syms = _iter_local_tick_symbols(data_dir=data_dir, lake_version=lake_version)
    target_syms: set[str] = set()
    if only is not None:
        target_syms = set(only)
    else:
        # If exchange/var filters are set, pull from catalog + local.
        if ex_filter and v_filter:
            cat = get_contract_catalog(exchange=ex_filter, var=v_filter, runs_dir=runs_dir, ttl_s=10**9, refresh=bool(refresh_catalog))
            cat_syms = {str(r.get("symbol") or "").strip() for r in (cat.get("contracts") or [])}
            target_syms = {s for s in cat_syms | local_syms if s}
        else:
            target_syms = {s for s in local_syms if s}

    # Build catalog metadata mapping for target symbols (best-effort, cached by var).
    by_sym: dict[str, dict[str, Any]] = {}
    if ex_filter and v_filter:
        cat = get_contract_catalog(exchange=ex_filter, var=v_filter, runs_dir=runs_dir, ttl_s=10**9, refresh=bool(refresh_catalog))
        for r in cat.get("contracts") or []:
            sym = str(r.get("symbol") or "").strip()
            if sym:
                by_sym[sym] = dict(r)
    else:
        # Group by (exchange,var) inferred from symbol.
        groups: dict[tuple[str, str], set[str]] = {}
        for s in sorted(target_syms):
            ex, vv = _parse_exchange_var(s)
            if not ex or not vv:
                continue
            if ex_filter and ex != ex_filter:
                continue
            if v_filter and vv != v_filter:
                continue
            groups.setdefault((ex, vv), set()).add(s)
        for (ex, vv), _syms in groups.items():
            cat = get_contract_catalog(exchange=ex, var=vv, runs_dir=runs_dir, ttl_s=10**9, refresh=bool(refresh_catalog))
            for r in cat.get("contracts") or []:
                sym = str(r.get("symbol") or "").strip()
                if sym:
                    by_sym[sym] = dict(r)

    findings: list[Finding] = []

    # Optional: QuestDB coverage for coarse DB completeness.
    questdb_cov: dict[str, dict[str, Any]] | None = None
    questdb_err: str | None = None
    try:
        from ghtrader.config import (
            get_questdb_host,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb_queries import QuestDBQueryConfig, query_contract_coverage

        cfg = QuestDBQueryConfig(
            host=get_questdb_host(),
            pg_port=int(get_questdb_pg_port()),
            pg_user=str(get_questdb_pg_user()),
            pg_password=str(get_questdb_pg_password()),
            pg_dbname=str(get_questdb_pg_dbname()),
        )
        questdb_cov = query_contract_coverage(
            cfg=cfg,
            table="ghtrader_ticks_raw_v2",
            symbols=sorted(target_syms),
            lake_version="v2",
            ticks_lake="raw",
        )
    except Exception as e:
        questdb_cov = None
        questdb_err = str(e)

    if questdb_cov is None and questdb_err:
        findings.append(Finding("warning", "questdb_unreachable", "QuestDB coverage check skipped", extra={"error": questdb_err}))

    for sym in sorted(target_syms):
        meta = by_sym.get(sym) or {"symbol": sym, "expired": None, "expire_datetime": None, "open_date": None, "catalog_source": "unknown"}
        expired = meta.get("expired")
        expired_b = bool(expired) if expired is not None else None
        open_date_s = str(meta.get("open_date") or "").strip() or None
        exp_dt_s = str(meta.get("expire_datetime") or "").strip() or None

        # Local done days
        local_dates = {d.isoformat() for d in list_available_dates(data_dir, sym, lake_version=lake_version)}
        no_data_dates = {d.isoformat() for d in _load_no_data_dates(data_dir, sym, lake_version=lake_version)}

        local_min = min(local_dates) if local_dates else None
        local_max = max(local_dates) if local_dates else None

        expected_first: date | None = None
        if open_date_s:
            try:
                expected_first = date.fromisoformat(open_date_s[:10])
            except Exception:
                expected_first = None
        if expected_first is None and local_min:
            try:
                expected_first = date.fromisoformat(local_min)
            except Exception:
                expected_first = None

        expire_trading: date | None = None
        if exp_dt_s:
            try:
                expire_day = date.fromisoformat(exp_dt_s[:10])
                expire_trading = _last_trading_day_leq(cal, expire_day)
            except Exception:
                expire_trading = None

        expected_last: date | None = None
        if expired_b is False:
            expected_last = today_trading if expected_first else None
        else:
            expected_last = expire_trading if (expire_trading and expected_first) else None
            if expected_last is None and local_max:
                try:
                    expected_last = date.fromisoformat(local_max) if expected_first else None
                except Exception:
                    expected_last = None

        if expected_first is None or expected_last is None or expected_last < expected_first:
            findings.append(
                Finding(
                    "warning",
                    "completeness_unknown",
                    "Could not determine expected trading-day window",
                    path=sym,
                    extra={"symbol": sym, "open_date": open_date_s, "expire_datetime": exp_dt_s, "local_min": local_min, "local_max": local_max},
                )
            )
            continue

        days_expected = _count_trading_days_between(cal=cal, start=expected_first, end=expected_last)
        done = {ds for ds in (local_dates | no_data_dates) if expected_first.isoformat() <= ds <= expected_last.isoformat()}
        days_done = int(len(done))
        missing_count = int(max(0, days_expected - days_done))

        if missing_count > 0:
            # Produce a small sample of missing days (bounded).
            if cal:
                from bisect import bisect_left, bisect_right

                i0 = bisect_left(cal, expected_first)
                i1 = bisect_right(cal, expected_last)
                exp_days = [d.isoformat() for d in cal[i0:i1]]
            else:
                exp_days = []
                cur = expected_first
                while cur <= expected_last:
                    if cur.weekday() < 5:
                        exp_days.append(cur.isoformat())
                    cur += timedelta(days=1)
            missing = [d for d in exp_days if d not in done]
            findings.append(
                Finding(
                    "error",
                    "completeness_missing_trading_days",
                    "Missing expected trading days in local Parquet mirror",
                    path=sym,
                    extra={
                        "symbol": sym,
                        "expected_first": expected_first.isoformat(),
                        "expected_last": expected_last.isoformat(),
                        "days_expected": int(days_expected),
                        "days_done": int(days_done),
                        "missing_count": int(len(missing)),
                        "missing_sample": missing[:10],
                        "catalog_source": str(meta.get("catalog_source") or ""),
                    },
                )
            )

        # Coarse QuestDB coverage checks (when available)
        if questdb_cov is not None:
            qc = questdb_cov.get(sym) or {}
            db_first = str(qc.get("first_tick_day") or "").strip() or None
            db_last = str(qc.get("last_tick_day") or "").strip() or None
            db_days = qc.get("tick_days")
            try:
                db_days_i = int(db_days) if db_days is not None else 0
            except Exception:
                db_days_i = 0

            if db_days_i <= 0 and (local_dates or no_data_dates):
                findings.append(
                    Finding(
                        "warning",
                        "questdb_empty",
                        "QuestDB has no tick coverage for a symbol that has local data",
                        path=sym,
                        extra={"symbol": sym, "expected_days": int(days_expected), "local_days": int(len(local_dates))},
                    )
                )
            else:
                if db_first and db_first > expected_first.isoformat():
                    findings.append(
                        Finding(
                            "warning",
                            "questdb_first_day_late",
                            "QuestDB first_tick_day is later than expected_first",
                            path=sym,
                            extra={"symbol": sym, "expected_first": expected_first.isoformat(), "db_first_tick_day": db_first},
                        )
                    )
                if db_last and db_last < expected_last.isoformat():
                    findings.append(
                        Finding(
                            "warning",
                            "questdb_last_day_early",
                            "QuestDB last_tick_day is earlier than expected_last",
                            path=sym,
                            extra={"symbol": sym, "expected_last": expected_last.isoformat(), "db_last_tick_day": db_last},
                        )
                    )
                if db_days_i > 0 and db_days_i < int(days_expected):
                    findings.append(
                        Finding(
                            "warning",
                            "questdb_days_missing",
                            "QuestDB tick_days is smaller than expected trading-day count (coarse)",
                            path=sym,
                            extra={"symbol": sym, "expected_days": int(days_expected), "db_tick_days": int(db_days_i)},
                        )
                    )

    return findings


def _schema_matches(expected: pa.Schema, actual: pa.Schema) -> bool:
    if expected.names != actual.names:
        return False
    for name in expected.names:
        if expected.field(name).type != actual.field(name).type:
            return False
    return True


def _check_parquet_schema(path: Path, expected: pa.Schema) -> list[Finding]:
    try:
        schema = pq.read_schema(path)
        if not _schema_matches(expected, schema):
            return [
                Finding(
                    severity="error",
                    code="schema_mismatch",
                    message="Parquet schema does not match expected",
                    path=str(path),
                    extra={"expected": str(expected), "actual": str(schema)},
                )
            ]
        return []
    except Exception as e:
        return [Finding("error", "schema_read_failed", str(e), path=str(path))]


def _check_checksum(path: Path) -> list[Finding]:
    expected = read_sha256_sidecar(path)
    if expected is None:
        return [Finding("error", "missing_checksum", "Missing .sha256 sidecar", path=str(path))]
    actual = sha256_file(path)
    if expected != actual:
        return [
            Finding(
                "error",
                "checksum_mismatch",
                "Checksum mismatch",
                path=str(path),
                extra={"expected": expected, "actual": actual},
            )
        ]
    return []


def _iter_datetime_rowgroups(pf: pq.ParquetFile) -> list[pa.Array]:
    out: list[pa.Array] = []
    for i in range(pf.num_row_groups):
        t = pf.read_row_group(i, columns=["datetime"])
        out.append(t.column("datetime").combine_chunks())
    return out


def _check_datetime_integrity(path: Path) -> list[Finding]:
    """
    Check datetime column is non-decreasing within the file and report duplicates.

    This is row-group streaming to avoid full-file memory blowups.
    """
    try:
        pf = pq.ParquetFile(path)
        last_val: int | None = None
        n_dups = 0
        n_total = 0
        for i in range(pf.num_row_groups):
            t = pf.read_row_group(i, columns=["datetime"])
            arr = t.column("datetime").to_numpy(zero_copy_only=False)
            if arr.size == 0:
                continue
            n_total += int(arr.size)

            # within-group monotonic
            if not np.all(arr[:-1] <= arr[1:]):
                return [Finding("error", "datetime_not_sorted", "datetime not non-decreasing", path=str(path), extra={"row_group": i})]

            # cross-group boundary
            if last_val is not None and int(arr[0]) < int(last_val):
                return [Finding("error", "datetime_not_sorted", "datetime decreases across row groups", path=str(path), extra={"row_group": i})]

            n_dups += int(np.sum(arr[1:] == arr[:-1]))
            last_val = int(arr[-1])

        if n_total > 0 and n_dups > 0:
            return [
                Finding(
                    "warning",
                    "datetime_duplicates",
                    "Duplicate datetime values found",
                    path=str(path),
                    extra={"n_duplicates": int(n_dups), "n_total": int(n_total), "dup_rate": float(n_dups) / float(n_total)},
                )
            ]
        return []
    except Exception as e:
        return [Finding("error", "datetime_check_failed", str(e), path=str(path))]


def _check_tick_sanity(path: Path) -> list[Finding]:
    """
    Basic sanity for ticks:
    - volume/open_interest >= 0
    - key price columns >= 0
    - ask_price1 >= bid_price1 when both present
    """
    try:
        pf = pq.ParquetFile(path)
        cols = ["bid_price1", "ask_price1", "last_price", "volume", "open_interest"]
        for i in range(pf.num_row_groups):
            t = pf.read_row_group(i, columns=cols)
            b = t.column("bid_price1").to_numpy(zero_copy_only=False)
            a = t.column("ask_price1").to_numpy(zero_copy_only=False)
            last = t.column("last_price").to_numpy(zero_copy_only=False)
            vol = t.column("volume").to_numpy(zero_copy_only=False)
            oi = t.column("open_interest").to_numpy(zero_copy_only=False)

            if np.any(vol < 0):
                return [Finding("error", "negative_volume", "Negative volume found", path=str(path), extra={"row_group": i})]
            if np.any(oi < 0):
                return [Finding("error", "negative_open_interest", "Negative open_interest found", path=str(path), extra={"row_group": i})]

            # Ignore NaNs for price checks
            if np.any(last[~np.isnan(last)] < 0):
                return [Finding("error", "negative_price", "Negative last_price found", path=str(path), extra={"row_group": i})]
            if np.any(b[~np.isnan(b)] < 0) or np.any(a[~np.isnan(a)] < 0):
                return [Finding("error", "negative_price", "Negative bid/ask found", path=str(path), extra={"row_group": i})]

            mask = (~np.isnan(a)) & (~np.isnan(b))
            if np.any(a[mask] < b[mask]):
                return [Finding("error", "crossed_book", "ask_price1 < bid_price1", path=str(path), extra={"row_group": i})]
        return []
    except Exception as e:
        return [Finding("error", "sanity_check_failed", str(e), path=str(path))]


def _read_date_manifest(path: Path) -> dict[str, Any] | None:
    p = path / "_manifest.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def audit_ticks_root(
    root: Path,
    *,
    dataset: str,
    expected_schema: pa.Schema,
    only_symbols: set[str] | None = None,
) -> list[Finding]:
    findings: list[Finding] = []
    if not root.exists():
        return [Finding("warning", "missing_dataset", f"Dataset root missing: {root}", path=str(root))]

    for sym_dir in sorted(root.iterdir()):
        if not sym_dir.is_dir() or not sym_dir.name.startswith("symbol="):
            continue
        if only_symbols is not None:
            sym = sym_dir.name.split("=", 1)[-1]
            if sym not in only_symbols:
                continue
        for date_dir in sorted(sym_dir.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue
            m = _read_date_manifest(date_dir)
            if m is None:
                findings.append(Finding("error", "missing_manifest", "Missing _manifest.json", path=str(date_dir / "_manifest.json")))

            rows_total_manifest = None
            files_manifest: dict[str, dict[str, Any]] = {}
            if m:
                rows_total_manifest = int(m.get("rows_total") or 0)
                for f in m.get("files", []):
                    if isinstance(f, dict) and "file" in f:
                        files_manifest[str(f["file"])] = f

            rows_total_actual = 0
            for p in sorted(date_dir.glob("*.parquet")):
                findings.extend(_check_parquet_schema(p, expected_schema))
                findings.extend(_check_checksum(p))
                findings.extend(_check_datetime_integrity(p))
                findings.extend(_check_tick_sanity(p))
                rows_total_actual += parquet_num_rows(p)

                if p.name in files_manifest:
                    exp_rows = int(files_manifest[p.name].get("rows") or 0)
                    if exp_rows and exp_rows != parquet_num_rows(p):
                        findings.append(
                            Finding(
                                "error",
                                "manifest_row_mismatch",
                                "Manifest rows != parquet metadata rows",
                                path=str(p),
                                extra={"manifest_rows": exp_rows, "parquet_rows": parquet_num_rows(p)},
                            )
                        )

            if rows_total_manifest is not None and rows_total_manifest != rows_total_actual:
                findings.append(
                    Finding(
                        "error",
                        "manifest_total_mismatch",
                        "Manifest rows_total != sum(parquet rows)",
                        path=str(date_dir),
                        extra={"manifest_rows_total": rows_total_manifest, "parquet_rows_total": rows_total_actual},
                    )
                )

    return findings


def _audit_main_l5_equivalence(data_dir: Path, *, lake_version: LakeVersion = "v2") -> list[Finding]:
    """
    Validate derived main_l5 ticks match underlying raw ticks (excluding 'symbol').
    """
    findings: list[Finding] = []
    root = lake_root_dir(data_dir, lake_version) / "main_l5" / "ticks"
    if not root.exists():
        return findings

    cols = [c for c in TICK_ARROW_SCHEMA.names if c != "symbol"]

    for sym_dir in sorted(root.iterdir()):
        if not sym_dir.is_dir() or not sym_dir.name.startswith("symbol="):
            continue
        schedule_path = sym_dir / "schedule.parquet"
        if not schedule_path.exists():
            findings.append(Finding("error", "missing_schedule", "Missing schedule.parquet for derived symbol", path=str(schedule_path)))
            continue

        try:
            import pandas as pd

            schedule = pq.read_table(schedule_path).to_pandas()
            if "date" in schedule.columns:
                schedule["date"] = pd.to_datetime(schedule["date"], errors="coerce").dt.date
        except Exception as e:
            findings.append(Finding("error", "schedule_read_failed", str(e), path=str(schedule_path)))
            continue

        if "date" not in schedule.columns or "main_contract" not in schedule.columns:
            findings.append(Finding("error", "schedule_schema_bad", "schedule.parquet missing required columns", path=str(schedule_path)))
            continue

        # Ensure segment_id exists for older schedules.
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

        mapping: dict[str, tuple[str, int]] = {}
        for _, r in schedule.iterrows():
            d = r.get("date")
            mc = r.get("main_contract")
            if not d or not mc:
                continue
            try:
                d_str = str(d)
                mapping[d_str] = (str(mc), int(r.get("segment_id", 0) or 0))
            except Exception:
                continue

        for date_dir in sorted(sym_dir.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue
            dt = date_dir.name.split("=", 1)[1]
            mapping_row = mapping.get(dt)
            if not mapping_row:
                findings.append(Finding("error", "missing_schedule_mapping", "No schedule mapping for date", path=str(date_dir), extra={"date": dt}))
                continue
            underlying, exp_seg_id = mapping_row

            raw_date_dir = lake_root_dir(data_dir, lake_version) / "ticks" / f"symbol={underlying}" / f"date={dt}"
            for p_der in sorted(date_dir.glob("*.parquet")):
                p_raw = raw_date_dir / p_der.name
                if not p_raw.exists():
                    findings.append(Finding("error", "raw_missing", "Underlying raw file missing for derived partition", path=str(p_der), extra={"raw_path": str(p_raw)}))
                    continue

                try:
                    import pyarrow.compute as pc

                    pf_raw = pq.ParquetFile(p_raw)
                    pf_der = pq.ParquetFile(p_der)
                    n_raw = int(pf_raw.metadata.num_rows) if pf_raw.metadata is not None else 0
                    n_der = int(pf_der.metadata.num_rows) if pf_der.metadata is not None else 0
                    if n_raw != n_der:
                        findings.append(Finding("error", "row_count_mismatch", "Derived vs raw row count mismatch", path=str(p_der), extra={"raw_rows": n_raw, "derived_rows": n_der}))
                        continue

                    # v2-only: derived ticks must include explicit metadata columns and match the schedule.
                    der_schema = pf_der.schema_arrow
                    if "underlying_contract" not in der_schema.names:
                        findings.append(Finding("error", "missing_metadata", "Missing underlying_contract column in derived ticks", path=str(p_der)))
                    if "segment_id" not in der_schema.names:
                        findings.append(Finding("error", "missing_metadata", "Missing segment_id column in derived ticks", path=str(p_der)))

                    # Sample row groups for metadata consistency checks.
                    groups_meta = list(range(pf_der.num_row_groups))
                    if groups_meta and n_der > 500_000:
                        groups_meta = sorted(set([0, groups_meta[-1], groups_meta[len(groups_meta) // 2]]))
                    for gi in groups_meta:
                        if "underlying_contract" in der_schema.names:
                            t_u = pf_der.read_row_group(gi, columns=["underlying_contract"])
                            arr_u = t_u.column("underlying_contract").combine_chunks()
                            if arr_u.null_count:
                                findings.append(Finding("error", "metadata_null", "Nulls in underlying_contract", path=str(p_der), extra={"row_group": gi}))
                            else:
                                ok_u = pc.all(pc.equal(arr_u, pa.scalar(underlying, type=pa.string()))).as_py()
                                if not bool(ok_u):
                                    findings.append(
                                        Finding(
                                            "error",
                                            "metadata_mismatch",
                                            "underlying_contract does not match schedule",
                                            path=str(p_der),
                                            extra={"row_group": gi, "expected": underlying},
                                        )
                                    )
                        if "segment_id" in der_schema.names:
                            t_s = pf_der.read_row_group(gi, columns=["segment_id"])
                            arr_s = t_s.column("segment_id").combine_chunks()
                            if arr_s.null_count:
                                findings.append(Finding("error", "metadata_null", "Nulls in segment_id", path=str(p_der), extra={"row_group": gi}))
                            else:
                                ok_s = pc.all(pc.equal(arr_s, pa.scalar(int(exp_seg_id), type=pa.int64()))).as_py()
                                if not bool(ok_s):
                                    findings.append(
                                        Finding(
                                            "error",
                                            "metadata_mismatch",
                                            "segment_id does not match schedule",
                                            path=str(p_der),
                                            extra={"row_group": gi, "expected": int(exp_seg_id)},
                                        )
                                    )

                    # Full compare for small-ish files; sample row groups for large.
                    full = n_raw <= 500_000
                    groups = list(range(pf_der.num_row_groups))
                    if not full and groups:
                        groups = sorted(set([0, groups[-1], groups[len(groups) // 2]]))

                    for gi in groups:
                        t_raw = pf_raw.read_row_group(gi, columns=cols)
                        t_der = pf_der.read_row_group(gi, columns=cols)
                        for c in cols:
                            a = t_raw.column(c).to_numpy(zero_copy_only=False)
                            b = t_der.column(c).to_numpy(zero_copy_only=False)
                            if a.dtype.kind == "f":
                                ok = np.array_equal(a, b, equal_nan=True)
                            else:
                                ok = np.array_equal(a, b)
                            if not ok:
                                findings.append(Finding("error", "derived_mismatch", "Derived data differs from raw (excluding symbol)", path=str(p_der), extra={"column": c, "row_group": gi, "raw": str(p_raw)}))
                                raise StopIteration
                except StopIteration:
                    continue
                except Exception as e:
                    findings.append(Finding("error", "derived_compare_failed", str(e), path=str(p_der), extra={"raw": str(p_raw)}))

    return findings


def audit_features_or_labels_root(root: Path, *, dataset: str) -> list[Finding]:
    findings: list[Finding] = []
    if not root.exists():
        return [Finding("warning", "missing_dataset", f"Dataset root missing: {root}", path=str(root))]

    for sym_dir in sorted(root.iterdir()):
        if not sym_dir.is_dir() or not sym_dir.name.startswith("symbol="):
            continue
        manifest_path = sym_dir / "manifest.json"
        if not manifest_path.exists():
            findings.append(Finding("error", "missing_manifest", "Missing manifest.json", path=str(manifest_path)))
            continue
        try:
            m = json.loads(manifest_path.read_text())
        except Exception as e:
            findings.append(Finding("error", "manifest_read_failed", str(e), path=str(manifest_path)))
            continue

        schema_hash = str(m.get("schema_hash") or "")
        row_counts = m.get("row_counts") or {}
        for date_dir in sorted(sym_dir.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue
            dt = date_dir.name.split("=", 1)[1]
            files = sorted(date_dir.glob("*.parquet"))
            if not files:
                continue
            rows_total = 0
            for p in files:
                findings.extend(_check_checksum(p))
                rows_total += parquet_num_rows(p)
                try:
                    from ghtrader.integrity import schema_hash_from_parquet

                    if schema_hash and schema_hash_from_parquet(p) != schema_hash:
                        findings.append(Finding("error", "schema_hash_mismatch", "Schema hash mismatch", path=str(p)))
                except Exception:
                    pass

            exp = row_counts.get(dt)
            if exp is not None and int(exp) != int(rows_total):
                findings.append(
                    Finding(
                        "error",
                        "manifest_total_mismatch",
                        "Manifest row_counts[date] != sum(parquet rows)",
                        path=str(date_dir),
                        extra={"date": dt, "manifest_rows": int(exp), "parquet_rows": int(rows_total)},
                    )
                )

    return findings


def write_audit_report(*, runs_dir: Path, report: dict[str, Any]) -> Path:
    out_dir = runs_dir / "audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = report.get("run_id") or uuid.uuid4().hex[:12]
    out_path = out_dir / f"{run_id}.json"
    write_json_atomic(out_path, report)
    return out_path


def run_audit(
    *,
    data_dir: Path,
    runs_dir: Path,
    scopes: list[str],
    lake_version: LakeVersion = "v2",
    symbols: list[str] | None = None,
    exchange: str | None = None,
    var: str | None = None,
    refresh_catalog: bool = False,
) -> tuple[Path, dict[str, Any]]:
    run_id = uuid.uuid4().hex[:12]
    findings: list[Finding] = []

    main_l5_schema_v2 = TICK_ARROW_SCHEMA.append(pa.field("underlying_contract", pa.string())).append(
        pa.field("segment_id", pa.int64())
    )
    lake_root = lake_root_dir(data_dir, lake_version)
    only = {str(s).strip() for s in (symbols or []) if str(s).strip()} or None

    if "ticks" in scopes or "all" in scopes:
        findings.extend(
            audit_ticks_root(lake_root / "ticks", dataset="ticks_raw", expected_schema=TICK_ARROW_SCHEMA, only_symbols=only)
        )
    if "main_l5" in scopes or "all" in scopes:
        findings.extend(
            audit_ticks_root(
                lake_root / "main_l5" / "ticks",
                dataset="ticks_main_l5",
                expected_schema=main_l5_schema_v2,
                only_symbols=only,
            )
        )
        # Equivalence audit is global and can be expensive; skip when symbol-filtered.
        if only is None:
            findings.extend(_audit_main_l5_equivalence(data_dir, lake_version=lake_version))
    if "features" in scopes or "all" in scopes:
        findings.extend(audit_features_or_labels_root(data_dir / "features", dataset="features"))
    if "labels" in scopes or "all" in scopes:
        findings.extend(audit_features_or_labels_root(data_dir / "labels", dataset="labels"))
    # Completeness can require external metadata/DB; keep it opt-in.
    if "completeness" in scopes:
        findings.extend(
            audit_completeness(
                data_dir=data_dir,
                runs_dir=runs_dir,
                lake_version=lake_version,
                symbols=only and sorted(only) or None,
                exchange=exchange,
                var=var,
                refresh_catalog=bool(refresh_catalog),
            )
        )

    n_errors = sum(1 for f in findings if f.severity == "error")
    n_warnings = sum(1 for f in findings if f.severity == "warning")

    report = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(data_dir),
        "scopes": scopes,
        "lake_version": lake_version,
        "completeness_exchange": str(exchange).upper().strip() if exchange else "",
        "completeness_var": str(var).lower().strip() if var else "",
        "completeness_refresh_catalog": bool(refresh_catalog),
        "summary": {"errors": int(n_errors), "warnings": int(n_warnings), "total": int(len(findings))},
        "findings": [
            {
                "severity": f.severity,
                "code": f.code,
                "message": f.message,
                "path": f.path,
                "extra": f.extra or {},
            }
            for f in findings
        ],
    }
    out_path = write_audit_report(runs_dir=runs_dir, report=report)
    return out_path, report

