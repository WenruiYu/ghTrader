from __future__ import annotations

import json
import time
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal


IngestKind = Literal["download", "download_contract_range", "record", "unknown"]


def _infer_market_from_symbol(symbol: str) -> str | None:
    """
    Best-effort market inference.

    Examples:
    - SHFE.cu2602 -> SHFE
    - KQ.m@SHFE.cu -> SHFE
    """
    s = str(symbol).strip()
    if "@" in s:
        s = s.split("@", 1)[1]
    if "." in s:
        return s.split(".", 1)[0].upper()
    return None


def _parse_yyyymmdd(s: str) -> date | None:
    try:
        return date.fromisoformat(str(s).strip())
    except Exception:
        return None


def _parse_contract_yymm(yymm: str) -> tuple[int, int]:
    s = str(yymm).strip()
    if len(s) != 4 or not s.isdigit():
        raise ValueError(f"Invalid contract code (expected YYMM): {yymm!r}")
    yy = int(s[:2])
    mm = int(s[2:])
    if mm < 1 or mm > 12:
        raise ValueError(f"Invalid contract month in {yymm!r}")
    return 2000 + yy, mm


def _iter_contract_yymms(start_contract: str, end_contract: str) -> list[str]:
    sy, sm = _parse_contract_yymm(start_contract)
    ey, em = _parse_contract_yymm(end_contract)
    cur = date(sy, sm, 1)
    end_dt = date(ey, em, 1)
    out: list[str] = []
    while cur <= end_dt:
        out.append(f"{cur.year % 100:02d}{cur.month:02d}")
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return out


def _read_json_list(path: Path) -> list[str]:
    try:
        if not path.exists():
            return []
        raw = json.loads(path.read_text())
        if not isinstance(raw, list):
            return []
        return [str(x) for x in raw]
    except Exception:
        return []


def _lake_root_dir(data_dir: Path, lake_version: str | None) -> Path:
    _ = lake_version  # v2-only
    return data_dir / "lake_v2"


def _ticks_symbol_dir(data_dir: Path, symbol: str, *, lake_version: str | None = None) -> Path:
    return _lake_root_dir(data_dir, lake_version) / "ticks" / f"symbol={symbol}"


def _no_data_dates_path(data_dir: Path, symbol: str, *, lake_version: str | None = None) -> Path:
    return _ticks_symbol_dir(data_dir, symbol, lake_version=lake_version) / "_no_data_dates.json"


def _scan_downloaded_date_dirs(symbol_dir: Path, *, start_iso: str, end_iso: str) -> tuple[set[str], str | None, str | None]:
    """
    Return (downloaded_date_strings, min_date_str, max_date_str) for date partitions in [start_iso, end_iso].

    We compare ISO strings lexicographically (safe for YYYY-MM-DD).
    """
    out: set[str] = set()
    min_s: str | None = None
    max_s: str | None = None
    if not symbol_dir.exists():
        return out, None, None
    for child in symbol_dir.iterdir():
        if not child.is_dir() or not child.name.startswith("date="):
            continue
        dt_s = child.name.split("=", 1)[1]
        if dt_s < start_iso or dt_s > end_iso:
            continue
        out.add(dt_s)
        if min_s is None or dt_s < min_s:
            min_s = dt_s
        if max_s is None or dt_s > max_s:
            max_s = dt_s
    return out, min_s, max_s


def _weekday_is_trading_day(d: date) -> bool:
    return d.weekday() < 5


def _load_calendar(data_dir: Path) -> list[date] | None:
    """
    Load the trading calendar as a sorted list of dates.

    Uses the same cached source as ghtrader.trading_calendar.
    """
    try:
        from ghtrader.trading_calendar import get_trading_calendar

        cal = get_trading_calendar(data_dir=data_dir, refresh=False)
        return cal if cal else None
    except Exception:
        return None


def _count_trading_days(cal: list[date] | None, *, start: date, end: date) -> int:
    if end < start:
        return 0
    if cal:
        i = bisect_left(cal, start)
        j = bisect_right(cal, end)
        return int(j - i)
    # fallback weekday approximation
    c = 0
    cur = start
    while cur <= end:
        if _weekday_is_trading_day(cur):
            c += 1
        cur += timedelta(days=1)
    return c


def _is_trading_day(cal_set: set[date] | None, d: date) -> bool:
    if cal_set is None:
        return _weekday_is_trading_day(d)
    return d in cal_set


def _parse_structlog_console_kvs(line: str) -> tuple[str | None, dict[str, str]]:
    """
    Parse a structlog ConsoleRenderer line into (event, kvs).

    Expected shape (example):
      2026-01-12T15:00:01.000000Z [info     ] tq_ingest.chunk_download symbol=... chunk_idx=3
    """
    ln = str(line).strip()
    if not ln:
        return None, {}

    # Fast path: find the event token after the log level bracket.
    # We avoid regex dependency; split around the first ']' which ends the level field.
    if "]" not in ln:
        return None, {}
    after = ln.split("]", 1)[1].strip()
    if not after:
        return None, {}
    parts = after.split()
    if not parts:
        return None, {}
    event = parts[0]

    kvs: dict[str, str] = {}
    for tok in parts[1:]:
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        kvs[k] = v

    return event, kvs


def parse_ingest_log_tail(log_text: str) -> dict[str, Any]:
    """
    Best-effort extraction of ingest progress hints from a mixed log tail.

    Returns keys like:
      current_symbol, chunk_idx, chunk_start, chunk_end, detected_end_contract
    """
    out: dict[str, Any] = {}
    if not log_text:
        return out

    lines = [ln for ln in str(log_text).splitlines() if ln.strip()]
    for ln in reversed(lines):
        if "tq_ingest." not in ln:
            continue
        event, kvs = _parse_structlog_console_kvs(ln)
        if not event:
            continue

        # Current symbol: prefer contract_download/chunk_download, but accept any tq_ingest.* line.
        if "current_symbol" not in out and "symbol" in kvs:
            out["current_symbol"] = kvs.get("symbol")

        if event.endswith("detected_end_contract") and "detected_end_contract" not in out:
            if "end_contract" in kvs:
                out["detected_end_contract"] = str(kvs["end_contract"])

        if event.endswith("chunk_download") and "chunk_idx" not in out:
            if "chunk_idx" in kvs:
                try:
                    out["chunk_idx"] = int(str(kvs["chunk_idx"]))
                except Exception:
                    pass
            if "chunk_start" in kvs:
                out["chunk_start"] = str(kvs["chunk_start"])
            if "chunk_end" in kvs:
                out["chunk_end"] = str(kvs["chunk_end"])

        # Stop early once we have the key hints we want.
        if "current_symbol" in out and "chunk_idx" in out:
            return out

    return out


def parse_ingest_command(argv: list[str]) -> dict[str, Any]:
    """
    Parse a ghtrader CLI argv list into an ingest command description.

    Supports both terminal invocations (e.g. ['ghtrader', 'download', ...]) and
    module invocations (e.g. [python, '-m', 'ghtrader.cli', 'download', ...]).
    """
    if not argv:
        return {"kind": "unknown", "args": {}}

    # Find first known subcommand token.
    token_to_kind: dict[str, IngestKind] = {
        "download": "download",
        "download-contract-range": "download_contract_range",
        "download_contract_range": "download_contract_range",
        "record": "record",
    }
    sub_idx = None
    kind: IngestKind = "unknown"
    for i, tok in enumerate(argv):
        if tok in token_to_kind:
            sub_idx = i
            kind = token_to_kind[tok]
            break

    if sub_idx is None:
        return {"kind": "unknown", "args": {}}

    args: dict[str, Any] = {}
    toks = list(argv[sub_idx + 1 :])

    # Minimal option parsing (Click-style long opts).
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.startswith("--no-"):
            k = t[5:].replace("-", "_")
            args[k] = False
            i += 1
            continue
        if t.startswith("--"):
            k = t[2:].replace("-", "_")
            if i + 1 < len(toks) and not str(toks[i + 1]).startswith("-"):
                args[k] = toks[i + 1]
                i += 2
                continue
            args[k] = True
            i += 1
            continue
        if t in {"-s", "-S", "-E"} and i + 1 < len(toks):
            # Support a few common short options used in ghtrader.cli
            if t == "-s":
                args["symbol"] = toks[i + 1]
            elif t == "-S":
                args["start"] = toks[i + 1]
            elif t == "-E":
                args["end"] = toks[i + 1]
            i += 2
            continue
        i += 1

    # Normalize key names to match dashboard expectations/tests.
    if kind == "download":
        # CLI uses --start/--end but tests expect start_date/end_date.
        if "start" in args and "start_date" not in args:
            args["start_date"] = str(args.get("start"))
        if "end" in args and "end_date" not in args:
            args["end_date"] = str(args.get("end"))
    if kind == "download_contract_range":
        # CLI uses --var -> we keep 'var' for callers.
        if "variety" in args and "var" not in args:
            args["var"] = str(args.get("variety"))

    return {"kind": kind, "args": args}


def _detect_latest_contract_yymm_from_tqsdk(*, exchange: str, var: str) -> str | None:
    """
    Best-effort: detect latest YYMM using TqSdk contract catalog (no akshare).
    """
    try:
        from ghtrader.config import get_runs_dir
        from ghtrader.tqsdk_catalog import get_contract_catalog

        ex = str(exchange).upper().strip()
        v = str(var).lower().strip()
        cat = get_contract_catalog(exchange=ex, var=v, runs_dir=get_runs_dir(), refresh=False)
        if not bool(cat.get("ok", False)):
            return None
        contracts = list(cat.get("contracts") or [])
        best_key = -1
        best_yymm = None
        for c in contracts:
            sym = str((c or {}).get("symbol") or "").strip()
            if not sym:
                continue
            tail = sym.split(".", 1)[-1]
            digits = "".join([ch for ch in tail if ch.isdigit()])
            if len(digits) < 4:
                continue
            yymm = digits[-4:]
            if not yymm.isdigit():
                continue
            yy, mm = _parse_contract_yymm(yymm)
            key = yy * 100 + mm
            if key > best_key:
                best_key = key
                best_yymm = yymm
        return best_yymm
    except Exception:
        return None


def compute_download_contract_range_status(
    *,
    exchange: str,
    var: str,
    start_contract: str,
    end_contract: str,
    start_date: date,
    end_date: date,
    data_dir: Path,
    log_hint: dict[str, Any] | None = None,
    lake_version: str | None = None,
) -> dict[str, Any]:
    """
    Compute progress for `ghtrader download-contract-range` using:
    - configured backfill date window (start_date/end_date)
    - Parquet lake partitions + _no_data_dates.json markers

    Note: akshare/active-ranges have been removed.
    """
    ex = str(exchange).upper().strip()
    var_l = str(var).lower().strip()
    var_u = var_l.upper()

    resolved_end = str(end_contract).strip()
    if resolved_end.lower() in {"auto", "latest"}:
        # Prefer log hint if available (matches tq_ingest.detected_end_contract).
        if log_hint and log_hint.get("detected_end_contract"):
            resolved_end = str(log_hint["detected_end_contract"])
        else:
            auto = _detect_latest_contract_yymm_from_tqsdk(exchange=ex, var=var_l)
            if auto:
                resolved_end = str(auto)

    yymms = _iter_contract_yymms(str(start_contract), str(resolved_end))
    raw_contracts = [f"{var_u}{yymm}" for yymm in yymms]

    cal = _load_calendar(data_dir)
    cal_set = set(cal) if cal else None

    current_symbol = str((log_hint or {}).get("current_symbol") or "").strip() or None

    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    expected_per_contract = _count_trading_days(cal, start=start_date, end=end_date)

    contracts: list[dict[str, Any]] = []
    days_expected_total = 0
    days_done_total = 0
    contracts_done = 0

    for raw in raw_contracts:
        tick_symbol = f"{ex}.{var_l}{raw[len(var_u):]}"
        expected = int(expected_per_contract)
        days_expected_total += int(expected_per_contract)

        downloaded_set, min_dl, max_dl = _scan_downloaded_date_dirs(
            _ticks_symbol_dir(data_dir, tick_symbol, lake_version=lake_version),
            start_iso=start_iso,
            end_iso=end_iso,
        )

        no_data_raw = _read_json_list(_no_data_dates_path(data_dir, tick_symbol, lake_version=lake_version))
        no_data_effective = 0
        for ds in no_data_raw:
            ds_s = str(ds).strip()
            if not ds_s:
                continue
            if ds_s < start_iso or ds_s > end_iso:
                continue
            d = _parse_yyyymmdd(ds_s)
            if d is None:
                continue
            if not _is_trading_day(cal_set, d):
                continue
            if ds_s in downloaded_set:
                continue
            no_data_effective += 1

        done = int(len(downloaded_set) + no_data_effective)
        if expected > 0 and done >= expected:
            contracts_done += 1
        days_done_total += int(min(done, expected) if expected > 0 else done)

        pct = float(done / expected) if expected > 0 else 0.0
        if pct < 0.0:
            pct = 0.0
        if pct > 1.0:
            pct = 1.0

        contracts.append(
            {
                "raw_contract": raw,
                "symbol": tick_symbol,
                "status": "active",
                "backfill_start": start_iso,
                "backfill_end": end_iso,
                "days_expected": int(expected),
                "days_downloaded": int(len(downloaded_set)),
                "days_no_data": int(no_data_effective),
                "days_done": int(min(done, expected) if expected > 0 else done),
                "pct": pct,
                "min_downloaded": min_dl,
                "max_downloaded": max_dl,
                "is_current": bool(current_symbol and tick_symbol == current_symbol),
            }
        )

    pct_total = float(days_done_total / days_expected_total) if days_expected_total > 0 else 0.0
    if pct_total < 0.0:
        pct_total = 0.0
    if pct_total > 1.0:
        pct_total = 1.0

    return {
        "kind": "download_contract_range",
        "exchange": ex,
        "var": var_l,
        "lake_version": "v2",
        "start_date": start_iso,
        "end_date": end_iso,
        "start_contract": str(start_contract),
        "end_contract": str(end_contract),
        "resolved_end_contract": str(resolved_end),
        "current_symbol": current_symbol or "",
        "summary": {
            "contracts_total": int(len(raw_contracts)),
            "contracts_done": int(contracts_done),
            "days_expected_total": int(days_expected_total),
            "days_done_total": int(days_done_total),
            "pct": pct_total,
        },
        "contracts": contracts,
    }


# ---------------------------------------------------------------------------
# TTL cache (used by API endpoints to avoid heavy rescans)
# ---------------------------------------------------------------------------


@dataclass
class _CacheItem:
    ts: float
    value: dict[str, Any]


_CACHE: dict[str, _CacheItem] = {}


def _cache_get(key: str, *, ttl_s: float) -> dict[str, Any] | None:
    it = _CACHE.get(key)
    if it is None:
        return None
    if (time.time() - it.ts) > float(ttl_s):
        return None
    return dict(it.value)


def _cache_put(key: str, value: dict[str, Any]) -> None:
    _CACHE[key] = _CacheItem(ts=time.time(), value=dict(value))


def _read_log_tail(path: Path, *, max_bytes: int = 128_000) -> str:
    try:
        if not path.exists():
            return ""
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            offset = max(0, size - int(max_bytes))
            f.seek(offset)
            chunk = f.read().decode("utf-8", errors="ignore")
        return chunk
    except Exception:
        return ""


def ingest_status_for_job(
    *,
    job_id: str,
    command: list[str],
    log_path: str | None,
    default_data_dir: Path,
    ttl_s: float = 30.0,
) -> dict[str, Any]:
    """
    Compute ingest status for a job (cached).
    """
    parsed = parse_ingest_command(command)
    kind: IngestKind = parsed.get("kind", "unknown")
    args: dict[str, Any] = dict(parsed.get("args") or {})

    data_dir = Path(str(args.get("data_dir") or default_data_dir))

    log_hint: dict[str, Any] = {}
    if log_path:
        log_hint = parse_ingest_log_tail(_read_log_tail(Path(log_path)))

    if kind == "download_contract_range":
        lv = "v2"
        ex = str(args.get("exchange") or "SHFE")
        var = str(args.get("var") or args.get("variety") or "").strip()
        start_c = str(args.get("start_contract") or "").strip()
        end_c = str(args.get("end_contract") or "").strip()
        start_s = str(args.get("start_date") or "").strip()
        end_s = str(args.get("end_date") or "").strip()
        if not var or not start_c or not end_c:
            return {"kind": "unknown", "job_id": job_id, "error": "missing required args for download-contract-range"}

        # New contract-range path is defined over an explicit date window; default is 2015-01-01..today.
        d0 = _parse_yyyymmdd(start_s) if start_s else date(2015, 1, 1)
        d1 = _parse_yyyymmdd(end_s) if end_s else date.today()
        if d0 is None or d1 is None:
            return {"kind": "unknown", "job_id": job_id, "error": "invalid start-date/end-date"}

        cache_key = f"range:{data_dir}:{lv}:{ex}:{var}:{start_c}:{end_c}:{d0.isoformat()}:{d1.isoformat()}:{log_hint.get('detected_end_contract','')}"
        cached = _cache_get(cache_key, ttl_s=ttl_s)
        if cached is not None:
            cached["job_id"] = job_id
            return cached

        val = compute_download_contract_range_status(
            exchange=ex,
            var=var,
            start_contract=start_c,
            end_contract=end_c,
            start_date=d0,
            end_date=d1,
            data_dir=data_dir,
            log_hint=log_hint,
            lake_version=lv,
        )
        val["job_id"] = job_id
        _cache_put(cache_key, val)
        return val

    if kind == "download":
        lv = "v2"
        symbol = str(args.get("symbol") or "").strip()
        start_s = str(args.get("start_date") or args.get("start") or "").strip()
        end_s = str(args.get("end_date") or args.get("end") or "").strip()
        if not symbol or not start_s or not end_s:
            return {"kind": "unknown", "job_id": job_id, "error": "missing required args for download"}

        d0 = _parse_yyyymmdd(start_s)
        d1 = _parse_yyyymmdd(end_s)
        if d0 is None or d1 is None:
            return {"kind": "unknown", "job_id": job_id, "error": "invalid start/end date"}

        cal = _load_calendar(data_dir)
        cal_set = set(cal) if cal else None

        # Expected trading-day count
        expected = _count_trading_days(cal, start=d0, end=d1)

        # Downloaded dates in range
        downloaded_set, min_dl, max_dl = _scan_downloaded_date_dirs(
            _ticks_symbol_dir(data_dir, symbol, lake_version=lv),
            start_iso=d0.isoformat(),
            end_iso=d1.isoformat(),
        )
        # No-data dates in range (exclude already-downloaded)
        no_data_raw = _read_json_list(_no_data_dates_path(data_dir, symbol, lake_version=lv))
        no_data_effective = 0
        for ds in no_data_raw:
            ds_s = str(ds).strip()
            if not ds_s:
                continue
            if ds_s < d0.isoformat() or ds_s > d1.isoformat():
                continue
            dd = _parse_yyyymmdd(ds_s)
            if dd is None:
                continue
            if not _is_trading_day(cal_set, dd):
                continue
            if ds_s in downloaded_set:
                continue
            no_data_effective += 1

        done = int(len(downloaded_set) + no_data_effective)
        pct = float(done / expected) if expected > 0 else 0.0
        if pct < 0.0:
            pct = 0.0
        if pct > 1.0:
            pct = 1.0

        return {
            "kind": "download",
            "job_id": job_id,
            "symbol": symbol,
            "start_date": d0.isoformat(),
            "end_date": d1.isoformat(),
            "lake_version": lv,
            "summary": {
                "days_expected": int(expected),
                "days_downloaded": int(len(downloaded_set)),
                "days_no_data": int(no_data_effective),
                "days_done": int(min(done, expected) if expected > 0 else done),
                "pct": pct,
            },
            "min_downloaded": min_dl,
            "max_downloaded": max_dl,
            "current_symbol": str(log_hint.get("current_symbol") or ""),
            "chunk_idx": log_hint.get("chunk_idx"),
            "chunk_start": log_hint.get("chunk_start"),
            "chunk_end": log_hint.get("chunk_end"),
        }

    if kind == "record":
        # Live recorder is unbounded; provide best-effort hints only.
        lv = "v2"
        return {
            "kind": "record",
            "job_id": job_id,
            "symbols": args.get("symbols"),
            "lake_version": lv,
            "current_symbol": log_hint.get("current_symbol"),
        }

    return {"kind": "unknown", "job_id": job_id}

