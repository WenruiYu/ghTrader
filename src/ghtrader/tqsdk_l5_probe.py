from __future__ import annotations

import json
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ghtrader.config import get_runs_dir, get_tqsdk_auth
from ghtrader.trading_calendar import get_trading_calendar


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache_root(*, runs_dir: Path | None = None) -> Path:
    rd = runs_dir or get_runs_dir()
    return rd / "control" / "cache" / "tqsdk_l5_probe"


def _safe_symbol_name(symbol: str) -> str:
    return str(symbol).strip().replace("/", "_")


def probe_cache_path(*, symbol: str, runs_dir: Path | None = None) -> Path:
    return _cache_root(runs_dir=runs_dir) / f"symbol={_safe_symbol_name(symbol)}.json"


def _write_json_atomic(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".tmp-{uuid.uuid4().hex}")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def load_probe_result(*, symbol: str, runs_dir: Path | None = None) -> dict[str, Any] | None:
    return _read_json(probe_cache_path(symbol=symbol, runs_dir=runs_dir))


def _parse_exchange_var(symbol: str) -> tuple[str | None, str | None]:
    """
    'SHFE.cu2003' -> ('SHFE', 'cu')
    """
    s = str(symbol).strip()
    if "." not in s:
        return None, None
    ex, tail = s.split(".", 1)
    ex = ex.strip().upper()
    tail = tail.strip()
    # var is leading letters in tail
    var = ""
    for ch in tail:
        if ch.isalpha():
            var += ch
        else:
            break
    var = var.lower().strip()
    return (ex or None, var or None)


def _last_trading_day_leq(cal: list[date], today: date) -> date:
    if not cal:
        d = today
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d
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


def _parse_date_any(x: object) -> date | None:
    """
    Best-effort: parse a date from a TqSdk field which may be a date/datetime/str/int.
    """
    try:
        if x is None:
            return None
        if isinstance(x, date) and not isinstance(x, datetime):
            return x
        if isinstance(x, datetime):
            return x.date()
        if isinstance(x, (int, float)):
            v = float(x)
            # Heuristic: interpret as epoch seconds/ms/ns depending on magnitude.
            if v > 1e15:
                v = v / 1e9  # ns -> s
            elif v > 1e12:
                v = v / 1e3  # ms -> s
            else:
                v = v  # s
            return datetime.fromtimestamp(v, tz=timezone.utc).date()
        s = str(x).strip()
        if len(s) >= 10:
            try:
                return date.fromisoformat(s[:10])
            except Exception:
                return None
        return None
    except Exception:
        return None


def _local_last_date_v2(*, data_dir: Path, symbol: str) -> date | None:
    """
    Return the maximum local partition date for a symbol (v2 raw ticks),
    or None if the symbol has no local data.
    """
    try:
        sym = str(symbol).strip()
        if not sym:
            return None
        base = data_dir / "lake_v2" / "ticks" / f"symbol={sym}"
        if not base.exists():
            return None
        out: date | None = None
        for p in base.iterdir():
            if not p.is_dir():
                continue
            if not p.name.startswith("date="):
                continue
            ds = p.name.split("=", 1)[-1].strip()
            if len(ds) != 10:
                continue
            try:
                d = date.fromisoformat(ds)
            except Exception:
                continue
            if out is None or d > out:
                out = d
        return out
    except Exception:
        return None


def _df_has_l5(df: pd.DataFrame) -> bool:
    """
    TqSdk returns float columns; L1-only data often has NaN for bid/ask levels 2-5.
    """
    if df is None or df.empty:
        return False
    cols = []
    for i in range(2, 6):
        cols.append(f"bid_price{i}")
        cols.append(f"ask_price{i}")
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return False
    sub = df[cols]
    # "L5 present" means any non-NaN positive price in levels 2-5.
    try:
        return bool(((sub > 0) & sub.notna()).any().any())
    except Exception:
        # fallback
        for c in cols:
            try:
                s = pd.to_numeric(df[c], errors="coerce")
                if bool((s > 0).any()):
                    return True
            except Exception:
                continue
        return False


def probe_l5_for_symbol(
    *,
    symbol: str,
    data_dir: Path,
    runs_dir: Path,
    probe_day: date | None = None,
) -> dict[str, Any]:
    """
    Probe TqSdk tick series for a single day and detect whether L5 depth is present.
    Writes result to runs/control/cache/tqsdk_l5_probe/.
    """
    sym = str(symbol).strip()
    ex, var = _parse_exchange_var(sym)

    today = datetime.now(timezone.utc).date()
    cal = get_trading_calendar(data_dir=data_dir, refresh=False)
    today_trading = _last_trading_day_leq(cal, today)

    # Auto probe-day selection: make expired contracts probe a day where ticks likely exist.
    probe_day_source = "explicit" if probe_day is not None else ""
    if probe_day is None:
        local_last = _local_last_date_v2(data_dir=data_dir, symbol=sym)
        if local_last is not None:
            probe_day = local_last
            probe_day_source = "local_max"

    # If still not decided, we'll try quote metadata once TqApi is available; otherwise fall back.

    try:
        from tqsdk import TqApi  # type: ignore

        auth = get_tqsdk_auth()
        api = TqApi(auth=auth, disable_print=True)
    except Exception as e:
        if probe_day is None:
            probe_day = today_trading
            probe_day_source = "today_trading"
        payload: dict[str, Any] = {
            "symbol": sym,
            "exchange": ex,
            "var": var,
            "probed_day": probe_day.isoformat(),
            "probe_day_source": probe_day_source,
            "ticks_rows": 0,
            "l5_present": None,
            "error": f"tqsdk_init_failed: {e}",
            "updated_at": _now_iso(),
            "updated_at_unix": float(time.time()),
        }
        payload["error"] = f"tqsdk_init_failed: {e}"
        _write_json_atomic(probe_cache_path(symbol=sym, runs_dir=runs_dir), payload)
        return payload

    try:
        if probe_day is None:
            # Try to pick an expired contract's last trading day from quote metadata.
            try:
                q = api.get_quote(sym)
                exp_raw = getattr(q, "expire_datetime", None)
                if exp_raw is None and isinstance(q, dict):
                    exp_raw = q.get("expire_datetime")
                exp_day = _parse_date_any(exp_raw)
                if exp_day is not None and exp_day < today_trading:
                    probe_day = _last_trading_day_leq(cal, exp_day)
                    probe_day_source = "quote_expire"
            except Exception:
                pass

        if probe_day is None:
            probe_day = today_trading
            probe_day_source = "today_trading"

        payload: dict[str, Any] = {
            "symbol": sym,
            "exchange": ex,
            "var": var,
            "probed_day": probe_day.isoformat(),
            "probe_day_source": probe_day_source,
            "ticks_rows": 0,
            "l5_present": None,
            "error": "",
            "updated_at": _now_iso(),
            "updated_at_unix": float(time.time()),
        }

        try:
            df = api.get_tick_data_series(symbol=sym, start_dt=probe_day, end_dt=probe_day + timedelta(days=1))
        except Exception as e:
            payload["error"] = f"probe_failed: {e}"
            df = pd.DataFrame()

        payload["ticks_rows"] = int(len(df)) if df is not None else 0
        if payload["ticks_rows"] <= 0:
            payload["l5_present"] = None
            if not payload["error"]:
                payload["error"] = "no_ticks"
        else:
            payload["l5_present"] = bool(_df_has_l5(df))
    finally:
        try:
            api.close()
        except Exception:
            pass

    try:
        _write_json_atomic(probe_cache_path(symbol=sym, runs_dir=runs_dir), payload)
    except Exception:
        pass
    return payload

