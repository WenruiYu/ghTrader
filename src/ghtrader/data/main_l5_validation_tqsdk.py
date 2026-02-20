from __future__ import annotations

from bisect import bisect_left
from datetime import date, datetime, timedelta
from typing import Any

from ghtrader.util.l5_detection import l5_mask_df


def verify_missing_segments_with_tqsdk(
    *,
    tqsdk_api: Any,
    symbol: str,
    day: date,
    day_missing_segments: list[dict[str, Any]],
    tqsdk_check_max_segments: int,
) -> dict[str, Any]:
    """
    Best-effort provider-side verification for missing segments in one trading day.

    Mutates `day_missing_segments[*]["tqsdk_status"]` for checked items.
    """
    try:
        df = tqsdk_api.get_tick_data_series(symbol=str(symbol), start_dt=day, end_dt=day + timedelta(days=1))
        if df is None or df.empty:
            checked_n = int(min(len(day_missing_segments), int(tqsdk_check_max_segments)))
            for seg in day_missing_segments[:checked_n]:
                seg["tqsdk_status"] = "provider_missing"
            return {
                "checked_days": 1,
                "checked_segments": checked_n,
                "provider_missing_segments": checked_n,
                "provider_has_data_segments": 0,
                "errors": 0,
                "error": None,
            }

        try:
            mask = l5_mask_df(df)
        except Exception:
            mask = None
        if mask is not None:
            df = df.loc[mask].copy()

        try:
            import pandas as pd

            tick_ns_tq = (
                pd.to_numeric(df.get("datetime"), errors="coerce")
                .dropna()
                .astype("int64")
                .tolist()
            )
        except Exception:
            tick_ns_tq = [int(x) for x in (df.get("datetime") if df is not None else []) if x is not None]
        tick_ns_tq.sort()

        checked = 0
        present = 0
        missing = 0
        max_segments = int(tqsdk_check_max_segments)
        for seg in day_missing_segments:
            if checked >= max_segments:
                break
            start_sec = int(datetime.fromisoformat(str(seg["start_ts"])).timestamp())
            end_sec = int(datetime.fromisoformat(str(seg["end_ts"])).timestamp())
            start_ns = int(start_sec * 1_000_000_000)
            end_ns = int((end_sec + 1) * 1_000_000_000 - 1)
            j = bisect_left(tick_ns_tq, start_ns)
            has_data = bool(j < len(tick_ns_tq) and tick_ns_tq[j] <= end_ns)
            seg["tqsdk_status"] = "provider_has_data" if has_data else "provider_missing"
            checked += 1
            if has_data:
                present += 1
            else:
                missing += 1

        return {
            "checked_days": 1,
            "checked_segments": int(checked),
            "provider_missing_segments": int(missing),
            "provider_has_data_segments": int(present),
            "errors": 0,
            "error": None,
        }
    except Exception as e:
        return {
            "checked_days": 0,
            "checked_segments": 0,
            "provider_missing_segments": 0,
            "provider_has_data_segments": 0,
            "errors": 1,
            "error": str(e),
        }


__all__ = ["verify_missing_segments_with_tqsdk"]

