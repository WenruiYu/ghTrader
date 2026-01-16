from __future__ import annotations

from datetime import date
from pathlib import Path


def is_continuous_alias(symbol: str) -> bool:
    # ghTrader uses TqSdk continuous main alias naming like: KQ.m@SHFE.cu
    return symbol.startswith("KQ.m@") and "." in symbol


def _extract_exchange_variety(symbol: str) -> tuple[str, str]:
    # KQ.m@SHFE.cu -> ("SHFE", "cu")
    s = str(symbol).strip()
    if not s.startswith("KQ.m@"):
        raise ValueError(f"Not a continuous alias: {symbol}")
    tail = s[len("KQ.m@") :]
    if "." not in tail:
        raise ValueError(f"Unexpected continuous alias format: {symbol}")
    ex, var = tail.split(".", 1)
    ex = ex.upper().strip()
    var = var.lower().strip()
    if not ex or not var:
        raise ValueError(f"Unexpected continuous alias format: {symbol}")
    return ex, var


def resolve_trading_symbol(*, symbol: str, data_dir: Path, trading_day: date | None = None) -> str:
    """
    Resolve a user-facing trading symbol to an actual underlying contract symbol.

    - Specific contracts pass through (e.g. SHFE.cu2602)
    - Continuous aliases (e.g. KQ.m@SHFE.cu) resolve using the persisted roll schedule
    """
    if not is_continuous_alias(symbol):
        return symbol

    if trading_day is None:
        # Best-effort trading-day determination (handles night session via 18:00 boundary).
        # We intentionally avoid full holiday logic here; schedule lookup will use <= and work well enough.
        from datetime import datetime, timezone

        # Ensure vendored tqsdk is importable if installed locally.
        # (Other modules add it to sys.path; keep resolver self-contained.)
        import sys

        tqsdk_path = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
        if tqsdk_path.exists() and str(tqsdk_path) not in sys.path:
            sys.path.insert(0, str(tqsdk_path))

        from tqsdk.datetime import _get_trading_day_from_timestamp, _timestamp_nano_to_datetime

        # Use CST-like trading day boundary (tqsdk uses its own 1990-based origin, but timestamp unit is ns).
        now = datetime.now(timezone.utc).astimezone(timezone.utc)
        now_ns = int(now.timestamp() * 1_000_000_000)
        td_ns = _get_trading_day_from_timestamp(now_ns)
        trading_day = _timestamp_nano_to_datetime(td_ns).date()

    ex, var = _extract_exchange_variety(symbol)

    # QuestDB is the canonical schedule source (PRD 5.12.5).
    from ghtrader.questdb_client import make_questdb_query_config_from_env
    from ghtrader.questdb_main_schedule import resolve_main_contract

    _ = data_dir  # unused; schedule is stored in QuestDB

    cfg = make_questdb_query_config_from_env()
    main_contract, _, _ = resolve_main_contract(cfg=cfg, exchange=ex, variety=var, trading_day=trading_day, connect_timeout_s=2)
    return str(main_contract)

