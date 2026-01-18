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
        from datetime import datetime, timezone

        from ghtrader.tq.runtime import trading_day_from_ts_ns

        now_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
        trading_day = trading_day_from_ts_ns(now_ns, data_dir=data_dir)

    ex, var = _extract_exchange_variety(symbol)

    # QuestDB is the canonical schedule source (PRD 5.12.5).
    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.main_schedule import resolve_main_contract

    cfg = make_questdb_query_config_from_env()
    main_contract, _, _ = resolve_main_contract(cfg=cfg, exchange=ex, variety=var, trading_day=trading_day, connect_timeout_s=2)
    return str(main_contract)

