from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ghtrader.questdb.client import QuestDBQueryConfig, connect_pg, make_questdb_query_config_from_env

TICK_GAPS_TABLE = "ghtrader_tick_gaps_v2"


@dataclass(frozen=True)
class TickGapLedgerRow:
    trading_day: str
    tick_count_actual: int
    tick_count_expected_min: int
    tick_count_expected_max: int
    max_interval_ms: int
    abnormal_gaps_count: int
    critical_gaps_count: int
    largest_gap_duration_ms: int
    updated_at: str | None


def list_tick_gap_ledger(
    *,
    symbol: str,
    ticks_kind: str = "main_l5",
    dataset_version: str = "v2",
    limit: int = 200,
    cfg: QuestDBQueryConfig | None = None,
    table: str = TICK_GAPS_TABLE,
    connect_timeout_s: int = 2,
) -> list[TickGapLedgerRow]:
    sym = str(symbol or "").strip()
    if not sym:
        return []
    qcfg = cfg or make_questdb_query_config_from_env()
    lim = max(1, min(int(limit or 200), 5000))
    tbl = str(table).strip() or TICK_GAPS_TABLE

    sql = (
        "SELECT trading_day, tick_count_actual, tick_count_expected_min, tick_count_expected_max, "
        "max_interval_ms, abnormal_gaps_count, critical_gaps_count, largest_gap_duration_ms, updated_at "
        f"FROM {tbl} "
        "WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s "
        "ORDER BY cast(trading_day as string) DESC "
        "LIMIT %s"
    )
    out: list[TickGapLedgerRow] = []
    with connect_pg(qcfg, connect_timeout_s=connect_timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [sym, str(ticks_kind), str(dataset_version), lim])
            for row in cur.fetchall() or []:
                out.append(
                    TickGapLedgerRow(
                        trading_day=str(row[0] or "").strip(),
                        tick_count_actual=int(row[1] or 0),
                        tick_count_expected_min=int(row[2] or 0),
                        tick_count_expected_max=int(row[3] or 0),
                        max_interval_ms=int(row[4] or 0),
                        abnormal_gaps_count=int(row[5] or 0),
                        critical_gaps_count=int(row[6] or 0),
                        largest_gap_duration_ms=int(row[7] or 0),
                        updated_at=(row[8].isoformat() if row[8] else None),
                    )
                )
    return out


def list_tick_gap_ledger_dicts(**kwargs: Any) -> list[dict[str, Any]]:
    return [row.__dict__ for row in list_tick_gap_ledger(**kwargs)]

