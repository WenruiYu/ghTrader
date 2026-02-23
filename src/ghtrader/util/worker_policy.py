from __future__ import annotations

import os

from ghtrader.config_service import get_config_resolver


def _cfg_int(key: str, default: int) -> int:
    return get_config_resolver().get_int(key, int(default))


def resolve_worker_count(
    *,
    kind: str,
    requested: int | None = None,
    cpu_count: int | None = None,
) -> int:
    """
    Resolve effective worker count for a task kind.

    Rules:
    - If requested > 0, honor it but clamp to caps.
    - If requested <= 0 / None, compute auto value and clamp to caps.
    """
    cpu = max(1, int(cpu_count or os.cpu_count() or 8))
    global_max = max(1, _cfg_int("GHTRADER_WORKERS_GLOBAL_MAX", 128))
    qdb_limit = max(1, _cfg_int("GHTRADER_QDB_PG_NET_CONNECTION_LIMIT", 512))
    qdb_reserve = max(0, _cfg_int("GHTRADER_QDB_CONN_RESERVE", 32))
    qdb_cap = max(1, qdb_limit - qdb_reserve)

    kind_norm = str(kind or "").strip().lower()
    if kind_norm in {"check", "diagnose", "health"}:
        auto = max(4, cpu // 4)
        cap = min(global_max, _cfg_int("GHTRADER_DIAGNOSE_MAX_WORKERS", 32), qdb_cap)
    elif kind_norm in {"download", "repair"}:
        auto = max(2, cpu // 32)
        # Download/build paths can still trigger QuestDB writes/reads; keep an
        # explicit connection budget linked to the global QuestDB PG limits.
        download_qdb_budget = max(1, _cfg_int("GHTRADER_DOWNLOAD_QDB_CONN_BUDGET", max(8, qdb_cap // 4)))
        cap = min(global_max, _cfg_int("GHTRADER_DOWNLOAD_MAX_WORKERS", 16), download_qdb_budget)
    elif kind_norm in {"query", "coverage", "sql"}:
        auto = max(2, cpu // 16)
        cap = min(global_max, _cfg_int("GHTRADER_QUERY_MAX_WORKERS", 64), qdb_cap)
    elif kind_norm in {"index", "index_bootstrap", "bootstrap"}:
        auto = max(1, cpu // 4)
        cap = min(global_max, _cfg_int("GHTRADER_INDEX_BOOTSTRAP_WORKERS", 32), cpu)
    else:
        auto = max(1, cpu // 4)
        cap = min(global_max, cpu)

    cap = max(1, int(cap))
    val = int(requested) if requested is not None and int(requested) > 0 else int(auto)
    return max(1, min(val, cap))
