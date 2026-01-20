from __future__ import annotations

import os


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except Exception:
        return int(default)


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
    global_max = max(1, _env_int("GHTRADER_WORKERS_GLOBAL_MAX", 128))
    qdb_limit = max(1, _env_int("GHTRADER_QDB_PG_NET_CONNECTION_LIMIT", 512))
    qdb_reserve = max(0, _env_int("GHTRADER_QDB_CONN_RESERVE", 32))
    qdb_cap = max(1, qdb_limit - qdb_reserve)

    kind_norm = str(kind or "").strip().lower()
    if kind_norm in {"check", "diagnose", "health"}:
        auto = max(4, cpu // 4)
        cap = min(global_max, _env_int("GHTRADER_DIAGNOSE_MAX_WORKERS", 32), qdb_cap)
    elif kind_norm in {"download", "repair"}:
        auto = max(2, cpu // 32)
        cap = min(global_max, _env_int("GHTRADER_DOWNLOAD_MAX_WORKERS", 16))
    elif kind_norm in {"index", "index_bootstrap", "bootstrap"}:
        auto = max(1, cpu // 4)
        cap = min(global_max, _env_int("GHTRADER_INDEX_BOOTSTRAP_WORKERS", 32), cpu)
    else:
        auto = max(1, cpu // 4)
        cap = min(global_max, cpu)

    cap = max(1, int(cap))
    val = int(requested) if requested is not None and int(requested) > 0 else int(auto)
    return max(1, min(val, cap))
