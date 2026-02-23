from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

ValueType = Literal["str", "int", "float", "bool", "json"]


@dataclass(frozen=True)
class ConfigField:
    key: str
    value_type: ValueType
    default: Any
    domain: str
    description: str


MANAGED_FIELDS: dict[str, ConfigField] = {
    "GHTRADER_MAX_PARALLEL_TQSDK_JOBS": ConfigField(
        key="GHTRADER_MAX_PARALLEL_TQSDK_JOBS",
        value_type="int",
        default=4,
        domain="control",
        description="Max parallel TqSdk-heavy jobs in dashboard scheduler.",
    ),
    "GHTRADER_PIPELINE_ENFORCE_HEALTH": ConfigField(
        key="GHTRADER_PIPELINE_ENFORCE_HEALTH",
        value_type="bool",
        default=True,
        domain="control",
        description="Global pipeline health gate.",
    ),
    "GHTRADER_MAIN_SCHEDULE_ENFORCE_HEALTH": ConfigField(
        key="GHTRADER_MAIN_SCHEDULE_ENFORCE_HEALTH",
        value_type="bool",
        default=True,
        domain="control",
        description="Main schedule health gate.",
    ),
    "GHTRADER_MAIN_L5_ENFORCE_HEALTH": ConfigField(
        key="GHTRADER_MAIN_L5_ENFORCE_HEALTH",
        value_type="bool",
        default=True,
        domain="control",
        description="Main L5 health gate.",
    ),
    "GHTRADER_LOCK_WAIT_TIMEOUT_S": ConfigField(
        key="GHTRADER_LOCK_WAIT_TIMEOUT_S",
        value_type="float",
        default=120.0,
        domain="control",
        description="Lock wait timeout in seconds.",
    ),
    "GHTRADER_LOCK_POLL_INTERVAL_S": ConfigField(
        key="GHTRADER_LOCK_POLL_INTERVAL_S",
        value_type="float",
        default=1.0,
        domain="control",
        description="Lock polling interval in seconds.",
    ),
    "GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT": ConfigField(
        key="GHTRADER_LOCK_FORCE_CANCEL_ON_TIMEOUT",
        value_type="bool",
        default=True,
        domain="control",
        description="Whether lock owner is preempted after timeout.",
    ),
    "GHTRADER_LOCK_PREEMPT_GRACE_S": ConfigField(
        key="GHTRADER_LOCK_PREEMPT_GRACE_S",
        value_type="float",
        default=8.0,
        domain="control",
        description="Grace period before lock preempt SIGKILL.",
    ),
    "GHTRADER_MAIN_L5_TOTAL_WORKERS": ConfigField(
        key="GHTRADER_MAIN_L5_TOTAL_WORKERS",
        value_type="int",
        default=0,
        domain="data.main_l5",
        description="Total download worker budget for one main_l5 run (0=auto).",
    ),
    "GHTRADER_MAIN_L5_SEGMENT_WORKERS": ConfigField(
        key="GHTRADER_MAIN_L5_SEGMENT_WORKERS",
        value_type="int",
        default=0,
        domain="data.main_l5",
        description="Parallel segment workers for main_l5 (0=auto).",
    ),
    "GHTRADER_MAIN_L5_MISSING_DAYS_TOLERANCE": ConfigField(
        key="GHTRADER_MAIN_L5_MISSING_DAYS_TOLERANCE",
        value_type="int",
        default=0,
        domain="data.main_l5",
        description="Provider-missing-day tolerance for main_l5 health gate.",
    ),
    "GHTRADER_WORKERS_GLOBAL_MAX": ConfigField(
        key="GHTRADER_WORKERS_GLOBAL_MAX",
        value_type="int",
        default=128,
        domain="workers",
        description="Global max workers across domains.",
    ),
    "GHTRADER_QDB_PG_NET_CONNECTION_LIMIT": ConfigField(
        key="GHTRADER_QDB_PG_NET_CONNECTION_LIMIT",
        value_type="int",
        default=512,
        domain="workers",
        description="QuestDB PG connection limit mirrored on app side.",
    ),
    "GHTRADER_QDB_CONN_RESERVE": ConfigField(
        key="GHTRADER_QDB_CONN_RESERVE",
        value_type="int",
        default=32,
        domain="workers",
        description="Reserved QuestDB PG connections not used by worker pools.",
    ),
    "GHTRADER_DIAGNOSE_MAX_WORKERS": ConfigField(
        key="GHTRADER_DIAGNOSE_MAX_WORKERS",
        value_type="int",
        default=32,
        domain="workers",
        description="Max workers for diagnose/check paths.",
    ),
    "GHTRADER_DOWNLOAD_MAX_WORKERS": ConfigField(
        key="GHTRADER_DOWNLOAD_MAX_WORKERS",
        value_type="int",
        default=16,
        domain="workers",
        description="Max workers for download/repair paths.",
    ),
    "GHTRADER_DOWNLOAD_QDB_CONN_BUDGET": ConfigField(
        key="GHTRADER_DOWNLOAD_QDB_CONN_BUDGET",
        value_type="int",
        default=0,
        domain="workers",
        description="Download-side QuestDB connection budget override.",
    ),
    "GHTRADER_QUERY_MAX_WORKERS": ConfigField(
        key="GHTRADER_QUERY_MAX_WORKERS",
        value_type="int",
        default=64,
        domain="workers",
        description="Max workers for SQL/query paths.",
    ),
    "GHTRADER_INDEX_BOOTSTRAP_WORKERS": ConfigField(
        key="GHTRADER_INDEX_BOOTSTRAP_WORKERS",
        value_type="int",
        default=32,
        domain="workers",
        description="Max workers for index/bootstrap paths.",
    ),
    "GHTRADER_PROGRESS_EVERY_S": ConfigField(
        key="GHTRADER_PROGRESS_EVERY_S",
        value_type="float",
        default=15.0,
        domain="runtime",
        description="Progress log interval in seconds.",
    ),
    "GHTRADER_PROGRESS_EVERY_N": ConfigField(
        key="GHTRADER_PROGRESS_EVERY_N",
        value_type="int",
        default=25,
        domain="runtime",
        description="Progress log interval in item count.",
    ),
}

# Dynamic policy keys (per-var/per-session style) still map to env-like names.
MANAGED_PREFIXES: tuple[str, ...] = (
    "GHTRADER_L5_VALIDATE_",
    "GHTRADER_MAIN_L5_MISSING_DAYS_TOLERANCE_",
)

# These remain env/bootstrap/secrets and are not migrated to config.db.
ALLOWED_ENV_PREFIXES: tuple[str, ...] = (
    "TQ_",
    "TQSDK_",
    "GHTRADER_JOB_",
)
ALLOWED_ENV_KEYS: set[str] = {
    "GHTRADER_LIVE_ENABLED",
    "GHTRADER_DATA_DIR",
    "GHTRADER_ARTIFACTS_DIR",
    "GHTRADER_RUNS_DIR",
    "GHTRADER_DASHBOARD_TOKEN",
    "GHTRADER_TQ_ACCOUNT_PROFILES",
    "GHTRADER_QUESTDB_PG_PASSWORD",
    "GHTRADER_DISABLE_DOTENV",
    "GHTRADER_CONFIG_ALLOW_LEGACY_ENV",
    "PYTEST_CURRENT_TEST",
}

UI_PROTECTED_PREFIXES: tuple[str, ...] = (
    "TQ_",
    "TQSDK_",
    "GHTRADER_JOB_",
)
UI_PROTECTED_KEYS: set[str] = set(ALLOWED_ENV_KEYS) | {
    "GHTRADER_DISABLE_TQSDK_SCHEDULER",
    "GHTRADER_ENABLE_TQSDK_SCHEDULER_IN_TESTS",
    "GHTRADER_DISABLE_GATEWAY_SUPERVISOR",
    "GHTRADER_ENABLE_GATEWAY_SUPERVISOR_IN_TESTS",
    "GHTRADER_DISABLE_STRATEGY_SUPERVISOR",
    "GHTRADER_ENABLE_STRATEGY_SUPERVISOR_IN_TESTS",
    "GHTRADER_LOG_LEVEL",
    "GHTRADER_JOB_VERBOSE",
    "GHTRADER_QUESTDB_SERVER_CONF_PATH",
    "GHTRADER_QUESTDB_SYSTEMD_SERVICE",
    "GHTRADER_QUESTDB_SYSTEMD_SCOPE",
}

NUMERIC_BOUNDS: dict[str, tuple[float | None, float | None]] = {
    "GHTRADER_MAX_PARALLEL_TQSDK_JOBS": (1, None),
    "GHTRADER_LOCK_WAIT_TIMEOUT_S": (0.0, None),
    "GHTRADER_LOCK_POLL_INTERVAL_S": (0.1, None),
    "GHTRADER_LOCK_PREEMPT_GRACE_S": (0.5, None),
    "GHTRADER_MAIN_L5_TOTAL_WORKERS": (0, None),
    "GHTRADER_MAIN_L5_SEGMENT_WORKERS": (0, None),
    "GHTRADER_WORKERS_GLOBAL_MAX": (1, None),
    "GHTRADER_QDB_PG_NET_CONNECTION_LIMIT": (1, None),
    "GHTRADER_QDB_CONN_RESERVE": (0, None),
    "GHTRADER_DIAGNOSE_MAX_WORKERS": (1, None),
    "GHTRADER_DOWNLOAD_MAX_WORKERS": (1, None),
    "GHTRADER_DOWNLOAD_QDB_CONN_BUDGET": (0, None),
    "GHTRADER_QUERY_MAX_WORKERS": (1, None),
    "GHTRADER_INDEX_BOOTSTRAP_WORKERS": (1, None),
    "GHTRADER_PROGRESS_EVERY_S": (0.1, None),
    "GHTRADER_PROGRESS_EVERY_N": (1, None),
}


def parse_bool(raw: Any, default: bool = False) -> bool:
    v = str(raw if raw is not None else "").strip().lower()
    if not v:
        return bool(default)
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def value_type_for_key(key: str) -> ValueType:
    spec = MANAGED_FIELDS.get(str(key or ""))
    if spec is not None:
        return spec.value_type

    k = str(key or "")
    if k.startswith("GHTRADER_L5_VALIDATE_"):
        if any(token in k for token in ("_BLOCKING", "_CACHE_ENABLED")):
            return "bool"
        if any(
            token in k
            for token in (
                "_MAX_DAYS",
                "_MAX_SEGMENTS",
                "_MAX_EVENTS",
                "_SEGMENTS_SAMPLE",
                "_GT30",
                "_PER_DAY",
                "_TOLERANCE",
            )
        ):
            return "int"
        if any(token in k for token in ("_RATIO", "_THRESHOLD_S", "_BLOCK_S")):
            return "float"
    if k.startswith("GHTRADER_MAIN_L5_MISSING_DAYS_TOLERANCE"):
        return "int"
    return "str"


def normalize_to_string(key: str, value: Any) -> str:
    vt = value_type_for_key(key)
    if vt == "bool":
        return "true" if parse_bool(value, False) else "false"
    if vt == "int":
        return str(int(value))
    if vt == "float":
        return str(float(value))
    if vt == "json":
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value if value is not None else "")


def key_is_managed(key: str) -> bool:
    k = str(key or "")
    if k in MANAGED_FIELDS:
        return True
    if any(k.startswith(prefix) for prefix in MANAGED_PREFIXES):
        return True
    if not k.startswith("GHTRADER_"):
        return False
    if k in ALLOWED_ENV_KEYS:
        return False
    if any(k.startswith(prefix) for prefix in ALLOWED_ENV_PREFIXES):
        return False
    return True


def key_is_ui_editable(key: str) -> bool:
    k = str(key or "").strip()
    if not k:
        return False
    if not key_is_managed(k):
        return False
    if k in UI_PROTECTED_KEYS:
        return False
    if any(k.startswith(prefix) for prefix in UI_PROTECTED_PREFIXES):
        return False
    return True


def _numeric_bounds_for_key(key: str) -> tuple[float | None, float | None]:
    k = str(key or "").strip()
    bounds = NUMERIC_BOUNDS.get(k)
    if bounds is not None:
        return bounds
    if "_RATIO" in k:
        return (0.0, 1.0)
    if k.endswith("_PORT"):
        return (1.0, 65535.0)
    if any(tok in k for tok in ("_WORKERS", "_MAX_", "_COUNT", "_BUDGET", "_TTL_S", "_SECONDS", "_MINUTES")):
        return (0.0, None)
    if any(tok in k for tok in ("_TIMEOUT", "_WAIT")):
        return (0.0, None)
    return (None, None)


def validate_value_for_key(key: str, value: Any) -> Any:
    vt = value_type_for_key(key)
    if vt not in {"int", "float"}:
        return value
    low, high = _numeric_bounds_for_key(key)
    num = float(value)
    if low is not None and num < float(low):
        raise ValueError(f"value for {key} must be >= {low}")
    if high is not None and num > float(high):
        raise ValueError(f"value for {key} must be <= {high}")
    return value


def coerce_value_for_key(key: str, value: Any, *, value_type_override: str | None = None) -> Any:
    vt = str(value_type_override or value_type_for_key(key)).strip().lower()
    if value is None:
        raise ValueError(f"{key} value is required")
    if vt == "bool":
        out = bool(parse_bool(value, False))
    elif vt == "int":
        raw = str(value).strip()
        if not raw:
            raise ValueError(f"{key} requires integer value")
        out = int(raw)
    elif vt == "float":
        raw = str(value).strip()
        if not raw:
            raise ValueError(f"{key} requires float value")
        out = float(raw)
    elif vt == "json":
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                raise ValueError(f"{key} requires JSON value")
            out = json.loads(raw)
        else:
            out = value
    else:
        out = str(value)
    return validate_value_for_key(key, out)


def schema_descriptor_for_key(key: str) -> dict[str, Any]:
    k = str(key or "").strip()
    spec = MANAGED_FIELDS.get(k)
    vt = value_type_for_key(k)
    default = spec.default if spec is not None else None
    default_str = normalize_to_string(k, default) if default is not None else None
    return {
        "key": k,
        "value_type": vt,
        "default": default_str,
        "domain": (spec.domain if spec is not None else "dynamic"),
        "description": (spec.description if spec is not None else ""),
        "editable": bool(key_is_ui_editable(k)),
    }


def list_schema_descriptors(*, snapshot_keys: list[str] | None = None) -> list[dict[str, Any]]:
    keys: set[str] = set(MANAGED_FIELDS.keys())
    for k in list(snapshot_keys or []):
        kk = str(k or "").strip()
        if kk and key_is_managed(kk):
            keys.add(kk)
    rows = [schema_descriptor_for_key(k) for k in sorted(keys)]
    return rows


def extract_managed_env(env: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (env or {}).items():
        kk = str(k or "").strip()
        if not kk:
            continue
        vv = "" if v is None else str(v).strip()
        if not vv:
            continue
        if key_is_managed(kk):
            out[kk] = vv
    return out


def detect_legacy_managed_env(env: dict[str, str]) -> list[str]:
    found: list[str] = []
    for k, v in (env or {}).items():
        kk = str(k or "").strip()
        if not kk or v is None:
            continue
        if str(v).strip() == "":
            continue
        if kk in ALLOWED_ENV_KEYS:
            continue
        if any(kk.startswith(prefix) for prefix in ALLOWED_ENV_PREFIXES):
            continue
        if key_is_managed(kk):
            found.append(kk)
    found.sort()
    return found

