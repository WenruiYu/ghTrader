from __future__ import annotations

import uuid
from pathlib import Path


def accounts_env_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "accounts.env"


def read_accounts_env_values(*, runs_dir: Path) -> dict[str, str]:
    path = accounts_env_path(runs_dir=runs_dir)
    if not path.exists():
        return {}
    try:
        from dotenv import dotenv_values

        raw = dotenv_values(path)
        out: dict[str, str] = {}
        for k, v in raw.items():
            if not k:
                continue
            out[str(k)] = "" if v is None else str(v)
        return out
    except Exception:
        # Fall back to an empty mapping; callers should handle "no accounts" gracefully.
        return {}


def _dotenv_quote(value: str) -> str:
    v = str(value or "")
    v = v.replace("\\", "\\\\").replace('"', '\\"').replace("\r", "\\r").replace("\n", "\\n")
    return f'"{v}"'


def write_accounts_env_atomic(*, runs_dir: Path, env: dict[str, str]) -> None:
    path = accounts_env_path(runs_dir=runs_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "# ghTrader dashboard-managed broker accounts",
        "# WARNING: contains secrets. Do not commit.",
        "",
    ]

    # Deterministic-ish ordering for readability.
    keys: list[str] = []
    if "GHTRADER_TQ_ACCOUNT_PROFILES" in env:
        keys.append("GHTRADER_TQ_ACCOUNT_PROFILES")
    for k in ["TQ_BROKER_ID", "TQ_ACCOUNT_ID", "TQ_ACCOUNT_PASSWORD"]:
        if k in env:
            keys.append(k)
    for k in sorted([k for k in env.keys() if k not in set(keys)]):
        keys.append(k)

    lines: list[str] = []
    lines.extend(header)
    for k in keys:
        kk = str(k).strip()
        if not kk:
            continue
        vv = env.get(k, "")
        lines.append(f"{kk}={_dotenv_quote(vv)}")
    lines.append("")  # trailing newline
    content = "\n".join(lines)

    tmp = path.with_suffix(f".tmp-{uuid.uuid4().hex}")
    tmp.write_text(content, encoding="utf-8")
    try:
        tmp.chmod(0o600)
    except Exception:
        pass
    tmp.replace(path)
    try:
        path.chmod(0o600)
    except Exception:
        pass


def canonical_profile(p: str) -> str:
    try:
        from ghtrader.tq.runtime import canonical_account_profile

        return canonical_account_profile(p)
    except Exception:
        return str(p or "").strip().lower() or "default"


def _profile_env_suffixes(profile: str) -> list[str]:
    p = canonical_profile(profile)
    if p == "default":
        return [""]
    up = "".join([ch.upper() if ch.isalnum() else "_" for ch in p]).strip("_")
    lo = "".join([ch.lower() if ch.isalnum() else "_" for ch in p]).strip("_")
    out: list[str] = []
    for s in [up, lo]:
        if s and s not in out:
            out.append(s)
    return out


def parse_profiles_csv(raw: str) -> list[str]:
    out: list[str] = []
    for part in [p.strip() for p in str(raw or "").split(",") if p.strip()]:
        p = canonical_profile(part)
        if p and p != "default" and p not in out:
            out.append(p)
    return out


def _set_profiles_csv(env: dict[str, str], profiles: list[str]) -> None:
    ps = [p for p in profiles if p and p != "default"]
    if ps:
        env["GHTRADER_TQ_ACCOUNT_PROFILES"] = ",".join(ps)
    else:
        env.pop("GHTRADER_TQ_ACCOUNT_PROFILES", None)


def accounts_env_upsert_profile(
    *,
    runs_dir: Path,
    profile: str,
    broker_id: str,
    account_id: str,
    password: str,
) -> None:
    p = canonical_profile(profile)
    env = read_accounts_env_values(runs_dir=runs_dir)

    # Maintain profile registry (excluding default).
    profiles = parse_profiles_csv(env.get("GHTRADER_TQ_ACCOUNT_PROFILES", ""))
    if p != "default" and p not in profiles:
        profiles.append(p)
    _set_profiles_csv(env, profiles)

    b = str(broker_id or "").strip()
    a = str(account_id or "").strip()
    pw = str(password or "").strip()

    if p == "default":
        env["TQ_BROKER_ID"] = b
        env["TQ_ACCOUNT_ID"] = a
        env["TQ_ACCOUNT_PASSWORD"] = pw
    else:
        suf = _profile_env_suffixes(p)[0]  # uppercase preferred
        env[f"TQ_BROKER_ID_{suf}"] = b
        env[f"TQ_ACCOUNT_ID_{suf}"] = a
        env[f"TQ_ACCOUNT_PASSWORD_{suf}"] = pw

    write_accounts_env_atomic(runs_dir=runs_dir, env=env)


def accounts_env_delete_profile(*, runs_dir: Path, profile: str) -> None:
    p = canonical_profile(profile)
    env = read_accounts_env_values(runs_dir=runs_dir)

    if p == "default":
        env.pop("TQ_BROKER_ID", None)
        env.pop("TQ_ACCOUNT_ID", None)
        env.pop("TQ_ACCOUNT_PASSWORD", None)
    else:
        for suf in _profile_env_suffixes(p):
            if not suf:
                continue
            env.pop(f"TQ_BROKER_ID_{suf}", None)
            env.pop(f"TQ_ACCOUNT_ID_{suf}", None)
            env.pop(f"TQ_ACCOUNT_PASSWORD_{suf}", None)

        profiles = parse_profiles_csv(env.get("GHTRADER_TQ_ACCOUNT_PROFILES", ""))
        profiles = [x for x in profiles if x != p]
        _set_profiles_csv(env, profiles)

    write_accounts_env_atomic(runs_dir=runs_dir, env=env)


def accounts_env_get_profile_values(*, env: dict[str, str], profile: str) -> dict[str, str]:
    p = canonical_profile(profile)
    if p == "default":
        return {
            "broker_id": str(env.get("TQ_BROKER_ID", "") or "").strip(),
            "account_id": str(env.get("TQ_ACCOUNT_ID", "") or "").strip(),
        }

    for suf in _profile_env_suffixes(p):
        if not suf:
            continue
        broker = str(env.get(f"TQ_BROKER_ID_{suf}", "") or "").strip()
        acc = str(env.get(f"TQ_ACCOUNT_ID_{suf}", "") or "").strip()
        if broker or acc:
            return {"broker_id": broker, "account_id": acc}
    return {"broker_id": "", "account_id": ""}


def accounts_env_is_configured(*, env: dict[str, str], profile: str) -> bool:
    p = canonical_profile(profile)
    if p == "default":
        broker = str(env.get("TQ_BROKER_ID", "") or "").strip()
        acc = str(env.get("TQ_ACCOUNT_ID", "") or "").strip()
        pwd = str(env.get("TQ_ACCOUNT_PASSWORD", "") or "").strip()
        return bool(broker and acc and pwd)
    for suf in _profile_env_suffixes(p):
        if not suf:
            continue
        broker = str(env.get(f"TQ_BROKER_ID_{suf}", "") or "").strip()
        acc = str(env.get(f"TQ_ACCOUNT_ID_{suf}", "") or "").strip()
        pwd = str(env.get(f"TQ_ACCOUNT_PASSWORD_{suf}", "") or "").strip()
        if broker and acc and pwd:
            return True
    return False


__all__ = [
    "accounts_env_path",
    "read_accounts_env_values",
    "write_accounts_env_atomic",
    "canonical_profile",
    "parse_profiles_csv",
    "accounts_env_upsert_profile",
    "accounts_env_delete_profile",
    "accounts_env_get_profile_values",
    "accounts_env_is_configured",
]

