from __future__ import annotations

import json
import os
from typing import Any

import click

from ghtrader.config_service import get_config_resolver


def _coerce_value(value: str, value_type: str) -> Any:
    t = str(value_type or "str").strip().lower()
    if t == "int":
        return int(str(value).strip())
    if t == "float":
        return float(str(value).strip())
    if t == "bool":
        raw = str(value).strip().lower()
        return raw in {"1", "true", "yes", "on"}
    if t == "json":
        return json.loads(str(value))
    return str(value)


def register(main: click.Group) -> None:
    @main.group("config")
    @click.pass_context
    def config_group(ctx: click.Context) -> None:
        """Manage Config Service revisions and effective values."""
        _ = ctx

    @config_group.command("effective")
    @click.option("--prefix", default="", type=str, help="Only show keys with this prefix")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON")
    def config_effective(prefix: str, as_json: bool) -> None:
        resolver = get_config_resolver()
        values = resolver.snapshot()
        pfx = str(prefix or "").strip()
        if pfx:
            values = {k: v for k, v in values.items() if k.startswith(pfx)}
        payload = {
            "ok": True,
            "revision": int(resolver.revision),
            "config_hash": str(resolver.snapshot_hash),
            "values": values,
        }
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return
        click.echo(f"revision={payload['revision']} config_hash={payload['config_hash']} keys={len(values)}")
        for k in sorted(values.keys()):
            click.echo(f"{k}={values[k]}")

    @config_group.command("history")
    @click.option("--limit", default=30, show_default=True, type=int, help="Max revisions")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON")
    def config_history(limit: int, as_json: bool) -> None:
        resolver = get_config_resolver()
        rows = resolver.list_revisions(limit=max(1, min(int(limit), 500)))
        payload = {
            "ok": True,
            "items": [
                {
                    "revision": int(r.revision),
                    "created_at": str(r.created_at),
                    "actor": str(r.actor),
                    "reason": str(r.reason),
                    "config_hash": str(r.snapshot_hash),
                    "changed_keys": list(r.changed_keys),
                }
                for r in rows
            ],
        }
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return
        for row in payload["items"]:
            click.echo(
                f"rev={row['revision']} hash={row['config_hash']} actor={row['actor']} "
                f"changed={len(row['changed_keys'])} reason={row['reason']}"
            )

    @config_group.command("set")
    @click.option("--key", required=True, type=str, help="Config key")
    @click.option("--value", required=True, type=str, help="Config value")
    @click.option(
        "--type",
        "value_type",
        default="str",
        show_default=True,
        type=click.Choice(["str", "int", "float", "bool", "json"]),
        help="Value type for coercion before save",
    )
    @click.option("--reason", default="cli_set", show_default=True, type=str, help="Change reason")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON")
    def config_set(key: str, value: str, value_type: str, reason: str, as_json: bool) -> None:
        from ghtrader.config_service.schema import key_is_env_only, key_is_managed

        key_s = str(key or "").strip()
        if not key_is_managed(key_s):
            detail = "env-only key" if key_is_env_only(key_s) else "unmanaged key"
            raise click.ClickException(f"refusing to set {key_s}: {detail}")
        resolver = get_config_resolver()
        coerced = _coerce_value(value, value_type)
        try:
            rev = resolver.set_values(
                values={key_s: coerced},
                actor="cli",
                reason=str(reason or "cli_set"),
                action="set",
            )
        except ValueError as e:
            raise click.ClickException(str(e)) from e
        payload = {
            "ok": True,
            "revision": int(rev.revision),
            "config_hash": str(rev.snapshot_hash),
            "changed_keys": list(rev.changed_keys),
        }
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return
        click.echo(
            f"updated revision={payload['revision']} config_hash={payload['config_hash']} changed={payload['changed_keys']}"
        )

    @config_group.command("rollback")
    @click.option("--revision", required=True, type=int, help="Target revision")
    @click.option("--reason", default="", type=str, help="Rollback reason")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON")
    def config_rollback(revision: int, reason: str, as_json: bool) -> None:
        resolver = get_config_resolver()
        rev = resolver.rollback(
            revision=int(revision),
            actor="cli",
            reason=(str(reason).strip() or f"rollback_to:{int(revision)}"),
        )
        payload = {
            "ok": True,
            "revision": int(rev.revision),
            "config_hash": str(rev.snapshot_hash),
            "changed_keys": list(rev.changed_keys),
            "rollback_to": int(revision),
        }
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return
        click.echo(
            f"rolled back to={payload['rollback_to']} -> new_revision={payload['revision']} hash={payload['config_hash']}"
        )

    @config_group.command("migrate-env")
    @click.option("--reason", default="cli_migrate_env", show_default=True, type=str, help="Migration reason")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON")
    def config_migrate_env(reason: str, as_json: bool) -> None:
        resolver = get_config_resolver()
        rev = resolver.store.migrate_from_env(
            env={str(k): str(v if v is not None else "") for k, v in os.environ.items()},
            actor="cli",
            reason=str(reason or "cli_migrate_env"),
        )
        resolver.refresh(force=True)
        payload = {
            "ok": True,
            "revision": int(rev.revision),
            "config_hash": str(rev.snapshot_hash),
            "migrated_count": int(len(rev.changed_keys)),
            "changed_keys": list(rev.changed_keys),
        }
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return
        click.echo(
            f"migrated managed env keys: count={payload['migrated_count']} "
            f"revision={payload['revision']} hash={payload['config_hash']}"
        )
        click.echo("Remove migrated business keys from .env/accounts.env before normal startup.")

