from __future__ import annotations

import click
import structlog

log = structlog.get_logger()


def register(main: click.Group) -> None:
    """
    Register `ghtrader features ...` commands onto the root CLI group.
    """

    @main.group("features")
    @click.pass_context
    def features_group(ctx: click.Context) -> None:
        """Feature store utilities (list, info, sync)."""
        _ = ctx

    @features_group.command("list")
    @click.option("--with-metadata/--no-metadata", default=True, show_default=True, help="Include factor metadata")
    def features_list(with_metadata: bool) -> None:
        """List all registered factors."""
        import json

        from ghtrader.datasets.features import list_factors, list_factors_with_metadata

        if with_metadata:
            factors = list_factors_with_metadata()
            for f in factors:
                log.info(
                    "factor",
                    name=f["name"],
                    input_columns=f["input_columns"],
                    lookback=f["lookback_ticks"],
                    ttl=f["ttl_seconds"],
                )
            print(json.dumps(factors, indent=2))
        else:
            factors = list_factors()
            for name in factors:
                print(name)

    @features_group.command("info")
    @click.argument("factor_name")
    def features_info(factor_name: str) -> None:
        """Show detailed information about a factor."""
        import json

        from ghtrader.datasets.features import get_factor

        try:
            factor = get_factor(factor_name)
            meta = factor.get_metadata()
            print(json.dumps(meta, indent=2))
        except ValueError as e:
            print(f"Error: {e}")
            raise SystemExit(1)

    @features_group.command("sync")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    def features_sync(host: str, pg_port: int) -> None:
        """Sync feature definitions to QuestDB registry table."""
        from ghtrader.questdb.client import QuestDBQueryConfig, make_questdb_query_config_from_env
        from ghtrader.datasets.feature_store import get_feature_registry
        from ghtrader.datasets.features import list_factors_with_metadata

        base = make_questdb_query_config_from_env()
        cfg = QuestDBQueryConfig(
            host=str(host or base.host),
            pg_port=int(pg_port or base.pg_port),
            pg_user=str(base.pg_user),
            pg_password=str(base.pg_password),
            pg_dbname=str(base.pg_dbname),
        )

        # Register all factors into the registry
        registry = get_feature_registry()
        factors_meta = list_factors_with_metadata()

        for meta in factors_meta:
            registry.register(
                name=meta["name"],
                input_columns=tuple(meta["input_columns"]),
                lookback_ticks=meta["lookback_ticks"],
                ttl_seconds=meta["ttl_seconds"],
                output_dtype=meta["output_dtype"],
            )

        # Sync to QuestDB
        try:
            count = registry.sync_to_questdb(cfg)
            log.info("features.synced", count=count)
            print(f"Synced {count} feature definitions to QuestDB")
        except Exception as e:
            log.error("features.sync_failed", error=str(e))
            print(f"Error syncing to QuestDB: {e}")
            raise SystemExit(1)

    @features_group.command("registry")
    @click.option("--active-only/--all", default=True, show_default=True, help="Show only active features")
    def features_registry(active_only: bool) -> None:
        """Show registered feature definitions."""
        import json

        from ghtrader.datasets.feature_store import get_feature_registry

        registry = get_feature_registry()

        if active_only:
            features = registry.list_active()
        else:
            features = registry.list_all()

        if not features:
            print("No features registered in this session.")
            print("Use 'ghtrader features sync' to register factors and sync to QuestDB.")
            return

        for feat in features:
            print(json.dumps(feat.to_dict(), indent=2))

