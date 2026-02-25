from __future__ import annotations

from datetime import datetime
from pathlib import Path

import click
import structlog

log = structlog.get_logger()


def register(main: click.Group) -> None:
    """
    Register `ghtrader db ...` commands onto the root CLI group.
    """

    @main.group("db")
    @click.pass_context
    def db_group(ctx: click.Context) -> None:
        """Database/query-layer utilities (QuestDB)."""
        _ = ctx

    @db_group.command("questdb-health")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--connect-timeout", default=1, type=int, show_default=True, help="Connect timeout seconds")
    def db_questdb_health(
        host: str,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        connect_timeout: int,
    ) -> None:
        """Check QuestDB reachability via PGWire (SELECT 1)."""
        import json
        import time

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.client import QuestDBQueryConfig, connect_pg_safe

        h = str(host).strip() or str(get_questdb_host())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        t0 = time.time()
        try:
            cfg = QuestDBQueryConfig(host=h, pg_port=int(p), pg_user=u, pg_password=pw, pg_dbname=dbn)
            with connect_pg_safe(cfg, connect_timeout_s=int(connect_timeout), retries=1, backoff_s=0.2, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            dt_ms = int((time.time() - t0) * 1000.0)
            click.echo(
                json.dumps(
                    {"ok": True, "host": h, "pg_port": int(p), "pg_user": u, "pg_dbname": dbn, "latency_ms": dt_ms}
                )
            )
        except Exception as e:
            dt_ms = int((time.time() - t0) * 1000.0)
            click.echo(
                json.dumps(
                    {
                        "ok": False,
                        "host": h,
                        "pg_port": int(p),
                        "pg_user": u,
                        "pg_dbname": dbn,
                        "latency_ms": dt_ms,
                        "error": str(e),
                    }
                )
            )
            raise SystemExit(1)

    @db_group.command("questdb-init")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--ilp-port", default=0, type=int, help="QuestDB ILP port (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--main-l5-table", default="ghtrader_ticks_main_l5_v2", show_default=True, help="Derived main_l5 ticks table name")
    def db_questdb_init(
        host: str,
        ilp_port: int,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        main_l5_table: str,
    ) -> None:
        """Ensure required ghTrader tables exist in QuestDB (DDL via PGWire)."""
        import json

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_ilp_port,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend

        h = str(host).strip() or str(get_questdb_host())
        ilp = int(ilp_port) if int(ilp_port) > 0 else int(get_questdb_ilp_port())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        cfg = ServingDBConfig(
            backend="questdb",
            host=h,
            questdb_ilp_port=int(ilp),
            questdb_pg_port=int(p),
            questdb_pg_user=u,
            questdb_pg_password=pw,
            questdb_pg_dbname=dbn,
        )
        backend = make_serving_backend(cfg)

        # Ensure main_l5 table schema exists (idempotent + fail-fast contract checks).
        backend.ensure_table(table=str(main_l5_table), include_segment_metadata=True)
        from ghtrader.questdb.client import QuestDBQueryConfig
        from ghtrader.questdb.migrate import ensure_schema_v2

        schema_out = ensure_schema_v2(
            cfg=QuestDBQueryConfig(host=h, pg_port=int(p), pg_user=u, pg_password=pw, pg_dbname=dbn),
            apply=True,
            connect_timeout_s=2,
        )
        if not bool(schema_out.get("ok", False)):
            click.echo(json.dumps({"ok": False, "error": "ensure_schema_failed", "detail": schema_out}))
            raise SystemExit(1)

        # Validate tables are queryable (so failures are surfaced loudly).
        try:
            import psycopg  # type: ignore

            with psycopg.connect(user=u, password=pw, host=h, port=int(p), dbname=dbn, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    for tbl in [str(main_l5_table)]:
                        cur.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                        cur.fetchone()
        except Exception as e:
            click.echo(json.dumps({"ok": False, "error": str(e)}))
            raise SystemExit(1)

        click.echo(
            json.dumps(
                {
                    "ok": True,
                    "host": h,
                    "pg_port": int(p),
                    "ilp_port": int(ilp),
                    "pg_user": u,
                    "pg_dbname": dbn,
                    "tables": {"main_l5": str(main_l5_table)},
                    "schema_migration": {"ok": True, "tables_checked": schema_out.get("tables_checked", 0)},
                }
            )
        )

    @db_group.command("migrate-columns-v2")
    @click.option("--apply/--dry-run", default=False, show_default=True, help="Apply schema changes (default: dry-run)")
    @click.option("--table-prefix", default="ghtrader_", show_default=True, help="Only migrate tables with this prefix")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--connect-timeout", default=2, type=int, show_default=True, help="Connect timeout seconds")
    def db_migrate_columns_v2(
        apply: bool,
        table_prefix: str,
        host: str,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        connect_timeout: int,
    ) -> None:
        """
        Migrate ghTrader QuestDB schemas (v2): rename provenance columns.
        """
        import json

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.client import QuestDBQueryConfig
        from ghtrader.questdb.migrate import migrate_column_names_v2

        h = str(host).strip() or str(get_questdb_host())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        cfg = QuestDBQueryConfig(host=h, pg_port=int(p), pg_user=u, pg_password=pw, pg_dbname=dbn)
        out = migrate_column_names_v2(
            cfg=cfg, table_prefix=str(table_prefix), apply=bool(apply), connect_timeout_s=int(connect_timeout)
        )
        click.echo(json.dumps(out, ensure_ascii=False))
        if not bool(out.get("ok", False)):
            raise SystemExit(1)

    @db_group.command("ensure-schema")
    @click.option("--apply/--dry-run", default=False, show_default=True, help="Apply schema changes (default: dry-run)")
    @click.option("--host", default="", help="QuestDB host (default: env/config)")
    @click.option("--pg-port", default=0, type=int, help="QuestDB PGWire port (default: env/config)")
    @click.option("--pg-user", default="", help="QuestDB PGWire user (default: env/config)")
    @click.option("--pg-password", default="", help="QuestDB PGWire password (default: env/config)")
    @click.option("--pg-dbname", default="", help="QuestDB PGWire dbname (default: env/config)")
    @click.option("--connect-timeout", default=2, type=int, show_default=True, help="Connect timeout seconds")
    def db_ensure_schema(
        apply: bool,
        host: str,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_dbname: str,
        connect_timeout: int,
    ) -> None:
        """
        Ensure all required columns exist on ghTrader QuestDB tables.

        This command adds missing columns (dataset_version, ticks_kind, etc.) to
        existing tables that were created before the schema was updated.

        Use --dry-run (default) to see what would be changed.
        Use --apply to execute the schema changes.
        """
        import json

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.client import QuestDBQueryConfig
        from ghtrader.questdb.migrate import ensure_schema_v2

        h = str(host).strip() or str(get_questdb_host())
        p = int(pg_port) if int(pg_port) > 0 else int(get_questdb_pg_port())
        u = str(pg_user).strip() or str(get_questdb_pg_user())
        pw = str(pg_password).strip() or str(get_questdb_pg_password())
        dbn = str(pg_dbname).strip() or str(get_questdb_pg_dbname())

        cfg = QuestDBQueryConfig(host=h, pg_port=int(p), pg_user=u, pg_password=pw, pg_dbname=dbn)
        out = ensure_schema_v2(cfg=cfg, apply=bool(apply), connect_timeout_s=int(connect_timeout))
        click.echo(json.dumps(out, ensure_ascii=False, indent=2))
        if not bool(out.get("ok", False)):
            raise SystemExit(1)

    @db_group.command("refresh-main-l5-daily-agg")
    @click.option("--start", type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help="Start date (YYYY-MM-DD)")
    @click.option("--end", type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help="End date (YYYY-MM-DD)")
    @click.option("--symbol", default="", help="Optional symbol filter (e.g. KQ.m@SHFE.cu)")
    @click.option("--ticks-kind", default="main_l5", show_default=True, help="Ticks kind filter")
    @click.option("--dataset-version", default="v2", show_default=True, help="Dataset version filter")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON output")
    def db_refresh_main_l5_daily_agg(
        start: datetime | None,
        end: datetime | None,
        symbol: str,
        ticks_kind: str,
        dataset_version: str,
        as_json: bool,
    ) -> None:
        """Refresh QuestDB main_l5 daily aggregate table for coverage queries."""
        import json

        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.main_l5_daily_agg import rebuild_main_l5_daily_agg

        d0 = start.date() if start is not None else None
        d1 = end.date() if end is not None else None
        if d0 is not None and d1 is not None and d1 < d0:
            raise click.ClickException("end must be >= start")

        cfg = make_questdb_query_config_from_env()
        out = rebuild_main_l5_daily_agg(
            cfg=cfg,
            start_day=d0,
            end_day=d1,
            symbol=(str(symbol).strip() or None),
            ticks_kind=str(ticks_kind).strip() or "main_l5",
            dataset_version=str(dataset_version).strip() or "v2",
        )
        if as_json:
            click.echo(json.dumps(out, ensure_ascii=False, indent=2))
            return
        click.echo(
            "ok={ok} source_groups={source_groups} upserted_rows={upserted_rows} "
            "invalid_rows={invalid_rows} distinct_symbols={distinct_symbols}".format(
                ok=bool(out.get("ok", False)),
                source_groups=int(out.get("source_groups", 0) or 0),
                upserted_rows=int(out.get("upserted_rows", 0) or 0),
                invalid_rows=int(out.get("invalid_rows", 0) or 0),
                distinct_symbols=int(out.get("distinct_symbols", 0) or 0),
            )
        )

    @db_group.command("benchmark")
    @click.option("--symbol", required=True, help="Symbol to benchmark (e.g. SHFE.cu2602 or KQ.m@SHFE.cu)")
    @click.option("--start", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date (YYYY-MM-DD)")
    @click.option("--end", required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date (YYYY-MM-DD)")
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--runs-dir", default="runs", help="Runs directory root")
    @click.option(
        "--ticks-kind",
        default="main_l5",
        type=click.Choice(["main_l5"]),
        show_default=True,
        help="Ticks kind to benchmark (main_l5 only).",
    )
    @click.option("--max-rows", default=200000, type=int, show_default=True, help="Max rows to load/ingest for the benchmark")
    @click.option("--host", default="127.0.0.1", show_default=True, help="QuestDB host")
    @click.pass_context
    def db_benchmark(
        ctx: click.Context,
        symbol: str,
        start: datetime,
        end: datetime,
        data_dir: str,
        runs_dir: str,
        ticks_kind: str,
        max_rows: int,
        host: str,
    ) -> None:
        """Benchmark QuestDB ingestion/query on a real tick sample."""
        from ghtrader.questdb.bench import run_db_benchmark

        _ = ctx

        out_path, report = run_db_benchmark(
            data_dir=Path(data_dir),
            runs_dir=Path(runs_dir),
            symbol=symbol,
            start_date=start.date(),
            end_date=end.date(),
            ticks_kind=ticks_kind,  # type: ignore[arg-type]
            dataset_version="v2",
            max_rows=int(max_rows),
            host=str(host),
        )
        log.info(
            "db.benchmark_done",
            report_path=str(out_path),
            n_results=len(report.get("results") or []),
            n_errors=len(report.get("errors") or []),
        )

    @db_group.command("export-main-l5")
    @click.option("--symbol", required=True, help="Derived symbol to export (e.g. KQ.m@SHFE.cu)")
    @click.option("--start", type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help="Start date (YYYY-MM-DD)")
    @click.option("--end", type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help="End date (YYYY-MM-DD)")
    @click.option("--limit-per-day", default=0, type=int, show_default=True, help="Max rows per trading day (0 = no limit)")
    @click.option("--max-rows", default=0, type=int, show_default=True, help="Global max rows across all days (0 = no limit)")
    @click.option("--order", type=click.Choice(["asc", "desc"]), default="asc", show_default=True, help="Row order per day")
    @click.option("--format", "fmt", type=click.Choice(["csv", "xlsx"]), default="csv", show_default=True, help="Export format")
    @click.option("--output", default="", help="Output path (defaults to runs/exports/...)")
    @click.option("--include-provenance/--no-include-provenance", default=True, show_default=True, help="Include schedule provenance columns")
    @click.pass_context
    def db_export_main_l5(
        ctx: click.Context,
        symbol: str,
        start: datetime | None,
        end: datetime | None,
        limit_per_day: int,
        max_rows: int,
        order: str,
        fmt: str,
        output: str,
        include_provenance: bool,
    ) -> None:
        """Export main_l5 ticks to CSV/XLSX for manual inspection."""
        import re

        import pandas as pd

        from ghtrader.config import get_runs_dir
        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.queries import (
            fetch_ticks_for_symbol_day,
            list_trading_days_for_symbol,
            query_symbol_day_bounds,
        )

        _ = ctx
        sym = str(symbol).strip()
        if not sym:
            raise click.ClickException("symbol is required")

        cfg = make_questdb_query_config_from_env()
        ticks_table = "ghtrader_ticks_main_l5_v2"
        dv = "v2"

        d0 = start.date() if start is not None else None
        d1 = end.date() if end is not None else None
        if d0 is None or d1 is None:
            bounds = query_symbol_day_bounds(
                cfg=cfg,
                table=ticks_table,
                symbols=[sym],
                dataset_version=dv,
                ticks_kind="main_l5",
                l5_only=False,
            )
            b = bounds.get(sym) or {}
            if d0 is None:
                d0s = str(b.get("first_day") or "").strip()
                if not d0s:
                    raise click.ClickException(f"No main_l5 data found for {sym} in QuestDB.")
                d0 = datetime.fromisoformat(d0s).date()
            if d1 is None:
                d1s = str(b.get("last_day") or "").strip()
                if not d1s:
                    raise click.ClickException(f"No main_l5 data found for {sym} in QuestDB.")
                d1 = datetime.fromisoformat(d1s).date()
        if d0 is None or d1 is None:
            raise click.ClickException("start/end could not be resolved from QuestDB.")
        if d1 < d0:
            raise click.ClickException("end must be >= start")

        days = list_trading_days_for_symbol(
            cfg=cfg,
            table=ticks_table,
            symbol=sym,
            start_day=d0,
            end_day=d1,
            dataset_version=dv,
            ticks_kind="main_l5",
        )
        if not days:
            raise click.ClickException(f"No trading days found for {sym} between {d0} and {d1}.")

        limit_per_day = int(limit_per_day or 0)
        max_rows = int(max_rows or 0)

        if output:
            out_path = Path(output)
        else:
            slug = re.sub(r"[^A-Za-z0-9]+", "_", sym).strip("_")
            out_dir = get_runs_dir() / "exports"
            out_dir.mkdir(parents=True, exist_ok=True)
            suffix = "xlsx" if fmt == "xlsx" else "csv"
            out_path = out_dir / f"main_l5_{slug}_{d0.isoformat()}_{d1.isoformat()}.{suffix}"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "xlsx" and out_path.suffix.lower() != ".xlsx":
            out_path = out_path.with_suffix(".xlsx")
        if fmt == "csv" and out_path.suffix.lower() != ".csv":
            out_path = out_path.with_suffix(".csv")

        log.info(
            "db.export_main_l5_start",
            symbol=sym,
            start=d0.isoformat(),
            end=d1.isoformat(),
            days_total=int(len(days)),
            limit_per_day=int(limit_per_day),
            max_rows=int(max_rows),
            format=str(fmt),
            output=str(out_path),
        )

        frames: list[pd.DataFrame] = []
        rows_total = 0
        days_done = 0
        for day in days:
            df = fetch_ticks_for_symbol_day(
                cfg=cfg,
                table=ticks_table,
                symbol=sym,
                trading_day=day.isoformat(),
                dataset_version=dv,
                ticks_kind="main_l5",
                limit=(limit_per_day if limit_per_day > 0 else None),
                order=str(order),
                include_provenance=bool(include_provenance),
                connect_timeout_s=2,
            )
            if df.empty:
                continue
            df = df.copy()
            if "datetime" in df.columns and "datetime_ns" not in df.columns:
                df = df.rename(columns={"datetime": "datetime_ns"})
            if "symbol" not in df.columns:
                df.insert(0, "symbol", sym)
            if "trading_day" not in df.columns:
                df.insert(1, "trading_day", str(day.isoformat()))
            if "datetime_ns" in df.columns and "ts" not in df.columns:
                df.insert(2, "ts", pd.to_datetime(pd.to_numeric(df["datetime_ns"], errors="coerce"), unit="ns"))

            if max_rows > 0 and (rows_total + len(df)) > max_rows:
                df = df.iloc[: max(0, max_rows - rows_total)]
            frames.append(df)
            rows_total += int(len(df))
            days_done += 1

            if max_rows > 0 and rows_total >= max_rows:
                break
            if days_done == 1 or days_done % 10 == 0 or days_done == len(days):
                log.info(
                    "db.export_main_l5_progress",
                    symbol=sym,
                    trading_day=str(day.isoformat()),
                    days_done=int(days_done),
                    days_total=int(len(days)),
                    rows_total=int(rows_total),
                )

        if not frames:
            raise click.ClickException("No rows collected for export.")
        out_df = pd.concat(frames, ignore_index=True)

        if fmt == "xlsx":
            try:
                import openpyxl  # type: ignore  # noqa: F401
            except Exception as e:
                raise click.ClickException("XLSX export requires openpyxl. Install: pip install openpyxl") from e
            out_df.to_excel(out_path, index=False)
        else:
            out_df.to_csv(out_path, index=False)

        log.info(
            "db.export_main_l5_done",
            symbol=sym,
            rows_total=int(len(out_df)),
            output=str(out_path),
            format=str(fmt),
        )
        click.echo(str(out_path))

    @db_group.command("purge-main-l5-l1")
    @click.option("--symbol", default="", help="Optional symbol filter (e.g. KQ.m@SHFE.cu)")
    @click.option("--start", type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help="Start date (YYYY-MM-DD)")
    @click.option("--end", type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help="End date (YYYY-MM-DD)")
    @click.option("--apply/--dry-run", default=False, show_default=True, help="Apply deletions (default: dry-run)")
    def db_purge_main_l5_l1(
        symbol: str,
        start: datetime | None,
        end: datetime | None,
        apply: bool,
    ) -> None:
        """Delete L1-only rows from main_l5 (keeps only true L5 depth)."""
        import json

        from ghtrader.config import (
            get_questdb_host,
            get_questdb_ilp_port,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES
        from ghtrader.questdb.client import make_questdb_query_config_from_env, connect_pg
        from ghtrader.questdb.row_cleanup import replace_table_delete_where
        from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend
        from ghtrader.util.l5_detection import l5_sql_condition

        cfg = make_questdb_query_config_from_env()
        backend = make_serving_backend(
            ServingDBConfig(
                backend="questdb",
                host=str(get_questdb_host()),
                questdb_ilp_port=int(get_questdb_ilp_port()),
                questdb_pg_port=int(get_questdb_pg_port()),
                questdb_pg_user=str(get_questdb_pg_user()),
                questdb_pg_password=str(get_questdb_pg_password()),
                questdb_pg_dbname=str(get_questdb_pg_dbname()),
            )
        )
        base_cols = [
            "symbol",
            "ts",
            "datetime_ns",
            "trading_day",
            "row_hash",
        ]
        tick_cols = [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}]
        prov_cols = ["dataset_version", "ticks_kind", "underlying_contract", "segment_id", "schedule_hash"]
        cols = base_cols + tick_cols + prov_cols

        def _ensure_main_l5_table(table_name: str) -> None:
            backend.ensure_table(table=str(table_name), include_segment_metadata=True)

        where = [f"NOT ({l5_sql_condition()})"]
        params: list[str] = []
        sym = str(symbol or "").strip()
        if sym:
            where.append("symbol = %s")
            params.append(sym)
        if start is not None:
            where.append("trading_day >= %s")
            params.append(start.date().isoformat())
        if end is not None:
            where.append("trading_day <= %s")
            params.append(end.date().isoformat())

        where_sql = " AND ".join(where)
        count_sql = f"SELECT count() FROM ghtrader_ticks_main_l5_v2 WHERE {where_sql}"
        with connect_pg(cfg, connect_timeout_s=2) as conn:
            try:
                conn.autocommit = True  # type: ignore[attr-defined]
            except Exception:
                pass
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                (count,) = cur.fetchone() or (0,)
        if not apply:
            click.echo(json.dumps({"ok": True, "mode": "dry_run", "rows_to_delete": int(count)}))
            return

        deleted = replace_table_delete_where(
            cfg=cfg,
            table="ghtrader_ticks_main_l5_v2",
            columns=cols,
            delete_where_sql=where_sql,
            delete_params=params,
            ensure_table=_ensure_main_l5_table,
            connect_timeout_s=2,
        )
        click.echo(
            json.dumps(
                {"ok": True, "mode": "apply", "rows_deleted": int(deleted), "method": "replace_table"},
                ensure_ascii=False,
            )
        )

    @db_group.command("schema-check")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON output")
    def db_schema_check(as_json: bool) -> None:
        """Check schema for main_schedule and main_l5 tables."""
        import json

        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.migrate import _list_tables, _list_columns
        from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

        cfg = make_questdb_query_config_from_env()
        tables = set(_list_tables(cfg=cfg, connect_timeout_s=2))
        expected: dict[str, list[str]] = {
            "ghtrader_main_schedule_v2": [
                "ts",
                "exchange",
                "variety",
                "trading_day",
                "main_contract",
                "segment_id",
                "schedule_hash",
                "updated_at",
            ],
            "ghtrader_ticks_main_l5_v2": [
                "symbol",
                "ts",
                "datetime_ns",
                "trading_day",
                "row_hash",
                "dataset_version",
                "ticks_kind",
                "underlying_contract",
                "segment_id",
                "schedule_hash",
            ]
            + [c for c in TICK_COLUMN_NAMES if c not in {"symbol", "datetime"}],
        }
        out: dict[str, dict[str, list[str] | bool]] = {}
        ok = True
        for tbl, cols in expected.items():
            if tbl not in tables:
                out[tbl] = {"ok": False, "missing": cols, "extra": []}
                ok = False
                continue
            actual = set(_list_columns(cfg=cfg, table=tbl, connect_timeout_s=2))
            missing = [c for c in cols if c not in actual]
            extra = sorted([c for c in actual if c not in cols])
            out[tbl] = {"ok": not missing, "missing": missing, "extra": extra}
            if missing:
                ok = False

        payload = {"ok": bool(ok), "tables": out}
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            for tbl, info in out.items():
                click.echo(f"{tbl}: ok={info.get('ok')} missing={len(info.get('missing') or [])} extra={len(info.get('extra') or [])}")

    @db_group.command("cleanup-tables")
    @click.option("--apply/--dry-run", default=False, show_default=True, help="Apply table drops (default: dry-run)")
    def db_cleanup_tables(apply: bool) -> None:
        """Drop unused ghtrader_* tables in QuestDB."""
        import json

        from ghtrader.questdb.client import make_questdb_query_config_from_env, connect_pg
        from ghtrader.questdb.migrate import _list_tables

        keep = {
            "ghtrader_main_schedule_v2",
            "ghtrader_ticks_main_l5_v2",
            "ghtrader_main_l5_daily_agg_v2",
            "ghtrader_features_v2",
            "ghtrader_labels_v2",
            "ghtrader_feature_builds_v2",
            "ghtrader_label_builds_v2",
        }
        cfg = make_questdb_query_config_from_env()
        tables = _list_tables(cfg=cfg, connect_timeout_s=2)
        drop = [t for t in tables if t.startswith("ghtrader_") and t not in keep]

        if not apply:
            click.echo(json.dumps({"ok": True, "mode": "dry_run", "drop_tables": drop}, ensure_ascii=False, indent=2))
            return

        results: list[dict[str, str | bool]] = []
        if drop:
            with connect_pg(cfg, connect_timeout_s=2) as conn:
                try:
                    conn.autocommit = True  # type: ignore[attr-defined]
                except Exception:
                    pass
                with conn.cursor() as cur:
                    for tbl in drop:
                        try:
                            cur.execute(f"DROP TABLE {tbl}")
                            results.append({"table": tbl, "ok": True})
                        except Exception as e:
                            results.append({"table": tbl, "ok": False, "error": str(e)})
        click.echo(json.dumps({"ok": True, "mode": "apply", "drop_tables": drop, "results": results}, ensure_ascii=False, indent=2))

