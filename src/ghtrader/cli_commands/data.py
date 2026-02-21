from __future__ import annotations

from datetime import date, datetime
import os
from pathlib import Path

import click
import structlog

log = structlog.get_logger()


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _set_current_job_progress_error(error: str) -> None:
    job_id = str(os.environ.get("GHTRADER_JOB_ID", "") or "").strip()
    if not job_id:
        return
    try:
        from ghtrader.config import get_runs_dir
        from ghtrader.control.progress import JobProgress

        JobProgress(job_id=job_id, runs_dir=get_runs_dir()).set_error(str(error))
    except Exception:
        pass


def register(main: click.Group) -> None:
    """
    Register data-domain commands on the root CLI group.
    """

    @main.group("data")
    @click.pass_context
    def data_group(ctx: click.Context) -> None:
        """Unified data management commands (QuestDB-first)."""
        _ = ctx

    @data_group.command("l5-start")
    @click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
    @click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu)")
    @click.option("--symbol", "-s", "symbols", multiple=True, help="Optional symbol(s) (default: all in catalog)")
    @click.option("--refresh-catalog", default=0, type=int, show_default=True, help="Refresh catalog cache (network) (0/1)")
    @click.option("--data-dir", default="data", show_default=True, help="Data directory root")
    @click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
    @click.option("--json", "as_json", is_flag=True, help="Print full JSON payload")
    @click.pass_context
    def data_l5_start(
        ctx: click.Context,
        exchange: str,
        variety: str,
        symbols: tuple[str, ...],
        refresh_catalog: int,
        data_dir: str,
        runs_dir: str,
        as_json: bool,
    ) -> None:
        import json

        from ghtrader.config import l5_start_env_key
        from ghtrader.tq.l5_start import resolve_l5_start_date

        _ = ctx, symbols
        ex = str(exchange).upper().strip()
        v = str(variety).lower().strip()
        res = resolve_l5_start_date(
            exchange=ex,
            variety=v,
            end=date.today(),
            data_dir=Path(data_dir),
            runs_dir=Path(runs_dir),
            refresh=True,
            refresh_catalog=bool(refresh_catalog),
        )
        env_key = l5_start_env_key(variety=v)
        payload = {
            "ok": True,
            "exchange": res.exchange,
            "var": res.variety,
            "l5_start_date": res.l5_start_date.isoformat(),
            "l5_start_contract": res.l5_start_contract,
            "source": res.source,
            "created_at": res.created_at,
            "cached_at_unix": res.cached_at_unix,
            "contracts_checked": res.contracts_checked,
            "probes_total": res.probes_total,
            "env_line": f"{env_key}={res.l5_start_date.isoformat()}",
        }
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            click.echo(f"{res.exchange}.{res.variety} l5_start_date={res.l5_start_date.isoformat()} source={res.source}")
            click.echo(payload["env_line"])

    @data_group.command("main-l5-validate")
    @click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
    @click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu)")
    @click.option("--symbol", "derived_symbol", default="", show_default=False, help="Derived symbol (default: KQ.m@EX.var)")
    @click.option("--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional start date (YYYY-MM-DD)")
    @click.option("--end", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional end date (YYYY-MM-DD)")
    @click.option("--tqsdk-check/--no-tqsdk-check", default=True, show_default=True, help="Check provider gaps via TqSdk (slow)")
    @click.option("--tqsdk-check-max-days", default=2, show_default=True, type=int, help="Max days to probe via TqSdk")
    @click.option("--tqsdk-check-max-segments", default=8, show_default=True, type=int, help="Max gap segments to probe per day")
    @click.option("--max-segments-per-day", default=200, show_default=True, type=int, help="Max gap segments stored per day")
    @click.option("--gap-threshold-s", default=None, type=float, help="Gap threshold in seconds for missing segments")
    @click.option("--strict-ratio", default=None, type=float, help="Min ratio of seconds with >=2 ticks for strict mode")
    @click.option("--incremental/--no-incremental", default=False, show_default=True, help="Validate only newly added days")
    @click.option("--data-dir", default="data", show_default=True, help="Data directory root")
    @click.option("--runs-dir", default="runs", show_default=True, help="Runs directory root")
    @click.option("--json", "as_json", is_flag=True, help="Print full JSON report")
    @click.pass_context
    def data_main_l5_validate(
        ctx: click.Context,
        exchange: str,
        variety: str,
        derived_symbol: str,
        start: datetime | None,
        end: datetime | None,
        tqsdk_check: bool,
        tqsdk_check_max_days: int,
        tqsdk_check_max_segments: int,
        max_segments_per_day: int,
        gap_threshold_s: float | None,
        strict_ratio: float | None,
        incremental: bool,
        data_dir: str,
        runs_dir: str,
        as_json: bool,
    ) -> None:
        import json

        from ghtrader.cli import _acquire_locks
        from ghtrader.data.main_l5_validation import validate_main_l5

        _ = ctx
        ex = str(exchange).upper().strip()
        v = str(variety).lower().strip()
        ds = str(derived_symbol or "").strip() or f"KQ.m@{ex}.{v}"
        start_day = start.date() if start else None
        end_day = end.date() if end else None
        _acquire_locks([f"main_l5:symbol={ds}"])
        try:
            report, out_path = validate_main_l5(
                exchange=ex,
                variety=v,
                derived_symbol=ds,
                data_dir=Path(data_dir),
                runs_dir=Path(runs_dir),
                start_day=start_day,
                end_day=end_day,
                tqsdk_check=bool(tqsdk_check),
                tqsdk_check_max_days=int(tqsdk_check_max_days),
                tqsdk_check_max_segments=int(tqsdk_check_max_segments),
                max_segments_per_day=int(max_segments_per_day),
                gap_threshold_s=gap_threshold_s,
                strict_ratio=strict_ratio,
                incremental=bool(incremental),
            )
        except Exception as e:
            _set_current_job_progress_error(str(e))
            raise
        if out_path:
            report = dict(report)
            report["_path"] = str(out_path)
        if as_json:
            click.echo(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        else:
            state = str(report.get("state") or ("ok" if report.get("ok") else "error")).strip()
            click.echo(
                f"main_l5 validate {ex}.{v} state={state} missing_days={report.get('missing_days')} "
                f"missing_segments={report.get('missing_segments_total')} "
                f"missing_half_seconds={report.get('missing_half_seconds_total')} "
                f"report={str(out_path or '')}"
            )

    @data_group.command("field-quality")
    @click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
    @click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu)")
    @click.option("--symbol", "derived_symbol", default="", show_default=False, help="Derived symbol (default: KQ.m@EX.var)")
    @click.option("--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional start date (YYYY-MM-DD)")
    @click.option("--end", default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Optional end date (YYYY-MM-DD)")
    @click.option("--max-rows", default=None, type=int, help="Optional max rows per day (safety cap)")
    @click.option("--json", "as_json", is_flag=True, help="Print full JSON payload")
    @click.pass_context
    def data_field_quality(
        ctx: click.Context,
        exchange: str,
        variety: str,
        derived_symbol: str,
        start: datetime | None,
        end: datetime | None,
        max_rows: int | None,
        as_json: bool,
    ) -> None:
        import json

        from ghtrader.data.field_quality import compute_field_quality_for_day, list_symbol_trading_days
        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.field_quality import ensure_field_quality_table, upsert_field_quality_rows

        _ = ctx
        ex = str(exchange).upper().strip()
        v = str(variety).lower().strip()
        symbol = str(derived_symbol or f"KQ.m@{ex}.{v}").strip()
        start_day = start.date() if start else None
        end_day = end.date() if end else None

        cfg = make_questdb_query_config_from_env()
        ensure_field_quality_table(cfg=cfg)
        days = list_symbol_trading_days(
            cfg=cfg,
            symbol=symbol,
            start_day=start_day,
            end_day=end_day,
        )
        rows = []
        for day in days:
            res = compute_field_quality_for_day(
                cfg=cfg,
                symbol=symbol,
                trading_day=day,
                limit=max_rows,
            )
            rows.append(res.row)
        upserted = upsert_field_quality_rows(cfg=cfg, rows=rows)

        payload = {
            "ok": True,
            "exchange": ex,
            "var": v,
            "symbol": symbol,
            "days": int(len(days)),
            "rows_upserted": int(upserted),
            "start": (start_day.isoformat() if start_day else None),
            "end": (end_day.isoformat() if end_day else None),
        }
        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            click.echo(f"{symbol} field_quality days={len(days)} rows_upserted={upserted}")

    @data_group.command("fill-labels")
    @click.option("--exchange", default="SHFE", show_default=True, help="Exchange (e.g. SHFE)")
    @click.option("--var", "variety", default="cu", show_default=True, help="Variety code (e.g. cu)")
    @click.option("--symbol", "derived_symbol", default="", show_default=False, help="Derived symbol (default: KQ.m@EX.var)")
    @click.option("--horizons", default="10,50,100,500", show_default=True, help="Comma-separated fill horizons (ticks)")
    @click.option("--price-levels", default="0,1", show_default=True, help="Comma-separated price level offsets from best quote")
    @click.option("--price-tick", default=1.0, type=float, show_default=True, help="Price tick size for level offsets")
    @click.option("--data-dir", default="data", show_default=True, help="Data directory root")
    @click.option("--json", "as_json", is_flag=True, help="Print full JSON payload")
    @click.pass_context
    def data_fill_labels(
        ctx: click.Context,
        exchange: str,
        variety: str,
        derived_symbol: str,
        horizons: str,
        price_levels: str,
        price_tick: float,
        data_dir: str,
        as_json: bool,
    ) -> None:
        import json

        from ghtrader.fill_labels import build_fill_labels_for_symbol

        _ = ctx
        ex = str(exchange).upper().strip()
        v = str(variety).lower().strip()
        symbol = str(derived_symbol or f"KQ.m@{ex}.{v}").strip()
        hs = [int(x.strip()) for x in str(horizons or "").split(",") if str(x).strip()]
        levels = [int(x.strip()) for x in str(price_levels or "").split(",") if str(x).strip()]

        out = build_fill_labels_for_symbol(
            symbol=symbol,
            data_dir=Path(data_dir),
            horizons=hs,
            price_levels=levels,
            price_tick=float(price_tick),
            ticks_kind="main_l5",
            dataset_version="v2",
        )
        if as_json:
            click.echo(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        else:
            click.echo(
                f"{symbol} fill_labels rows_total={int(out.get('rows_total') or 0)} "
                f"days={int(out.get('days') or 0)} build_id={str(out.get('build_id') or '')}"
            )

    @main.command("main-schedule")
    @click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.pass_context
    def main_schedule(
        ctx: click.Context,
        variety: str,
        data_dir: str,
    ) -> None:
        from ghtrader.cli import _acquire_locks
        from ghtrader.data.main_schedule import build_main_schedule

        _ = ctx
        enforce_health = _env_bool(
            "GHTRADER_MAIN_SCHEDULE_ENFORCE_HEALTH",
            _env_bool("GHTRADER_PIPELINE_ENFORCE_HEALTH", True),
        )
        _acquire_locks([f"main_schedule:var={variety.lower()}"])
        try:
            res = build_main_schedule(
                var=variety,
                data_dir=Path(data_dir),
                enforce_health=bool(enforce_health),
            )
        except Exception as e:
            _set_current_job_progress_error(str(e))
            raise
        log.info(
            "main_schedule.done",
            schedule_table=str(res.questdb_table),
            schedule_hash=res.schedule_hash,
            rows=len(res.schedule),
        )

    @main.command("main-l5")
    @click.option("--var", "variety", required=True, type=str, help="Variety code (e.g., cu, au, ag)")
    @click.option(
        "--symbol",
        "derived_symbol",
        default="",
        help="Derived symbol (default: KQ.m@SHFE.<var>)",
    )
    @click.option("--data-dir", default="data", help="Data directory root")
    @click.option("--update", "update_mode", is_flag=True, help="Backfill missing days only (no full rebuild)")
    @click.pass_context
    def main_l5(
        ctx: click.Context,
        variety: str,
        derived_symbol: str,
        data_dir: str,
        update_mode: bool,
    ) -> None:
        from ghtrader.cli import _acquire_locks
        from ghtrader.data.main_l5 import build_main_l5

        _ = ctx
        var_l = variety.lower().strip()
        ds = (derived_symbol or "").strip() or f"KQ.m@SHFE.{var_l}"
        enforce_health = _env_bool(
            "GHTRADER_MAIN_L5_ENFORCE_HEALTH",
            _env_bool("GHTRADER_PIPELINE_ENFORCE_HEALTH", True),
        )
        _acquire_locks([f"main_l5:symbol={ds}"])
        try:
            res = build_main_l5(
                var=var_l,
                derived_symbol=ds,
                exchange="SHFE",
                data_dir=str(data_dir),
                update_mode=bool(update_mode),
                enforce_health=bool(enforce_health),
            )
        except Exception as e:
            _set_current_job_progress_error(str(e))
            raise
        log.info(
            "main_l5.done",
            derived_symbol=res.derived_symbol,
            schedule_hash=str(res.schedule_hash),
            rows_total=int(res.rows_total),
            days_total=int(res.days_total),
        )
