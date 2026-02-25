from __future__ import annotations

from datetime import date, datetime
import os
from pathlib import Path
from typing import Any

import click
import structlog

from ghtrader.config import env_bool, env_float, get_env

log = structlog.get_logger()


def _set_current_job_progress_error(error: str) -> None:
    job_id = str(get_env("GHTRADER_JOB_ID", "") or "").strip()
    if not job_id:
        return
    try:
        from ghtrader.config import get_runs_dir
        from ghtrader.control.progress import JobProgress

        JobProgress(job_id=job_id, runs_dir=get_runs_dir()).set_error(str(error))
    except Exception:
        pass


def _is_running_main_l5_job(job: Any) -> bool:
    if job is None:
        return False
    if str(getattr(job, "status", "") or "").strip().lower() != "running":
        return False
    meta = getattr(job, "metadata", None)
    if isinstance(meta, dict):
        sub1 = str(meta.get("subcommand") or "").strip().lower()
        sub2 = str(meta.get("subcommand2") or "").strip().lower()
        kind = str(meta.get("kind") or "").strip().lower()
        if (sub1 == "data" and sub2 == "main-l5") or kind == "main_l5":
            return True
    parts = [str(x).strip().lower() for x in (getattr(job, "command", []) or [])]
    if "main-l5" in parts:
        return True
    title = str(getattr(job, "title", "") or "").strip().lower()
    return title.startswith("main-l5 ")


def _pid_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def _find_running_main_l5_owner_for_symbol(symbol: str) -> dict[str, str] | None:
    from ghtrader.config import get_runs_dir
    from ghtrader.control.db import JobStore
    from ghtrader.control.locks import LockStore

    ds = str(symbol or "").strip()
    if not ds:
        return None
    lock_key = f"main_l5:symbol={ds}"
    db_path = Path(get_runs_dir()) / "control" / "jobs.db"
    store = JobStore(db_path)
    locks = LockStore(db_path)
    current_job_id = str(get_env("GHTRADER_JOB_ID", "") or "").strip()
    for lock in locks.list_locks():
        if str(lock.key) != lock_key:
            continue
        owner_job_id = str(lock.job_id or "").strip()
        if not owner_job_id or owner_job_id == current_job_id:
            continue
        owner = store.get_job(owner_job_id)
        if not _is_running_main_l5_job(owner):
            continue
        if not _pid_alive(getattr(owner, "pid", None)):
            continue
        return {
            "job_id": owner_job_id,
            "title": str(getattr(owner, "title", "") or "").strip(),
        }
    return None


def _main_l5_validate_lock_conflict_message(*, symbol: str, owner_job_id: str) -> str:
    ds = str(symbol or "").strip()
    jid = str(owner_job_id or "").strip()
    return (
        f"main-l5 build in progress for {ds} (job_id={jid}). "
        "Please retry main-l5-validate after the build completes."
    )


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
        conflict = _find_running_main_l5_owner_for_symbol(ds)
        if conflict is not None:
            msg = _main_l5_validate_lock_conflict_message(symbol=ds, owner_job_id=str(conflict.get("job_id") or ""))
            _set_current_job_progress_error(msg)
            raise RuntimeError(msg)
        validate_lock_wait_timeout_s = max(0.1, float(env_float("GHTRADER_MAIN_L5_VALIDATE_LOCK_WAIT_TIMEOUT_S", 3.0)))
        try:
            _acquire_locks(
                [f"main_l5:symbol={ds}"],
                wait_timeout_s=float(validate_lock_wait_timeout_s),
                preempt_on_timeout=False,
            )
        except Exception as e:
            conflict = _find_running_main_l5_owner_for_symbol(ds)
            if conflict is not None:
                msg = _main_l5_validate_lock_conflict_message(symbol=ds, owner_job_id=str(conflict.get("job_id") or ""))
                _set_current_job_progress_error(msg)
                raise RuntimeError(msg) from e
            _set_current_job_progress_error(str(e))
            raise
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

    @data_group.command("contract-check")
    @click.option(
        "--input-path",
        default="",
        show_default=False,
        help="Optional CSV/Parquet/JSON tick file for contract validation",
    )
    @click.option(
        "--rows",
        "synthetic_rows",
        default=64,
        type=int,
        show_default=True,
        help="Synthetic rows when --input-path is omitted",
    )
    @click.option(
        "--min-rows",
        default=32,
        type=int,
        show_default=True,
        help="Minimum required rows for contract pass",
    )
    @click.option(
        "--max-core-null-rate",
        default=0.01,
        type=float,
        show_default=True,
        help="Max null rate for core columns",
    )
    @click.option(
        "--max-l5-null-rate",
        default=0.05,
        type=float,
        show_default=True,
        help="Max mean null rate across L5 columns",
    )
    @click.option("--json", "as_json", is_flag=True, help="Print full JSON payload")
    @click.pass_context
    def data_contract_check(
        ctx: click.Context,
        input_path: str,
        synthetic_rows: int,
        min_rows: int,
        max_core_null_rate: float,
        max_l5_null_rate: float,
        as_json: bool,
    ) -> None:
        import json

        from ghtrader.data.contract_checks import (
            DataContractThresholds,
            evaluate_tick_data_contract,
            load_tick_frame_for_contract_check,
        )

        _ = ctx
        try:
            df, source = load_tick_frame_for_contract_check(
                path=(input_path or None),
                synthetic_rows=int(synthetic_rows),
            )
        except Exception as e:
            raise click.ClickException(f"failed to load tick input: {e}") from e

        report = evaluate_tick_data_contract(
            df=df,
            thresholds=DataContractThresholds(
                min_rows=int(min_rows),
                max_core_null_rate=float(max_core_null_rate),
                max_l5_null_rate=float(max_l5_null_rate),
            ),
        )
        report = dict(report)
        report["source"] = str(source)

        if as_json:
            click.echo(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        else:
            click.echo(
                f"data contract source={source} ok={bool(report.get('ok'))} "
                f"rows={int(report.get('rows_total') or 0)} "
                f"violations={int(len(report.get('violations') or []))}"
            )
        if not bool(report.get("ok")):
            raise click.ClickException("data contract check failed")

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
        enforce_health = env_bool(
            "GHTRADER_MAIN_SCHEDULE_ENFORCE_HEALTH",
            env_bool("GHTRADER_PIPELINE_ENFORCE_HEALTH", True),
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
        enforce_health = env_bool(
            "GHTRADER_MAIN_L5_ENFORCE_HEALTH",
            env_bool("GHTRADER_PIPELINE_ENFORCE_HEALTH", True),
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
