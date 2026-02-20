from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click


def register(main: click.Group) -> None:
    """
    Register runtime/control commands on the root CLI group.
    """

    @main.group("account")
    @click.pass_context
    def account_group(ctx: click.Context) -> None:
        """Broker account profiles (env-based): list and verify (read-only)."""
        _ = ctx

    @account_group.command("list")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON results to stdout")
    def account_list(as_json: bool) -> None:
        import json

        from ghtrader.tq.runtime import is_trade_account_configured, list_account_profiles_from_env

        profiles = list_account_profiles_from_env()
        out = [{"profile": p, "configured": bool(is_trade_account_configured(profile=p))} for p in profiles]
        if as_json:
            click.echo(json.dumps({"ok": True, "profiles": out}, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            for r in out:
                click.echo(f"{r['profile']}\tconfigured={r['configured']}")

    @account_group.command("verify")
    @click.option("--account", "account_profile", default="default", show_default=True, type=str, help="Account profile to verify")
    @click.option("--timeout-sec", default=20.0, show_default=True, type=float, help="Timeout for connect + snapshot")
    @click.option("--json", "as_json", is_flag=True, help="Print JSON results to stdout")
    @click.pass_context
    def account_verify(ctx: click.Context, account_profile: str, timeout_sec: float, as_json: bool) -> None:
        """
        Verify a broker account profile (read-only): connect and capture a single snapshot.
        """
        import json
        import signal
        import threading
        import time

        from ghtrader.cli import _acquire_locks
        from ghtrader.config import get_runs_dir
        from ghtrader.tq.runtime import (
            canonical_account_profile,
            create_tq_account,
            create_tq_api,
            is_trade_account_configured,
            load_trade_account_config_from_env,
            now_utc_iso,
            snapshot_account_state,
        )

        _ = ctx
        prof = canonical_account_profile(account_profile)
        _acquire_locks([f"trade:account={prof}"])

        runs_dir = get_runs_dir()
        cache_dir = runs_dir / "control" / "cache" / "accounts"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"account={prof}.json"

        payload: dict[str, Any] = {
            "profile": prof,
            "ok": False,
            "configured": bool(is_trade_account_configured(profile=prof)),
            "verified_at": now_utc_iso(),
            "timeout_sec": float(timeout_sec or 0.0),
            "error": "",
        }

        try:
            cfg = load_trade_account_config_from_env(profile=prof)
            payload["broker_id"] = str(cfg.broker_id)
            aid = str(cfg.account_id)
            payload["account_id_masked"] = (aid[:2] + "***" + aid[-2:]) if len(aid) >= 6 else "***"

            timeout_s = float(timeout_sec or 0.0)
            api = None

            class _Timeout:
                def __init__(self, seconds: float):
                    self.seconds = float(seconds or 0.0)
                    self._old = None

                def __enter__(self):
                    if self.seconds <= 0:
                        return self

                    def _handler(_signum, _frame):  # type: ignore[no-untyped-def]
                        raise TimeoutError(f"timeout after {self.seconds:.1f}s")

                    self._old = signal.signal(signal.SIGALRM, _handler)
                    signal.setitimer(signal.ITIMER_REAL, self.seconds)
                    return self

                def __exit__(self, _exc_type, _exc, _tb):  # type: ignore[no-untyped-def]
                    if self.seconds > 0:
                        try:
                            signal.setitimer(signal.ITIMER_REAL, 0.0)
                        except Exception:
                            pass
                        try:
                            if self._old is not None:
                                signal.signal(signal.SIGALRM, self._old)
                        except Exception:
                            pass
                    return False

            def _safe_close(a) -> None:  # type: ignore[no-untyped-def]
                try:
                    a.close()
                except Exception:
                    return

            account = create_tq_account(mode="live", monitor_only=True, account_profile=prof)  # type: ignore[arg-type]
            with _Timeout(timeout_s):
                api = create_tq_api(account=account)
                try:
                    ok_update = bool(api.wait_update(deadline=time.time() + min(5.0, max(0.0, timeout_s))))
                    if not ok_update:
                        raise TimeoutError("timeout waiting for first update")
                except Exception:
                    pass

                snap = snapshot_account_state(api=api, symbols=[], account=account, account_meta={"account_profile": prof})
                payload["snapshot"] = snap
                payload["ok"] = True

            if api is not None:
                t = threading.Thread(target=_safe_close, args=(api,), daemon=True)
                t.start()
                t.join(timeout=2.0)
        except Exception as e:
            payload["ok"] = False
            payload["error"] = str(e)

        tmp = cache_path.with_suffix(f".tmp-{int(time.time()*1000)}")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(cache_path)

        if as_json:
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str, sort_keys=True))

    @main.group("gateway")
    @click.pass_context
    def gateway_group(ctx: click.Context) -> None:
        """AccountGateway (OMS/EMS): per-account-profile gateway process."""
        _ = ctx

    @gateway_group.command("run")
    @click.option("--account", "account_profile", default="default", show_default=True, type=str, help="Account profile to run")
    @click.option("--runs-dir", default="runs", show_default=True, type=str, help="Runs directory root")
    @click.option("--snapshot-interval-sec", default=10.0, show_default=True, type=float, help="Snapshot interval (seconds)")
    @click.option("--poll-interval-sec", default=0.5, show_default=True, type=float, help="wait_update polling interval (seconds)")
    @click.pass_context
    def gateway_run(
        ctx: click.Context,
        account_profile: str,
        runs_dir: str,
        snapshot_interval_sec: float,
        poll_interval_sec: float,
    ) -> None:
        """
        Run the AccountGateway loop for a broker account profile.
        """
        from ghtrader.cli import _acquire_locks
        from ghtrader.tq.gateway import run_gateway
        from ghtrader.tq.runtime import canonical_account_profile

        _ = ctx
        prof = canonical_account_profile(account_profile)
        _acquire_locks([f"trade:account={prof}"])
        run_gateway(
            account_profile=prof,
            runs_dir=Path(runs_dir),
            snapshot_interval_sec=float(snapshot_interval_sec),
            poll_interval_sec=float(poll_interval_sec),
        )

    @main.group("strategy")
    @click.pass_context
    def strategy_group(ctx: click.Context) -> None:
        """AI StrategyRunner: reads gateway state, writes targets."""
        _ = ctx

    @strategy_group.command("run")
    @click.option("--account", "account_profile", default="default", show_default=True, type=str, help="Account profile to bind targets to")
    @click.option(
        "--symbols",
        "-s",
        required=True,
        multiple=True,
        help="Execution symbols (can specify multiple). Must match gateway subscribed symbols.",
    )
    @click.option(
        "--model",
        "model_name",
        default="xgboost",
        show_default=True,
        type=click.Choice(["logistic", "xgboost", "lightgbm", "deeplob", "transformer", "tcn", "tlob", "ssm"]),
        help="Model type",
    )
    @click.option("--horizon", default=50, show_default=True, type=int, help="Label horizon")
    @click.option("--threshold-up", default=0.6, show_default=True, type=float, help="Long threshold")
    @click.option("--threshold-down", default=0.6, show_default=True, type=float, help="Short threshold")
    @click.option("--position-size", default=1, show_default=True, type=int, help="Target position size")
    @click.option("--artifacts-dir", default="artifacts", show_default=True, type=str, help="Artifacts directory")
    @click.option("--runs-dir", default="runs", show_default=True, type=str, help="Runs directory")
    @click.option("--poll-interval-sec", default=0.5, show_default=True, type=float, help="Poll interval (seconds)")
    @click.pass_context
    def strategy_run(
        ctx: click.Context,
        account_profile: str,
        symbols: tuple[str, ...],
        model_name: str,
        horizon: int,
        threshold_up: float,
        threshold_down: float,
        position_size: int,
        artifacts_dir: str,
        runs_dir: str,
        poll_interval_sec: float,
    ) -> None:
        from ghtrader.tq.runtime import canonical_account_profile
        from ghtrader.trading.strategy_runner import StrategyConfig, run_strategy_runner

        _ = ctx
        prof = canonical_account_profile(account_profile)
        cfg = StrategyConfig(
            account_profile=prof,
            symbols=[str(s).strip() for s in symbols if str(s).strip()],
            model_name=model_name,  # type: ignore[arg-type]
            horizon=int(horizon),
            threshold_up=float(threshold_up),
            threshold_down=float(threshold_down),
            position_size=int(position_size),
            artifacts_dir=Path(artifacts_dir),
            runs_dir=Path(runs_dir),
            poll_interval_sec=float(poll_interval_sec),
        )
        run_strategy_runner(cfg)

    @main.command("dashboard")
    @click.option("--host", default="127.0.0.1", show_default=True, help="Bind host (use 127.0.0.1 for SSH-only access)")
    @click.option("--port", default=8000, type=int, show_default=True, help="Bind port")
    @click.option("--reload/--no-reload", default=False, show_default=True, help="Auto-reload on code changes (dev only)")
    @click.option("--token", default=None, help="Optional access token for the dashboard")
    @click.pass_context
    def dashboard(ctx: click.Context, host: str, port: int, reload: bool, token: str | None) -> None:
        """Start the SSH-only web dashboard (FastAPI)."""
        _ = ctx
        if token:
            os.environ["GHTRADER_DASHBOARD_TOKEN"] = token

        import uvicorn

        uvicorn.run(
            "ghtrader.control.app:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info",
        )
