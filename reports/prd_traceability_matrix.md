# PRD Traceability Matrix (Big Bang)

- Generated from repository snapshot under `/home/ops/ghTrader`
- Covered files: matrix-maintained (src/tests/infra)
- Unmapped files: **0**

## File-to-PRD Mapping

| File | Subsystem | PRD Sections | Status | Notes |
|---|---|---|---|---|
| `infra/questdb/docker-compose.yml` | Infrastructure | `5.2, 6.3.1` | Implemented |  |
| `infra/questdb/native/README.md` | Infrastructure | `5.2, 6.3.1` | Implemented |  |
| `infra/questdb/native/questdb.env` | Infrastructure | `5.2, 6.3.1` | Implemented |  |
| `infra/questdb/native/questdb.service` | Infrastructure | `5.2, 6.3.1` | Implemented |  |
| `infra/questdb/native/questdb.system.service` | Infrastructure | `5.2, 6.3.1` | Implemented |  |
| `infra/questdb/native/server.conf` | Infrastructure | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/__init__.py` | Package | `1, 3.2` | Implemented |  |
| `src/ghtrader/cli.py` | CLI | `9, 5.10, 5.11` | Implemented | Thin entrypoint; command implementations delegated to `cli_commands/*`. |
| `src/ghtrader/cli_commands/__init__.py` | CLI commands | `9` | Implemented |  |
| `src/ghtrader/cli_commands/data.py` | CLI commands | `9, 5.1, 5.2` | Implemented | Data-domain command registration (`data`, `main-schedule`, `main-l5`). |
| `src/ghtrader/cli_commands/db.py` | CLI commands | `9, 5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/cli_commands/features.py` | CLI commands | `9, 5.3, 5.4` | Implemented |  |
| `src/ghtrader/cli_commands/research.py` | CLI commands | `9, 5.5-5.9, 5.18` | Implemented | Build/train/eval/sweep lifecycle commands. |
| `src/ghtrader/cli_commands/runtime.py` | CLI commands | `9, 5.10, 5.11` | Implemented | Account/gateway/strategy/dashboard runtime commands. |
| `src/ghtrader/config.py` | Config | `7.2, 6.5` | Implemented |  |
| `src/ghtrader/control/__init__.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/app.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/auth.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/db.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/jobs.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/locks.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/ops_compat.py` | Control | `5.10, 6.3.1.1` | Implemented | `/ops` compatibility governance contract + canonical mapping registry. |
| `src/ghtrader/control/progress.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/routes/__init__.py` | Control/API | `5.10` | Implemented |  |
| `src/ghtrader/control/routes/core.py` | Control/API | `5.10` | Implemented | Core modularized endpoints (`/health`, `/api/system`, `/api/questdb/metrics`). |
| `src/ghtrader/control/routes/health.py` | Control/API | `5.10` | Implemented |  |
| `src/ghtrader/control/routes/jobs.py` | Control/API | `5.10` | Implemented |  |
| `src/ghtrader/control/settings.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/slo.py` | Control/Observability | `6.3.1.1, 5.10` | Implemented | Unified SLO snapshot across data/training/control planes (`/api/observability/slo`). |
| `src/ghtrader/control/static/app.css` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/static/trading.js` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/system_info.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/base.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/data.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/explorer.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/index.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/job_detail.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/jobs.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/models.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/system.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/templates/trading.html` | Control/UI | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/control/views.py` | Control | `5.10, 5.11` | Implemented |  |
| `src/ghtrader/data/__init__.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/exchange_events.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/field_quality.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/gap_detection.py` | Data lifecycle | `5.2.7` | Implemented | QuestDB tick-gap ledger query layer; gap write path is in `questdb/main_l5_validate.py` |
| `src/ghtrader/data/main_l5.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/main_l5_validation.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/main_schedule.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/manifest.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/ticks_schema.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/trading_calendar.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data/trading_sessions.py` | Data lifecycle | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/data_provider.py` | Provider abstraction | `5.1.3` | Implemented |  |
| `src/ghtrader/fill_labels.py` | Labeling | `5.4.2` | Implemented | Fill-probability label builder + QuestDB storage (`ghtrader_fill_labels_v2`) |
| `src/ghtrader/datasets/__init__.py` | Datasets | `5.3, 5.4` | Implemented |  |
| `src/ghtrader/datasets/feature_store.py` | Datasets | `5.3, 5.4` | Implemented |  |
| `src/ghtrader/datasets/features.py` | Datasets | `5.3, 5.4` | Implemented |  |
| `src/ghtrader/datasets/labels.py` | Datasets | `5.3, 5.4` | Implemented |  |
| `src/ghtrader/questdb/__init__.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/bench.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/client.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/features_labels.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/field_quality.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/main_l5_validate.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/main_schedule.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/migrate.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/queries.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/questdb/serving_db.py` | QuestDB | `5.2, 6.3.1` | Implemented |  |
| `src/ghtrader/regime.py` | Research | `5.12` | Implemented |  |
| `src/ghtrader/research/__init__.py` | Research | `5.5-5.9, 5.18` | Implemented |  |
| `src/ghtrader/research/benchmark.py` | Research/benchmark | `5.9` | Implemented |  |
| `src/ghtrader/research/distributed.py` | Research | `5.5-5.9, 5.18` | Implemented |  |
| `src/ghtrader/research/eval.py` | Research/eval | `5.7` | Implemented |  |
| `src/ghtrader/research/loop.py` | Research workflow | `5.8, 5.18, 10.2-10.4` | Implemented | Qlib/RD-Agent/Kronos pattern-borrowed research loop template scaffold. |
| `src/ghtrader/research/models.py` | Research/models | `5.5` | Implemented |  |
| `src/ghtrader/research/online.py` | Research/online | `5.6` | Implemented |  |
| `src/ghtrader/research/pipeline.py` | Research/pipeline | `5.8, 5.18` | Implemented |  |
| `src/ghtrader/tq/__init__.py` | Tq runtime | `5.1, 5.11` | Implemented |  |
| `src/ghtrader/tq/catalog.py` | Data ingest | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/tq/eval.py` | Backtest | `5.7.1` | Implemented |  |
| `src/ghtrader/tq/execution.py` | Execution adapter | `5.11.4` | Implemented |  |
| `src/ghtrader/tq/gateway.py` | Trading gateway | `5.11` | Implemented |  |
| `src/ghtrader/tq/ingest.py` | Data ingest | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/tq/l5_start.py` | Data ingest | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/tq/main_schedule.py` | Data ingest | `5.1, 5.2` | Implemented |  |
| `src/ghtrader/tq/paper.py` | Paper trading | `5.6, 5.11` | Implemented |  |
| `src/ghtrader/tq/runtime.py` | Tq runtime | `5.1, 5.11` | Implemented |  |
| `src/ghtrader/trading/__init__.py` | Trading runtime | `5.11` | Implemented |  |
| `src/ghtrader/trading/execution.py` | Trading runtime | `5.11` | Implemented |  |
| `src/ghtrader/trading/strategy_control.py` | Trading runtime | `5.11` | Implemented |  |
| `src/ghtrader/trading/strategy_runner.py` | Trading runtime | `5.11` | Implemented |  |
| `src/ghtrader/trading/symbol_resolver.py` | Trading runtime | `5.11` | Implemented |  |
| `src/ghtrader/capacity.py` | Capacity/Benchmark | `6.3.1.1, 6.4` | Implemented | Capacity regression matrix + smoke snapshot report scaffold (`ghtrader capacity-matrix`). |
| `src/ghtrader/util/__init__.py` | Utilities | `3.2, 6.4` | Implemented |  |
| `src/ghtrader/util/hash.py` | Utilities | `3.2, 6.4` | Implemented |  |
| `src/ghtrader/util/json_io.py` | Utilities | `3.2, 6.4` | Implemented |  |
| `src/ghtrader/util/l5_detection.py` | Utilities | `3.2, 6.4` | Implemented |  |
| `src/ghtrader/util/safe_parse.py` | Utilities | `3.2, 6.4` | Implemented |  |
| `src/ghtrader/util/time.py` | Utilities | `3.2, 6.4` | Implemented |  |
| `src/ghtrader/util/worker_policy.py` | Utilities | `3.2, 6.4` | Implemented |  |
| `tests/__init__.py` | Testing | `6.4` | Implemented |  |
| `tests/conftest.py` | Testing | `6.4` | Implemented |  |
| `tests/test_accounts_profiles.py` | Testing | `6.4` | Implemented |  |
| `tests/test_benchmark.py` | Testing | `6.4` | Implemented |  |
| `tests/test_cli.py` | Testing | `6.4` | Implemented | Updated for removed legacy CLI placeholder commands. |
| `tests/test_cli_session_registry.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_accounts_api.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_api.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_explorer_page.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_gateway_api.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_jobs.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_train_launch.py` | Testing | `6.4, 6.3.1.1` | Implemented | Regression for torchrun auto-launch/fallback and sweep GPU governance under active DDP. |
| `tests/test_control_locks.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_strategy_api.py` | Testing | `6.4` | Implemented |  |
| `tests/test_control_trading_console_api.py` | Testing | `6.4` | Implemented |  |
| `tests/test_dashboard_ui_polish_v2.py` | Testing | `6.4` | Implemented |  |
| `tests/test_data_page_cache.py` | Testing | `6.4` | Implemented |  |
| `tests/test_ddp_smoke.py` | Testing | `6.4` | Implemented |  |
| `tests/test_distributed.py` | Testing | `6.4` | Implemented |  |
| `tests/test_feature_store.py` | Testing | `6.4` | Implemented |  |
| `tests/test_fill_labels.py` | Testing | `6.4, 5.4.2` | Implemented | Unit coverage for fill label generation semantics |
| `tests/test_features.py` | Testing | `6.4` | Implemented |  |
| `tests/test_field_quality_invariants.py` | Testing | `6.4` | Implemented |  |
| `tests/test_field_quality_upsert.py` | Testing | `6.4` | Implemented |  |
| `tests/test_job_progress.py` | Testing | `6.4` | Implemented |  |
| `tests/test_l5_detection.py` | Testing | `6.4` | Implemented |  |
| `tests/test_labels.py` | Testing | `6.4` | Implemented |  |
| `tests/test_main_l5_update.py` | Testing | `6.4` | Implemented |  |
| `tests/test_main_l5_validate_hybrid.py` | Testing | `6.4` | Implemented |  |
| `tests/test_main_l5_validate_top_gaps.py` | Testing | `6.4` | Implemented |  |
| `tests/test_main_schedule.py` | Testing | `6.4` | Implemented |  |
| `tests/test_main_schedule_update.py` | Testing | `6.4` | Implemented |  |
| `tests/test_models.py` | Testing | `6.4` | Implemented |  |
| `tests/test_online.py` | Testing | `6.4` | Implemented |  |
| `tests/test_ops_page.py` | Testing | `6.4` | Implemented |  |
| `tests/test_ops_compat_contract.py` | Testing | `6.4, 5.10` | Implemented | Locks `/ops` compatibility contract endpoint + alias route behavior. |
| `tests/test_observability_slo.py` | Testing | `6.4, 6.3.1.1` | Implemented | SLO API shape and queue-threshold state regression. |
| `tests/test_pipeline.py` | Testing | `6.4` | Implemented |  |
| `tests/test_questdb_client_retry.py` | Testing | `6.4` | Implemented |  |
| `tests/test_questdb_ingest_shape.py` | Testing | `6.4` | Implemented |  |
| `tests/test_questdb_semantics.py` | Testing | `6.4` | Implemented |  |
| `tests/test_questdb_sender_compat.py` | Testing | `6.4` | Implemented |  |
| `tests/test_capacity_matrix.py` | Testing | `6.4, 6.3.1.1` | Implemented | Capacity matrix report generation regression. |
| `tests/test_research_loop_template.py` | Testing | `6.4, 5.18` | Implemented | Research loop template command scaffold regression. |
| `tests/test_sota_models.py` | Testing | `6.4` | Implemented |  |
| `tests/test_strategy_control.py` | Testing | `6.4` | Implemented |  |
| `tests/test_strategy_runner.py` | Testing | `6.4` | Implemented |  |
| `tests/test_strategy_supervisor_tick.py` | Testing | `6.4` | Implemented |  |
| `tests/test_symbol_normalization.py` | Testing | `6.4` | Implemented |  |
| `tests/test_symbol_resolver_schedule.py` | Testing | `6.4` | Implemented |  |
| `tests/test_tq_gateway_mvp.py` | Testing | `6.4` | Implemented |  |
| `tests/test_trading_calendar.py` | Testing | `6.4` | Implemented |  |
| `tests/test_trading_observability.py` | Testing | `6.4` | Implemented |  |
| `tests/test_trading_runtime_utils.py` | Testing | `6.4` | Implemented |  |
| `tests/test_trading_symbol_and_orders.py` | Testing | `6.4` | Implemented |  |
| `tests/test_worker_policy.py` | Testing | `6.4` | Implemented |  |

## PRD-Expected But Missing Modules

Total currently missing modules: **10**.

| Expected Path | PRD Section | Status |
|---|---|---|
| `src/ghtrader/execution_labels.py` | `5.4.3` | Pending |
| `src/ghtrader/ensemble.py` | `5.5.8` | Pending |
| `src/ghtrader/simulator.py` | `5.7.3-5.7.6` | Pending |
| `src/ghtrader/anomaly.py` | `5.13` | Pending |
| `src/ghtrader/drift.py` | `5.13` | Pending |
| `src/ghtrader/tca.py` | `5.14` | Pending |
| `src/ghtrader/model_registry.py` | `5.15` | Pending |
| `src/ghtrader/experiments.py` | `5.16` | Pending |
| `src/ghtrader/explain.py` | `5.17` | Pending |
| `src/ghtrader/hpo.py` | `5.18` | Pending |

## Big-Bang Disposition Notes

Default disposition for mapped files is **Keep** unless explicitly listed below.

| Path | Disposition | Rationale |
|---|---|---|
| `src/ghtrader/data/gap_detection.py` | Keep | Canonical query surface for `ghtrader_tick_gaps_v2`; aligned with PRD §5.2.7 after mapping correction. |
| `src/ghtrader/cli.py` + `src/ghtrader/cli_commands/*` | Keep/Migrate | Command logic migrated out of monolithic `cli.py`; old deferred placeholders removed from exposed surface. |
| `src/ghtrader/control/app.py` + `src/ghtrader/control/routes/core.py` | Keep/Migrate | Core endpoints split into route modules; deferred API chains removed or converted to explicit compatibility errors. |
| `src/ghtrader/control/routes/jobs.py` | Keep/Migrate | Jobs API contract aligned with `JobManager` (`start_job/cancel_job/read_log_tail`) and mounted through `build_api_router()`. |
| `src/ghtrader/control/ops_compat.py` + `src/ghtrader/control/slo.py` | Keep/Add | Added governance contracts for `/ops` compatibility and unified SLO observability snapshots. |
| `src/ghtrader/research/loop.py` + `src/ghtrader/capacity.py` | Keep/Add | Added workflow template + capacity regression matrix scaffolds for PRD-first optimization cycle. |
| `src/ghtrader/questdb/bench.py` | Deferred | Standalone performance utility; not runtime-critical, keep for profiling until dedicated perf playbook is extracted. |
| `tests/test_ops_page.py` | Keep | Backward-compatibility validation for `/ops` → `/data` redirects in control plane. |
| `tests/test_dashboard_ui_polish_v2.py` | Keep | UI regression coverage for trading dashboard interactions. |
| `tests/test_ddp_smoke.py` | Deferred | Environment-dependent distributed smoke test; retain as optional quality signal. |
