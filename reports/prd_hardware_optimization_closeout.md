# PRD Hardware Optimization Closeout

## Scope

- Source plan: `prd_hardware_optimization_c73c8349.plan.md`
- Governing contract: `PRD.md` (PRD-first)
- Hardware baseline: 2x EPYC / 512GB RAM / 8x GPU / NVMe

## Todo Execution Status

| Todo ID | Status | Main Deliverables |
|---|---|---|
| `prd-hardware-contract-sync` | Completed | PRD upgraded to 8-GPU DDP baseline, capacity/SLO contract text, external-pattern borrow-only policy. |
| `data-plane-throughput-pack` | Completed | `tq/ingest.py` worker-policy integration, `questdb/serving_db.py` ILP batch/persistent sender, `questdb/queries.py` Redis+TTL+timeout parameterization, `worker_policy.py` connection budgets. |
| `training-ddp-8gpu-pack` | Completed | Dashboard train route supports torchrun auto-launch + fallback, UI GPU max=8, DataLoader knobs exposed (`num_workers/prefetch/pin_memory`), sweep GPU governance under active DDP. |
| `control-plane-concurrency-pack` | Completed | `JobManager` cancellation escalation + reconciliation hardening, atomic cancel marker in `JobStore`, stale queued-pid recovery path. |
| `route-modularization-pack` | Completed | Jobs APIs moved into `control/routes/jobs.py`, app mounted via `build_api_router()`, contract method names aligned with `JobManager`. |
| `ops-compat-governance-pack` | Completed | `/ops` compatibility registry (`control/ops_compat.py`), compat contract API (`/api/ops/compat`), compat-hit logging in `views.py`. |
| `observability-slo-pack` | Completed | Unified SLO API (`/api/observability/slo`) with data/training/control planes and configurable thresholds. |
| `research-loop-pattern-pack` | Completed | `research/loop.py` + CLI `research-loop-template` for propose->implement->evaluate->learn workflow scaffold. |
| `benchmark-capacity-validation-pack` | Completed | `capacity.py` + CLI `capacity-matrix` for capacity regression matrix + smoke snapshot output. |
| `docs-and-traceability-pack` | Completed | Updated `reports/prd_traceability_matrix.md` and `reports/prd_bigbang_closeout.md`; added this closeout report. |

## Post-Closeout Cleanup Addendum

- PRD-first deep cleanup completed on top of hardware optimization baseline:
  - unreachable daily-update scheduler removed from `control/app.py`
  - removed/deferred endpoint errors centralized in `control/removed_endpoints.py`
  - env parsing and Redis config reads deduplicated via `config.py`
  - route composition continued into `control/routes/{data,models,accounts,gateway}.py`
  - job command parsing deduplicated via `control/job_command.py`

## Verification Notes

- Syntax checks: `python3 -m py_compile` across edited modules and new tests (pass).
- `.venv` regression checks completed:
  - Targeted suites for control/data-path changes (pass)
  - Full `.venv/bin/python -m pytest -q` (pass, warnings only)

## Follow-up

- Execute full test suite in project environment with pytest installed.
- Run capacity matrix and research-loop templates in staging with real QuestDB/Redis/GPU telemetry.
