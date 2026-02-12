# PRD Big-Bang Closeout

## Scope

- Canonical spec: `PRD.md`
- Code/test/infra coverage baseline: `src/ghtrader`, `tests`, `infra`
- Traceability source: `reports/prd_traceability_matrix.md`

## End-State Snapshot

- File-level mapping: updated in `reports/prd_traceability_matrix.md` (includes new CLI/route modules).
- PRD-expected but missing modules: **10**.
- `src/ghtrader/data/` is no longer blocked by `.gitignore` and is now visible to Git tracking.
- Web UI remains a core control plane (`control/app.py`, `control/static/trading.js`) with WebSocket-first updates and bounded polling fallback.

## Alignment Decisions Applied

1. PRD structural normalization:
   - `5.2.2a` normalized to `5.2.3`.
   - Data Hub wording consolidated: `/data` is canonical; `/ops` is compatibility redirect.
2. Data-system status corrections:
   - `5.2.7` changed to **Partial**; implementation split clarified:
     - write path: `questdb/main_l5_validate.py` + `data/main_l5_validation.py`
     - query path: `data/gap_detection.py`
  - `5.4` remains **Partial** (direction + fill labels implemented; execution-cost labels pending).
3. Research/trading status corrections:
   - `5.12` changed to **Partial** (HMM + persistence + pipeline integration implemented; regime-conditional strategy pending).
   - `5.18` changed to **Partial** (Ray sweep implemented; dedicated `hpo.py` + Optuna workflow pending).
4. Hot-path safety alignment:
   - `trading/strategy_runner.py` now emits explicit safe-halt lifecycle events and state flags:
     - `safe_halt_entered`
     - `gateway_state_timeout`
     - `safe_halt_cleared`
5. Label stack progression:
   - Added `src/ghtrader/fill_labels.py` with QuestDB-first fill-label generation/storage.
   - Added CLI entry `ghtrader data fill-labels`.
   - Added unit coverage in `tests/test_fill_labels.py`.
6. Big-bang cleanup and surface normalization:
   - `cli.py` is now a thin entrypoint; business commands were moved into:
     - `src/ghtrader/cli_commands/data.py`
     - `src/ghtrader/cli_commands/research.py`
     - `src/ghtrader/cli_commands/runtime.py`
   - Legacy deferred CLI placeholders were removed from exposed command surface (e.g. `download`, `update`, `record`, `audit`, `contracts-snapshot-build`).
   - Control API dead-code chains were removed; deferred routes now return explicit `410` where retained for compatibility.
   - Core health/system endpoints were modularized into `src/ghtrader/control/routes/core.py`, and `control/app.py` now assembles these routers.
   - Models page form actions were normalized to canonical `/models/*` POST endpoints while `/ops/*` compatibility handlers remain.

## Keep/Migrate/Delete Decisions

- **Deleted files in this pass**: none.
- **Migrated/clarified truth sources**:
  - `reports/prd_traceability_matrix.md`: updated with big-bang cleanup structure (CLI module split + route modularization + pending list kept intact).
  - `reports/comprehensive_implementation_audit.md`: corrected stale status claims for `5.12` and `5.18`.
- **Deferred review (kept)**:
  - `src/ghtrader/questdb/bench.py` (performance utility)
  - `tests/test_ddp_smoke.py` (environment-dependent smoke test)

## Quality Gates

Executed in project virtual environment (`.venv`):

1. Syntax guard for refactored modules:
   - `python3 -m compileall src/ghtrader/cli.py src/ghtrader/cli_commands/data.py src/ghtrader/cli_commands/research.py src/ghtrader/cli_commands/runtime.py src/ghtrader/control/app.py src/ghtrader/control/views.py src/ghtrader/control/routes/core.py src/ghtrader/control/routes/__init__.py`
   - Result: **pass**
2. Control/CLI targeted regression:
   - `.venv/bin/pytest -q tests/test_cli.py tests/test_ops_page.py tests/test_control_api.py tests/test_control_jobs.py tests/test_control_explorer_page.py`
   - Result: **pass**
3. Full regression:
   - `.venv/bin/pytest -q`
   - Result: **pass** (warnings only; no test failures)
4. Lint diagnostics for changed modules:
   - `ReadLints` on modified CLI/control files
   - Result: **no remaining lints**.

## Residual Gaps (Authoritative Pending List)

Pending modules tracked in `reports/prd_traceability_matrix.md`:

- `src/ghtrader/execution_labels.py`
- `src/ghtrader/ensemble.py`
- `src/ghtrader/simulator.py`
- `src/ghtrader/anomaly.py`
- `src/ghtrader/drift.py`
- `src/ghtrader/tca.py`
- `src/ghtrader/model_registry.py`
- `src/ghtrader/experiments.py`
- `src/ghtrader/explain.py`
- `src/ghtrader/hpo.py`

## Recommended Next Stage

1. Complete label stack with execution-cost labels (`execution_labels.py`) for end-to-end execution supervision.
2. Add model registry + experiments before enabling promotion automation beyond current gates.
3. Implement drift and TCA modules to close real-time risk feedback loop.
