# PRD Big-Bang Closeout

Last updated: 2026-02-21

## Goal

Track alignment decisions for the 2026-02 PRD cleanup boundary:

- remove obsolete/deferred dead surfaces,
- keep compatibility behavior explicit and minimal,
- preserve canonical entrypoints under `/data`, `/models`, `/trading`.

## Decisions

### 1) Compatibility Layer Policy

- `/ops` remains a compatibility layer for legacy navigation and form actions.
- Compatibility is restricted to `redirect` or `alias` behavior for active canonical routes.
- Long-term placeholder `removed_410` route registrations are not retained once migration completes.

### 2) Control-Plane Surface Cleanup

- Deprecated API stubs previously exposed via `routes/data.py` and `app.py` are removed from active route registration.
- Data Hub and model/trading workflows continue through canonical endpoints only.

### 3) Dashboard Status Semantics

- Dashboard summary no longer treats `main_l5` as hardcoded `deferred`.
- `data_status` and `pipeline.main_l5` are derived from actual QuestDB/runtime state.

### 4) CLI Surface Normalization

- Root command decorators are normalized to explicit command names for stable PRD mapping.
- `cli.py` remains a thin composition root.

### 5) Quality Gate Baseline

- CI baseline includes:
  - `pytest`
  - `pytest --cov=ghtrader --cov-fail-under=50`
- Integration tests remain skipped by default unless explicitly enabled via env markers.

## Residual Gaps (Intentionally Pending)

- TCA module, explainability suite, model registry, experiment tracking, and drift package remain pending per PRD phase boundaries.
- These pending capabilities are tracked in `reports/prd_traceability_matrix.md`.
