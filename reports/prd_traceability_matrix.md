# PRD Traceability Matrix

Last updated: 2026-02-23

## Scope

This matrix is the PRD crosswalk companion to `PRD.md`.

- **Source file-level audit (one source file per row)** lives in `reports/architecture_file_audit_index.md`.
- This document keeps:
  - domain-to-code status,
  - tests/infra quality-gate mapping,
  - pending PRD module tracker.

## Domain-to-Code Mapping

| PRD Domain | Primary Paths | Status | Canonical evidence |
|---|---|---|---|
| Data ingest + storage | `src/ghtrader/data`, `src/ghtrader/tq`, `src/ghtrader/questdb`, `infra/questdb` | Implemented (core), Partial (governance hardening pending) | `reports/architecture_file_audit_index.md`, `reports/architecture_critical_path_deep_dive.md` |
| Features + labels | `src/ghtrader/datasets`, `src/ghtrader/fill_labels.py`, `src/ghtrader/questdb/features_labels.py` | Partial (direction/fill labels implemented; stricter leakage guardrails pending) | `reports/architecture_file_audit_index.md`, `reports/system_optimization_roadmap.md` |
| Modeling + benchmark + HPO | `src/ghtrader/research` | Partial (core implemented; lifecycle governance pending) | `reports/architecture_file_audit_index.md` |
| Trading runtime + control plane | `src/ghtrader/trading`, `src/ghtrader/tq/gateway.py`, `src/ghtrader/control` | Implemented with P0/P1 hardening backlog | `reports/architecture_critical_path_deep_dive.md`, `reports/system_optimization_roadmap.md` |
| Regime detection | `src/ghtrader/regime.py` | Partial (HMM + persistence integrated; strategy conditioning pending) | `reports/architecture_file_audit_index.md` |
| Testing + quality gates | `tests`, `.github/workflows` | Partial (canonical CI normalized; e2e/data-contract CI lanes pending) | `.github/workflows/ci.yml`, `reports/system_optimization_roadmap.md` |

## Tests and Infra Matrix

| Area | Paths | Current status | Gaps to close |
|---|---|---|---|
| Unit tests | `tests/test_*.py` | Broad unit coverage exists across control/data/trading/research | Add targeted e2e and contract-level tests |
| Integration markers | `tests` (`integration`, `ddp_integration`) | Supported but mostly optional/non-default in CI | Add controlled integration lane with explicit env gating |
| CI workflow | `.github/workflows/ci.yml` | Canonical single pipeline with deterministic coverage gate | Extend with dedicated data-contract/e2e lanes |
| QuestDB infra | `infra/questdb/docker-compose.yml`, `infra/questdb/native/*` | Present and operational | Add CI smoke for schema/health probe |

## PRD-Expected But Missing Modules

The following module namespaces are still pending and remain out of scope unless PRD is re-scoped:

1. `src/ghtrader/tca/`
2. `src/ghtrader/explainability/`
3. `src/ghtrader/model_registry/`
4. `src/ghtrader/experiments/`
5. `src/ghtrader/drift/`
6. `src/ghtrader/risk/`
7. `src/ghtrader/research/optuna_hpo.py`
8. `src/ghtrader/simulation/order_book.py`
9. `src/ghtrader/simulation/market_generator.py`
10. `src/ghtrader/simulation/multi_agent_env.py`

## Related Audit Artifacts

- `reports/architecture_file_audit_index.md`: 100 source files, per-file audit card.
- `reports/architecture_critical_path_deep_dive.md`: trading/data/control deep findings with P0/P1.
- `reports/system_optimization_roadmap.md`: prioritized P0/P1/P2 implementation batches.
- `reports/prd_bigbang_closeout.md`: cleanup decisions and historical closeout.

## Notes

- `PRD.md` remains the sole canonical planning source.
- This matrix tracks placement and status; sequencing and rollout are in roadmap documents.
- Deferred modules above should not be exposed as placeholder CLI/API surfaces.
