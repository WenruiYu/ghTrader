# PRD Traceability Matrix

Last updated: 2026-02-21

## Scope

This matrix links PRD domains to the current repository structure under `src/`, `tests/`, and `infra/`.
It is intentionally high-level for maintainability and is the canonical companion to `PRD.md`.

## Domain-to-Code Mapping

| PRD Domain | Primary Paths | Status |
|---|---|---|
| Data ingest + storage | `src/ghtrader/data`, `src/ghtrader/tq`, `src/ghtrader/questdb`, `infra/questdb` | Implemented (core), Partial (advanced governance roadmap) |
| Features + labels | `src/ghtrader/datasets`, `src/ghtrader/fill_labels.py`, `src/ghtrader/questdb/features_labels.py` | Partial |
| Modeling + benchmark + HPO | `src/ghtrader/research` | Partial |
| Trading runtime + control plane | `src/ghtrader/trading`, `src/ghtrader/tq/gateway.py`, `src/ghtrader/control` | Implemented |
| Regime detection | `src/ghtrader/regime.py` | Partial |
| Testing + quality gates | `tests`, `.github/workflows` | Partial |

## PRD-Expected But Missing Modules

The following module namespaces are still pending and remain out of scope for this alignment pass unless PRD is re-scoped:

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

## Notes

- `PRD.md` remains the sole planning source of truth.
- This matrix tracks implementation placement and status, not roadmap priority.
- Deferred modules above should not be exposed in active control-plane/CLI surfaces as placeholder stubs.
