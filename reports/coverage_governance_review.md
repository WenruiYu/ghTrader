# Coverage Governance Review

Last updated: 2026-02-23

This document tracks why specific modules are currently excluded from the deterministic coverage gate in `pyproject.toml`, and what conditions must be met to remove each exclusion.

## Exclusion Rationale Matrix

| Module | Current rationale | Exit criteria to remove exclusion |
|---|---|---|
| `src/ghtrader/data_provider.py` | Abstract provider interfaces with thin runtime wiring; low direct behavior value in unit gate. | Add direct contract tests for provider edge cases and remove exclusion. |
| `src/ghtrader/questdb/bench.py` | Benchmark-oriented helper, not part of deterministic functional path. | Add deterministic benchmark harness assertions (or move to tooling package). |
| `src/ghtrader/regime.py` | Regime training path is compute-heavy and only partially covered through higher-level flows. | Add focused unit tests for model fit/infer boundaries and state serialization. |
| `src/ghtrader/research/eval.py` | Promotion/backtest eval contains optional-runtime branches and heavy numeric paths. | Add deterministic promotion-gate + metrics fixtures and remove exclusion. |
| `src/ghtrader/tq/catalog.py` | Live provider catalog path depends on external credentials/network. | Add stable offline fixture mode tests covering parse/cache/error branches. |
| `src/ghtrader/tq/eval.py` | Tier1 backtest requires TqSdk runtime and market simulation context. | Add deterministic mock-based unit coverage for non-Tq business logic branches. |
| `src/ghtrader/tq/execution.py` | Execution adapter paths are runtime/provider coupled and not covered in deterministic unit gate. | Add adapter contract tests with fake execution provider. |
| `src/ghtrader/tq/l5_start.py` | First-L5 probing is network/provider dependent and can be slow/flaky without controlled fixtures. | Add fixture-driven probe planner tests plus cache fallback matrix. |
| `src/ghtrader/tq/main_schedule.py` | Main-contract extraction path depends on provider quote metadata and timing behavior. | Add fixture-driven event extraction tests across roll/holiday edge cases. |
| `src/ghtrader/tq/paper.py` | Paper-trading adapter path is optional and depends on Tq runtime behavior. | Add deterministic strategy-paper adapter tests for order lifecycle transitions. |

## Governance Rules

- Every exclusion must have a documented rationale and explicit exit criteria.
- New exclusions require updating both `pyproject.toml` and this file in the same change.
- CI includes a coverage-governance test to prevent stale/undocumented exclusions.
