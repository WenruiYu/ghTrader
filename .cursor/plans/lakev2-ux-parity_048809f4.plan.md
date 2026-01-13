---
name: lakev2-ux-parity
overview: Make the dashboard Data coverage page and continuous-symbol schedule resolution fully lake-version aware (v1/v2), aligning behavior with PRD requirements and the GHTRADER_LAKE_VERSION default.
todos:
  - id: prd-lakev2-ux-requirements
    content: Update PRD control-plane + symbol resolution requirements to explicitly cover lake_v1/lake_v2 UX behavior.
    status: completed
  - id: dashboard-data-coverage-lakev
    content: Add lake_version=v1|v2|both support to /data and update data.html to render v1/v2 coverage accordingly.
    status: completed
  - id: symbol-resolver-lakev2
    content: Update continuous alias resolver to search schedule.parquet under lake_v2 derived dataset roots and prefer GHTRADER_LAKE_VERSION.
    status: completed
  - id: tests-lakev2-parity
    content: Add tests for /data lake_version switching and v2-only schedule resolution fallback.
    status: completed
---

# lake_v2 UX parity (Data coverage + schedule resolution)

## Goal

- Make dashboard **Data coverage** reflect both `lake_v1` and `lake_v2` tick stores.
- Make continuous-symbol **schedule resolution** work when only `lake_v2` derived datasets exist, and prefer the configured lake version.

## Why (PRD alignment)

- PRD requires the control plane to show **data coverage** (lake partitions by symbol/date) (see [PRD.md](/home/ops/ghTrader/PRD.md) around the “Observability” bullets).
- PRD requires trading to resolve continuous aliases via the persisted roll schedule / schedule copies (see [PRD.md](/home/ops/ghTrader/PRD.md) `§5.12.5 Symbol semantics`).

## Implementation plan

### 1) Update PRD to encode lake-version UX requirements

- In [PRD.md](/home/ops/ghTrader/PRD.md):
- Extend the control-plane “data coverage” requirement to explicitly mention `lake_v1` vs `lake_v2` support.
- Clarify that continuous-alias resolution may fall back to `schedule.parquet` copied under **either** lake root (`data/lake/...` or `data/lake_v2/...`), with preference for the selected `GHTRADER_LAKE_VERSION`.

### 2) Dashboard Data coverage page: support `lake_version` selection

Files:

- [src/ghtrader/control/views.py](/home/ops/ghTrader/src/ghtrader/control/views.py)
- [src/ghtrader/control/templates/data.html](/home/ops/ghTrader/src/ghtrader/control/templates/data.html)

Changes:

- Add query param handling on `/data`:
- Accept `lake_version=v1|v2|both`.
- Default to `get_lake_version()` (env `GHTRADER_LAKE_VERSION`).
- Scan the correct roots:
- raw ticks: `data/lake*/ticks`
- derived main_l5 ticks: `data/lake*/main_l5/ticks`
- Render UI with a small dropdown (GET form) to switch lake version; when `both`, show both sections.

### 3) Continuous symbol schedule resolution: include `lake_v2` derived schedule copy

Files:

- [src/ghtrader/symbol_resolver.py](/home/ops/ghTrader/src/ghtrader/symbol_resolver.py)
- (optional) [src/ghtrader/trade.py](/home/ops/ghTrader/src/ghtrader/trade.py)

Changes:

- Update `_candidate_schedule_paths` / `resolve_trading_symbol` so that:
- It still prefers the canonical roll schedule under `data/rolls/...`.
- If that is missing, it searches for `schedule.parquet` copied under:
  - `data/lake_v2/main_l5/ticks/symbol=<KQ.m@...>/schedule.parquet`
  - `data/lake/main_l5/ticks/symbol=<KQ.m@...>/schedule.parquet`
- When both exist, prefer the configured `GHTRADER_LAKE_VERSION`.
- Keep call-sites stable by defaulting to env selection; if we add an explicit `lake_version` arg, update `trade.py` to pass `get_lake_version()`.

### 4) Tests

Files:

- [tests/test_control_api.py](/home/ops/ghTrader/tests/test_control_api.py)
- New test (suggested): `tests/test_symbol_resolver_lake_version.py`

Add unit tests to prevent regressions:

- `/data` route:
- Create distinct v1 vs v2 tick partition directories; assert `lake_version=v1` shows only v1 symbol, `v2` shows only v2 symbol, `both` shows both.
- `resolve_trading_symbol`:
- Create only the **v2** derived schedule copy and assert continuous alias resolves successfully.

## Notes / non-goals

- This pass focuses on **ticks coverage** and **schedule resolution**. Enhancing features/labels coverage to display the manifest’s `lake_version` is useful but can be a follow-up.