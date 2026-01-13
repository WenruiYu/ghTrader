---
name: CI+MainDepthScaling
overview: "Add CI quality gates (GitHub Actions + coverage threshold) and scale main-with-depth pipeline: faster Arrow-based materialization plus roll-boundary-safe feature/label building for derived KQ.m@ symbols."
todos:
  - id: prd-ci-main-depth
    content: Update PRD.md with CI gates and roll-boundary-safe derived dataset requirements.
    status: pending
  - id: ci-workflow
    content: Add GitHub Actions workflow to run pytest + coverage gate (skip integration tests by default).
    status: pending
    dependencies:
      - prd-ci-main-depth
  - id: main-depth-arrow
    content: Refactor main_depth materialization to Arrow-first partition processing + copy schedule/provenance into derived root.
    status: pending
    dependencies:
      - prd-ci-main-depth
  - id: derived-lake-read
    content: Add ticks lake selection to lake read/list APIs and wire through build/feature/label paths.
    status: pending
    dependencies:
      - main-depth-arrow
  - id: roll-boundary-safety
    content: Implement roll-boundary resets in features/labels when building from main_l5 derived ticks.
    status: pending
    dependencies:
      - derived-lake-read
  - id: main-depth-tests
    content: Add/extend tests to prove no roll leakage and keep existing tests passing.
    status: pending
    dependencies:
      - roll-boundary-safety
      - ci-workflow
---

# CI quality gates + main-with-depth scaling

## Goal

Implement two improvement tracks you selected:

- **CI quality gates**: add automated test runs + a minimal coverage gate.
- **Main-with-depth scaling**: speed up `main-depth` materialization and ensure **no roll-boundary leakage** when building labels/features on the derived `KQ.m@...` dataset.

## PRD-first updates

Update [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md) to explicitly define:

- **CI requirements**:
- GitHub Actions workflow runs on PR/push.
- Runs `pytest` and enforces coverage `--cov=ghtrader --cov-fail-under=<MIN>`.
- Skips credentialed/integration tests by default.
- **Derived main-with-depth correctness**:
- Builders must not carry rolling windows or label lookahead across days where the underlying contract changes.
- Materialization should persist schedule provenance in the derived dataset directory.

## Implementation plan

### 1) CI workflow (GitHub Actions)

- Add `.github/workflows/ci.yml`:
- Setup Python (3.12)
- Install deps: `pip install -e .[dev]` (and optionally `.[control]` if we want to run dashboard tests too)
- Run: `pytest --cov=ghtrader --cov-report=term-missing --cov-fail-under=<MIN>`
- Ensure integration tests remain skipped unless explicitly enabled.

### 2) Scale main-depth materialization (Arrow-first)

Update [`/home/ops/ghTrader/src/ghtrader/main_depth.py`](/home/ops/ghTrader/src/ghtrader/main_depth.py):

- Avoid `read_ticks_for_symbol()` → pandas roundtrip per day.
- Instead, for each schedule day:
- discover underlying parquet partition files directly under `data/lake/ticks/symbol=<UNDERLYING>/date=<YYYY-MM-DD>/`
- read each file as an Arrow table (`pq.read_table(schema=TICK_ARROW_SCHEMA)`)
- replace the `symbol` column with the derived symbol using Arrow ops
- write out derived parquet(s) to `data/lake/main_l5/ticks/symbol=<DERIVED>/date=<YYYY-MM-DD>/`
- Persist provenance:
- copy the schedule into the derived root (e.g. `.../symbol=<DERIVED>/schedule.parquet`)
- include schedule hash + scan range in manifest

### 3) Enable roll-boundary-safe feature/label builds on derived lake

Currently, `build`/features/labels read only from the raw lake. Add support for selecting the tick source.

- Update [`/home/ops/ghTrader/src/ghtrader/lake.py`](/home/ops/ghTrader/src/ghtrader/lake.py):
- add optional `ticks_lake` (e.g., `raw` vs `main_l5`) to `list_available_dates()` and `read_ticks_for_symbol()`.
- Update [`/home/ops/ghTrader/src/ghtrader/features.py`](/home/ops/ghTrader/src/ghtrader/features.py) and [`/home/ops/ghTrader/src/ghtrader/labels.py`](/home/ops/ghTrader/src/ghtrader/labels.py):
- accept `ticks_lake` parameter
- when `ticks_lake == main_l5`, load the derived schedule mapping and:
  - **reset lookback tail** when underlying changes (rolling factors don’t span the roll)
  - **disable next-day lookahead** for labels when underlying changes (so end-of-day labels become NaN rather than leaking)
- Update [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py):
- extend `ghtrader build` with `--ticks-lake raw|main_l5` (default `raw`)

### 4) Tests-first

- Add unit tests to prove no roll leakage:
- synthetic schedule where day1 uses underlying A and day2 uses underlying B
- verify labels on day1 end don’t use day2 data when underlying switches
- verify rolling factor (e.g. `return_10`) at start of day2 is NaN when underlying switches
- Keep existing `test_main_depth.py` passing; extend with at least one boundary case.

## Files expected to change/add

- **Update**: [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md)
- **Add**: `.github/workflows/ci.yml`
- **Update**: [`/home/ops/ghTrader/src/ghtrader/main_depth.py`](/home/ops/ghTrader/src/ghtrader/main_depth.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/lake.py`](/home/ops/ghTrader/src/ghtrader/lake.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/features.py`](/home/ops/ghTrader/src/ghtrader/features.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/labels.py`](/home/ops/ghTrader/src/ghtrader/labels.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py)
- **Add/Update tests**: `tests/test_main_depth.py` (+ new tests for roll-boundary safety)

## Acceptance criteria

- CI runs on PR/push and fails if tests fail or coverage drops below the chosen minimum.
- `ghtrader main-depth ...` is faster (no pandas roundtrip per day) and persists schedule provenance under the derived root.
- `ghtrader build --symbol KQ.m@SHFE.cu --ticks-lake main_l5` builds labels/features without any roll-boundary leakage.