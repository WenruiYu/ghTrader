---
name: scale-tick-reads-incremental-build
overview: Eliminate O(n²) per-day tick reads by adding a per-date fast path and Arrow Dataset scanning, then make features/labels builds incremental/resumable by default with strict manifest-based safety checks and explicit overwrite for rebuilds.
todos:
  - id: prd-scale-read-incremental
    content: Update PRD.md to specify scalable tick reads (per-date O(1) + Arrow dataset scanning) and incremental/resumable build default with overwrite-only full rebuild.
    status: completed
  - id: tdd-lake-read-scaling
    content: Add/extend tests in tests/test_lake.py for per-date fast path and Arrow dataset range scan.
    status: completed
  - id: lake-read-impl
    content: Implement per-date tick read helpers and Arrow dataset range scan in src/ghtrader/lake.py; update read_ticks_for_symbol to use start==end fast path.
    status: completed
    dependencies:
      - tdd-lake-read-scaling
  - id: tdd-incremental-build
    content: Add/extend tests for incremental/resumable features+labels build behavior (idempotent reruns, new day append, backfill neighbor recompute).
    status: completed
  - id: incremental-features-labels
    content: Implement overwrite flag + manifest safety gate + deterministic per-date outputs + neighbor recompute in src/ghtrader/features.py and src/ghtrader/labels.py.
    status: completed
    dependencies:
      - tdd-incremental-build
      - lake-read-impl
  - id: cli-dashboard-overwrite-flag
    content: Add --overwrite/--no-overwrite to ghtrader build and wire it through dashboard ops_build form + views argv.
    status: completed
    dependencies:
      - incremental-features-labels
---

# Scale tick reads + incremental build (features/labels)

## Goals

- Fix the current **O(n²)** behavior where feature/label builders call `read_ticks_for_symbol(... start_date=dt, end_date=dt ...)` and each call re-scans all `date=` partitions.
- Add **Arrow-first range scanning** for tick reads (so large ranges don’t require manual directory scans and we can avoid pandas unless needed).
- Make `ghtrader build` (**features + labels**) **incremental/resumable by default** (no destructive deletes), with **explicit `--overwrite`** for full rebuild.
- Add a **safety gate**: incremental build must refuse to mix outputs when existing `manifest.json` config/schema differs.

## PRD-first update

Update **[PRD.md](/home/ops/ghTrader/PRD.md)** to codify:

- Scalable lake reads: per-date reads must be **O(#parquet files for that day)**; range reads should use **Arrow Dataset scanning**.
- Incremental build default: `ghtrader build` is resumable by default; full rebuild requires explicit `--overwrite`.
- Safety gate: incremental build must fail on existing output **manifest mismatch** unless `--overwrite`.

## Part A — Tick lake scaling (remove O(n²) + Arrow Dataset)

Update **[src/ghtrader/lake.py](/home/ops/ghTrader/src/ghtrader/lake.py)**:

- Add helpers:
- `ticks_symbol_dir(data_dir, symbol, ticks_lake)`
- `ticks_date_dir(data_dir, symbol, dt, ticks_lake)`
- `read_ticks_for_symbol_date(...)` reading only `.../date=YYYY-MM-DD/*.parquet`
- Modify `read_ticks_for_symbol(...)`:
- **Fast path**: if `start_date == end_date`, directly read that date dir (no `iterdir()` over all partitions).
- **Range path**: use `pyarrow.dataset` scanning under the symbol directory with hive partitioning on `date=` and schema `TICK_ARROW_SCHEMA`.
- Keep return type as `pd.DataFrame` for compatibility, but add a new Arrow-native helper (e.g. `read_ticks_for_symbol_arrow(...) -> pa.Table`) for future scaling.
- Update the main O(n²) call sites to use the fast path implicitly (no call-site changes needed once `read_ticks_for_symbol` has the `start==end` fast path), and keep `pipeline.OfflineMicroSim.load_ticks()` functional.

## Part B — Incremental/resumable features + labels (default)

Update **[src/ghtrader/features.py](/home/ops/ghTrader/src/ghtrader/features.py)** and **[src/ghtrader/labels.py](/home/ops/ghTrader/src/ghtrader/labels.py)**:

- Add `overwrite: bool = False` parameter to `FactorEngine.build_features_for_symbol(...)` and `build_labels_for_symbol(...)`.
- **Default incremental behavior**:
- Do **not** `shutil.rmtree(out_root)`.
- Load existing symbol-level `manifest.json` if present and validate:
- `ticks_lake` matches
- for features: `enabled_factors` matches
- for labels: `horizons` and `threshold_k` match
- `schema_hash` matches
- If mismatch: **raise** with “use `--overwrite`”.
- **Deterministic per-date filenames** to prevent duplicates:
- Write `date=YYYY-MM-DD/part.parquet` (not uuid-named).
- Use atomic replace (write temp then `os.replace`) and rewrite `.sha256`.
- **Backfill correctness guards** (minimal, but important):
- **features**: if a previously-missing tick day `D` appears, recompute `D` and also recompute the **next available tick day** (to refresh the first `lookback` rows that depend on `D`).
- **labels**: if a previously-missing tick day `D` appears, recompute `D` and also recompute the **previous available tick day** (to refresh end-of-day labels that look into `D`).
- Keep existing roll-boundary logic for `ticks_lake="main_l5"`.
- Rebuild symbol-level `manifest.json` after the run by scanning date partitions and using `.sha256` sidecars for the file list.

## Part C — CLI + dashboard parity

Update **[src/ghtrader/cli.py](/home/ops/ghTrader/src/ghtrader/cli.py)**:

- Add `--overwrite/--no-overwrite` to `ghtrader build` (default `--no-overwrite`).
- Pass `overwrite` to both label and feature builders.

Update dashboard build form:

- **[src/ghtrader/control/templates/ops_build.html](/home/ops/ghTrader/src/ghtrader/control/templates/ops_build.html)**: add an “Overwrite” select.
- **[src/ghtrader/control/views.py](/home/ops/ghTrader/src/ghtrader/control/views.py)**: include `--overwrite/--no-overwrite` in the spawned `ghtrader build` argv.

## Tests-first (TDD)

Add/extend tests to lock in the new behavior:

- **Tick read fast path + range scan**: extend **[tests/test_lake.py](/home/ops/ghTrader/tests/test_lake.py)**
- ensure `read_ticks_for_symbol(... start=end ...)` only reads the requested day and returns correct rows
- add a range read test that exercises Arrow Dataset path
- **Incremental build**:
- extend **[tests/test_features.py](/home/ops/ghTrader/tests/test_features.py)** and **[tests/test_labels.py](/home/ops/ghTrader/tests/test_labels.py)**:
- running build twice without overwrite is idempotent (no duplicates)
- adding a new tick day causes only new partitions to be created
- backfill case: inserting a missing tick day triggers the minimal neighbor recompute (features: next day; labels: prev day)

## Acceptance criteria

- Feature/label builds no longer take O(n²) time just to locate one day’s ticks.
- `read_ticks_for_symbol(start=end)` does not scan all partitions.
- `ghtrader build` is **incremental by default**, and refuses to mix configs unless `--overwrite`.
- Test suite passes.