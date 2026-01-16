---
name: Phase 3 Cleanup
overview: Consolidate duplicate build functions, extract common utilities, and remove dead code paths to further align the codebase with the PRD's QuestDB-first architecture.
todos:
  - id: extract-row-hash-util
    content: Extract _row_hash_from_ticks_df() to a shared module and update imports in features.py and labels.py
    status: completed
  - id: consolidate-main-depth
    content: Evaluate and consolidate materialize_main_with_depth functions (Parquet vs QuestDB)
    status: completed
  - id: consolidate-features-build
    content: Consolidate build_features_for_symbol into single function with optional Parquet export
    status: completed
  - id: consolidate-labels-build
    content: Consolidate build_labels_for_symbol into single function with optional Parquet export
    status: completed
  - id: evaluate-cli-main-depth
    content: Evaluate if main-depth CLI should be deprecated in favor of main-l5
    status: completed
  - id: update-db-bench
    content: Update db_bench.py to remove or mark Parquet benchmarking as optional
    status: completed
  - id: run-tests
    content: Run full test suite to verify all consolidation changes
    status: completed
---

# Phase 3: Codebase Consolidation and Cleanup

## Summary

This phase focuses on consolidating duplicate code patterns, extracting shared utilities, and removing dead code paths. The PRD states: "No Parquet tick lake in the primary workflow" (5.3) and "Tick ingestion writes directly to QuestDB" (5.3).

## 1. Consolidate Duplicate `_row_hash_from_ticks_df` Functions

The same function is defined identically in two files:

- [src/ghtrader/features.py](src/ghtrader/features.py) (lines 31-44)
- [src/ghtrader/labels.py](src/ghtrader/labels.py) (lines 29-42)

**Action**: Extract to a shared module (e.g., `lake.py` or a new `utils.py`) and import in both places.

## 2. Evaluate Parquet-Based Build Functions

There are duplicate Parquet vs QuestDB function pairs:

| Parquet-based | QuestDB-based |
|--------------|---------------|
| `materialize_main_with_depth()` | `materialize_main_with_depth_questdb()` |
| `build_features_for_symbol()` | `build_features_for_symbol_questdb()` |
| `build_labels_for_symbol()` | `build_labels_for_symbol_questdb()` |

**Files affected**:

- [src/ghtrader/main_depth.py](src/ghtrader/main_depth.py) (lines 101 and 381)
- [src/ghtrader/features.py](src/ghtrader/features.py) (lines 445 and 758)
- [src/ghtrader/labels.py](src/ghtrader/labels.py) (lines 189 and 491)

**Options**:

1. **Keep both**: Mark Parquet-based as "export-only/offline" (for users who need Parquet output)
2. **Consolidate**: Merge into a single function with optional Parquet export
3. **Remove Parquet-based**: Delete entirely, keeping only QuestDB versions

**Recommended**: Option 2 - Consolidate by adding `export_parquet` flag to QuestDB functions and deprecating the standalone Parquet functions.

## 3. Remove Unused Parquet Lake Reading Functions

After Phase 2, several lake functions are only used by the Parquet-based build functions:

- `list_available_dates_in_lake()` - only used in Parquet build paths
- `read_ticks_for_symbol()` - only used in Parquet build paths
- `read_ticks_for_symbol_date()` - helper for above
- `read_ticks_for_symbol_arrow()` - used by `db_bench.py`

If we consolidate/remove Parquet-based builds, these can be removed or marked as export-only.

## 4. Consolidate `main-depth` and `main-l5` CLI Commands

Two CLI commands do similar things:

- `main-depth` (line 2639) - calls `materialize_main_with_depth()` (Parquet)
- `main-l5` (line 2593) - calls `build_main_l5_l5_era_only()` (QuestDB)

**Action**: Evaluate if `main-depth` should be deprecated in favor of `main-l5`.

## 5. Remove db_bench.py Parquet Dependency

[src/ghtrader/db_bench.py](src/ghtrader/db_bench.py) uses `read_ticks_for_symbol_arrow()` to benchmark Parquet reading against QuestDB. Since QuestDB is now the only primary backend:

**Options**:

1. Remove Parquet benchmarking (QuestDB-only)
2. Keep as a comparison tool for optional export workflows

## 6. Clean Up `export_parquet` Flag in CLI

The `build` command has `--export-parquet` flag (line 2023 in cli.py). This should be:

- Kept if optional Parquet export is needed
- Updated with clearer help text indicating it's non-primary

## 7. Update Test Assertions

Several tests still reference Parquet patterns that may be deprecated. After consolidation, update tests to:

- Use QuestDB-based functions as primary
- Mark Parquet-specific tests as optional/export-only

## Impact Assessment

- **Functions to consolidate**: 6 (3 pairs of Parquet/QuestDB functions)
- **Code to extract**: 1 utility function (`_row_hash_from_ticks_df`)
- **Potential removals**: 3-4 lake reading functions (if Parquet builds removed)
- **CLI commands to evaluate**: 1 (`main-depth` vs `main-l5`)

## Risk Mitigation

- The `export_parquet` option should be preserved for users needing offline Parquet workflows
- Tests should continue to verify both QuestDB primary path and optional export
- Consolidation should be backward-compatible for existing workflows