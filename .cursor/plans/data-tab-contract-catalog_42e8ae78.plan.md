---
name: data-tab-contract-catalog
overview: Add a Data-tab contract catalog for a chosen variety (e.g. cu) that shows all available contracts (TqSdk instrument DB + Akshare active-ranges), highlights what’s downloaded locally, and provides one-click actions to fill missing data and run integrity/completeness checks.
todos:
  - id: prd-contract-catalog
    content: Update PRD to specify Data-tab contract catalog (TqSdk + active ranges + completeness + actions).
    status: completed
  - id: tqsdk-contracts-cache
    content: Implement cached TqSdk contract listing helper (query_quotes) with safe failure modes.
    status: completed
  - id: catalog-status-backend
    content: Compute per-contract completeness using active-ranges + lake partitions + no-data markers (with TTL cache).
    status: completed
  - id: catalog-ui-data-tab
    content: Update /data UI to add contract catalog form + table + client-side filtering.
    status: completed
  - id: catalog-actions-fill-audit
    content: Wire Fill-all / Fill-contract / Audit actions via existing ops endpoints.
    status: completed
  - id: catalog-tests
    content: Add unit + control-plane tests (no real TqSdk) for contract catalog rendering and status computation.
    status: completed
  - id: catalog-validate
    content: Run pytest and verify Data tab behaves correctly with real CU lake.
    status: completed
---

# Data tab contract catalog (TqSdk + local lake + fill)

## Goal

On the **Data** tab, allow an operator to select `(exchange, variety)` (e.g. `SHFE` + `cu`) and see:

- **All contracts available** (primary source: **TqSdk `query_quotes`**; also show Akshare active-ranges coverage)
- Which contracts are **downloaded locally** in the selected lake (`v1`/`v2`)
- **Completeness** per contract (expected trading days vs downloaded + no-data markers)
- **Actions**:
- Fill missing data for the entire variety (runs existing `download-contract-range`)
- Fill missing data for a single contract (runs existing `download`)
- Run integrity audit (`audit --scope ticks`) and link to latest report

## 0) PRD update (source of truth)

Update [`PRD.md`](/home/ops/ghTrader/PRD.md) §5.11 Observability / Data coverage:

- Add requirement for a “Variety contract catalog” view:
- contract list from **TqSdk** (cached)
- cross-check vs **Akshare active-ranges** cache
- per-contract completeness using lake partitions + no-data markers
- UI actions to start fills and audits

## 1) Backend: contract sources + status computation

### 1.1 TqSdk contract list (cached)

Add a helper module, e.g. [`src/ghtrader/tqsdk_catalog.py`](/home/ops/ghTrader/src/ghtrader/tqsdk_catalog.py):

- `list_tqsdk_contracts(exchange, var) -> {contracts, yymm_min, yymm_max, error}`
- Implementation: `TqApi(auth=get_tqsdk_auth()).query_quotes(ins_class="FUTURE", exchange_id=exchange, product_id=var)`
- Cache in-memory with TTL (e.g. 10–30 min) to avoid hitting TqSdk on every Data page load.

### 1.2 Completeness per contract (reuse existing logic)

Reuse [`src/ghtrader/control/ingest_status.py`](/home/ops/ghTrader/src/ghtrader/control/ingest_status.py) `compute_download_contract_range_status(...)`:

- Determine `start_contract`/`end_contract` from TqSdk yymm min/max.
- Compute per-contract:
- expected trading days (active range + trading calendar)
- downloaded day partitions in the selected lake root
- no-data markers
- pct complete
- Filter/label results to match **TqSdk contract set** (hybrid view):
- contracts present in TqSdk but missing active-range → show as “unknown range / skipped”
- contracts present in active-ranges but absent in TqSdk → show as “extra (not in TqSdk)”

Add a small TTL cache for this computed table (e.g. 15–60s) to keep `/data` snappy.

## 2) Data tab integration

Update [`src/ghtrader/control/views.py`](/home/ops/ghTrader/src/ghtrader/control/views.py) `/data` handler:

- Add query params like `catalog_exchange`, `catalog_var`, `catalog_lake`.
- When present, compute and pass a `catalog` object into the template:
- tqsdk list/range + errors
- active-range availability summary
- per-contract rows (completeness)
- computed `start_contract`/`end_contract` used for fill actions

Update [`src/ghtrader/control/templates/data.html`](/home/ops/ghTrader/src/ghtrader/control/templates/data.html):

- Add a new section (below existing coverage tables): **“Contract catalog (L5 ticks)”**
- Form fields:
- Exchange (default SHFE)
- Var (cu/au/ag)
- Target lake version for status/actions (v1/v2)
- Render:
- Summary pills (TqSdk contracts count + range; active-range coverage; local coverage)
- Contract table with:
  - contract code, status pill, expected/done/%
  - downloaded min/max
  - action buttons (forms) that post to existing endpoints:
  - `/ops/ingest/download_contract_range` for “Fill all missing”
  - `/ops/ingest/download` for “Fill this contract” (only when active range is known)
  - `/ops/integrity/audit` (scope=ticks, selected lake)
- Add lightweight client-side filtering (show only incomplete / only downloaded) similar to Jobs page filtering.

## 3) Tests

Add tests that do not require real TqSdk:

- Unit-test the yymm range parsing + hybrid merge (monkeypatch TqSdk listing function).
- FastAPI TestClient test:
- `GET /data?...catalog_var=cu...` renders and includes expected contract rows
- When active-ranges cache is missing, page shows the expected error/CTA
- Ensure no network calls by stubbing the TqSdk catalog function.

## 4) Validation

- `pytest`
- Manual:
- Open Data tab → select `cu` → confirm:
  - contracts list renders
  - downloaded contracts are marked
  - “Fill all missing” launches a `download-contract-range` job
  - per-contract “Fill” launches a `download` job
  - audit action launches an audit job