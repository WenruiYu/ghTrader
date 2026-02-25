# Incident Response Runbook

Last updated: 2026-02-23

This runbook defines operational response steps for the core ghTrader failure modes:

- QuestDB outage/degradation
- Data integrity regression (schema/null-rate/row-hash)
- `main_l5` validation gate failure
- Model regression after candidate promotion

## 1) Severity Levels

- **SEV-1**: Trading/runtime safety risk or hard platform outage (data/control unavailable).
- **SEV-2**: Partial degradation with material quality impact (pipeline blocked, stale data).
- **SEV-3**: Localized or recoverable issue with workaround available.

## 2) First 10 Minutes Checklist

1. Confirm scope:
   - affected variety/account (`cu/au/ag`, profile id)
   - affected plane (`data`, `control`, `trading`)
2. Capture evidence:
   - latest job logs under `runs/control/logs/`
   - latest reports under `runs/control/reports/`
   - dashboard/API snapshots for impacted endpoints
3. Stabilize:
   - pause risky downstream actions (new training/promotion/trading triggers)
   - keep current production artifacts unchanged until diagnosis is clear
4. Assign incident owner and channel, then start timeline notes.

## 3) Playbooks by Failure Mode

### 3.1 QuestDB Outage or Unhealthy PGWire

Detection:

- API/CLI errors with connection refused/reset or query timeout patterns.
- `questdb_reachable_pg()` returns `ok=false`.

Immediate actions:

1. Verify reachability and service status.
2. Stop write-heavy pipeline jobs temporarily (`main-schedule`, `main-l5`, validation backfills).
3. Keep control-plane read APIs in degraded mode (no forced restarts loop).

Recovery checks:

- Run a minimal PGWire probe (`SELECT 1`) and one representative read query.
- Resume pipeline in order:
  1) `main-schedule`
  2) `main-l5` (update mode first)
  3) `data main-l5-validate`

### 3.2 Data Contract Regression (Schema/Null-Rate/Row-Hash)

Detection:

- CI `data-contract` preflight failure.
- `ghtrader data contract-check` returns non-zero.

Immediate actions:

1. Freeze merge/deploy of data-affecting changes.
2. Identify failure class:
   - schema missing columns
   - null-rate threshold breach
   - row-hash integrity mismatch
3. Compare failing artifact source (fixture/input file) with last green run.

Recovery checks:

- Re-run:
  - `pytest -q tests/test_data_contract_checks.py`
  - `ghtrader data contract-check --json`
- If schema changed intentionally, update migration/tests/contracts in one change set before unfreezing.

### 3.3 `main_l5` Validation Gate Failure

Detection:

- `main_l5` job fails with validation gate message (`reason_code`/`action_hint` present).
- readiness endpoint reports `overall_state=warn/error`.

Immediate actions:

1. Read latest validation report in `runs/control/reports/main_l5_validate/`.
2. Classify by layered state:
   - `engineering_state`: session/schedule/write integrity
   - `source_state`: provider anomaly windows
   - `policy_state`: threshold calibration issue
3. Execute hinted action from `action_hint`, then rerun validation before any train/promotion workflow.

Recovery checks:

- `state` transitions to `ok` (or an accepted documented warn profile).
- gap/outside-session metrics return below configured thresholds.

### 3.4 Model Regression After Promotion

Detection:

- Benchmark/backtest gate degrades beyond expected band after candidate activation.

Immediate actions:

1. Freeze new promotions.
2. Roll back to previous known-good production model artifact.
3. Record candidate id, production id, and affected symbol/horizon.

Recovery checks:

- Post-rollback benchmark returns to baseline envelope.
- Promotion gate criteria and input data contracts are re-verified before re-attempt.

## 4) Communication Template

- **Summary**: <what failed, impact, start time>
- **Current severity**: <SEV-1/2/3>
- **Containment**: <what is paused/isolated>
- **Next action**: <owner + ETA>
- **Recovery status**: <in progress / restored / monitoring>

## 5) Post-Incident Closeout

Required before closure:

1. Root cause statement (code/config/data/provider).
2. Why existing gates did not prevent it earlier.
3. Permanent corrective action with owner and due date.
4. Added/updated test, alert, or runbook step.
