# Data Quality Operations Manual

This runbook describes how to operate the layered data quality flow for:

- `main-schedule`
- `main-l5`
- `main-l5-validate`

The target model is a 3-layer readiness contract:

- `engineering_state`: lock/write/schedule integrity (hard blocking)
- `source_state`: provider availability anomalies (warn by default, optionally blocking)
- `policy_state`: strategy thresholds (var/session-specific)

`overall_state` is the max severity across the three layers.

## 1) Parameter Priority

Validation parameters are resolved with this order:

1. UI payload
2. CLI args
3. ENV
4. defaults

The validation report stores effective values plus `policy_sources` so operators can audit why a threshold was used.

## 2) Key Environment Variables

Validation policy (`main-l5-validate`):

- `GHTRADER_L5_VALIDATE_GAP_THRESHOLD_S`
- `GHTRADER_L5_VALIDATE_STRICT_RATIO`
- `GHTRADER_L5_VALIDATE_MISSING_HALF_INFO_RATIO`
- `GHTRADER_L5_VALIDATE_MISSING_HALF_BLOCK_RATIO`
- `GHTRADER_L5_VALIDATE_GAP_BLOCK_GT30`
- `GHTRADER_L5_VALIDATE_SOURCE_BLOCKING`
- `GHTRADER_L5_VALIDATE_SOURCE_MISSING_DAYS_TOLERANCE`

Var/session overrides:

- `GHTRADER_L5_VALIDATE_*_CU_*`
- `GHTRADER_L5_VALIDATE_*_AU_*`
- `GHTRADER_L5_VALIDATE_*_AG_*`
- Session suffixes: `_DAY`, `_NIGHT`

`main-l5` health gate:

- `GHTRADER_MAIN_L5_ENFORCE_HEALTH`
- `GHTRADER_MAIN_L5_MISSING_DAYS_TOLERANCE`

WAL visibility wait:

- `GHTRADER_HEALTH_WAL_WAIT_S`

## 3) Readiness Triage

When `overall_state != ok`, use this decision path:

1. Check `reason_code`
2. Follow `action_hint`
3. Inspect layered states:
   - `engineering_state=error`: fix pipeline correctness first
   - `source_state=warn/error`: evaluate provider anomaly windows and tolerance
   - `policy_state=warn/error`: adjust var/session thresholds to strategy reality

## 4) Data Hub / API

UI:

- `/data` shows layered readiness cards + reason/action + effective policy sources.

API:

- `GET /api/data/quality/readiness`
- `GET /api/data/quality/anomalies`
- `GET /api/data/quality/profiles`

These are intended for operational dashboards and alert routing.

## 5) SLO Metrics and Alert Thresholds

Track these SLIs and route incidents by threshold:

| Metric | SLO target (7d rolling) | Warn threshold | Page/block threshold |
|---|---|---|---|
| `main_l5_build_success_rate` | >= 99% | < 99% | < 97% |
| `validation_runtime_p95` | <= 600s | > 600s | > 1200s |
| `provider_missing_day_rate` | <= 1% | > 1% | > 3% |
| `outside_session_seconds` | 0 | > 0 | > 60 |
| `gap_count_gt_30` | 0 | > 0 | > 3 |

Layered severity routing:

- Engineering: page/block on `outside_session_seconds` or hard schedule/write integrity breaks.
- Source: warn/page by `provider_missing_day_rate` and tolerance profile.
- Policy: strategy-owner review by gap/half-second policy limits.

## 6) Dry-Run Drill Checklist

Run this checklist monthly (or before major release):

1. Simulate a failed `main-l5-validate` report by injecting controlled missing segments in a staging symbol.
2. Verify dashboard/API signal consistency:
   - `/data` readiness cards
   - `GET /api/data/quality/readiness`
   - `GET /api/data/quality/anomalies`
3. Confirm alert routes by severity:
   - warn -> owner triage queue
   - error -> on-call page
4. Record MTTA/MTTR and update thresholds if repeated false-positive/false-negative patterns appear.
