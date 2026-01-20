# PRD Best-in-Class Gap Analysis

This report compares the current PRD against best-in-class AI quant platform capabilities and lists prioritized optimization proposals. It is aligned to the canonical PRD in `/home/ops/ghTrader/PRD.md`.

## Capability rubric (mapped to PRD sections)

| Domain | PRD coverage | Best-in-class gaps | PRD refs |
| --- | --- | --- | --- |
| Data ingest + storage | Strong | SLIs/SLOs, lineage manifests, backfill governance | §§5.1-5.3, 6.3.1 |
| Data quality + integrity | Strong | Operational error budgets, incident runbooks | §§5.3.0.2, 5.11 |
| Feature store + labels | Partial | Parity tests, schema governance, staleness SLAs | §§5.4-5.5 |
| Modeling + evaluation | Strong | Statistical significance gates, stress protocols | §§5.6-5.10 |
| Online calibration + drift | Partial | Explicit drift SLAs and rollback playbooks | §§5.7, 5.14 |
| MLOps + lifecycle | Partial | Repro bundles, promotion checklists | §§5.16-5.19 |
| Trading + risk | Partial | Pre-trade checks, kill switch, execution SLIs | §§5.12, 6.1 |
| Control plane + observability | Strong | SLO dashboards and incident response | §5.11 |
| Non-functional excellence | Partial | Backup/restore, dependency security, change mgmt | §6 |
| Architecture + roadmap | Strong | Phase sequencing and dependency mapping | §§8, 11 |

## Gap analysis and optimization proposals

### 1) Data ingest and QuestDB governance
**Current**: QuestDB-first ingest, row_hash integrity, robust diagnose/repair workflow.  
**Gaps**: No explicit SLIs/SLOs, lineage manifests, or backfill governance.  
**Optimizations**:
- Define data SLIs/SLOs (freshness, completeness, error budget) and surface them in `/system`.
- Persist lineage manifests per build (config hash, source tables, row_hash summary).
- Define backfill policy with approval gates and a change log.

### 2) Feature store and labels
**Current**: Strong causal requirements, taxonomy, and QuestDB storage.  
**Gaps**: Parity tests and schema evolution rules are not enforced; label leakage guardrails need formal split rules.  
**Optimizations**:
- Add offline/online parity tests using golden replays.
- Enforce TTL and staleness SLAs per feature group.
- Require purged/embargoed split rules for label generation and evaluation.

### 3) Modeling and evaluation rigor
**Current**: Rich model zoo and metrics.  
**Gaps**: Lacks statistical significance gates and standardized stress protocols.  
**Optimizations**:
- Add bootstrap confidence intervals for PnL and Sharpe before promotion.
- Enforce stress scenarios (volatility spikes, liquidity droughts, spread widening).
- Require leakage audits and regime-split validation for promotion.

### 4) Drift detection and online calibration
**Current**: Drift methods defined.  
**Gaps**: No explicit drift SLAs or rollback playbooks.  
**Optimizations**:
- Define warning/critical thresholds per model family.
- Tie rollback and fallback to drift + execution anomaly thresholds.

### 5) Trading and risk controls
**Current**: Safety gating and phased trading.  
**Gaps**: Pre-trade checks and kill-switch rules are not explicit.  
**Optimizations**:
- Define pre-trade risk checks (max order size, max position, max loss, cooldown).
- Implement kill-switch policy with audit logging and manual override.
- Track execution SLIs (fill rate, slippage, latency, reject rates).

### 6) MLOps and governance
**Current**: Registry and experiment tracking are defined.  
**Gaps**: Reproducibility bundles and promotion checklists are not required.  
**Optimizations**:
- Require code+config+data manifest hashes for each model artifact.
- Add explicit promotion checklist with recorded approvals.

### 7) Operational excellence
**Current**: Strong control plane and job model.  
**Gaps**: No explicit SLO dashboards, incident response runbooks, or backup policy.  
**Optimizations**:
- Add SLO dashboards and incident runbooks for outages and data corruption.
- Define backup/restore policy for QuestDB and model artifacts.
- Add dependency security scanning and change management policy.

## Prioritized optimization list

**P0 (foundation)**:
- Define data SLIs/SLOs and surface in dashboard.
- Persist lineage manifests for data/features/labels/models.
- Add backfill governance and change logs.
- Add parity tests and label leakage guards.

**P1 (quality and governance)**:
- Add statistical significance gates and stress protocols.
- Define drift SLAs and rollback playbooks.
- Add pre-trade risk checks and kill-switch policy.

**P2 (operational excellence)**:
- Add SLO dashboards, incident runbooks, backup/restore policy.
- Add reproducibility bundles and promotion checklists.

## Dependency notes
- Data SLIs/SLOs and lineage are prerequisites for reliable model promotion.
- Parity tests depend on feature/label schema versioning.
- Risk controls and execution SLIs should be added before expanding live trading modes.

## Phased alignment roadmap (summary)
- Phase 0: Data governance + reliability
- Phase 1: Feature/label parity + evaluation rigor
- Phase 2: MLOps + lifecycle hardening
- Phase 3: Execution + risk feedback loop
- Phase 4: Real-time streaming (optional)
