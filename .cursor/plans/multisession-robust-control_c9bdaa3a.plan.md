---
name: multisession-robust-control
overview: "Harden ghTrader’s control plane so multiple concurrent terminal/dashboard sessions are safe: centralized session registry (auto-register all CLI runs), strict cross-session resource locks, and dashboard visibility into all sessions and locks, robust across restarts."
todos: []
---

# Multi-session robustness: auto-registered sessions + strict locks + dashboard visibility

## Goal

Ensure **multiple concurrent terminal sessions** (and multiple dashboard instances) do not conflict and the **dashboard reflects all sessions**.

You chose:

- **Auto-register** every `ghtrader ...` CLI run into the shared job DB.
- **Strict locks** to prevent conflicting operations (refuse/queue rather than race).

## Key issues in current design

- **SQLite write contention risk**: `JobStore` opens new connections and writes from concurrent request handlers + background threads, but does not enable WAL/busy_timeout.
- **Restart robustness gap**: job completion status currently relies on the dashboard process’s background waiter thread. If the dashboard restarts mid-job, the job may finish without updating DB.
- **No cross-session conflict protection**: multiple sessions can run commands like `build` that `rm -rf` and rewrite `data/features/symbol=...`, causing races.
- **Dashboard only shows dashboard-launched jobs**: terminal-launched runs aren’t reflected.

## Design

### 1) Central session registry (single source of truth)

- Use `runs/control/jobs.db` as the registry for **all sessions**.
- Introduce a **CLI entrypoint wrapper** that:
- creates/updates a `jobs` record for the current run
- records PID/start/finish/exit_code
- writes a log file path for terminal sessions (so dashboard can tail logs)

### 2) Strict cross-session resource locks

- Add a `locks` table in SQLite and a small `LockStore` API:
- acquire/release lock keys atomically
- detect and reap **stale locks** (PID not alive)
- optionally wait (queue) until locks are available
- Define lock keys for high-conflict operations:
- `ticks:symbol=<SYM>` for `download`, `record`
- `ticks_range:exchange=<EX>,var=<VAR>` for `download-contract-range`
- `build:symbol=<SYM>,ticks_lake=<raw|main_l5>` for `build`
- `train:symbol=<SYM>,model=<M>,h=<H>` for `train`
- `main_schedule:var=<VAR>` for `main-schedule`
- `main_depth:symbol=<DERIVED>` for `main-depth`
- `trade:mode=<MODE>` (and later: per-account key) for `trade`

### 3) Dashboard reflects all sessions + lock state

- Jobs list already reads the shared DB; after auto-registration it will show **terminal sessions too**.
- Add UI surfacing:
- job `source` column (`terminal` vs `dashboard`)
- a “Locks” page (or section) listing active locks and owning job
- job detail shows held locks + whether job is queued on locks

## PRD-first updates

Update [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md) under **§5.11 Control plane** to add:

- **Multi-session guarantees**: WAL/busy-timeout, job self-reporting, restart-safe status.
- **Strict lock policy**: define lock keys and queuing semantics.
- **Dashboard parity**: dashboard must show all CLI sessions and locks.

## Implementation steps

### A) Harden SQLite for concurrency

- Update [`/home/ops/ghTrader/src/ghtrader/control/db.py`](/home/ops/ghTrader/src/ghtrader/control/db.py):
- enable `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=...`
- add schema migrations for new columns (`source`, `locks_json`, etc. as needed)

### B) Implement LockStore

- Add `src/ghtrader/control/locks.py`:
- `acquire(lock_keys, job_id, pid, wait: bool, poll_interval)`
- `release(job_id)`
- `list_locks()`
- stale lock reap on acquire

### C) Auto-register every CLI run (terminal + dashboard)

- Update [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py):
- add `entrypoint()` wrapper that runs the Click app with `standalone_mode=False` and captures exit codes
- create/update job record in `jobs.db` for every run
- for terminal sessions, attach a file logger to `runs/control/logs/job-<id>.log` (so dashboard can tail)
- acquire strict locks before executing the command; remain `queued` while waiting

### D) Make dashboard-launched jobs use the same job_id

- Update [`/home/ops/ghTrader/src/ghtrader/control/jobs.py`](/home/ops/ghTrader/src/ghtrader/control/jobs.py):
- pass env vars to child process: `GHTRADER_JOB_ID`, `GHTRADER_JOB_SOURCE=dashboard`
- stop relying on the dashboard waiter thread for completion; let the child session wrapper write final status

### E) Dashboard UI additions

- Update [`/home/ops/ghTrader/src/ghtrader/control/views.py`](/home/ops/ghTrader/src/ghtrader/control/views.py) + templates to:
- show `source` and `locks` on job list/detail
- add `/ops/locks` page listing active locks

### F) Tests-first

- Add unit tests for:
- lock acquisition/release + stale lock reaping
- CLI session auto-registration writes job record + updates on exit
- lock key derivation for key commands

## Files expected to change/add

- **Update**: [`/home/ops/ghTrader/PRD.md`](/home/ops/ghTrader/PRD.md)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/control/db.py`](/home/ops/ghTrader/src/ghtrader/control/db.py)
- **Add**: `src/ghtrader/control/locks.py`
- **Update**: [`/home/ops/ghTrader/src/ghtrader/control/jobs.py`](/home/ops/ghTrader/src/ghtrader/control/jobs.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/cli.py`](/home/ops/ghTrader/src/ghtrader/cli.py)
- **Update**: [`/home/ops/ghTrader/src/ghtrader/control/views.py`](/home/ops/ghTrader/src/ghtrader/control/views.py) and templates
- **Add tests**: `tests/test_control_locks.py`, `tests/test_cli_session_registry.py`

## Acceptance criteria

- Two terminal sessions running conflicting operations (e.g. `build` same symbol) will **not** corrupt outputs; one will be queued/refused by locks.
- Dashboard shows all sessions (terminal + dashboard) in Jobs, with source and logs.
- Jobs finish with correct status/exit code even if the dashboard restarts.
- SQLite remains stable under concurrent job updates (no “database is locked” flakiness).