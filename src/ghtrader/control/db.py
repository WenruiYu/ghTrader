from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class JobRecord:
    id: str
    created_at: str
    updated_at: str
    status: str  # queued|running|succeeded|failed|cancelled
    title: str
    command: list[str]
    cwd: str
    source: str = "terminal"  # terminal|dashboard
    pid: int | None = None
    exit_code: int | None = None
    log_path: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    waiting_locks: list[str] | None = None
    held_locks: list[str] | None = None


class JobStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        # Concurrency hardening: multiple sessions/processes will write this DB.
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        con.execute("PRAGMA foreign_keys=ON;")
        return con

    def _init_schema(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  status TEXT NOT NULL,
                  title TEXT NOT NULL,
                  source TEXT NOT NULL DEFAULT 'terminal',
                  command_json TEXT NOT NULL,
                  cwd TEXT NOT NULL,
                  pid INTEGER,
                  exit_code INTEGER,
                  log_path TEXT,
                  started_at TEXT,
                  finished_at TEXT,
                  error TEXT,
                  waiting_locks_json TEXT,
                  held_locks_json TEXT
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")

            # Migrations for older DBs (best-effort)
            for stmt in [
                "ALTER TABLE jobs ADD COLUMN source TEXT NOT NULL DEFAULT 'terminal'",
                "ALTER TABLE jobs ADD COLUMN waiting_locks_json TEXT",
                "ALTER TABLE jobs ADD COLUMN held_locks_json TEXT",
            ]:
                try:
                    con.execute(stmt)
                except sqlite3.OperationalError:
                    pass
            con.commit()

    def create_job(
        self,
        *,
        job_id: str,
        title: str,
        command: list[str],
        cwd: Path,
        source: str = "terminal",
        log_path: Path | None = None,
    ) -> JobRecord:
        now = _now_iso()
        rec = JobRecord(
            id=job_id,
            created_at=now,
            updated_at=now,
            status="queued",
            title=title,
            source=source,
            command=command,
            cwd=str(cwd),
            log_path=str(log_path) if log_path is not None else None,
        )
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO jobs
                  (id, created_at, updated_at, status, title, source, command_json, cwd, log_path)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec.id,
                    rec.created_at,
                    rec.updated_at,
                    rec.status,
                    rec.title,
                    rec.source,
                    json.dumps(rec.command),
                    rec.cwd,
                    rec.log_path,
                ),
            )
            con.commit()
        return rec

    def update_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        pid: int | None = None,
        exit_code: int | None = None,
        log_path: Path | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
        title: str | None = None,
        source: str | None = None,
        command: list[str] | None = None,
        cwd: Path | None = None,
        waiting_locks: list[str] | None = None,
        held_locks: list[str] | None = None,
    ) -> None:
        fields: dict[str, Any] = {"updated_at": _now_iso()}
        if status is not None:
            fields["status"] = status
        if pid is not None:
            fields["pid"] = pid
        if exit_code is not None:
            fields["exit_code"] = exit_code
        if log_path is not None:
            fields["log_path"] = str(log_path)
        if started_at is not None:
            fields["started_at"] = started_at
        if finished_at is not None:
            fields["finished_at"] = finished_at
        if error is not None:
            fields["error"] = error
        if title is not None:
            fields["title"] = title
        if source is not None:
            fields["source"] = source
        if command is not None:
            fields["command_json"] = json.dumps(command)
        if cwd is not None:
            fields["cwd"] = str(cwd)
        if waiting_locks is not None:
            fields["waiting_locks_json"] = json.dumps(waiting_locks)
        if held_locks is not None:
            fields["held_locks_json"] = json.dumps(held_locks)

        assignments = ", ".join([f"{k}=?" for k in fields.keys()])
        values = list(fields.values()) + [job_id]
        with self._connect() as con:
            con.execute(f"UPDATE jobs SET {assignments} WHERE id=?", values)
            con.commit()

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def list_jobs(self, *, limit: int = 200) -> list[JobRecord]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def list_running_jobs(self) -> list[JobRecord]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM jobs WHERE status='running' ORDER BY created_at DESC",
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def list_active_jobs(self) -> list[JobRecord]:
        """
        Return jobs that are logically active in the system (running, or queued while waiting on locks).

        Important: lock acquisition happens inside the CLI process which may temporarily mark the job as
        status='queued' while the PID is already running and waiting.
        """
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM jobs WHERE status IN ('running','queued') AND pid IS NOT NULL ORDER BY created_at DESC",
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def list_unstarted_queued_jobs(self, *, limit: int = 200) -> list[JobRecord]:
        """
        Return queued jobs that have not been spawned yet (pid is NULL).
        """
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM jobs WHERE status='queued' AND pid IS NULL ORDER BY created_at ASC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def try_mark_started(self, *, job_id: str, pid: int, started_at: str, log_path: Path | None = None) -> bool:
        """
        Atomically mark a queued (unstarted) job as running.

        This is used by schedulers to avoid double-starting the same queued job when multiple dashboard
        instances are running.
        """
        now = _now_iso()
        with self._connect() as con:
            cur = con.execute(
                """
                UPDATE jobs
                SET
                  updated_at=?,
                  status='running',
                  pid=?,
                  started_at=?,
                  log_path=COALESCE(log_path, ?)
                WHERE
                  id=?
                  AND status='queued'
                  AND pid IS NULL
                """,
                (now, int(pid), str(started_at), str(log_path) if log_path is not None else None, str(job_id)),
            )
            con.commit()
            return bool(cur.rowcount == 1)

    def _row_to_record(self, row: sqlite3.Row) -> JobRecord:
        cmd = json.loads(row["command_json"])
        waiting = row["waiting_locks_json"]
        held = row["held_locks_json"]
        return JobRecord(
            id=row["id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            status=row["status"],
            title=row["title"],
            source=row["source"] if "source" in row.keys() else "terminal",
            command=list(cmd),
            cwd=row["cwd"],
            pid=row["pid"],
            exit_code=row["exit_code"],
            log_path=row["log_path"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error=row["error"],
            waiting_locks=json.loads(waiting) if waiting else None,
            held_locks=json.loads(held) if held else None,
        )

