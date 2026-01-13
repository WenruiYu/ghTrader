from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


@dataclass(frozen=True)
class LockRecord:
    key: str
    job_id: str
    pid: int
    created_at: str
    updated_at: str


class LockStore:
    """
    Cross-session locks stored in SQLite (single host).

    Locks are identified by a string key. Each key may be held by only one job at a time.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        con.execute("PRAGMA foreign_keys=ON;")
        return con

    def _init_schema(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS locks (
                  key TEXT PRIMARY KEY,
                  job_id TEXT NOT NULL,
                  pid INTEGER NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_locks_job_id ON locks(job_id)")
            con.commit()

    def list_locks(self) -> list[LockRecord]:
        with self._connect() as con:
            rows = con.execute("SELECT * FROM locks ORDER BY created_at ASC").fetchall()
        return [
            LockRecord(
                key=r["key"],
                job_id=r["job_id"],
                pid=int(r["pid"]),
                created_at=r["created_at"],
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    def release_all(self, *, job_id: str) -> int:
        with self._connect() as con:
            cur = con.execute("DELETE FROM locks WHERE job_id=?", (job_id,))
            con.commit()
            return int(cur.rowcount or 0)

    def acquire(
        self,
        *,
        lock_keys: list[str],
        job_id: str,
        pid: int,
        wait: bool = True,
        poll_interval_sec: float = 1.0,
        timeout_sec: float | None = None,
    ) -> tuple[bool, list[dict[str, Any]]]:
        """
        Attempt to acquire all lock_keys.

        Returns:
          (acquired, conflicts) where conflicts is a list of {key, job_id, pid}.
        """
        keys = [k.strip() for k in lock_keys if k.strip()]
        if not keys:
            return True, []

        start = time.time()
        while True:
            conflicts: list[dict[str, Any]] = []
            with self._connect() as con:
                # Serialize acquisition to avoid partial interleavings.
                con.execute("BEGIN IMMEDIATE;")

                # Reap stale locks for requested keys (best-effort).
                placeholders = ",".join(["?"] * len(keys))
                rows = con.execute(
                    f"SELECT key, job_id, pid FROM locks WHERE key IN ({placeholders})",
                    keys,
                ).fetchall()
                stale_keys: list[str] = []
                for r in rows:
                    owner_job = str(r["job_id"])
                    owner_pid = int(r["pid"])
                    owner_row = con.execute("SELECT status, pid FROM jobs WHERE id=?", (owner_job,)).fetchone()
                    owner_status = str(owner_row["status"]) if owner_row is not None else ""
                    owner_pid_in_jobs = int(owner_row["pid"]) if owner_row is not None and owner_row["pid"] is not None else None
                    alive = _is_pid_alive(owner_pid)
                    if owner_row is None or owner_status != "running" or (owner_pid_in_jobs is not None and owner_pid_in_jobs != owner_pid) or not alive:
                        stale_keys.append(str(r["key"]))

                if stale_keys:
                    ph = ",".join(["?"] * len(stale_keys))
                    con.execute(f"DELETE FROM locks WHERE key IN ({ph})", stale_keys)

                # Re-check conflicts after reaping
                rows2 = con.execute(
                    f"SELECT key, job_id, pid FROM locks WHERE key IN ({placeholders})",
                    keys,
                ).fetchall()
                if rows2:
                    conflicts = [{"key": str(r["key"]), "job_id": str(r["job_id"]), "pid": int(r["pid"])} for r in rows2]
                    con.commit()
                else:
                    now = _now_iso()
                    for k in keys:
                        con.execute(
                            "INSERT INTO locks(key, job_id, pid, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                            (k, job_id, int(pid), now, now),
                        )
                    con.commit()
                    return True, []

            if not wait:
                return False, conflicts
            if timeout_sec is not None and (time.time() - start) >= float(timeout_sec):
                return False, conflicts
            time.sleep(float(poll_interval_sec))

