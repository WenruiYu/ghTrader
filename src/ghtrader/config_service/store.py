from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ghtrader.config_service.schema import extract_managed_env, key_is_env_only, key_is_managed, normalize_to_string


from ghtrader.util.time import now_iso as _now_iso


def _snapshot_hash(snapshot: dict[str, str]) -> str:
    payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


@dataclass(frozen=True)
class ConfigRevision:
    revision: int
    created_at: str
    actor: str
    reason: str
    snapshot_hash: str
    changed_keys: list[str]


class ConfigStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()
        self._ensure_baseline()
        self._sanitize_illegal_snapshot_keys()

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
                CREATE TABLE IF NOT EXISTS config_revisions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  actor TEXT NOT NULL,
                  reason TEXT NOT NULL,
                  snapshot_json TEXT NOT NULL,
                  snapshot_hash TEXT NOT NULL,
                  changed_keys_json TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS config_current (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  revision_id INTEGER NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS config_audit (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  actor TEXT NOT NULL,
                  action TEXT NOT NULL,
                  key TEXT NOT NULL,
                  old_value TEXT,
                  new_value TEXT,
                  from_revision INTEGER,
                  to_revision INTEGER,
                  metadata_json TEXT
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_config_revisions_created_at ON config_revisions(created_at)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_config_audit_created_at ON config_audit(created_at)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_config_audit_key ON config_audit(key)")
            con.commit()

    def _ensure_baseline(self) -> None:
        with self._connect() as con:
            row = con.execute("SELECT id FROM config_revisions ORDER BY id DESC LIMIT 1").fetchone()
            if row is not None:
                return
            ts = _now_iso()
            snapshot: dict[str, str] = {}
            con.execute(
                """
                INSERT INTO config_revisions
                  (created_at, actor, reason, snapshot_json, snapshot_hash, changed_keys_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    "system",
                    "baseline",
                    json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                    _snapshot_hash(snapshot),
                    json.dumps([], ensure_ascii=False),
                ),
            )
            con.commit()

    def _sanitize_illegal_snapshot_keys(self) -> None:
        _, snapshot, _ = self.get_latest_snapshot()
        remove_keys = sorted([k for k in snapshot.keys() if not key_is_managed(k)])
        if not remove_keys:
            return
        self.set_values(
            values={k: None for k in remove_keys},
            actor="system",
            reason="sanitize_illegal_keys",
            action="sanitize",
            metadata={"removed_keys": list(remove_keys)},
        )

    def _latest_row(self, con: sqlite3.Connection) -> sqlite3.Row:
        row = con.execute(
            "SELECT id, created_at, actor, reason, snapshot_json, snapshot_hash, changed_keys_json "
            "FROM config_revisions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("config_revisions not initialized")
        return row

    def get_latest_snapshot(self) -> tuple[int, dict[str, str], str]:
        with self._connect() as con:
            row = self._latest_row(con)
        snap_raw = json.loads(row["snapshot_json"]) if row["snapshot_json"] else {}
        snapshot = {str(k): str(v if v is not None else "") for k, v in dict(snap_raw or {}).items()}
        return int(row["id"]), snapshot, str(row["snapshot_hash"])

    def get_snapshot(self, revision: int) -> tuple[int, dict[str, str], str]:
        with self._connect() as con:
            row = con.execute(
                "SELECT id, snapshot_json, snapshot_hash FROM config_revisions WHERE id=?",
                (int(revision),),
            ).fetchone()
        if row is None:
            raise KeyError(f"revision_not_found:{revision}")
        snap_raw = json.loads(row["snapshot_json"]) if row["snapshot_json"] else {}
        snapshot = {str(k): str(v if v is not None else "") for k, v in dict(snap_raw or {}).items()}
        return int(row["id"]), snapshot, str(row["snapshot_hash"])

    def list_revisions(self, *, limit: int = 50) -> list[ConfigRevision]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT id, created_at, actor, reason, snapshot_hash, changed_keys_json
                FROM config_revisions
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        out: list[ConfigRevision] = []
        for row in rows:
            changed = json.loads(row["changed_keys_json"]) if row["changed_keys_json"] else []
            out.append(
                ConfigRevision(
                    revision=int(row["id"]),
                    created_at=str(row["created_at"]),
                    actor=str(row["actor"]),
                    reason=str(row["reason"]),
                    snapshot_hash=str(row["snapshot_hash"]),
                    changed_keys=[str(x) for x in (changed or [])],
                )
            )
        return out

    def set_values(
        self,
        *,
        values: dict[str, Any],
        actor: str,
        reason: str,
        action: str = "set",
        metadata: dict[str, Any] | None = None,
        replace: bool = False,
    ) -> ConfigRevision:
        actor_s = str(actor or "unknown")
        reason_s = str(reason or "").strip() or "update"
        incoming: dict[str, str | None] = {}
        for key, val in dict(values or {}).items():
            k = str(key or "").strip()
            if not k:
                continue
            if val is None:
                incoming[k] = None
                continue
            if not key_is_managed(k):
                reason = "env_only" if key_is_env_only(k) else "unmanaged"
                raise ValueError(f"config_key_not_writable:{k}:{reason}")
            incoming[k] = normalize_to_string(k, val)

        with self._connect() as con:
            latest = self._latest_row(con)
            old_snapshot_raw = json.loads(latest["snapshot_json"]) if latest["snapshot_json"] else {}
            old_snapshot = {str(k): str(v if v is not None else "") for k, v in dict(old_snapshot_raw or {}).items()}

            new_snapshot = {} if bool(replace) else dict(old_snapshot)
            changed_keys: list[str] = []
            if bool(replace):
                changed_keys.extend(list(old_snapshot.keys()))
            for key, value in incoming.items():
                if value is None:
                    if key in new_snapshot:
                        changed_keys.append(key)
                        new_snapshot.pop(key, None)
                    continue
                if new_snapshot.get(key) != value:
                    changed_keys.append(key)
                    new_snapshot[key] = value

            changed_keys = sorted(set(changed_keys))
            if not changed_keys:
                return ConfigRevision(
                    revision=int(latest["id"]),
                    created_at=str(latest["created_at"]),
                    actor=str(latest["actor"]),
                    reason=str(latest["reason"]),
                    snapshot_hash=str(latest["snapshot_hash"]),
                    changed_keys=[],
                )

            created_at = _now_iso()
            snap_hash = _snapshot_hash(new_snapshot)
            cur = con.execute(
                """
                INSERT INTO config_revisions
                  (created_at, actor, reason, snapshot_json, snapshot_hash, changed_keys_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    actor_s,
                    reason_s,
                    json.dumps(new_snapshot, ensure_ascii=False, sort_keys=True),
                    snap_hash,
                    json.dumps(changed_keys, ensure_ascii=False),
                ),
            )
            to_revision = int(cur.lastrowid)
            from_revision = int(latest["id"])

            con.execute("DELETE FROM config_current")
            if new_snapshot:
                con.executemany(
                    """
                    INSERT INTO config_current (key, value, updated_at, revision_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    [(k, v, created_at, to_revision) for k, v in sorted(new_snapshot.items())],
                )

            metadata_blob = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
            for key in changed_keys:
                con.execute(
                    """
                    INSERT INTO config_audit
                      (created_at, actor, action, key, old_value, new_value, from_revision, to_revision, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        created_at,
                        actor_s,
                        str(action or "set"),
                        key,
                        old_snapshot.get(key),
                        new_snapshot.get(key),
                        from_revision,
                        to_revision,
                        metadata_blob,
                    ),
                )
            con.commit()

        return ConfigRevision(
            revision=to_revision,
            created_at=created_at,
            actor=actor_s,
            reason=reason_s,
            snapshot_hash=snap_hash,
            changed_keys=changed_keys,
        )

    def rollback(self, *, revision: int, actor: str, reason: str) -> ConfigRevision:
        rev_id, snapshot, _snap_hash = self.get_snapshot(int(revision))
        return self.set_values(
            values={k: v for k, v in snapshot.items()},
            actor=str(actor or "unknown"),
            reason=(str(reason or "").strip() or f"rollback_to:{rev_id}"),
            action="rollback",
            metadata={"rollback_to_revision": int(rev_id)},
            replace=True,
        )

    def migrate_from_env(self, *, env: dict[str, str], actor: str, reason: str = "migrate_env") -> ConfigRevision:
        managed = extract_managed_env(env)
        return self.set_values(values=managed, actor=actor, reason=reason, action="migrate_env")

