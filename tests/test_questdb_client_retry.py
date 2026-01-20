from __future__ import annotations

from ghtrader.questdb.client import QuestDBQueryConfig, connect_pg_safe


def test_connect_pg_safe_retries_transient(monkeypatch) -> None:
    calls = {"n": 0}

    def fake_connect(_cfg, connect_timeout_s: int = 2):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("server closed the connection unexpectedly")

        class DummyConn:
            def __init__(self):
                self.autocommit = False

            def close(self):
                return None

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        return DummyConn()

    monkeypatch.setattr("ghtrader.questdb.client.connect_pg", fake_connect)

    cfg = QuestDBQueryConfig(
        host="localhost",
        pg_port=8812,
        pg_user="user",
        pg_password="pass",
        pg_dbname="qdb",
    )

    conn = connect_pg_safe(cfg, retries=1, backoff_s=0.0, autocommit=True)
    assert calls["n"] == 2
    assert conn.autocommit is True
