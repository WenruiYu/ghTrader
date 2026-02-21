from __future__ import annotations

import pandas as pd

from ghtrader.questdb.serving_db import QuestDBBackend, ServingDBConfig


def test_questdb_backend_ingest_uses_sender_from_conf(monkeypatch) -> None:
    """
    Regression: questdb==4.1.0 does NOT support Sender(host, port) constructor.
    We must use Sender.from_conf("tcp::addr=host:port;").
    """
    calls: dict[str, object] = {}

    class FakeSender:
        def __init__(self) -> None:
            # If code tries to call Sender(host, port), this test should fail (signature mismatch).
            calls["init_called"] = True

        @classmethod
        def from_conf(cls, conf_str: str, **_kwargs):
            calls["conf_str"] = conf_str
            return cls()

        def __enter__(self):
            calls["enter"] = True
            return self

        def __exit__(self, exc_type, exc, tb):
            calls["exit"] = True
            return False

        def dataframe(self, df: pd.DataFrame, *, table_name: str, at: str):
            calls["table_name"] = table_name
            calls["at"] = at
            calls["cols"] = list(df.columns)

        def flush(self) -> None:
            calls["flush"] = True

    import questdb.ingress as ingress

    monkeypatch.setattr(ingress, "Sender", FakeSender, raising=True)

    cfg = ServingDBConfig(backend="questdb", host="127.0.0.1", questdb_ilp_port=9009)
    backend = QuestDBBackend(config=cfg)
    df = pd.DataFrame(
        {
            "symbol": ["X"],
            "ts": [pd.Timestamp("2026-01-01")],
            "datetime_ns": [1],
            "trading_day": ["2026-01-01"],
            "row_hash": [1],
            "ticks_kind": ["main_l5"],
            "dataset_version": ["v2"],
        }
    )

    backend.ingest_df(table="ticks_test", df=df)

    assert calls["conf_str"] == "tcp::addr=127.0.0.1:9009;"
    assert calls["table_name"] == "ticks_test"
    assert calls["at"] == "ts"
    assert calls.get("flush") is True


def test_questdb_backend_persistent_sender_establishes_connection(monkeypatch) -> None:
    class FakeSender:
        created = 0
        established = 0

        def __init__(self) -> None:
            type(self).created += 1
            self.ready = False

        @classmethod
        def from_conf(cls, _conf_str: str, **_kwargs):
            return cls()

        def establish(self) -> None:
            self.ready = True
            type(self).established += 1

        def dataframe(self, df: pd.DataFrame, *, table_name: str, at: str):
            _ = (df, table_name, at)
            if not self.ready:
                raise RuntimeError("dataframe() can't be called: Sender is closed.")

        def flush(self) -> None:
            return None

        def close(self) -> None:
            self.ready = False

    import questdb.ingress as ingress

    monkeypatch.setattr(ingress, "Sender", FakeSender, raising=True)
    monkeypatch.setenv("GHTRADER_QDB_ILP_PERSISTENT_SENDER", "true")
    monkeypatch.setenv("GHTRADER_QDB_ILP_RETRY_MAX", "0")

    cfg = ServingDBConfig(backend="questdb", host="127.0.0.1", questdb_ilp_port=9009)
    backend = QuestDBBackend(config=cfg)
    df = pd.DataFrame(
        {
            "symbol": ["X"],
            "ts": [pd.Timestamp("2026-01-01")],
            "datetime_ns": [1],
            "trading_day": ["2026-01-01"],
            "row_hash": [1],
            "ticks_kind": ["main_l5"],
            "dataset_version": ["v2"],
        }
    )

    backend.ingest_df(table="ticks_test", df=df)
    assert FakeSender.created == 1
    assert FakeSender.established == 1


def test_questdb_backend_ingest_recovers_closed_persistent_sender(monkeypatch) -> None:
    """
    Regression: long-running ingest can leave a thread-local sender in closed state.
    The next write should reconnect once and continue instead of failing the whole build.
    """

    class FakeSender:
        created = 0

        def __init__(self) -> None:
            type(self).created += 1
            self.closed = False
            self.dataframe_calls = 0

        @classmethod
        def from_conf(cls, _conf_str: str, **_kwargs):
            return cls()

        def dataframe(self, df: pd.DataFrame, *, table_name: str, at: str):
            _ = (df, table_name, at)
            self.dataframe_calls += 1
            if self.closed:
                raise RuntimeError("dataframe() can't be called: Sender is closed.")

        def flush(self) -> None:
            return None

        def close(self) -> None:
            self.closed = True

    import questdb.ingress as ingress

    monkeypatch.setattr(ingress, "Sender", FakeSender, raising=True)

    cfg = ServingDBConfig(backend="questdb", host="127.0.0.1", questdb_ilp_port=9009)
    backend = QuestDBBackend(config=cfg)
    df = pd.DataFrame(
        {
            "symbol": ["X"],
            "ts": [pd.Timestamp("2026-01-01")],
            "datetime_ns": [1],
            "trading_day": ["2026-01-01"],
            "row_hash": [1],
            "ticks_kind": ["main_l5"],
            "dataset_version": ["v2"],
        }
    )

    backend.ingest_df(table="ticks_test", df=df)
    old_sender = backend._sender_thread_local().sender
    old_sender.closed = True

    backend.ingest_df(table="ticks_test", df=df)
    new_sender = backend._sender_thread_local().sender

    assert FakeSender.created == 2
    assert new_sender is not old_sender


def test_questdb_backend_ingest_retries_when_flush_reports_closed_sender(monkeypatch) -> None:
    class FakeSender:
        created = 0
        flush_failures = 0

        def __init__(self) -> None:
            type(self).created += 1
            self.flush_calls = 0

        @classmethod
        def from_conf(cls, _conf_str: str, **_kwargs):
            return cls()

        def dataframe(self, df: pd.DataFrame, *, table_name: str, at: str):
            _ = (df, table_name, at)

        def flush(self) -> None:
            self.flush_calls += 1
            if type(self).flush_failures == 0:
                type(self).flush_failures += 1
                raise RuntimeError("sender is closed")

        def close(self) -> None:
            return None

    import questdb.ingress as ingress

    monkeypatch.setattr(ingress, "Sender", FakeSender, raising=True)
    cfg = ServingDBConfig(backend="questdb", host="127.0.0.1", questdb_ilp_port=9009)
    backend = QuestDBBackend(config=cfg)
    df = pd.DataFrame(
        {
            "symbol": ["X"],
            "ts": [pd.Timestamp("2026-01-01")],
            "datetime_ns": [1],
            "trading_day": ["2026-01-01"],
            "row_hash": [1],
            "ticks_kind": ["main_l5"],
            "dataset_version": ["v2"],
        }
    )

    backend.ingest_df(table="ticks_test", df=df)
    assert FakeSender.flush_failures == 1
    assert FakeSender.created == 2
