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
