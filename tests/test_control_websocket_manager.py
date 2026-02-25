from __future__ import annotations

import asyncio
import importlib
from pathlib import Path

import pytest
from fastapi import FastAPI


class _GoodSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_text(self, message: str) -> None:
        self.messages.append(str(message))


class _FailingSocket:
    async def send_text(self, message: str) -> None:
        raise RuntimeError("socket_gone")


def _load_connection_manager_cls(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(mod)
    return getattr(mod, "ConnectionManager")


def test_websocket_broadcast_prunes_dead_clients_and_counts_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection_manager_cls = _load_connection_manager_cls(tmp_path, monkeypatch)
    manager = connection_manager_cls()
    good = _GoodSocket()
    bad = _FailingSocket()
    manager.active_connections = [good, bad]

    asyncio.run(manager.broadcast("hello"))

    assert good.messages == ["hello"]
    assert manager.active_connections == [good]
    assert int(manager.broadcast_attempt_total) == 2
    assert int(manager.broadcast_success_total) == 1
    assert int(manager.broadcast_failures_total) == 1
    assert int(manager.dead_clients_pruned_total) == 1
    assert str(manager.last_broadcast_error) == "socket_gone"


def test_websocket_broadcast_counts_success_without_pruning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection_manager_cls = _load_connection_manager_cls(tmp_path, monkeypatch)
    manager = connection_manager_cls()
    c1 = _GoodSocket()
    c2 = _GoodSocket()
    manager.active_connections = [c1, c2]

    asyncio.run(manager.broadcast("world"))

    assert c1.messages == ["world"]
    assert c2.messages == ["world"]
    assert manager.active_connections == [c1, c2]
    assert int(manager.broadcast_attempt_total) == 2
    assert int(manager.broadcast_success_total) == 2
    assert int(manager.broadcast_failures_total) == 0
    assert int(manager.dead_clients_pruned_total) == 0
    assert str(manager.last_broadcast_error) == ""


def test_connection_manager_comes_from_dedicated_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GHTRADER_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("GHTRADER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GHTRADER_ARTIFACTS_DIR", str(tmp_path / "artifacts"))

    app_mod = importlib.import_module("ghtrader.control.app")
    importlib.reload(app_mod)
    ws_mod = importlib.import_module("ghtrader.control.websocket_manager")

    app_cls = getattr(app_mod, "ConnectionManager")
    ws_cls = getattr(ws_mod, "ConnectionManager")
    assert getattr(app_cls, "__module__", "") == "ghtrader.control.websocket_manager"
    assert getattr(ws_cls, "__module__", "") == "ghtrader.control.websocket_manager"


def test_mount_dashboard_websocket_registers_route_and_state() -> None:
    ws_mod = importlib.import_module("ghtrader.control.websocket_manager")
    app = FastAPI()
    manager = ws_mod.mount_dashboard_websocket(app)
    assert getattr(app.state, "websocket_manager", None) is manager
    assert any(getattr(route, "path", "") == "/ws/dashboard" for route in app.routes)
