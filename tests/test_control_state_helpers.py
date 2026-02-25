from __future__ import annotations

import json
from pathlib import Path

import pytest

from ghtrader.control import state_helpers as sh


def test_choose_latest_state_prefers_higher_revision() -> None:
    redis_state = {"state_revision": 3, "updated_at": "2026-02-23T00:00:03Z"}
    file_state = {"state_revision": 5, "updated_at": "2026-02-23T00:00:02Z"}
    picked, source = sh.choose_latest_state(redis_state, file_state)
    assert source == "file"
    assert isinstance(picked, dict)
    assert int(picked.get("state_revision") or 0) == 5


def test_choose_latest_state_falls_back_to_updated_at_when_no_revision() -> None:
    redis_state = {"updated_at": "2026-02-23T00:00:01Z"}
    file_state = {"updated_at": "2026-02-23T00:00:02Z"}
    picked, source = sh.choose_latest_state(redis_state, file_state)
    assert source == "file"
    assert isinstance(picked, dict)
    assert str(picked.get("updated_at")) == "2026-02-23T00:00:02Z"


def test_read_state_with_revision_prefers_file_when_redis_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps({"state_revision": 9, "updated_at": "2026-02-23T00:00:09Z"}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(sh, "read_redis_json", lambda key: {"state_revision": 3, "updated_at": "2026-02-23T00:00:03Z"})
    picked, source = sh.read_state_with_revision(redis_key="ghtrader:gateway:state:alt", file_path=state_path)
    assert source == "file"
    assert isinstance(picked, dict)
    assert int(picked.get("state_revision") or 0) == 9


def test_choose_latest_state_prefers_revision_over_updated_at() -> None:
    redis_state = {"state_revision": 9, "updated_at": "2026-02-23T00:00:01Z"}
    file_state = {"state_revision": 8, "updated_at": "2026-02-23T00:00:09Z"}
    picked, source = sh.choose_latest_state(redis_state, file_state)
    assert source == "redis"
    assert isinstance(picked, dict)
    assert int(picked.get("state_revision") or 0) == 9


def test_choose_latest_state_prefers_revisioned_snapshot_when_peer_missing_revision() -> None:
    redis_state = {"updated_at": "2026-02-23T00:00:09Z"}
    file_state = {"state_revision": 2, "updated_at": "2026-02-23T00:00:01Z"}
    picked, source = sh.choose_latest_state(redis_state, file_state)
    assert source == "file"
    assert isinstance(picked, dict)
    assert int(picked.get("state_revision") or 0) == 2
