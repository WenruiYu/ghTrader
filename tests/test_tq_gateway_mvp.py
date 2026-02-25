from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ghtrader.tq.gateway import (
    _CommandIdDeduper,
    _should_emit_hot_path_heartbeat,
    GatewayDesired,
    GatewayWriter,
    read_gateway_desired,
    read_gateway_targets,
    targets_path,
    write_gateway_desired,
)


def test_gateway_desired_roundtrip(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    prof = "Alt-01"

    desired = GatewayDesired(
        mode="sim",
        symbols=["SHFE.cu2602", "SHFE.au2602"],
        executor="direct",
        sim_account="tqkq",
        confirm_live="",
        max_abs_position=2,
        max_order_size=3,
        max_ops_per_sec=7,
        max_daily_loss=123.0,
        enforce_trading_time=False,
    )
    write_gateway_desired(runs_dir=runs_dir, profile=prof, desired=desired)

    got = read_gateway_desired(runs_dir=runs_dir, profile=prof)
    assert got.mode == "sim"
    assert got.executor == "direct"
    assert got.sim_account == "tqkq"
    assert got.symbols_list() == ["SHFE.cu2602", "SHFE.au2602"]
    assert got.max_abs_position == 2
    assert got.max_order_size == 3
    assert got.max_ops_per_sec == 7
    assert got.max_daily_loss == 123.0
    assert got.enforce_trading_time is False


def test_gateway_targets_parse(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    prof = "default"
    p = targets_path(runs_dir=runs_dir, profile=prof)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "updated_at": "2026-01-01T00:00:00Z",
                "targets": {"SHFE.cu2602": 1, "SHFE.au2602": "2", "": 3, "BAD": "x"},
                "meta": {"model": "xgboost"},
            }
        ),
        encoding="utf-8",
    )

    out = read_gateway_targets(runs_dir=runs_dir, profile=prof)
    assert out["targets"] == {"SHFE.cu2602": 1, "SHFE.au2602": 2}
    assert out["meta"]["model"] == "xgboost"


def test_gateway_writer_updates_state(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    prof = "ALT"
    w = GatewayWriter(runs_dir=runs_dir, profile=prof)
    w.set_health(ok=True, connected=False, error="")
    w.set_effective(mode="idle", symbols=[], executor="targetpos")
    w.append_event({"type": "test_event", "k": 1})
    w.append_snapshot({"schema_version": 2, "ts": "2026-01-01T00:00:00Z", "account": {"balance": 1.0}, "positions": {}, "orders_alive": []})

    state_path = runs_dir / "gateway" / "account=alt" / "state.json"
    assert state_path.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["schema_version"] == 1
    assert state["account_profile"] == "alt"
    assert isinstance(state.get("recent_events"), list)
    assert state["last_snapshot"]["schema_version"] == 2


def test_gateway_writer_state_revision_monotonic(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    w = GatewayWriter(runs_dir=runs_dir, profile="alt")
    w.set_health(ok=True, connected=False, error="")
    w.flush_state()
    state_path = runs_dir / "gateway" / "account=alt" / "state.json"
    rev1 = int(json.loads(state_path.read_text(encoding="utf-8")).get("state_revision") or 0)
    w.append_event({"type": "evt1"})
    rev2 = int(json.loads(state_path.read_text(encoding="utf-8")).get("state_revision") or 0)
    w.append_event({"type": "evt2"})
    rev3 = int(json.loads(state_path.read_text(encoding="utf-8")).get("state_revision") or 0)
    assert rev1 >= 1
    assert rev2 > rev1
    assert rev3 > rev2


def test_gateway_writer_state_revision_rehydrates_after_restart(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    w1 = GatewayWriter(runs_dir=runs_dir, profile="alt")
    w1.set_health(ok=True, connected=True, error="")
    w1.append_event({"type": "evt1"})
    state_path = runs_dir / "gateway" / "account=alt" / "state.json"
    rev_before_restart = int(json.loads(state_path.read_text(encoding="utf-8")).get("state_revision") or 0)

    # Simulate process restart: a new writer should continue from persisted revision.
    w2 = GatewayWriter(runs_dir=runs_dir, profile="alt")
    w2.set_health(ok=True, connected=True, error="")
    w2.append_event({"type": "evt2"})
    rev_after_restart = int(json.loads(state_path.read_text(encoding="utf-8")).get("state_revision") or 0)

    assert rev_before_restart >= 1
    assert rev_after_restart == (rev_before_restart + 1)


class _PublishFailRedis:
    def __init__(self) -> None:
        self.set_calls: list[tuple[str, str]] = []

    def publish(self, channel: str, payload: str) -> None:
        raise RuntimeError("publish_failed")

    def set(self, key: str, payload: str) -> None:
        self.set_calls.append((str(key), str(payload)))


def test_gateway_writer_redis_set_survives_publish_failure(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    fake_redis = _PublishFailRedis()
    w = GatewayWriter(runs_dir=runs_dir, profile="alt", redis_client=fake_redis)  # type: ignore[arg-type]
    w.set_health(ok=True, connected=True, error="")
    w.append_event({"type": "evt"})
    assert len(fake_redis.set_calls) >= 1
    _, payload = fake_redis.set_calls[-1]
    obj: dict[str, Any] = json.loads(payload)
    assert int(obj.get("state_revision") or 0) >= 1


class _PublishAndSetFailRedis:
    def publish(self, channel: str, payload: str) -> None:
        _ = (channel, payload)
        raise RuntimeError("publish_failed")

    def set(self, key: str, payload: str) -> None:
        _ = (key, payload)
        raise RuntimeError("set_failed")


def test_gateway_writer_redis_write_status_tracks_failures_and_streak(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    fake_redis = _PublishAndSetFailRedis()
    w = GatewayWriter(runs_dir=runs_dir, profile="alt", redis_client=fake_redis)  # type: ignore[arg-type]
    w.set_health(ok=True, connected=True, error="")
    w.append_event({"type": "evt"})
    stats = w.redis_write_status()
    assert int(stats.get("publish_failures_total") or 0) >= 1
    assert int(stats.get("set_failures_total") or 0) >= 1
    assert int(stats.get("fail_streak") or 0) >= 1
    assert str(stats.get("last_error") or "")


class _FlakyRedis:
    def __init__(self) -> None:
        self.fail = True

    def publish(self, channel: str, payload: str) -> None:
        _ = (channel, payload)
        if self.fail:
            raise RuntimeError("publish_failed")

    def set(self, key: str, payload: str) -> None:
        _ = (key, payload)
        if self.fail:
            raise RuntimeError("set_failed")


def test_gateway_writer_redis_write_status_streak_resets_after_success(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    fake_redis = _FlakyRedis()
    w = GatewayWriter(runs_dir=runs_dir, profile="alt", redis_client=fake_redis)  # type: ignore[arg-type]
    w.set_health(ok=True, connected=True, error="")
    w.append_event({"type": "evt1"})
    first = w.redis_write_status()
    assert int(first.get("fail_streak") or 0) >= 1

    fake_redis.fail = False
    w.append_event({"type": "evt2"})
    second = w.redis_write_status()
    assert int(second.get("publish_failures_total") or 0) >= int(first.get("publish_failures_total") or 0)
    assert int(second.get("set_failures_total") or 0) >= int(first.get("set_failures_total") or 0)
    assert int(second.get("fail_streak") or 0) == 0
    assert str(second.get("last_error") or "") == ""


def test_command_id_deduper_blocks_duplicates_within_ttl() -> None:
    deduper = _CommandIdDeduper(max_ids=32, ttl_s=10.0)
    assert deduper.is_duplicate_and_remember("cmd-1", now_ts=100.0) is False
    assert deduper.is_duplicate_and_remember("cmd-1", now_ts=108.0) is True


def test_command_id_deduper_allows_reuse_after_ttl_expiry() -> None:
    deduper = _CommandIdDeduper(max_ids=32, ttl_s=5.0)
    assert deduper.is_duplicate_and_remember("cmd-1", now_ts=100.0) is False
    assert deduper.is_duplicate_and_remember("cmd-1", now_ts=106.0) is False


def test_command_id_deduper_evicts_when_capacity_reached() -> None:
    deduper = _CommandIdDeduper(max_ids=2, ttl_s=3600.0)
    assert deduper.is_duplicate_and_remember("cmd-a", now_ts=1.0) is False
    assert deduper.is_duplicate_and_remember("cmd-b", now_ts=2.0) is False
    assert deduper.is_duplicate_and_remember("cmd-c", now_ts=3.0) is False
    # cmd-a should be evicted by max_ids bound; replay is accepted.
    assert deduper.is_duplicate_and_remember("cmd-a", now_ts=4.0) is False


def test_hot_path_heartbeat_emit_interval() -> None:
    assert _should_emit_hot_path_heartbeat(now_ts=10.0, last_emit_at=9.1, interval_s=1.0) is False
    assert _should_emit_hot_path_heartbeat(now_ts=10.0, last_emit_at=9.0, interval_s=1.0) is True


def test_hot_path_heartbeat_emit_with_invalid_interval_defaults_safe() -> None:
    assert _should_emit_hot_path_heartbeat(now_ts=10.0, last_emit_at=9.95, interval_s=-1.0) is False
    assert _should_emit_hot_path_heartbeat(now_ts=10.0, last_emit_at=9.85, interval_s=-1.0) is True

