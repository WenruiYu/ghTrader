from __future__ import annotations

from ghtrader.util.observability import ObservabilityStore


def test_observability_store_snapshot_shape_and_error_rate() -> None:
    store = ObservabilityStore(max_samples=8)
    store.observe(metric="m1", latency_s=0.010, ok=True)
    store.observe(metric="m1", latency_s=0.020, ok=False)
    store.observe(metric="m1", latency_s=0.030, ok=True)

    snap = store.snapshot()
    assert "m1" in snap
    m1 = snap["m1"]
    assert int(m1["count"]) == 3
    assert int(m1["error_count"]) == 1
    assert float(m1["error_rate"]) > 0.0
    assert float(m1["p95_ms"]) >= float(m1["p50_ms"])

