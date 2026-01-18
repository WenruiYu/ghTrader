from __future__ import annotations

from ghtrader.research.pipeline import LatencyContext, LatencyTracker


def test_latency_tracker_records_and_stats():
    tr = LatencyTracker()
    tr.record("ingest", 0.001)
    tr.record("ingest", 0.002)

    stats = tr.get_stats("ingest")
    assert stats["mean"] > 0
    assert stats["p50"] > 0


def test_context_records_time():
    tr = LatencyTracker()
    with LatencyContext(tr, "inference"):
        pass
    assert len(tr.measurements["inference"]) == 1


def test_check_budget_fails_when_over_threshold():
    tr = LatencyTracker()
    # Force p95 above threshold by setting a tiny threshold
    tr.thresholds["inference"] = 0.0
    tr.record("inference", 0.01)
    passed, report = tr.check_budget()
    assert passed is False
    assert report["violations"]

