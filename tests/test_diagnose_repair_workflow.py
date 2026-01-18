from __future__ import annotations

from datetime import datetime, timezone


def test_generate_repair_plan_includes_bootstrap_and_fill_actions() -> None:
    from ghtrader.data.diagnose import DiagnoseFinding, DiagnoseReport
    from ghtrader.data.repair import generate_repair_plan

    rep = DiagnoseReport(
        run_id="diag123",
        generated_at=datetime.now(timezone.utc).isoformat(),
        exchange="SHFE",
        var="cu",
        dataset_version="v2",
        ticks_kind="raw",
        thoroughness="quick",
        symbols=["SHFE.cu2602"],
        summary={},
        auto_fixable=[
            DiagnoseFinding(
                tier="auto_fixable",
                severity="warning",
                code="index_missing",
                message="index missing",
                symbol="SHFE.cu2602",
                ticks_kind="raw",
                dataset_version="v2",
                extra={},
            ),
            DiagnoseFinding(
                tier="auto_fixable",
                severity="warning",
                code="missing_days",
                message="missing days",
                symbol="SHFE.cu2602",
                ticks_kind="raw",
                dataset_version="v2",
                extra={"missing_first": "2026-01-02", "missing_last": "2026-01-10"},
            ),
        ],
        manual_review=[],
        unfixable=[],
    )

    plan = generate_repair_plan(report=rep, include_refresh_catalog=False, chunk_days=7)
    kinds = [a.kind for a in plan.actions]
    assert "bootstrap_index" in kinds
    assert "fill_missing_days" in kinds
    fill = next(a for a in plan.actions if a.kind == "fill_missing_days")
    assert fill.params["symbol"] == "SHFE.cu2602"
    assert fill.params["chunk_days"] == 7

