from __future__ import annotations

import importlib


def test_views_pipeline_helpers_are_loaded_from_dedicated_module() -> None:
    views_mod = importlib.import_module("ghtrader.control.views")

    assert getattr(getattr(views_mod, "_l5_calendar_context"), "__module__", "") == "ghtrader.control.views_pipeline_helpers"
    assert getattr(getattr(views_mod, "_pipeline_guardrails_context"), "__module__", "") == "ghtrader.control.views_pipeline_helpers"
    assert getattr(getattr(views_mod, "_validation_profile_suggestion"), "__module__", "") == "ghtrader.control.views_pipeline_helpers"
    assert getattr(getattr(views_mod, "_pipeline_health_context"), "__module__", "") == "ghtrader.control.views_pipeline_helpers"


def test_pipeline_health_context_prefers_revisioned_status_tokens() -> None:
    helper_mod = importlib.import_module("ghtrader.control.views_pipeline_helpers")
    out = helper_mod.pipeline_health_context(
        main_schedule_coverage={"n_days": 10, "schedule_hashes": ["abc"]},
        main_l5_coverage={"n_days": 10},
        main_l5_validation={
            "overall_state": "warn",
            "checked_days": 10,
            "missing_days": 1,
            "missing_segments_total": 3,
        },
        coverage_error="",
        validation_error="",
    )
    assert out["schedule"]["state"] == "ok"
    assert out["main_l5"]["state"] == "ok"
    assert out["validation"]["state"] == "warn"
