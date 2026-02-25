from __future__ import annotations

from pathlib import Path
import tomllib


def test_coverage_exclusions_are_documented_and_resolvable() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    omit = list((((pyproject.get("tool") or {}).get("coverage") or {}).get("run") or {}).get("omit") or [])
    assert omit, "coverage omit list should not be empty without explicit governance decision"
    assert len(omit) == len(set(omit)), "coverage omit list should not contain duplicate paths"

    report = (repo_root / "reports" / "coverage_governance_review.md").read_text(encoding="utf-8")
    for path_str in omit:
        assert path_str in report, f"missing coverage rationale entry for {path_str}"
        assert (repo_root / path_str).exists(), f"coverage omit path does not exist: {path_str}"
