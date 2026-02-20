from __future__ import annotations

import ast
from pathlib import Path


def _has_direct_tqsdk_import(py_file: Path) -> bool:
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except Exception:
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "tqsdk" or alias.name.startswith("tqsdk."):
                    return True
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == "tqsdk" or mod.startswith("tqsdk."):
                return True
    return False


def test_tqsdk_imports_are_isolated_to_tq_and_config() -> None:
    """
    PRD §5.1.3:
    direct `tqsdk` imports are only allowed under `src/ghtrader/tq/`
    and in `src/ghtrader/config.py` for TqAuth creation.
    """
    root = Path(__file__).resolve().parents[1] / "src" / "ghtrader"
    violations: list[str] = []

    for py_file in root.rglob("*.py"):
        rel = py_file.relative_to(root).as_posix()
        if not _has_direct_tqsdk_import(py_file):
            continue
        allowed = rel == "config.py" or rel.startswith("tq/")
        if not allowed:
            violations.append(rel)

    assert not violations, f"Direct tqsdk imports outside allowed boundaries: {violations}"

