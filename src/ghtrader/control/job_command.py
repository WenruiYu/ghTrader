from __future__ import annotations

from typing import Iterable


def _parts(argv: Iterable[object] | None) -> list[str]:
    return [str(x) for x in (argv or [])]


def extract_cli_subcommands(argv: Iterable[object] | None) -> tuple[str | None, str | None]:
    """
    Best-effort extraction of `ghtrader <subcommand> [subcommand2]`.
    """
    parts = _parts(argv)
    try:
        # python -m ghtrader.cli data health ...
        if "ghtrader.cli" in parts:
            i = parts.index("ghtrader.cli")
            if i + 1 < len(parts):
                sub1 = parts[i + 1]
                sub2 = None
                if i + 2 < len(parts) and not str(parts[i + 2]).startswith("-"):
                    sub2 = parts[i + 2]
                return sub1, sub2

        # ghtrader data health ...
        if parts and str(parts[0]).endswith("ghtrader") and len(parts) >= 2 and not str(parts[1]).startswith("-"):
            sub1 = parts[1]
            sub2 = None
            if len(parts) >= 3 and not str(parts[2]).startswith("-"):
                sub2 = parts[2]
            return sub1, sub2
    except Exception:
        return None, None
    return None, None


def extract_cli_subcommand(argv: Iterable[object] | None) -> str | None:
    sub1, _ = extract_cli_subcommands(argv)
    return sub1


def infer_job_kind(argv: Iterable[object] | None) -> str:
    """
    Infer coarse job kind from command argv.
    """
    sub1, _sub2 = extract_cli_subcommands(argv)
    if sub1:
        return str(sub1).replace("-", "_")

    parts = _parts(argv)
    if "ghtrader" in parts:
        i = parts.index("ghtrader")
        if i + 1 < len(parts):
            return str(parts[i + 1]).replace("-", "_")
    return "unknown"
