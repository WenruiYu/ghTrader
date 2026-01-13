from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class SymbolCoverage:
    symbol: str
    n_dates: int
    min_date: date | None
    max_date: date | None


def _parse_partition_dates(symbol_dir: Path) -> list[date]:
    dates: list[date] = []
    if not symbol_dir.exists():
        return dates
    for child in symbol_dir.iterdir():
        if not child.is_dir() or not child.name.startswith("date="):
            continue
        try:
            dt = date.fromisoformat(child.name.split("=", 1)[1])
            dates.append(dt)
        except Exception:
            continue
    return sorted(dates)


def scan_partitioned_store(root: Path) -> list[SymbolCoverage]:
    """
    Scan a directory of the form:
      root/symbol=.../date=YYYY-MM-DD/*.parquet
    """
    out: list[SymbolCoverage] = []
    if not root.exists():
        return out

    for sym_dir in sorted(root.iterdir()):
        if not sym_dir.is_dir() or not sym_dir.name.startswith("symbol="):
            continue
        symbol = sym_dir.name.split("=", 1)[1]
        dates = _parse_partition_dates(sym_dir)
        out.append(
            SymbolCoverage(
                symbol=symbol,
                n_dates=len(dates),
                min_date=dates[0] if dates else None,
                max_date=dates[-1] if dates else None,
            )
        )
    return out

