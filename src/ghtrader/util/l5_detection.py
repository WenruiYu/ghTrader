"""
Unified L5 (Level 5 order book depth) detection logic.

This module provides a single source of truth for detecting whether tick data
contains true L5 depth (levels 2-5 with non-null, non-NaN, positive values).

All modules that need L5 detection should import from here instead of
implementing their own logic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# L5 depth columns (levels 2-5; level 1 is always present)
L5_PRICE_COLUMNS: tuple[str, ...] = (
    "bid_price2", "ask_price2",
    "bid_price3", "ask_price3",
    "bid_price4", "ask_price4",
    "bid_price5", "ask_price5",
)

L5_VOLUME_COLUMNS: tuple[str, ...] = (
    "bid_volume2", "ask_volume2",
    "bid_volume3", "ask_volume3",
    "bid_volume4", "ask_volume4",
    "bid_volume5", "ask_volume5",
)

# All L5 columns (price + volume for levels 2-5)
L5_COLUMNS: tuple[str, ...] = L5_PRICE_COLUMNS + L5_VOLUME_COLUMNS


# ---------------------------------------------------------------------------
# DataFrame-based detection
# ---------------------------------------------------------------------------

def has_l5_df(df: "pd.DataFrame") -> bool:
    """
    Check if a DataFrame contains true L5 depth data.

    True L5 means at least one of levels 2-5 has positive, non-NaN values
    for price or volume columns.

    Args:
        df: DataFrame with tick data (must have L5 columns)

    Returns:
        True if any L5 data is present, False otherwise
    """
    import pandas as pd

    for col in L5_PRICE_COLUMNS:
        if col not in df.columns:
            continue
        try:
            s = pd.to_numeric(df[col], errors="coerce")
            if bool((s > 0).any()):
                return True
        except Exception:
            continue

    for col in L5_VOLUME_COLUMNS:
        if col not in df.columns:
            continue
        try:
            s = pd.to_numeric(df[col], errors="coerce")
            if bool((s > 0).any()):
                return True
        except Exception:
            continue

    return False


def has_l5_row(row: dict) -> bool:
    """
    Check if a single tick row contains true L5 depth data.

    Args:
        row: Dictionary with tick data

    Returns:
        True if any L5 data is present, False otherwise
    """
    import math

    for col in L5_COLUMNS:
        val = row.get(col)
        if val is None:
            continue
        try:
            v = float(val)
            if v > 0 and not math.isnan(v):
                return True
        except (ValueError, TypeError):
            continue

    return False


def count_l5_rows(df: "pd.DataFrame") -> int:
    """
    Count the number of rows in a DataFrame that have true L5 depth.

    Args:
        df: DataFrame with tick data

    Returns:
        Number of rows with L5 data
    """
    import pandas as pd

    mask = pd.Series([False] * len(df), index=df.index)

    for col in L5_COLUMNS:
        if col not in df.columns:
            continue
        try:
            s = pd.to_numeric(df[col], errors="coerce")
            mask = mask | (s > 0)
        except Exception:
            continue

    return int(mask.sum())


# ---------------------------------------------------------------------------
# SQL-based detection (for QuestDB queries)
# ---------------------------------------------------------------------------

def l5_sql_condition() -> str:
    """
    Return a SQL WHERE clause fragment that filters for rows with true L5 depth.

    This is for use in QuestDB queries. The condition checks if any L5
    price or volume column has a positive value.

    Returns:
        SQL string like "(bid_price2 > 0 OR ask_price2 > 0 OR ...)"
    """
    parts: list[str] = []
    for col in L5_COLUMNS:
        parts.append(f"{col} > 0")
    return "(" + " OR ".join(parts) + ")"


def l5_sql_case_expression() -> str:
    """
    Return a SQL CASE expression that returns 1 if L5 is present, 0 otherwise.

    Useful for aggregations like: max(l5_case_expression) AS l5_present_i

    Returns:
        SQL CASE expression string
    """
    return f"CASE WHEN {l5_sql_condition()} THEN 1 ELSE 0 END"


# ---------------------------------------------------------------------------
# Parquet detection removed (QuestDB-only system)
# ---------------------------------------------------------------------------
