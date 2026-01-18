"""
Shared hashing utilities for deterministic data fingerprinting.
"""

from __future__ import annotations

import hashlib

import pandas as pd


def hash_csv(values: list[str]) -> str:
    """Hash a list of string values (comma-separated) to a short hex digest."""
    s = ",".join([str(v) for v in values])
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def stable_hash_df(df: pd.DataFrame) -> str:
    """Hash a DataFrame's CSV representation to a short hex digest."""
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]
