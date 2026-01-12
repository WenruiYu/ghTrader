from __future__ import annotations

import pytest

from ghtrader.akshare_daily import normalize_akshare_symbol_to_tqsdk


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("CU2602", "SHFE.cu2602"),
        ("cu2602", "SHFE.cu2602"),
        ("SHFE.cu2602", "SHFE.cu2602"),
        ("SHFE.CU2602", "SHFE.cu2602"),
    ],
)
def test_normalize_akshare_symbol_to_tqsdk(raw: str, expected: str):
    assert normalize_akshare_symbol_to_tqsdk(raw, exchange="SHFE") == expected

