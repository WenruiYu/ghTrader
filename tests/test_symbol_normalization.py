from __future__ import annotations

import pytest

from ghtrader.data.main_schedule import normalize_contract_symbol


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("CU2602", "SHFE.cu2602"),
        ("cu2602", "SHFE.cu2602"),
        ("SHFE.cu2602", "SHFE.cu2602"),
        ("SHFE.CU2602", "SHFE.cu2602"),
    ],
)
def test_normalize_contract_symbol(raw: str, expected: str):
    assert normalize_contract_symbol(raw, exchange="SHFE") == expected

