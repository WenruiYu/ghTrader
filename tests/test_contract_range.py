from __future__ import annotations

from ghtrader.tq.ingest import _iter_contract_yymms


def test_iter_contract_yymms_inclusive_months():
    yymms = _iter_contract_yymms("1601", "1603")
    assert yymms == ["1601", "1602", "1603"]

