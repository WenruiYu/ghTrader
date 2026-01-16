"""
Tests for the unified L5 detection module.
"""

from __future__ import annotations

import pandas as pd
import pytest


class TestL5Constants:
    """Test L5 column constants."""

    def test_l5_price_columns(self):
        from ghtrader.l5_detection import L5_PRICE_COLUMNS

        assert len(L5_PRICE_COLUMNS) == 8
        assert "bid_price2" in L5_PRICE_COLUMNS
        assert "ask_price5" in L5_PRICE_COLUMNS

    def test_l5_volume_columns(self):
        from ghtrader.l5_detection import L5_VOLUME_COLUMNS

        assert len(L5_VOLUME_COLUMNS) == 8
        assert "bid_volume2" in L5_VOLUME_COLUMNS
        assert "ask_volume5" in L5_VOLUME_COLUMNS

    def test_l5_columns_complete(self):
        from ghtrader.l5_detection import L5_COLUMNS, L5_PRICE_COLUMNS, L5_VOLUME_COLUMNS

        assert len(L5_COLUMNS) == 16
        assert set(L5_COLUMNS) == set(L5_PRICE_COLUMNS) | set(L5_VOLUME_COLUMNS)


class TestHasL5Df:
    """Test DataFrame-based L5 detection."""

    def test_empty_df_no_l5(self):
        from ghtrader.l5_detection import has_l5_df

        df = pd.DataFrame()
        assert has_l5_df(df) is False

    def test_no_l5_columns(self):
        from ghtrader.l5_detection import has_l5_df

        df = pd.DataFrame({"symbol": ["SHFE.cu2502"], "bid_price1": [10.0], "ask_price1": [11.0]})
        assert has_l5_df(df) is False

    def test_l5_columns_all_zero(self):
        from ghtrader.l5_detection import has_l5_df

        df = pd.DataFrame({
            "symbol": ["SHFE.cu2502"],
            "bid_price1": [10.0],
            "bid_price2": [0.0],
            "ask_price2": [0.0],
            "bid_volume2": [0],
            "ask_volume2": [0],
        })
        assert has_l5_df(df) is False

    def test_l5_columns_all_nan(self):
        from ghtrader.l5_detection import has_l5_df

        df = pd.DataFrame({
            "symbol": ["SHFE.cu2502"],
            "bid_price1": [10.0],
            "bid_price2": [float("nan")],
            "ask_price2": [float("nan")],
            "bid_volume2": [float("nan")],
        })
        assert has_l5_df(df) is False

    def test_l5_present_price(self):
        from ghtrader.l5_detection import has_l5_df

        df = pd.DataFrame({
            "symbol": ["SHFE.cu2502"],
            "bid_price1": [10.0],
            "bid_price2": [9.5],  # Positive L5 price
            "ask_price2": [0.0],
        })
        assert has_l5_df(df) is True

    def test_l5_present_volume(self):
        from ghtrader.l5_detection import has_l5_df

        df = pd.DataFrame({
            "symbol": ["SHFE.cu2502"],
            "bid_price1": [10.0],
            "bid_price2": [0.0],
            "bid_volume3": [100],  # Positive L5 volume
        })
        assert has_l5_df(df) is True

    def test_l5_in_higher_level(self):
        from ghtrader.l5_detection import has_l5_df

        df = pd.DataFrame({
            "symbol": ["SHFE.cu2502"],
            "bid_price2": [0.0],
            "bid_price3": [0.0],
            "bid_price4": [0.0],
            "bid_price5": [8.0],  # Only L5 level 5 has data
        })
        assert has_l5_df(df) is True


class TestHasL5Row:
    """Test single-row L5 detection."""

    def test_empty_row_no_l5(self):
        from ghtrader.l5_detection import has_l5_row

        row: dict = {}
        assert has_l5_row(row) is False

    def test_row_with_l5(self):
        from ghtrader.l5_detection import has_l5_row

        row = {"bid_price2": 9.5, "ask_price2": 0.0}
        assert has_l5_row(row) is True

    def test_row_no_l5(self):
        from ghtrader.l5_detection import has_l5_row

        row = {"bid_price1": 10.0, "ask_price1": 11.0, "bid_price2": 0.0}
        assert has_l5_row(row) is False

    def test_row_nan_no_l5(self):
        from ghtrader.l5_detection import has_l5_row

        row = {"bid_price2": float("nan"), "ask_price2": float("nan")}
        assert has_l5_row(row) is False


class TestCountL5Rows:
    """Test L5 row counting."""

    def test_count_empty(self):
        from ghtrader.l5_detection import count_l5_rows

        df = pd.DataFrame()
        assert count_l5_rows(df) == 0

    def test_count_no_l5(self):
        from ghtrader.l5_detection import count_l5_rows

        df = pd.DataFrame({
            "bid_price2": [0.0, 0.0, 0.0],
            "ask_price2": [0.0, 0.0, 0.0],
        })
        assert count_l5_rows(df) == 0

    def test_count_partial_l5(self):
        from ghtrader.l5_detection import count_l5_rows

        df = pd.DataFrame({
            "bid_price2": [0.0, 9.5, 0.0],  # Second row has L5
            "ask_price2": [0.0, 0.0, 10.0],  # Third row has L5
        })
        assert count_l5_rows(df) == 2

    def test_count_all_l5(self):
        from ghtrader.l5_detection import count_l5_rows

        df = pd.DataFrame({
            "bid_price2": [9.5, 9.6, 9.7],
        })
        assert count_l5_rows(df) == 3


class TestL5SqlCondition:
    """Test SQL condition generation."""

    def test_sql_condition_structure(self):
        from ghtrader.l5_detection import l5_sql_condition

        cond = l5_sql_condition()
        assert cond.startswith("(")
        assert cond.endswith(")")
        assert " OR " in cond
        assert "bid_price2 > 0" in cond
        assert "ask_price5 > 0" in cond
        assert "bid_volume2 > 0" in cond

    def test_sql_case_expression(self):
        from ghtrader.l5_detection import l5_sql_case_expression

        expr = l5_sql_case_expression()
        assert "CASE WHEN" in expr
        assert "THEN 1 ELSE 0 END" in expr


class TestL5DetectionConsistency:
    """Test that different detection methods are consistent."""

    def test_df_and_row_consistency(self):
        from ghtrader.l5_detection import has_l5_df, has_l5_row

        # Create a DataFrame with L5 data
        df = pd.DataFrame({
            "bid_price2": [9.5],
            "ask_price2": [10.0],
        })

        # Check consistency
        assert has_l5_df(df) is True
        assert has_l5_row({"bid_price2": 9.5, "ask_price2": 10.0}) is True

    def test_df_and_row_consistency_no_l5(self):
        from ghtrader.l5_detection import has_l5_df, has_l5_row

        # Create a DataFrame without L5 data
        df = pd.DataFrame({
            "bid_price2": [0.0],
            "ask_price2": [0.0],
        })

        # Check consistency
        assert has_l5_df(df) is False
        assert has_l5_row({"bid_price2": 0.0, "ask_price2": 0.0}) is False
