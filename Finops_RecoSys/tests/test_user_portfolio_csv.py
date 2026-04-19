"""Regression tests for a representative portfolio CSV (Linux-heavy, mixed edge cases).

Production extracts are often **10k+ rows** with **many** columns (CUR / inventory exports).
`TestUserPortfolioCsvAtScale` verifies enrichment completes for 10k+ rows in the same **wide**
shape as the fixture. For Streamlit uploads over the default byte cap, set `FINOPS_MAX_UPLOAD_BYTES`.
"""
from __future__ import annotations
import time
import unittest
from pathlib import Path
import pandas as pd
from data_loader import ColumnBinding, require_unique_column_names
from pricing_engine import PRICING_LOOKUP_REGION, get_price
from processor import INSERT_COLS, apply_na_fill, process

_FIXTURE = Path(__file__).resolve().parent / 'fixtures' / 'sample_portfolio_input.csv'


class TestUserPortfolioCsvFixture(unittest.TestCase):
    """Uses tests/fixtures/sample_portfolio_input.csv — same shape as typical internal export (pre-enrichment)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._df = pd.read_csv(_FIXTURE, dtype=object, keep_default_na=False)
        require_unique_column_names(cls._df.columns)
        cls._binding = ColumnBinding(
            instance='Instance_Type',
            os='Product',
            actual_cost='Actual_Cost_per_Hour_USD',
        )
        cls._out = apply_na_fill(
            process(
                cls._df,
                cls._binding,
                region='eu-west-1',
                service='both',
                cpu_filter='both',
            )
        )

    def test_row_count_unchanged(self) -> None:
        self.assertEqual(len(self._out), 20)

    def test_inserts_finops_columns_after_instance(self) -> None:
        cols = list(self._out.columns)
        ix = cols.index('Instance_Type')
        self.assertEqual(cols[ix + 1 : ix + 1 + len(INSERT_COLS)], INSERT_COLS)

    def test_original_columns_and_values_preserved(self) -> None:
        for c in self._df.columns:
            self.assertIn(c, self._out.columns)
        # Strict check: all original values/columns preserved exactly.
        cols = list(self._df.columns)
        ix = cols.index('Instance_Type')
        reconstructed = pd.concat(
            [
                self._out.iloc[:, : ix + 1].copy(),
                self._out.iloc[:, ix + 1 + len(INSERT_COLS) :].copy(),
            ],
            axis=1,
        )
        reconstructed.columns = cols
        pd.testing.assert_frame_equal(
            reconstructed,
            self._df,
            check_dtype=True,
            check_exact=True,
            check_names=True,
        )

    def test_m5_large_linux_list_price_matches_bundle(self) -> None:
        rid = self._out[self._out['RecordID'].astype(str) == '601'].iloc[0]
        self.assertEqual(rid['Pricing OS'], 'Linux')
        want = get_price('m5.large', region=PRICING_LOOKUP_REGION, os='linux')
        self.assertIsNotNone(want)
        cp = pd.to_numeric(rid['Current Price ($/hr)'], errors='coerce')
        self.assertFalse(pd.isna(cp))
        self.assertAlmostEqual(float(cp), float(want), places=4)

    def test_invalid_instance_row_is_na_alts(self) -> None:
        rid = self._out[self._out['RecordID'].astype(str) == '611'].iloc[0]
        self.assertEqual(rid['Alt1 Instance'], 'N/A')
        self.assertEqual(rid['Current Price ($/hr)'], 'N/A')

    def test_graviton_row_has_alt2_or_placeholder(self) -> None:
        rid = self._out[self._out['RecordID'].astype(str) == '617'].iloc[0]
        self.assertEqual(str(rid['Instance_Type']), 'c6g.large')
        self.assertIn(rid['Alt2 Instance'], ('N/A (No distinct alternative)', 'N/A'))

    def test_no_crash_all_rows_have_finops_columns(self) -> None:
        for c in INSERT_COLS:
            self.assertIn(c, self._out.columns)
            self.assertEqual(len(self._out[c]), 20)


class TestUserPortfolioCsvAtScale(unittest.TestCase):
    """Wide table (same columns as sample_portfolio) at 10k+ rows — no crash, row count preserved."""

    def test_10500_rows_portfolio_column_shape(self) -> None:
        template = pd.read_csv(_FIXTURE, dtype=object, keep_default_na=False).iloc[[0]]
        n = 10_500
        data = {c: [template[c].iloc[0]] * n for c in template.columns}
        df = pd.DataFrame(data)
        df['RecordID'] = [str(800_000 + i) for i in range(n)]
        require_unique_column_names(df.columns)
        binding = ColumnBinding(
            instance='Instance_Type',
            os='Product',
            actual_cost='Actual_Cost_per_Hour_USD',
        )
        t0 = time.perf_counter()
        out = apply_na_fill(process(df, binding, region='eu-west-1', service='both', cpu_filter='both'))
        elapsed = time.perf_counter() - t0
        self.assertEqual(len(out), n)
        self.assertEqual(len(out.columns), len(df.columns) + len(INSERT_COLS))
        self.assertLess(elapsed, 180.0, msg=f'10.5k wide rows took {elapsed:.1f}s')
        want = get_price('m5.large', region=PRICING_LOOKUP_REGION, os='linux')
        cp0 = pd.to_numeric(out['Current Price ($/hr)'].iloc[0], errors='coerce')
        self.assertFalse(pd.isna(cp0))
        self.assertAlmostEqual(float(cp0), float(want), places=4)


if __name__ == '__main__':
    unittest.main()
