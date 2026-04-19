from __future__ import annotations
import unittest
import pandas as pd
from data_loader import analyze_load
from processor import _to_float


class TestLatestMonthCostSelection(unittest.TestCase):
    def test_prefers_latest_monthly_column(self):
        df = pd.DataFrame(
            {
                'API name': ['m5.large'],
                'Product': ['linux'],
                'Cost Jan 2026': [100.0],
                'Cost Mar 2026': [120.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.actual_cost, 'Cost Mar 2026')

    def test_prefers_latest_when_month_names_only(self):
        df = pd.DataFrame(
            {
                'API name': ['m5.large'],
                'Product': ['linux'],
                'Monthly Cost - Jan': [90.0],
                'Monthly Cost - Dec': [120.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.actual_cost, 'Monthly Cost - Dec')
        self.assertFalse(lr.needs_cost_pick)


class TestMonthlyToHourlyParser(unittest.TestCase):
    def test_parse_monthly_currency_text_to_hourly(self):
        v = _to_float('120', column_name='Monthly Cost')
        self.assertIsNotNone(v)
        self.assertAlmostEqual(float(v), 120.0 / 730.0, places=8)

    def test_parse_monthly_numeric_to_hourly_when_column_marked_monthly(self):
        v = _to_float(730.0, column_name='Actual Monthly Cost')
        self.assertIsNotNone(v)
        self.assertAlmostEqual(float(v), 1.0, places=8)


if __name__ == '__main__':
    unittest.main()
