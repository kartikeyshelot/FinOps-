from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from processor import process


class TestStrictMonthlyCostPriority(unittest.TestCase):
    def test_when_month_columns_exist_non_month_cost_not_used(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large', 'm5.large'],
                'Product': ['linux', 'linux'],
                'Mar_2026': [146.0, pd.NA],
                'Jan_2026': [pd.NA, pd.NA],
                'Random Price': [999.0, 888.0],  # must not backfill actual when month columns exist
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='Mar_2026')
        out = process(df, b, region='eu-west-1', service='both')
        # Month-like column names are treated as monthly and converted to hourly.
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[0]), 146.0 / 730.0, places=8)
        # Month columns exist but row has no month value -> must stay missing (not 888 fallback)
        self.assertTrue(pd.isna(out['Actual Cost ($)'].iloc[1]))

    def test_monthly_marker_column_converts_to_hourly(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'Monthly Cost Mar_2026': [146.0],
                'Random Price': [999.0],
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='Monthly Cost Mar_2026')
        out = process(df, b, region='eu-west-1', service='both')
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[0]), 146.0 / 730.0, places=8)


if __name__ == '__main__':
    unittest.main()
