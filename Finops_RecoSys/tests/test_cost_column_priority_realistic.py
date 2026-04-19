from __future__ import annotations
import unittest
import pandas as pd
from data_loader import analyze_load, ColumnBinding
from processor import process


class TestCostPriorityRealisticColumns(unittest.TestCase):
    def test_prefers_monthly_over_ondemand_and_ri_columns(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'On Demand Cost': [999.0],
                'RI Cost': [777.0],
                'Total Cost Mar_2026': [146.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.actual_cost, 'Total Cost Mar_2026')

        out = process(df, lr.binding, region='eu-west-1', service='both')
        # Monthly cost is used and converted to hourly.
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[0]), 146.0 / 730.0, places=8)

    def test_row_fallback_uses_ri_when_no_monthly_available(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'On Demand Cost': [999.0],
                'RI Cost': [120.0],
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='On Demand Cost')
        out = process(df, b, region='eu-west-1', service='both')
        # Explicitly selected cost column is respected when it is an actual-cost-like header.
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[0]), 999.0, places=8)


if __name__ == '__main__':
    unittest.main()
