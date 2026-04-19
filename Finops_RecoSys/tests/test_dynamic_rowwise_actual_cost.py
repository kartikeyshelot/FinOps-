from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from processor import process


class TestDynamicRowwiseActualCost(unittest.TestCase):
    def test_rowwise_latest_non_null_cost_fallback(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large', 'm5.large'],
                'Product': ['linux', 'linux'],
                'Cost Jan 2026': [pd.NA, 73.0],
                'Cost Feb 2026': [146.0, pd.NA],
                'Rate Card Price': [999.0, 999.0],  # should not be used as actual cost
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='Cost Feb 2026')
        out = process(df, b, region='eu-west-1', service='both')
        # Monthly headers are converted to hourly (/730).
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[0]), 146.0 / 730.0, places=8)
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[1]), 73.0 / 730.0, places=8)


if __name__ == '__main__':
    unittest.main()
