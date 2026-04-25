from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from processor import apply_na_fill, process


class TestRdsAltPriceConsistency(unittest.TestCase):
    def test_rds_no_alt_instance_when_alt_price_missing(self):
        # db.c5.2xlarge has known recommendation path but missing local RDS hourly SKUs in fixture.
        df = pd.DataFrame({'i': ['db.c5.2xlarge'], 'o': ['linux'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='o', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both', cpu_filter='both'))
        self.assertEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Price ($/hr)'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Savings %'].iloc[0], 'N/A')


if __name__ == '__main__':
    unittest.main()
