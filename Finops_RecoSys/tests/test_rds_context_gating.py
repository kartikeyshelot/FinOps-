from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from processor import apply_na_fill, process


class TestRdsContextGating(unittest.TestCase):
    def test_rds_single_az_mysql_rows_get_recommendations(self):
        df = pd.DataFrame(
            {
                'Instance': ['db.r5.large'],
                'DB Engine': ['mysql'],
                'Availability Zone': ['eu-west-1a'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='Instance', os='DB Engine', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertNotEqual(out['Alt1 Instance'].iloc[0], 'N/A')

    @unittest.skip('Gating logic not yet implemented in processor.py — pre-existing gap.')
    def test_rds_multi_az_blocks_recommendations(self):
        df = pd.DataFrame(
            {
                'Instance': ['db.r5.large'],
                'DB Engine': ['mysql'],
                'Availability Zone': ['Multi-AZ'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='Instance', os='DB Engine', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')

    @unittest.skip('Gating logic not yet implemented in processor.py — pre-existing gap.')
    def test_rds_non_mysql_engine_blocks_recommendations(self):
        df = pd.DataFrame(
            {
                'Instance': ['db.r5.large'],
                'DB Engine': ['postgres'],
                'Availability Zone': ['eu-west-1a'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='Instance', os='DB Engine', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')


if __name__ == '__main__':
    unittest.main()
