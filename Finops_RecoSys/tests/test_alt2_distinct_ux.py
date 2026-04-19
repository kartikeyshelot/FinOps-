from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from processor import ALT2_NO_DISTINCT, apply_na_fill, process


class TestAlt2DistinctMessage(unittest.TestCase):
    def test_c6g_graviton_no_cheaper_x86_shows_na(self):
        """c6g is already cheapest in compute category — no cheaper AMD/Intel at gen 6+."""
        df = pd.DataFrame({'Instance': ['c6g.large'], 'OS': ['linux'], 'Spend': [100.0]})
        b = ColumnBinding(instance='Instance', os='OS', actual_cost='Spend')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both', cpu_filter='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')

    def test_m5_has_two_distinct_alts_no_placeholder(self):
        df = pd.DataFrame({'Instance': ['m5.large'], 'OS': ['linux'], 'Spend': [50.0]})
        b = ColumnBinding(instance='Instance', os='OS', actual_cost='Spend')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both', cpu_filter='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'm5a.large')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'm6a.large')
        self.assertNotEqual(out['Alt2 Instance'].iloc[0], ALT2_NO_DISTINCT)

    def test_invalid_instance_both_alts_generic_na(self):
        df = pd.DataFrame({'Instance': ['not-valid'], 'OS': ['linux'], 'Spend': [10.0]})
        b = ColumnBinding(instance='Instance', os='OS', actual_cost='Spend')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')


if __name__ == '__main__':
    unittest.main()
