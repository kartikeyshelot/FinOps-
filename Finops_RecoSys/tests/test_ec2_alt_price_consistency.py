from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from pricing_engine import get_price
from processor import apply_na_fill, process


class TestEc2AltPriceConsistency(unittest.TestCase):
    def test_ec2_invalid_current_price_suppresses_alternatives(self) -> None:
        df = pd.DataFrame({'i': ['m5.fakeclass'], 'p': ['linux'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Price ($/hr)'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Price ($/hr)'].iloc[0], 'N/A')

    def test_ec2_alt_without_price_is_not_shown(self) -> None:
        # Build a scenario where an alt recommendation may exist but local price is missing
        # by using a known class and validating consistency relation directly.
        df = pd.DataFrame({'i': ['m5.large'], 'p': ['linux'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        alt1 = out['Alt1 Instance'].iloc[0]
        alt1p = out['Alt1 Price ($/hr)'].iloc[0]
        if alt1 != 'N/A':
            self.assertNotEqual(alt1p, 'N/A')
            want = get_price(str(alt1), region='eu-west-1', os='linux')
            self.assertIsNotNone(want)


if __name__ == '__main__':
    unittest.main()
