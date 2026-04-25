from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from processor import process, apply_na_fill


class TestRegionAliasMapping(unittest.TestCase):
    def test_ca_central_1_alias_maps_to_us_east_1_prices(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'Region': ['ca-central-1'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertNotEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')

    def test_eu_west_3_alias_maps_to_eu_west_1_prices(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'Region': ['eu-west-3'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertNotEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')


if __name__ == '__main__':
    unittest.main()
