from __future__ import annotations

import unittest

import pandas as pd

from data_loader import ColumnBinding
from processor import process


class TestRiOnDemandRowwiseSelection(unittest.TestCase):
    def test_uses_on_demand_when_selected_ri_is_zero(self) -> None:
        df = pd.DataFrame(
            {
                "API Name": ["m5.large"],
                "Product": ["linux"],
                "RI Cost": [0.0],
                "On Demand Cost": [120.0],
                "Total Cost": [500.0],
            }
        )
        b = ColumnBinding(instance="API Name", os="Product", actual_cost="RI Cost")
        out = process(df, b, region="eu-west-1", service="both")
        self.assertAlmostEqual(float(out["Actual Cost ($)"].iloc[0]), 120.0, places=8)

    def test_uses_ri_when_selected_on_demand_is_zero(self) -> None:
        df = pd.DataFrame(
            {
                "API Name": ["m5.large"],
                "Product": ["linux"],
                "On Demand Cost": [0.0],
                "RI Cost": [95.0],
                "Total Cost": [500.0],
            }
        )
        b = ColumnBinding(instance="API Name", os="Product", actual_cost="On Demand Cost")
        out = process(df, b, region="eu-west-1", service="both")
        self.assertAlmostEqual(float(out["Actual Cost ($)"].iloc[0]), 95.0, places=8)


if __name__ == "__main__":
    unittest.main()
