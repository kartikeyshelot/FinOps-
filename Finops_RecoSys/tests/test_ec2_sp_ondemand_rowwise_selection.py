from __future__ import annotations

import unittest

import pandas as pd

from data_loader import ColumnBinding
from processor import process


class TestEc2SpOnDemandRowwiseSelection(unittest.TestCase):
    def test_uses_ondemand_when_selected_sp_is_zero(self) -> None:
        df = pd.DataFrame(
            {
                "instance_type": ["m5.large"],
                "operating_system": ["linux"],
                "savings_plan_cost": [0.0],
                "ondemand_cost": [132.0],
                "total_cost": [150.0],
            }
        )
        b = ColumnBinding(instance="instance_type", os="operating_system", actual_cost="savings_plan_cost")
        out = process(df, b, region="eu-west-1", service="ec2")
        self.assertAlmostEqual(float(out["Actual Cost ($)"].iloc[0]), 132.0, places=8)

    def test_uses_sp_when_selected_ondemand_is_zero(self) -> None:
        df = pd.DataFrame(
            {
                "instance_type": ["m5.large"],
                "operating_system": ["linux"],
                "savings_plan_cost": [118.0],
                "ondemand_cost": [0.0],
                "total_cost": [150.0],
            }
        )
        b = ColumnBinding(instance="instance_type", os="operating_system", actual_cost="ondemand_cost")
        out = process(df, b, region="eu-west-1", service="ec2")
        self.assertAlmostEqual(float(out["Actual Cost ($)"].iloc[0]), 118.0, places=8)


if __name__ == "__main__":
    unittest.main()
