from __future__ import annotations

import unittest

import pandas as pd

from data_loader import ColumnBinding
from pricing_engine import get_price, get_rds_hourly
from processor import apply_na_fill, process


class TestRowRegionPricing(unittest.TestCase):
    def test_ec2_row_region_drives_current_price(self) -> None:
        df = pd.DataFrame(
            {
                "inst_type": ["m5.large", "m5.large"],
                "product": ["linux", "linux"],
                "region": ["eu-west-1", "us-east-1"],
                "ondemand_cost": [0.0, 0.0],
            }
        )
        b = ColumnBinding(instance="inst_type", os="product", actual_cost="ondemand_cost")
        out = apply_na_fill(process(df, b, region="eu-west-1", service="both"))

        p_eu = get_price("m5.large", region="eu-west-1", os="linux")
        p_us = get_price("m5.large", region="us-east-1", os="linux")
        self.assertIsNotNone(p_eu)
        self.assertIsNotNone(p_us)
        self.assertAlmostEqual(float(out["Current Price ($/hr)"].iloc[0]), float(p_eu), places=6)
        self.assertAlmostEqual(float(out["Current Price ($/hr)"].iloc[1]), float(p_us), places=6)

    def test_rds_row_region_drives_current_price(self) -> None:
        df = pd.DataFrame(
            {
                "inst_type": ["db.r5.large", "db.r5.large"],
                "db_engine": ["mysql", "mysql"],
                "region": ["eu-west-1", "us-east-1"],
                "ri_cost": [0.0, 0.0],
                "ondemand_cost": [1.0, 1.0],
            }
        )
        b = ColumnBinding(instance="inst_type", os="db_engine", actual_cost="ondemand_cost")
        out = apply_na_fill(process(df, b, region="eu-west-1", service="both"))

        p_eu = get_rds_hourly("db.r5.large", region="eu-west-1", os="linux")
        p_us = get_rds_hourly("db.r5.large", region="us-east-1", os="linux")
        self.assertIsNotNone(p_eu)
        self.assertIsNotNone(p_us)
        self.assertAlmostEqual(float(out["Current Price ($/hr)"].iloc[0]), float(p_eu), places=6)
        self.assertAlmostEqual(float(out["Current Price ($/hr)"].iloc[1]), float(p_us), places=6)

    def test_region_aliases_for_user_regions(self) -> None:
        df = pd.DataFrame(
            {
                "inst_type": ["m5.large", "db.r5.large"],
                "db_engine": ["linux", "mysql"],
                "region": ["eu-west-3", "ca-central-1"],
                "ondemand_cost": [1.0, 1.0],
            }
        )
        b = ColumnBinding(instance="inst_type", os="db_engine", actual_cost="ondemand_cost")
        out = apply_na_fill(process(df, b, region="eu-west-1", service="both"))

        # Aliases:
        # eu-west-3 -> eu-west-1
        # ca-central-1 -> us-east-1
        p_ec2_alias = get_price("m5.large", region="eu-west-1", os="linux")
        p_rds_alias = get_rds_hourly("db.r5.large", region="us-east-1", os="linux")
        self.assertIsNotNone(p_ec2_alias)
        self.assertIsNotNone(p_rds_alias)
        self.assertAlmostEqual(float(out["Current Price ($/hr)"].iloc[0]), float(p_ec2_alias), places=6)
        self.assertAlmostEqual(float(out["Current Price ($/hr)"].iloc[1]), float(p_rds_alias), places=6)


if __name__ == "__main__":
    unittest.main()
