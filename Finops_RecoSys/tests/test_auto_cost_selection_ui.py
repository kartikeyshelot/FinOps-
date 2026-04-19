from __future__ import annotations

import unittest
import pandas as pd

from data_loader import analyze_load


class TestAutoCostSelectionUIBehavior(unittest.TestCase):
    def test_multiple_cost_columns_auto_selects_first_ranked(self) -> None:
        df = pd.DataFrame(
            {
                "inst_type": ["db.r5.large"],
                "db_engine": ["mysql"],
                "ri_cost": [0.0],
                "ondemand_cost": [120.0],
                "Total cost": [130.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        # Should no longer force manual "Confirm actual cost column" step.
        self.assertFalse(lr.needs_cost_pick)
        self.assertIn(lr.binding.actual_cost, lr.cost_candidates)


if __name__ == "__main__":
    unittest.main()
