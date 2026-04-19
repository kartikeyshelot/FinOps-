from __future__ import annotations
import unittest
import pandas as pd
from data_loader import analyze_load


class TestAirbusColumnDetection(unittest.TestCase):
    def test_api_name_detected_as_instance_column(self):
        df = pd.DataFrame(
            {
                'API name': ['m5.large', 'db.r5.large'],
                'Product': ['linux', 'windows'],
                'Cost': [1.0, 2.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertFalse(lr.needs_instance_pick)
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.instance, 'API name')

    def test_product_detected_as_os_column_by_values(self):
        df = pd.DataFrame(
            {
                'X': ['m5.large', 'm6i.large'],
                'Product': ['Ubuntu 22.04', 'Windows Server'],
                'Amt': [10.0, 20.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.os, 'Product')

    def test_missing_os_defaults_to_linux_flow(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large', 'm6i.large'],
                'Notes': ['n/a', 'prod'],
                'Cost': [5.0, 6.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertIsNone(lr.binding.os)
        self.assertTrue(any('linux for all rows' in w.lower() for w in lr.warnings))

    def test_missing_instance_fails_detection(self):
        df = pd.DataFrame(
            {
                'Product': ['linux', 'windows'],
                'Amount': [1.0, 2.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertTrue(lr.needs_instance_pick)
        self.assertIsNone(lr.binding)


if __name__ == '__main__':
    unittest.main()
