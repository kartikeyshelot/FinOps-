from __future__ import annotations
import unittest
import pandas as pd
from instance_api import canonicalize_instance_api_name
from pricing_engine import get_price, get_rds_hourly
from data_loader import ColumnBinding
from excel_export import sanitize_formula_injection_dataframe
from processor import INSERT_COLS, apply_na_fill, process


class TestCanonicalizeApiName(unittest.TestCase):

    def test_valid_ec2(self):
        self.assertEqual(canonicalize_instance_api_name('m5.large'), 'm5.large')
        self.assertEqual(canonicalize_instance_api_name('C6I.XLARGE'), 'c6i.xlarge')

    def test_valid_rds(self):
        self.assertEqual(canonicalize_instance_api_name('db.r5.large'), 'db.r5.large')

    def test_invalid_rejected(self):
        self.assertIsNone(canonicalize_instance_api_name('m5_large'))
        self.assertIsNone(canonicalize_instance_api_name('unknown.type'))
        self.assertIsNone(canonicalize_instance_api_name(''))
        self.assertIsNone(canonicalize_instance_api_name('m5'))
        self.assertIsNone(canonicalize_instance_api_name('db.r5'))
        self.assertIsNone(canonicalize_instance_api_name('db.r5.large.extra'))


class TestStrictPricing(unittest.TestCase):

    def test_exact_region_no_fallback(self):
        self.assertIsNotNone(get_price('m5.large', region='eu-west-1', os='linux'))
        self.assertIsNone(get_price('m5.large', region='eu-west-99', os='linux'))

    def test_exact_key_no_interpolation(self):
        self.assertIsNone(get_price('m5.fakeclass', region='eu-west-1', os='linux'))

    def test_unknown_os_no_default_surcharge(self):
        self.assertIsNone(get_price('m5.large', region='eu-west-1', os='freebsd'))

    def test_rds_missing_skus_na(self):
        self.assertIsNone(get_rds_hourly('db.c5.2xlarge', region='eu-west-1', os='linux'))

    def test_rds_known_skus(self):
        self.assertIsNotNone(get_rds_hourly('db.m5.large', region='eu-west-1', os='linux'))


class TestExportSafety(unittest.TestCase):

    def test_formula_injection_prefix_on_export_strings(self):
        df = pd.DataFrame({'a': ['=1+1', '+evil', '-bad', '@ref', 'ok', 3.0, True]})
        out = sanitize_formula_injection_dataframe(df)
        self.assertTrue(str(out['a'].iloc[0]).startswith("'"))
        self.assertTrue(str(out['a'].iloc[1]).startswith("'"))
        self.assertTrue(str(out['a'].iloc[2]).startswith("'"))
        self.assertTrue(str(out['a'].iloc[3]).startswith("'"))
        self.assertEqual(out['a'].iloc[4], 'ok')
        self.assertEqual(out['a'].iloc[5], 3.0)
        self.assertEqual(out['a'].iloc[6], True)


class TestProcessorStrict(unittest.TestCase):

    def test_reserved_enrichment_column_name_rejected(self):
        df = pd.DataFrame({'API Name': ['m5.large'], 'Pricing OS': ['linux'], 'Spend': [10.0]})
        b = ColumnBinding(instance='API Name', os='Pricing OS', actual_cost='Spend')
        with self.assertRaises(ValueError) as ctx:
            process(df, b, region='eu-west-1')
        self.assertIn('reserved', str(ctx.exception).lower())

    def test_duplicate_input_column_names_rejected(self):
        df = pd.DataFrame(
            [['m5.large', 'm5.large', 'linux']],
            columns=['Instance', 'Instance', 'OS'],
        )
        b = ColumnBinding(instance='Instance', os='OS', actual_cost=None)
        with self.assertRaises(ValueError) as ctx:
            process(df, b, region='eu-west-1', service='both', cpu_filter='both')
        self.assertIn('duplicate', str(ctx.exception).lower())

    def test_duplicate_cost_column_names_rejected(self):
        df = pd.DataFrame([['m5.large', 'linux', 10.0, 99.0]], columns=['API Name', 'OS', 'Amt', 'Amt'])
        b = ColumnBinding(instance='API Name', os='OS', actual_cost='Amt')
        with self.assertRaises(ValueError) as ctx:
            process(df, b, region='eu-west-1', service='both', cpu_filter='both')
        self.assertIn('duplicate', str(ctx.exception).lower())

    def test_missing_rds_price_recommendations_without_cost(self):
        df = pd.DataFrame({'API Name': ['db.c5.2xlarge'], 'OS': ['linux'], 'Spend': [100.0]})
        b = ColumnBinding(instance='API Name', os='OS', actual_cost='Spend')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both', cpu_filter='both'))
        self.assertIn('Alt1 Instance', out.columns)
        # Consistency rule: if current RDS SKU has no local hourly price, alternatives must be suppressed.
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')
        self.assertEqual(out['Discount %'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Price ($/hr)'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Savings %'].iloc[0], 'N/A')

    def test_invalid_row_no_crash(self):
        df = pd.DataFrame({'API Name': ['not-valid'], 'OS': ['linux'], 'Spend': [50.0]})
        b = ColumnBinding(instance='API Name', os='OS', actual_cost='Spend')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')

    def test_empty_dataframe_returns_enrichment_columns(self):
        df = pd.DataFrame(columns=['Instance', 'OS', 'Spend'])
        b = ColumnBinding(instance='Instance', os='OS', actual_cost='Spend')
        out = process(df, b, region='eu-west-1')
        self.assertEqual(len(out), 0)
        self.assertIn('Pricing OS', out.columns)
        self.assertIn('Alt1 Instance', out.columns)


if __name__ == '__main__':
    unittest.main()
