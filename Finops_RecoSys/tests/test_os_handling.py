from __future__ import annotations
import unittest
from decimal import Decimal
import pandas as pd
from data_loader import ColumnBinding, analyze_load, finalize_binding
from os_resolve import LINUX_FALLBACK_LABEL, cell_matches_valid_os_pattern, engine_os_for_pricing, normalize_pricing_os_display
from pricing_engine import normalize_pricing_region
from pricing_normalize import LINUX_FALLBACK_LABEL as PN_LINUX_FALLBACK
from pricing_normalize import normalize_instance_string, normalize_os, normalize_os_engine_key, normalize_pricing_os_label

assert LINUX_FALLBACK_LABEL == PN_LINUX_FALLBACK, 'LINUX_FALLBACK_LABEL must match pricing_normalize'
from processor import INSERT_COLS, _to_float, apply_na_fill, process


class TestPricingNormalization(unittest.TestCase):

    def test_instance_strip_lower(self):
        self.assertEqual(normalize_instance_string('   M5.LARGE   '), 'm5.large')

    def test_os_windows_substring_and_sql(self):
        self.assertEqual(normalize_os('Windows Server 2022'), 'Windows')
        self.assertEqual(normalize_os_engine_key('Microsoft SQL Server'), 'windows')

    def test_region_exact_id(self):
        self.assertEqual(normalize_pricing_region('EU-WEST-1 '), 'eu-west-1')
        self.assertEqual(normalize_pricing_region('eu-west-99'), 'eu-west-1')


class TestOsValuePatterns(unittest.TestCase):

    def test_detection_linux_variants(self):
        for v in ('linux', 'Ubuntu 22', 'DEBIAN', 'rhel 8', 'amazon linux 2', 'AMAZON LINUX'):
            self.assertTrue(cell_matches_valid_os_pattern(v), msg=v)

    def test_detection_windows_variants(self):
        for v in ('windows', 'Windows Server', 'win', 'WIN2019'):
            self.assertTrue(cell_matches_valid_os_pattern(v), msg=v)

    def test_normalize_display_buckets(self):
        self.assertEqual(normalize_pricing_os_display('ubuntu'), 'Linux')
        self.assertEqual(normalize_pricing_os_display('Win10'), 'Windows')
        self.assertEqual(normalize_pricing_os_display(''), LINUX_FALLBACK_LABEL)
        self.assertEqual(normalize_pricing_os_display(None), LINUX_FALLBACK_LABEL)

    def test_engine_default_linux(self):
        self.assertEqual(engine_os_for_pricing(''), 'linux')
        self.assertEqual(engine_os_for_pricing('garbage xyz'), 'linux')


class TestDynamicOsColumnDetection(unittest.TestCase):

    def test_os_in_arbitrary_column_name(self):
        df = pd.DataFrame(
            {
                'VM': ['m5.large', 'm5.large'],
                'Totally_Not_OS': ['linux', 'ubuntu'],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.os, 'Totally_Not_OS')
        self.assertFalse(lr.needs_os_pick)

    def test_mixed_values_normalized(self):
        df = pd.DataFrame({'i': ['m5.large', 'm5.large'], 'x': ['Win', 'debian']})
        lr = analyze_load(df, [])
        self.assertEqual(lr.binding.instance, 'i')
        self.assertEqual(lr.binding.os, 'x')
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1', service='both'))
        ins = list(out.columns).index('i')
        self.assertEqual(out.columns[ins + 1], 'Pricing OS')
        self.assertEqual(out['Pricing OS'].iloc[0], 'Windows')
        self.assertEqual(out['Pricing OS'].iloc[1], 'Linux')

    def test_missing_os_values_fallback_linux_column(self):
        df = pd.DataFrame({'i': ['m5.large', 'm5.large'], 'x': ['linux', '']})
        lr = analyze_load(df, [])
        self.assertEqual(lr.binding.os, 'x')
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1'))
        self.assertEqual(out['Pricing OS'].iloc[0], 'Linux')
        self.assertEqual(out['Pricing OS'].iloc[1], LINUX_FALLBACK_LABEL)

    def test_no_os_column_auto_binding(self):
        df = pd.DataFrame({'shape': ['m5.large'], 'note': ['prod'], 'amt': [10.0]})
        lr = analyze_load(df, [])
        self.assertIsNone(lr.binding.os)
        self.assertFalse(lr.needs_os_pick)
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1', service='both'))
        self.assertEqual(out['Pricing OS'].tolist(), [LINUX_FALLBACK_LABEL])

    def test_ambiguous_two_os_columns(self):
        df = pd.DataFrame(
            {
                'inst': ['m5.large'],
                'col_a': ['linux'],
                'col_b': ['ubuntu'],
            }
        )
        lr = analyze_load(df, [])
        self.assertTrue(lr.needs_os_pick)
        self.assertIsNone(lr.binding)
        self.assertGreaterEqual(len(lr.os_candidates), 2)

    def test_binding_os_none_process(self):
        df = pd.DataFrame({'i': ['m5.large'], 'c': [100.0]})
        b = ColumnBinding(instance='i', os=None, actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1'))
        self.assertEqual(out['Pricing OS'].iloc[0], LINUX_FALLBACK_LABEL)
        self.assertIn('Alt1 Instance', out.columns)


class TestPricingOsInsertionOrder(unittest.TestCase):

    def test_after_instance_column(self):
        cols = ['A', 'Instance', 'Z']
        df = pd.DataFrame([['a', 'm5.large', 'z']], columns=cols)
        b = ColumnBinding(instance='Instance', os='Z', actual_cost=None)
        out = process(df, b, region='eu-west-1')
        idx = list(out.columns).index('Instance')
        self.assertEqual(list(out.columns[idx : idx + 1 + len(INSERT_COLS)][:2]), ['Instance', 'Pricing OS'])
        self.assertEqual(list(out.columns[idx + 1 : idx + 1 + len(INSERT_COLS)]), INSERT_COLS)


class TestProductColumnUnixAndCostInference(unittest.TestCase):

    def test_product_column_linux_unix_detected_as_os(self):
        df = pd.DataFrame(
            {
                'RecordID': [1, 2, 3],
                'Product': ['linux', 'unix', 'WIN2019'],
                'Compute_Class': ['m5.large', 'm5.large', 'm5.large'],
                'SpendUSD': [10.0, 20.0, 30.0],
            }
        )
        lr = analyze_load(df, [])
        self.assertEqual(lr.binding.instance, 'Compute_Class')
        self.assertEqual(lr.binding.os, 'Product')
        self.assertEqual(lr.binding.actual_cost, 'SpendUSD')
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1'))
        self.assertEqual(out['Pricing OS'].iloc[0], 'Linux')
        self.assertEqual(out['Pricing OS'].iloc[1], 'Linux')
        self.assertEqual(out['Pricing OS'].iloc[2], 'Windows')

    def test_cost_inferred_when_header_not_standard(self):
        df = pd.DataFrame({'vm': ['m5.large', 'm5.large'], 'Product': ['linux', 'linux'], 'MyMoneyCol': [100.0, 200.0]})
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.actual_cost, 'MyMoneyCol')
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1'))
        self.assertNotEqual(out['Actual Cost ($)'].iloc[0], 'N/A')


class TestOsValueScanAcrossColumns(unittest.TestCase):

    def test_os_picked_from_product_when_bound_os_column_empty(self):
        df = pd.DataFrame(
            {
                'vm': ['m5.large'],
                'System': [pd.NA],
                'Product': ['Amazon Linux 2'],
                'Spend': [100.0],
            }
        )
        b = ColumnBinding(instance='vm', os='System', actual_cost='Spend')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both', cpu_filter='both'))
        self.assertEqual(out['Pricing OS'].iloc[0], 'Linux')
        self.assertNotEqual(out['Alt1 Price ($/hr)'].iloc[0], 'N/A')


class TestNonPositiveActualCost(unittest.TestCase):

    def test_zero_spend_na_actual_list_prices_unaffected(self):
        df = pd.DataFrame({'Instance': ['m5.large'], 'Product': ['linux'], 'SpendUSD': [0.0]})
        lr = analyze_load(df, [])
        b = ColumnBinding(instance=lr.binding.instance, os=lr.binding.os, actual_cost=lr.binding.actual_cost)
        out = apply_na_fill(process(lr.df, b, region='eu-west-1'))
        self.assertTrue(pd.isna(out['Actual Cost ($)'].iloc[0]))
        self.assertNotEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')
        self.assertNotEqual(out['Alt1 Price ($/hr)'].iloc[0], 'N/A')


class TestToFloatParsing(unittest.TestCase):

    def test_currency_commas_whitespace(self):
        self.assertEqual(_to_float('$1,234.56'), 1234.56)
        self.assertEqual(_to_float('€ 99.5'), 99.5)
        self.assertEqual(_to_float(' 42 '), 42.0)

    def test_decimal_type(self):
        self.assertEqual(_to_float(Decimal('88.25')), 88.25)


class TestFinalizeBindingOptionalOs(unittest.TestCase):

    def test_finalize_none_os(self):
        df = pd.DataFrame({'i': ['m5.large'], 'c': [1.0]})
        lr = analyze_load(df, [])
        lr2 = finalize_binding(lr, 'i', None, 'c')
        self.assertIsNone(lr2.binding.os)


if __name__ == '__main__':
    unittest.main()
