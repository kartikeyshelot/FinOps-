"""Adversarial / abuse-style checks — expect no crash; some ops may raise ValueError by design."""
from __future__ import annotations
import io
import unittest
import pandas as pd
from data_loader import analyze_load, finalize_binding, load_file
from processor import apply_na_fill, process
from sheet_merger import merge_primary_with_secondary
from data_loader import ColumnBinding


class TestAdversarialProcessor(unittest.TestCase):

    def test_negative_and_zero_cost_no_crash(self):
        df = pd.DataFrame({'i': ['m5.large', 'm5.large'], 'o': ['linux', 'linux'], 'c': [-10.0, 0.0]})
        b = ColumnBinding(instance='i', os='o', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1'))
        self.assertEqual(len(out), 2)
        self.assertTrue(all(x in ('N/A', 'No Savings', None) or isinstance(x, (int, float, str)) for x in out['Alt1 Price ($/hr)']))

    def test_formula_injection_string_cell_no_exec(self):
        df = pd.DataFrame({'i': ["=1+1 cmd|'/c calc'"], 'o': ['linux'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='o', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1'))
        self.assertEqual(len(out), 1)

    def test_very_long_cell_string(self):
        s = 'm5.large' + ' ' * 5000
        df = pd.DataFrame({'i': [s], 'o': ['linux'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='o', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1'))
        self.assertEqual(len(out), 1)

    def test_all_invalid_instances(self):
        df = pd.DataFrame({'i': ['nope', 'bad_name'], 'o': ['linux', 'linux'], 'c': [1.0, 2.0]})
        b = ColumnBinding(instance='i', os='o', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1'))
        self.assertEqual(out['Alt1 Instance'].tolist(), ['N/A', 'N/A'])

    def test_reserved_column_rejected(self):
        df = pd.DataFrame({'i': ['m5.large'], 'Alt1 Instance': ['x'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='Alt1 Instance', actual_cost='c')
        with self.assertRaises(ValueError):
            process(df, b, region='eu-west-1')

    def test_10k_rows_completes(self):
        df = pd.DataFrame({'i': ['m5.large'] * 8000, 'o': ['linux'] * 8000, 'c': [1.0] * 8000})
        b = ColumnBinding(instance='i', os='o', actual_cost='c')
        out = process(df, b, region='eu-west-1', service='both')
        self.assertEqual(len(out), 8000)


class TestAdversarialLoader(unittest.TestCase):

    def test_empty_csv_raises(self):
        buf = io.BytesIO(b'a,b\n')
        with self.assertRaises(ValueError):
            load_file(buf, 'x.csv')

    def test_analyze_all_null_instance_col(self):
        df = pd.DataFrame({'x': [None, None], 'cost': [1, 2]})
        lr = analyze_load(df, [])
        self.assertTrue(lr.needs_instance_pick or lr.binding is None)


class TestAdversarialMerge(unittest.TestCase):

    def test_merge_empty_secondary_values(self):
        d1 = pd.DataFrame({'k': ['1'], 'i': ['m5.large'], 'c': [10.0]})
        d2 = pd.DataFrame({'k': ['2'], 'extra': ['a']})
        out, w = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertTrue((out['extra'].isna()).all() or out['extra'].tolist() == [pd.NA])

    def test_merge_duplicate_primary_columns_rejected(self):
        d1 = pd.DataFrame([[1, 2, 3]], columns=['k', 'a', 'a'])
        d2 = pd.DataFrame({'k': [1], 'b': [9]})
        with self.assertRaises(ValueError) as ctx:
            merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertIn('duplicate', str(ctx.exception).lower())


if __name__ == '__main__':
    unittest.main()
