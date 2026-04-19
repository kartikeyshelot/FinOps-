from __future__ import annotations
import unittest
from pathlib import Path
from data_loader import ColumnBinding, analyze_load, dataframe_from_bytes
from processor import apply_na_fill, process
from sheet_merger import merge_primary_with_secondary, suggest_key_pairs

_ROOT = Path(__file__).resolve().parent.parent
_S1 = _ROOT / 'test_data' / 'sheet1.csv'
_S2 = _ROOT / 'test_data' / 'sheet2.csv'


@unittest.skipUnless(_S1.is_file() and _S2.is_file(), 'test_data/sheet1.csv and sheet2.csv required')
class TestUserSheet1Sheet2Merge(unittest.TestCase):
    def test_merge_shape_columns_and_excludes_d2_only_row(self):
        d1 = dataframe_from_bytes(_S1.read_bytes(), 'sheet1.csv')
        d2 = dataframe_from_bytes(_S2.read_bytes(), 'sheet2.csv')
        self.assertIn(('RecordID', 'RecordID'), suggest_key_pairs(list(d1.columns), list(d2.columns)))
        merged, w = merge_primary_with_secondary(d1, d2, 'RecordID', 'RecordID')
        self.assertEqual(len(merged), 10)
        self.assertEqual(list(merged.columns)[:9], list(d1.columns))
        _flags = [
            'FinOps_Merge_DuplicateSecondaryRows',
            'FinOps_Merge_SecondaryRowGroupIndex',
            'FinOps_Merge_DuplicatePrimaryKey',
        ]
        self.assertEqual(
            list(merged.columns),
            list(d1.columns) + ['Billing_Amount', 'Region', 'BackupCost', 'ExtraC', 'Notes'] + _flags,
        )
        self.assertNotIn('311', set(merged['RecordID'].astype(str)))
        self.assertEqual(str(merged.loc[merged['RecordID'].astype(str) == '301', 'Billing_Amount'].iloc[0]), '125')

    def test_end_to_end_enrichment(self):
        d1 = dataframe_from_bytes(_S1.read_bytes(), 'sheet1.csv')
        d2 = dataframe_from_bytes(_S2.read_bytes(), 'sheet2.csv')
        merged, _ = merge_primary_with_secondary(d1, d2, 'RecordID', 'RecordID')
        lr = analyze_load(merged, [])
        b = ColumnBinding(instance='VM_Size', os='System', actual_cost='Billing_Amount')
        out = apply_na_fill(process(lr.df, b, region='eu-west-1', service='both', cpu_filter='both'))
        self.assertEqual(len(out), 10)
        self.assertIn('Alt1 Instance', out.columns)
        row304 = out[out['RecordID'].astype(str) == '304'].iloc[0]
        self.assertEqual(row304['VM_Size'], 'db.c5.2xlarge')
        # Strict consistency: if local RDS alt hourly SKU is unavailable, alt instance is suppressed.
        self.assertEqual(row304['Alt1 Instance'], 'N/A')
        self.assertEqual(row304['Alt1 Savings %'], 'N/A')


if __name__ == '__main__':
    unittest.main()
