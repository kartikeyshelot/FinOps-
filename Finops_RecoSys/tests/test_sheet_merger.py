from __future__ import annotations
import unittest
import pandas as pd
from sheet_merger import merge_primary_with_secondary, suggest_key_pairs

_MERGE_FLAGS = [
    'FinOps_Merge_DuplicateSecondaryRows',
    'FinOps_Merge_SecondaryRowGroupIndex',
    'FinOps_Merge_DuplicatePrimaryKey',
]


class TestCoreIdMergeValidation(unittest.TestCase):
    """Strict core id [a-z]+[0-9]+ — no partial/fuzzy false positives."""

    def test_exact_core_match(self) -> None:
        d1 = pd.DataFrame({'k': ['ab101'], 'A': [1]})
        d2 = pd.DataFrame({'k': ['ab101'], 'Spend': [99.0]})
        out, w = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertEqual(float(out.iloc[0]['Spend']), 99.0)
        self.assertFalse(any('no secondary match' in x.lower() for x in w))

    def test_embedded_core_in_secondary(self) -> None:
        d1 = pd.DataFrame({'k': ['ab101'], 'A': [1]})
        d2 = pd.DataFrame({'k': ['aasss_ab101'], 'Spend': [42.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertEqual(float(out.iloc[0]['Spend']), 42.0)

    def test_embedded_primary_embedded_secondary(self) -> None:
        d1 = pd.DataFrame({'k': ['prefix_ab101_suffix'], 'A': [1]})
        d2 = pd.DataFrame({'k': ['asdsdas_ab101'], 'Spend': [7.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertEqual(float(out.iloc[0]['Spend']), 7.0)

    def test_no_match_shorter_partial_core(self) -> None:
        d1 = pd.DataFrame({'k': ['ab101'], 'A': [1]})
        d2 = pd.DataFrame({'k': ['ab10'], 'Spend': [1.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertTrue(pd.isna(out.iloc[0]['Spend']))

    def test_no_match_different_letter_prefix(self) -> None:
        d1 = pd.DataFrame({'k': ['ab101'], 'A': [1]})
        d2 = pd.DataFrame({'k': ['xy101'], 'Spend': [1.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertTrue(pd.isna(out.iloc[0]['Spend']))


class TestSuggestKeyPairs(unittest.TestCase):
    def test_same_name_resource_id_first(self):
        c1 = ['resource_id', 'Instance', 'OS']
        c2 = ['resource_id', 'Cost', 'Spend']
        pairs = suggest_key_pairs(c1, c2)
        self.assertTrue(any((p == ('resource_id', 'resource_id') for p in pairs)))

    def test_cross_name_instance_id(self):
        c1 = ['instance_id', 'vm', 'linux']
        c2 = ['resource_id', 'amount']
        pairs = suggest_key_pairs(c1, c2)
        self.assertTrue(any((p[0] == 'instance_id' and p[1] == 'resource_id' for p in pairs)))


class TestMergePrimaryWithSecondary(unittest.TestCase):
    def test_duplicate_column_names_in_either_sheet_rejected(self):
        d1 = pd.DataFrame([[1, 1, 2]], columns=['id', 'x', 'x'])
        d2 = pd.DataFrame({'id': [1]})
        with self.assertRaises(ValueError):
            merge_primary_with_secondary(d1, d2, 'id', 'id')
        d1ok = pd.DataFrame({'id': [1], 'a': [1]})
        d2bad = pd.DataFrame([[1, 2, 3]], columns=['id', 'y', 'y'])
        with self.assertRaises(ValueError):
            merge_primary_with_secondary(d1ok, d2bad, 'id', 'id')

    def test_missing_cost_in_d1_filled_from_d2(self):
        d1 = pd.DataFrame({'resource_id': ['ab101', 'ab102'], 'Instance': ['m5.large', 'c5.xlarge'], 'OS': ['linux', 'linux'], 'Cost': [pd.NA, pd.NA]})
        d2 = pd.DataFrame({'resource_id': ['ab101', 'ab102'], 'Cost': [100.0, 200.0]})
        out, w = merge_primary_with_secondary(d1, d2, 'resource_id', 'resource_id')
        self.assertEqual(list(out.columns), ['resource_id', 'Instance', 'OS', 'Cost'] + _MERGE_FLAGS)
        self.assertEqual(list(out['Cost']), [100.0, 200.0])

    def test_d1_column_order_preserved_d2_only_appended(self):
        d1 = pd.DataFrame({'id': ['ab101'], 'A': ['x'], 'B': ['y']})
        d2 = pd.DataFrame({'id': ['ab101'], 'B': ['ignored'], 'Z': ['new']})
        out, _ = merge_primary_with_secondary(d1, d2, 'id', 'id')
        self.assertEqual(list(out.columns), ['id', 'A', 'B', 'Z'] + _MERGE_FLAGS)
        self.assertEqual(out.iloc[0]['B'], 'y')
        self.assertEqual(out.iloc[0]['Z'], 'new')

    def test_prefer_d1_when_non_empty(self):
        d1 = pd.DataFrame({'id': ['ab101'], 'Cost': [50.0]})
        d2 = pd.DataFrame({'id': ['ab101'], 'Cost': [999.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'id', 'id')
        self.assertEqual(float(out.iloc[0]['Cost']), 50.0)

    def test_partial_key_overlap(self):
        d1 = pd.DataFrame({'resource_id': ['ab101', 'ab102', 'ab103'], 'Instance': ['m5.large', 'm5.large', 'm5.large']})
        d2 = pd.DataFrame({'resource_id': ['ab101', 'ab103'], 'Spend': [10.0, 30.0]})
        out, w = merge_primary_with_secondary(d1, d2, 'resource_id', 'resource_id')
        self.assertEqual(list(out.columns), ['resource_id', 'Instance', 'Spend'] + _MERGE_FLAGS)
        self.assertTrue(any('no secondary match on core_id' in x for x in w))
        self.assertEqual(float(out.loc[out['resource_id'] == 'ab101', 'Spend'].iloc[0]), 10.0)
        self.assertTrue(pd.isna(out.loc[out['resource_id'] == 'ab102', 'Spend'].iloc[0]))

    def test_duplicate_d2_keys_uses_first_row_only(self):
        d1 = pd.DataFrame({'id': ['ab101'], 'Instance': ['m5.large']})
        d2 = pd.DataFrame({'id': ['ab101', 'ab101'], 'Spend': [1.0, 2.0]})
        out, w = merge_primary_with_secondary(d1, d2, 'id', 'id')
        self.assertEqual(len(out), 1)
        self.assertEqual(float(out.iloc[0]['Spend']), 1.0)
        self.assertEqual(out.iloc[0]['FinOps_Merge_DuplicateSecondaryRows'], 'Yes')
        self.assertEqual(out.iloc[0]['FinOps_Merge_SecondaryRowGroupIndex'], '1/2')
        self.assertTrue(any('first row' in x.lower() for x in w))

    def test_missing_instance_in_d2_does_not_drop_d1_columns(self):
        d1 = pd.DataFrame({'arn': ['asset_ab101'], 'Instance': ['m5.large'], 'OS': ['linux']})
        d2 = pd.DataFrame({'arn': ['x_asset_ab101_y'], 'Spend': [42.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'arn', 'arn')
        self.assertEqual(list(out.columns), ['arn', 'Instance', 'OS', 'Spend'] + _MERGE_FLAGS)
        self.assertEqual(out.iloc[0]['Instance'], 'm5.large')
        self.assertEqual(float(out.iloc[0]['Spend']), 42.0)

    def test_fuzzy_key_embedded_short_code_in_long_secondary(self):
        d1 = pd.DataFrame({'app_code': ['a1105'], 'Name': ['App A']})
        d2 = pd.DataFrame({'application_type': ['asdsd_asa_a1105'], 'Spend': [12.5]})
        out, w = merge_primary_with_secondary(d1, d2, 'app_code', 'application_type')
        self.assertEqual(float(out.iloc[0]['Spend']), 12.5)
        self.assertFalse(any('no secondary match' in x.lower() for x in w))

    def test_case_insensitive_exact_key(self):
        d1 = pd.DataFrame({'k': ['A1105'], 'x': [1]})
        d2 = pd.DataFrame({'k': ['a1105'], 'y': ['ok']})
        out, w = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertEqual(out.iloc[0]['y'], 'ok')
        self.assertFalse(any('fuzzy' in x.lower() for x in w))

    def test_non_calc_columns_never_become_none(self):
        d1 = pd.DataFrame(
            {
                'Application Code': ['ab101', 'ab102'],
                'Owner Name': ['Alice', 'Bob'],   # unrelated to FinOps calculations
                'API Name': ['m5.large', 'm6i.large'],
            }
        )
        d2 = pd.DataFrame(
            {
                'Instance Id': ['x_ab101', 'x_ab102'],
                'Instance details': ['foo', 'bar'],
            }
        )
        out, _ = merge_primary_with_secondary(d1, d2, 'Application Code', 'Instance Id')
        self.assertEqual(out['Owner Name'].tolist(), ['Alice', 'Bob'])
        self.assertFalse(out['Owner Name'].map(lambda v: v is None).any())

    def test_no_pandas_suffix_columns_added(self):
        d1 = pd.DataFrame({'k': ['ab101'], 'Spend': [pd.NA], 'Cost': [10.0]})
        d2 = pd.DataFrame({'k': ['ab101'], 'Spend': [22.0], 'Cost': [99.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertTrue(all(not str(c).endswith('_x') and not str(c).endswith('_y') for c in out.columns))
        # Fill only missing in D1.
        self.assertEqual(float(out.iloc[0]['Spend']), 22.0)
        # Existing D1 value must not be overwritten.
        self.assertEqual(float(out.iloc[0]['Cost']), 10.0)

    def test_primary_columns_values_unchanged_when_non_empty(self):
        d1 = pd.DataFrame(
            {
                'k': ['ab101', 'ab102'],
                'A': ['x', 'y'],
                'B': [1.0, 2.0],
            }
        )
        d2 = pd.DataFrame(
            {
                'k': ['ab101', 'ab102'],
                'A': ['override-x', 'override-y'],
                'B': [10.0, 20.0],
                'NewCol': ['n1', 'n2'],
            }
        )
        out, _ = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertEqual(out['A'].tolist(), ['x', 'y'])
        self.assertEqual(out['B'].tolist(), [1.0, 2.0])
        self.assertEqual(out['NewCol'].tolist(), ['n1', 'n2'])


if __name__ == '__main__':
    unittest.main()
