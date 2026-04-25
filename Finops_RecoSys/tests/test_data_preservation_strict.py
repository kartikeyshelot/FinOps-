from __future__ import annotations
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

from data_loader import ColumnBinding
from processor import INSERT_COLS, apply_na_fill, process


class TestStrictOriginalDataPreservation(unittest.TestCase):

    def test_original_columns_values_and_order_remain_exact(self) -> None:
        df = pd.DataFrame(
            {
                'RecordID': ['1', '2', '3'],
                'Instance': ['   m5.large   ', 'unknown.type', 'm5_large'],
                'Unused_Unknown_Field': ['  Keep  ', '', '=CMD("calc")'],
                'System': ['linux', None, 'windows'],
                'Actual': [0.096, -1.0, 0.0],
            },
            dtype=object,
        )
        original_df = df.copy()
        b = ColumnBinding(instance='Instance', os='System', actual_cost='Actual')

        out = apply_na_fill(process(df, b, region='eu-west-1', service='both', cpu_filter='both'))

        cols = list(original_df.columns)
        ins_idx = cols.index('Instance')
        reconstructed_original = pd.concat(
            [
                out.iloc[:, : ins_idx + 1].copy(),
                out.iloc[:, ins_idx + 1 + len(INSERT_COLS) :].copy(),
            ],
            axis=1,
        )
        reconstructed_original.columns = cols

        assert_frame_equal(
            reconstructed_original,
            original_df,
            check_dtype=True,
            check_exact=True,
            check_names=True,
        )

        self.assertEqual(
            list(out.columns[ins_idx + 1 : ins_idx + 1 + len(INSERT_COLS)]),
            INSERT_COLS,
        )


if __name__ == '__main__':
    unittest.main()
