from __future__ import annotations
import unittest
import pandas as pd
from instance_api import canonicalize_instance_api_name


def _infer_instance_col(frame: pd.DataFrame, mapped_instance: str | None) -> str | None:
    if mapped_instance and mapped_instance in frame.columns:
        return mapped_instance
    for c in frame.columns:
        s = str(c).strip().lower()
        if any((k in s for k in ('api', 'instance', 'vm', 'type'))):
            return c
    return None


def _service_filter(frame: pd.DataFrame, vf_svc: str, mapped_instance: str | None) -> pd.DataFrame:
    view = frame.copy()
    inst_col = _infer_instance_col(view, mapped_instance)
    if inst_col is None:
        return view
    inst_vals = view[inst_col].map(lambda x: canonicalize_instance_api_name(x) or '')
    if vf_svc == 'ec2':
        return view[~inst_vals.str.startswith('db.')]
    if vf_svc == 'rds':
        return view[inst_vals.str.startswith('db.')]
    return view


class TestResultsFilterLogic(unittest.TestCase):
    def test_ec2_rds_filter_uses_inferred_instance_column(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large', 'db.r5.large', 'm6i.large', 'db.m5.large'],
                'Owner': ['a', 'b', 'c', 'd'],
            }
        )
        out_ec2 = _service_filter(df, 'ec2', mapped_instance=None)
        out_rds = _service_filter(df, 'rds', mapped_instance=None)
        self.assertEqual(out_ec2['API Name'].tolist(), ['m5.large', 'm6i.large'])
        self.assertEqual(out_rds['API Name'].tolist(), ['db.r5.large', 'db.m5.large'])

    def test_filter_handles_whitespace_case(self):
        df = pd.DataFrame({'Instance Type': ['  DB.R5.LARGE ', ' c5.xlarge ']})
        out_ec2 = _service_filter(df, 'ec2', mapped_instance='Instance Type')
        out_rds = _service_filter(df, 'rds', mapped_instance='Instance Type')
        self.assertEqual(out_ec2['Instance Type'].tolist(), [' c5.xlarge '])
        self.assertEqual(out_rds['Instance Type'].tolist(), ['  DB.R5.LARGE '])


if __name__ == '__main__':
    unittest.main()
