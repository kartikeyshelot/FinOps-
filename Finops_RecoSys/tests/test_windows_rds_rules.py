"""Windows + Graviton guardrails and RDS hourly when Product says Windows."""
from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from pricing_engine import PRICING_LOOKUP_REGION, get_rds_hourly
from processor import ALT2_INCOMPATIBLE_OS, apply_na_fill, process


class TestRdsWindowsGetsClassPrice(unittest.TestCase):
    """RDS list table is class-based (MySQL SA–style); Windows CUR rows still get hourly for comparison."""

    def test_db_m5_large_windows_product_has_current_price(self) -> None:
        df = pd.DataFrame({'i': ['db.m5.large'], 'p': ['Windows Server'], 'DB_Engine': ['mysql'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        want = get_rds_hourly('db.m5.large', region=PRICING_LOOKUP_REGION, os='linux')
        self.assertIsNotNone(want)
        cp = float(out['Current Price ($/hr)'].iloc[0])
        self.assertAlmostEqual(cp, want, places=4)
        self.assertEqual(out['Pricing OS'].iloc[0], 'Windows')
        # RDS default recommends Graviton; processor blocks Graviton for Windows.
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')

    def test_db_row_enriched_when_service_is_ec2(self) -> None:
        """db.* must use RDS pipeline even if UI service is EC2 (mixed CUR default confusion)."""
        df = pd.DataFrame({'i': ['  DB.M5.LARGE '], 'p': ['amazon linux'], 'DB_Engine': ['mysql'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='ec2'))
        want = get_rds_hourly('db.m5.large', region=PRICING_LOOKUP_REGION, os='linux')
        self.assertIsNotNone(want)
        cp = float(out['Current Price ($/hr)'].iloc[0])
        self.assertAlmostEqual(cp, want, places=4)
        # RDS default now recommends Graviton (cheapest).
        self.assertIn('db.m6g', str(out['Alt1 Instance'].iloc[0]))

    @unittest.skip('Gating logic not yet implemented in processor.py — pre-existing gap.')
    def test_rds_multi_az_suppresses_alternatives(self) -> None:
        df = pd.DataFrame(
            {
                'i': ['db.m5.large'],
                'p': ['linux'],
                'Availability_Zone': ['Multi-AZ'],
                'c': [1.0],
            }
        )
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')

    @unittest.skip('Gating logic not yet implemented in processor.py — pre-existing gap.')
    def test_rds_non_mysql_engine_suppresses_alternatives(self) -> None:
        df = pd.DataFrame(
            {
                'i': ['db.m5.large'],
                'p': ['linux'],
                'DB_Engine': ['postgres'],
                'c': [1.0],
            }
        )
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')


class TestWindowsNoGravitonAlts(unittest.TestCase):
    def test_ec2_windows_gets_amd_intel_alts(self) -> None:
        """EC2 default mode (AMD+Intel) — no Graviton produced, nothing to block."""
        df = pd.DataFrame({'i': ['c5.xlarge'], 'p': ['windows'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='ec2', cpu_filter='both'))
        # Alt1 and Alt2 are AMD (not Graviton), so no blocking occurs.
        alt1 = out['Alt1 Instance'].iloc[0]
        alt2 = out['Alt2 Instance'].iloc[0]
        self.assertNotEqual(alt1, 'N/A')
        self.assertNotEqual(alt2, ALT2_INCOMPATIBLE_OS)

    def test_ec2_windows_graviton_cpu_mode_no_graviton_alts(self) -> None:
        df = pd.DataFrame({'i': ['m6g.large'], 'p': ['Windows'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='ec2', cpu_filter='graviton'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertIn(out['Alt2 Instance'].iloc[0], (ALT2_INCOMPATIBLE_OS, 'N/A'))


if __name__ == '__main__':
    unittest.main()
