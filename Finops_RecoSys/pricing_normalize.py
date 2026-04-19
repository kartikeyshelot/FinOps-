"""Strict normalization for pricing lookups (instance shape, OS label + engine key)."""
from __future__ import annotations
import math
import pandas as pd

LINUX_FALLBACK_LABEL: str = 'Linux'


def normalize_instance_string(val: object) -> str:
    """Strip and lowercase raw instance / DB class text before validation (e.g. '   M5.LARGE   ' → 'm5.large')."""
    if val is None:
        return ''
    try:
        if pd.isna(val):
            return ''
    except (TypeError, ValueError):
        pass
    if isinstance(val, float) and (not math.isfinite(val)):
        return ''
    return str(val).strip().lower()


def normalize_pricing_os_label(val: object) -> str:
    """
    Values for **Pricing OS** column: exactly **Linux** or **Windows** only.
    Missing / unrecognized → **Linux** (priced with Linux on-demand SKUs in eu-west-1).
    """
    if val is None:
        return LINUX_FALLBACK_LABEL
    if isinstance(val, float) and math.isnan(val):
        return LINUX_FALLBACK_LABEL
    s = str(val).strip()
    if not s or s.lower() in ('nan', 'n/a', 'none'):
        return LINUX_FALLBACK_LABEL
    v = s.lower()
    if 'win' in v:
        return 'Windows'
    if 'sql server' in v or 'microsoft sql' in v:
        return 'Windows'
    if any(
        x in v
        for x in (
            'linux',
            'unix',
            'ubuntu',
            'rhel',
            'amazon',
            'debian',
            'suse',
            'sles',
            'centos',
            'fedora',
            'oracle linux',
        )
    ):
        return 'Linux'
    return LINUX_FALLBACK_LABEL


def normalize_os(val: object) -> str:
    """Alias for normalize_pricing_os_label (spec / backward compatibility)."""
    return normalize_pricing_os_label(val)


def normalize_os_engine_key(val: object) -> str:
    """Key for get_price / get_rds_hourly: linux | windows only."""
    return 'windows' if normalize_pricing_os_label(val) == 'Windows' else 'linux'
