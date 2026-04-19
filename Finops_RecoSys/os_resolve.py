from __future__ import annotations

"""Value-based OS detection labels and pricing-engine OS keys (no column-name coupling)."""

import re
from typing import Literal
import pandas as pd
from pricing_normalize import LINUX_FALLBACK_LABEL, normalize_os_engine_key, normalize_pricing_os_label

PRICING_OS_METADATA_NOTE: str = (
    'Pricing OS is exactly Linux or Windows. Missing or unrecognized values are shown as Linux '
    'and priced with Linux on-demand rates (eu-west-1 SKU table).'
)

CellOsKind = Literal['linux', 'windows'] | None

# Linux-type tokens (substring / phrase); longer phrases first; include unix for Product/SKU-style cols
_LINUX_PHRASES: tuple[str, ...] = (
    'amazon linux',
    'linux/unix',
    'linux unix',
    'rhel',
    'ubuntu',
    'debian',
    'centos',
    'fedora',
    'oracle linux',
    'unix',
    'linux',
)
# Windows: avoid matching unrelated words containing "win"
_WIN_RE = re.compile('\\b(?:windows|win)\\b', re.I)
_WIN_PREFIX_RE = re.compile('^win\\d{2,4}', re.I)
_WIN_DIGITS_RE = re.compile('^win\\d+$', re.I)
_MS_SQL_RE = re.compile('sql\\s*server|microsoft\\s*sql', re.I)


def _cell_str(cell: object) -> str:
    if cell is None:
        return ''
    try:
        if pd.isna(cell):
            return ''
    except (TypeError, ValueError):
        pass
    s = str(cell).strip().lower()
    if not s or s in ('nan', 'none', 'n/a'):
        return ''
    return s


def cell_matches_valid_os_pattern(cell: object) -> bool:
    """True if cell value matches allowed Linux- or Windows-type patterns (for column detection)."""
    s = _cell_str(cell)
    if not s:
        return False
    if _MS_SQL_RE.search(s):
        return True
    if _WIN_RE.search(s) or _WIN_PREFIX_RE.match(s) or _WIN_DIGITS_RE.match(s):
        return True
    for ph in _LINUX_PHRASES:
        if ph in s:
            return True
    return False


def classify_os_kind(cell: object) -> CellOsKind:
    """Classify cell as linux-family or windows-family for detection; invalid → None."""
    s = _cell_str(cell)
    if not s:
        return None
    if _MS_SQL_RE.search(s):
        return 'windows'
    if _WIN_RE.search(s) or _WIN_PREFIX_RE.match(s) or _WIN_DIGITS_RE.match(s):
        return 'windows'
    for ph in _LINUX_PHRASES:
        if ph in s:
            return 'linux'
    return None


def normalize_pricing_os_display(cell: object) -> str:
    """User-facing bucket: Linux | Windows only (missing → Linux)."""
    return normalize_pricing_os_label(cell)


def engine_os_for_pricing(cell: object) -> str:
    """OS key for get_price / get_rds_hourly: linux or windows only (strict normalization)."""
    return normalize_os_engine_key(cell)
