from __future__ import annotations
import io
import math
import os
import re
import statistics
import logging
from dataclasses import dataclass, field
from typing import BinaryIO
import pandas as pd
from instance_api import canonicalize_instance_api_name
from os_resolve import cell_matches_valid_os_pattern
logger = logging.getLogger(__name__)
_VALUE_SAMPLE_CAP = 2000
_MIN_AUTO_CONF = 0.48
_TIE_BAND = 0.09

# Default suits typical spreadsheets; wide CUR-style CSVs (10k–100k+ rows) often need FINOPS_MAX_UPLOAD_BYTES.
_DEFAULT_MAX_UPLOAD_BYTES = 25 * 1024 * 1024


def max_upload_bytes() -> int:
    raw = (os.environ.get('FINOPS_MAX_UPLOAD_BYTES') or '').strip()
    if not raw:
        return _DEFAULT_MAX_UPLOAD_BYTES
    try:
        n = int(raw, 10)
        return max(1024 * 1024, min(n, 512 * 1024 * 1024))
    except ValueError:
        return _DEFAULT_MAX_UPLOAD_BYTES


def require_unique_column_names(columns) -> None:
    """Reject duplicate headers — pandas column access is ambiguous; enrichment requires stable positions."""
    cols = list(columns)
    if len(cols) == len(set(cols)):
        return
    raise ValueError(
        'Duplicate column names detected. Each column must have a unique name. '
        'Rename the duplicates in your source file and re-upload.'
    )


def _reject_oversized(raw: bytes, filename: str) -> None:
    limit = max_upload_bytes()
    if len(raw) > limit:
        raise ValueError(
            f"File exceeds maximum size ({limit // (1024 * 1024)} MB). "
            'Split the dataset or increase FINOPS_MAX_UPLOAD_BYTES for trusted deployments only.'
        )

def _norm_header(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub('[_\\-]+', ' ', s)
    s = re.sub('\\s+', ' ', s)
    return s
INSTANCE_HINTS: frozenset[str] = frozenset({'instance type', 'instancetype', 'instance', 'instance type id', 'ec2 type', 'ec2type', 'ec2 type id', 'instance size', 'resource type', 'vm type', 'vm size', 'vmsize', 'ec2 instance type', 'instance class', 'db instance class', 'database class', 'compute class', 'computeclass', 'instance type name', 'api name', 'ec2 api name', 'instance api name'})
INSTANCE_HEADER_KEYWORDS: frozenset[str] = frozenset({'api', 'instance', 'vm', 'type'})
COST_HINTS: frozenset[str] = frozenset({
    'cost', 'monthly cost', 'total cost', 'charge', 'charges', 'cost ($)', 'cost(usd)', 'cost (usd)', 'cost_usd',
    'billed cost', 'blended cost', 'unblended cost', 'amortized cost', 'spend', 'amount', 'total amount',
    'billed amount', 'usage cost', 'line item cost', 'cost usd', 'usd cost', 'monthly spend',
    'price', 'pricing', 'fee', 'fees', 'usd', 'payment', 'net amount', 'gross', 'pre tax', 'pretax',
    'cur', 'currency', 'total usd', 'amount usd', 'usage amount', 'resource cost',
})

def _header_matches(h: str, hints: frozenset[str]) -> bool:
    n = _norm_header(h)
    if n in hints:
        return True
    for hint in hints:
        if len(hint) >= 4 and hint in n:
            return True
    return False


def _instance_header_keyword_hit(h: str) -> bool:
    """
    Airbus compatibility rule:
    header keywords api / instance / vm / type should boost instance-column detection.
    """
    n = _norm_header(h)
    if not n:
        return False
    tokens = [t for t in n.split(' ') if t]
    for token in tokens:
        if token in INSTANCE_HEADER_KEYWORDS:
            return True
    return False
def _cell_looks_like_instance_type(cell: object) -> bool:
    if cell is None:
        return False
    try:
        if pd.isna(cell):
            return False
    except (TypeError, ValueError):
        pass
    return canonicalize_instance_api_name(cell) is not None

def _value_match_ratio(df: pd.DataFrame, col: str, predicate) -> float:
    n = min(len(df), _VALUE_SAMPLE_CAP)
    if n == 0:
        return 0.0
    ser = df[col].iloc[:n]
    mask = ser.notna()
    sstr = ser.astype(str).str.strip()
    mask &= ~sstr.str.lower().isin(('nan', 'none', 'n/a', ''))
    if not mask.any():
        return 0.0
    sub = ser[mask]
    hits = sum((1 for v in sub if predicate(v)))
    return hits / len(sub)

def _score_instance_columns(df: pd.DataFrame) -> list[tuple[str, float]]:
    scored: list[tuple[str, float]] = []
    for col in df.columns:
        hdr = _header_matches(str(col), INSTANCE_HINTS) or _instance_header_keyword_hit(str(col))
        vr = _value_match_ratio(df, col, _cell_looks_like_instance_type)
        if hdr and vr >= 0.25:
            sc = 0.6 + 0.4 * min(1.0, vr / 0.95)
        elif hdr:
            # Header-only hits are not enough to auto-accept without value-pattern support.
            sc = 0.42 + 0.2 * min(1.0, vr * 3.0) if vr > 0 else 0.30
        else:
            sc = vr
        scored.append((col, min(1.0, sc)))
    scored.sort(key=lambda x: (-x[1], str(x[0])))
    return scored

def _score_os_columns(df: pd.DataFrame) -> list[tuple[str, float]]:
    """Score columns by share of cells matching allowed OS value patterns only (no header hints)."""
    scored: list[tuple[str, float]] = []
    for col in df.columns:
        vr = _value_match_ratio(df, col, cell_matches_valid_os_pattern)
        hn = _norm_header(str(col)).replace(' ', '')
        if vr >= 0.08 and any((sub in hn for sub in _OS_HEADER_HINT_SUBSTR)):
            vr = min(1.0, vr + 0.14)
        scored.append((col, min(1.0, vr)))
    scored.sort(key=lambda x: (-x[1], str(x[0])))
    return scored


def _rank_cost_columns(candidates: list[str]) -> list[str]:
    """Prefer total / primary cost columns when multiple are detected (e.g. Total_Cost_USD over Backup_Cost)."""
    if len(candidates) <= 1:
        return list(candidates)

    def rank_key(name: object) -> tuple[int, str]:
        n = _norm_header(str(name)).replace(' ', '')
        if n == 'totalcostusd' or n == 'total_cost_usd':
            return (0, str(name))
        if 'total' in n and 'cost' in n and 'usd' in n:
            return (1, str(name))
        if 'total' in n and 'cost' in n:
            return (2, str(name))
        if n.startswith('total'):
            return (3, str(name))
        return (4, str(name))

    return sorted(candidates, key=rank_key)


_MIN_OS_AUTO_CONF = 0.22
_OS_TIE_BAND = 0.09
_OS_HEADER_HINT_SUBSTR: tuple[str, ...] = ('product', 'platform', 'engine', 'operatingsystem', 'os type')

def _resolve_os_column(df: pd.DataFrame, scored: list[tuple[str, float]]) -> tuple[str | None, bool, list[str]]:
    all_cols = list(df.columns)
    if not scored:
        return (None, False, [])
    (best_c, best_s) = scored[0]
    second_s = scored[1][1] if len(scored) > 1 else -1.0
    if best_s < _MIN_OS_AUTO_CONF:
        return (None, False, [])
    if second_s >= best_s - _OS_TIE_BAND and second_s >= 0.3:
        tied = [c for (c, s) in scored[:12] if s >= second_s - 0.005]
        return (None, True, tied or [best_c])
    return (best_c, False, [best_c])

def _resolve_best_column(df: pd.DataFrame, scored: list[tuple[str, float]]) -> tuple[str | None, bool, list[str]]:
    all_cols = list(df.columns)
    if not scored:
        return (None, True, all_cols)
    (best_c, best_s) = scored[0]
    second_s = scored[1][1] if len(scored) > 1 else -1.0
    ui_cands = [c for (c, s) in scored if s >= 0.22]
    if len(ui_cands) < 2 and best_s >= 0.35:
        ui_cands = [c for (c, s) in scored[:min(12, len(scored))] if s > 0.15]
    if not ui_cands:
        ui_cands = all_cols
    if best_s < _MIN_AUTO_CONF:
        return (None, True, ui_cands)
    if second_s >= best_s - _TIE_BAND and second_s >= 0.42:
        tied = [c for (c, s) in scored[:12] if s >= second_s - 0.005]
        return (None, True, tied or ui_cands)
    return (best_c, False, [best_c])

def find_cost_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if _header_matches(str(c), COST_HINTS)]


def _header_looks_like_identifier_only(h: str) -> bool:
    """Skip ID/line columns when inferring cost from numeric values (avoids RecordID-style cols)."""
    n = _norm_header(h)
    n_compact = n.replace(' ', '')
    lone = {'id', 'uuid', 'guid', 'index', 'seq', 'sequence', 'recordid', 'rowid', 'lineid'}
    if n in lone or n_compact in lone:
        return True
    for frag in ('lineitemid', 'line item id', 'resource id', 'subscription id', 'transaction id'):
        if frag.replace(' ', '') in n_compact or frag in n:
            return True
    if re.fullmatch(r'[a-z]{2,18}id', n_compact) and not n_compact.endswith('guid'):
        return True
    return False


def _parse_monetary_cell(cell: object) -> float | None:
    if cell is None:
        return None
    try:
        if pd.isna(cell):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(cell, bool):
        return None
    if _cell_looks_like_instance_type(cell):
        return None
    if isinstance(cell, (int, float)):
        x = float(cell)
        if not math.isfinite(x):
            return None
        return x if x >= 0 else None
    s = str(cell).strip()
    if not s or s.lower() in ('nan', 'n/a', 'none', ''):
        return None
    s = re.sub(r'^[\$€£]\s*', '', s)
    s = s.replace(',', '').strip()
    if not s:
        return None
    try:
        x = float(s)
    except ValueError:
        return None
    if not math.isfinite(x):
        return None
    return x if x >= 0 else None


def _cell_looks_like_monetary_value(cell: object) -> bool:
    return _parse_monetary_cell(cell) is not None


def _median_monetary_sample(df: pd.DataFrame, col: str) -> float | None:
    n = min(len(df), _VALUE_SAMPLE_CAP)
    vals: list[float] = []
    for v in df[col].iloc[:n]:
        x = _parse_monetary_cell(v)
        if x is not None:
            vals.append(x)
    if len(vals) < 2:
        return None
    return float(statistics.median(vals))


def find_cost_columns_combined(df: pd.DataFrame, skip_cols: set[str] | None=None) -> tuple[list[str], bool]:
    """Header hints plus value-based money detection. Returns (columns, value_only_flag)."""
    skip = skip_cols or set()
    by_header = [c for c in find_cost_columns(df) if c not in skip]
    scored: list[tuple[str, float]] = []
    for col in df.columns:
        if col in skip:
            continue
        if _header_looks_like_identifier_only(str(col)):
            continue
        ratio = _value_match_ratio(df, col, _cell_looks_like_monetary_value)
        med = _median_monetary_sample(df, col)
        if ratio < 0.36 or med is None:
            continue
        if med < 0.005 and max((_parse_monetary_cell(v) or 0) for v in df[col].iloc[: min(len(df), _VALUE_SAMPLE_CAP)]) < 1.0:
            continue
        scored.append((col, ratio))
    scored.sort(key=lambda x: (-x[1], str(x[0])))
    by_value = [c for (c, _) in scored]
    merged = list(dict.fromkeys(by_header + [c for c in by_value if c not in by_header]))
    if not merged and by_value:
        merged = by_value[: min(5, len(by_value))]
    value_only = len(by_header) == 0 and len(merged) > 0
    return (merged, value_only)

OS_COLUMN_NONE_OPTION: str = '— No OS column (Linux pricing for all rows) —'


@dataclass
class ColumnBinding:
    instance: str
    os: str | None = None
    actual_cost: str | None = None

@dataclass
class LoadResult:
    df: pd.DataFrame
    warnings: list[str] = field(default_factory=list)
    instance_candidates: list[str] = field(default_factory=list)
    os_candidates: list[str] = field(default_factory=list)
    cost_candidates: list[str] = field(default_factory=list)
    binding: ColumnBinding | None = None
    needs_instance_pick: bool = False
    needs_os_pick: bool = False
    needs_cost_pick: bool = False

    @property
    def needs_manual_mapping(self) -> bool:
        return self.needs_instance_pick or self.needs_os_pick

    def with_binding(self, instance: str, os: str | None, actual_cost: str | None) -> LoadResult:
        b = ColumnBinding(instance=instance, os=os, actual_cost=actual_cost)
        return LoadResult(df=self.df, warnings=self.warnings.copy(), instance_candidates=self.instance_candidates, os_candidates=self.os_candidates, cost_candidates=self.cost_candidates, binding=b, needs_instance_pick=False, needs_os_pick=False, needs_cost_pick=False)

def _parse_dataframe(raw_bytes: bytes, ext: str) -> pd.DataFrame:
    if ext in ('xlsx', 'xls'):
        return pd.read_excel(io.BytesIO(raw_bytes), engine='openpyxl', dtype=object, keep_default_na=False)
    if ext == 'csv':
        for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
            try:
                return pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, dtype=object, keep_default_na=False)
            except (UnicodeDecodeError, Exception):
                continue
        raise ValueError('Could not decode CSV file (tried utf-8, latin-1, cp1252).')
    raise ValueError(f"Unsupported format '.{ext}'. Upload CSV (.csv) or Excel (.xlsx / .xls).")

def _coerce_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(',', '', regex=False), errors='coerce')

def analyze_load(df: pd.DataFrame, base_warnings: list[str]) -> LoadResult:
    warnings = base_warnings[:]
    require_unique_column_names(df.columns)
    inst_scored = _score_instance_columns(df)
    os_scored = _score_os_columns(df)
    (inst_col, inst_amb, inst_ui) = _resolve_best_column(df, inst_scored)
    (os_col, os_amb, os_ui) = _resolve_os_column(df, os_scored)
    logger.info('instance_col = %s', inst_col if inst_col is not None else 'None')
    logger.info('os_col = %s', os_col if os_col is not None else 'None')
    skip_for_cost: set[str] = set()
    if inst_col is not None and (not inst_amb):
        skip_for_cost.add(inst_col)
    if os_col is not None and (not os_amb):
        skip_for_cost.add(os_col)
    (cost_c, cost_inferred_values) = find_cost_columns_combined(df, skip_for_cost)
    cost_c = _rank_cost_columns(cost_c)
    # Auto-pick best cost candidate even when multiple are detected.
    selected_cost: str | None = cost_c[0] if len(cost_c) >= 1 else None
    needs_cost_pick = False
    needs_i = inst_amb or inst_col is None
    needs_o = os_amb
    inst_c_list = list(dict.fromkeys(inst_ui if needs_i else ([inst_col] if inst_col is not None else [])))
    os_c_list = list(dict.fromkeys(os_ui if needs_o else ([os_col] if os_col is not None else [])))
    if needs_i:
        warnings.append('Instance column ambiguous or low-confidence — pick the column with AWS API Name values (e.g. m5.large, db.r5.xlarge).')
    if needs_o:
        warnings.append('Multiple columns match OS-like values — pick the one that represents OS / engine.')
    if (not needs_o) and os_col is None and (not needs_i):
        warnings.append('No OS-like column detected from cell values — pricing uses Linux for all rows (see Pricing OS column).')
    if len(cost_c) == 0:
        warnings.append('No cost/spend/amount column auto-detected — savings will be N/A without selection.')
    elif len(cost_c) > 1:
        warnings.append(
            f"Multiple cost-like columns found ({len(cost_c)}) — auto-selected '{selected_cost}'."
        )
    elif cost_inferred_values:
        warnings.append('Cost column inferred from numeric values — confirm it is the spend you want for savings %.')
    binding: ColumnBinding | None = None
    if not needs_i and (not needs_o) and inst_col is not None:
        binding = ColumnBinding(instance=inst_col, os=os_col, actual_cost=selected_cost)
    return LoadResult(df=df, warnings=warnings, instance_candidates=inst_c_list, os_candidates=os_c_list, cost_candidates=cost_c, binding=binding, needs_instance_pick=needs_i, needs_os_pick=needs_o, needs_cost_pick=needs_cost_pick and len(cost_c) > 1)

def _normalize_loaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Blank strings → NA without deprecated replace downcasting; keeps non-object dtypes."""
    df = df.copy()

    def _is_blank(x: object) -> bool:
        if x is None:
            return True
        try:
            if pd.isna(x):
                return True
        except (TypeError, ValueError):
            pass
        return isinstance(x, str) and x.strip() == ''

    for col in df.columns:
        s = df[col]
        if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
            continue
        df[col] = s.mask(s.map(_is_blank), pd.NA)
    return df


def load_file(file_obj: BinaryIO, filename: str) -> LoadResult:
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    try:
        raw = file_obj.read() if hasattr(file_obj, 'read') else bytes(file_obj)
        _reject_oversized(raw, filename)
        df = _parse_dataframe(raw, ext)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Failed to read file '{filename}': {exc}") from exc
    if df.empty:
        raise ValueError('The uploaded file contains no data rows.')
    df = _normalize_loaded_dataframe(df)
    df.dropna(how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)
    if df.empty:
        raise ValueError('All rows are empty after stripping blank lines.')
    return analyze_load(df, [])

def dataframe_from_bytes(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse upload bytes to a cleaned DataFrame (no column analysis). For Fix Your Sheet merge."""
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    try:
        _reject_oversized(raw_bytes, filename)
        df = _parse_dataframe(raw_bytes, ext)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Failed to read file '{filename}': {exc}") from exc
    if df.empty:
        raise ValueError('The uploaded file contains no data rows.')
    df = _normalize_loaded_dataframe(df)
    df.dropna(how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)
    if df.empty:
        raise ValueError('All rows are empty after stripping blank lines.')
    require_unique_column_names(df.columns)
    return df

def finalize_binding(lr: LoadResult, instance_col: str, os_col: str | None, actual_cost_col: str | None) -> LoadResult:
    if instance_col not in lr.df.columns:
        raise ValueError('Selected column not found in file.')
    if os_col is not None and os_col not in lr.df.columns:
        raise ValueError('Selected OS column not found in file.')
    if actual_cost_col is not None and actual_cost_col not in lr.df.columns:
        raise ValueError('Selected cost column not found in file.')
    return lr.with_binding(instance_col, os_col, actual_cost_col)
