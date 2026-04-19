from __future__ import annotations
import logging
import os
import math
import re
from decimal import Decimal
from typing import Literal
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
from data_loader import ColumnBinding, require_unique_column_names
from instance_api import canonicalize_instance_api_name
from os_resolve import cell_matches_valid_os_pattern, classify_os_kind
from pricing_engine import DEFAULT_REGION, SUPPORTED_REGIONS, get_price, get_rds_hourly
from pricing_normalize import LINUX_FALLBACK_LABEL, normalize_instance_string, normalize_os_engine_key, normalize_pricing_os_label
from recommender import CPUFilterMode, get_recommendations, is_graviton_family
from rds_recommender import get_rds_recommendations
logger = logging.getLogger(__name__)
ServiceMode = Literal['ec2', 'rds', 'both']
INSERT_COLS: list[str] = [
    'Pricing OS',
    'Actual Cost ($)',
    'Discount %',
    'Current Price ($/hr)',
    'Alt1 Instance',
    'Alt1 Price ($/hr)',
    'Alt1 Savings %',
    'Alt2 Instance',
    'Alt2 Price ($/hr)',
    'Alt2 Savings %',
]
NA = 'N/A'
NO_SAVINGS = 'No Savings'
NO_DISCOUNT = 'No Discount'
ALT2_NO_DISTINCT = 'N/A (No distinct alternative)'
# Windows on EC2 does not offer Graviton (Arm) in the same way as Linux; block Graviton alts.
ALT2_INCOMPATIBLE_OS = 'N/A (No compatible alternative)'
_PRICING_WINDOWS_LABEL = 'Windows'
NA_FILL_COLS: tuple[str, ...] = (
    'Pricing OS',
    'Discount %',
    'Current Price ($/hr)',
    'Alt1 Instance',
    'Alt1 Price ($/hr)',
    'Alt1 Savings %',
    'Alt2 Instance',
    'Alt2 Price ($/hr)',
    'Alt2 Savings %',
)

def _first_col_index(cols: list, name: str) -> int:
    for i, c in enumerate(cols):
        if c == name:
            return i
    raise ValueError(f'Column {name!r} not found.')


def _nonempty_cell(v: object) -> bool:
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    s = str(v).strip().lower()
    return bool(s) and s not in ('nan', 'n/a', 'none', '')


def _raw_os_cell_for_row(
    work: pd.DataFrame,
    row_i: int,
    cols: list,
    ins_idx: int,
    cc_idx: int | None,
    os_idx: int | None,
) -> object | None:
    """Use bound OS column when populated; otherwise scan row cells (value-based) for OS — e.g. Product."""
    if os_idx is not None:
        v = work.iat[row_i, os_idx]
        if _nonempty_cell(v):
            return v
    for j in range(len(cols)):
        if j == ins_idx:
            continue
        if cc_idx is not None and j == cc_idx:
            continue
        v = work.iat[row_i, j]
        if not _nonempty_cell(v):
            continue
        if cell_matches_valid_os_pattern(v) or classify_os_kind(v) is not None:
            return v
    return None


def _row_matches_service(inst: str, service: ServiceMode) -> bool:
    s = normalize_instance_string(inst)
    if not s or s in ('nan', 'none'):
        return False
    is_rds = s.startswith('db.')
    if service == 'both':
        return True
    if service == 'rds':
        return is_rds
    # ec2: enrich every valid row; db.* still uses RDS hourly + rds_recommender via _pricing_backend.
    return True


def _pricing_backend(inst: str) -> Literal['ec2', 'rds']:
    """Hourly + alt SKUs: db.* → RDS tables + rds_recommender; everything else → EC2."""
    return 'rds' if normalize_instance_string(inst).startswith('db.') else 'ec2'

def _hourly_cur(inst: str, os_engine: str, backend: Literal['ec2', 'rds'], *, region: str) -> float | None:
    """Use row-resolved region for on-demand hourly SKUs."""
    inst_key = normalize_instance_string(inst)
    if not inst_key:
        return None
    if backend == 'rds':
        # Bundled RDS table is MySQL Single-AZ class rates (Linux-oriented); use linux key for lookup.
        return get_rds_hourly(inst_key, region=region, os='linux')
    return get_price(inst_key, region=region, os=os_engine)


def _hourly_alt(alt: str | None, os_engine: str, backend: Literal['ec2', 'rds'], *, region: str) -> float | None:
    if not alt or not isinstance(alt, str):
        return None
    a = str(alt).strip()
    if a in (NA, ALT2_NO_DISTINCT, ALT2_INCOMPATIBLE_OS) or not a or a.lower() in ('nan', 'none'):
        return None
    return _hourly_cur(normalize_instance_string(alt), os_engine, backend, region=region)


_SUPPORTED_REGION_IDS: frozenset[str] = frozenset((rid for rid, _label in SUPPORTED_REGIONS))
_ROW_REGION_ALIASES: dict[str, str] = {
    # Local bundled tables do not have direct entries for these in this build.
    'eu-west-3': 'eu-west-1',
    'ca-central-1': 'us-east-1',
}
_REGION_ID_RE = re.compile(r'\b([a-z]{2}-[a-z-]+-\d)\b', re.IGNORECASE)


def _region_for_pricing(raw_region: object, *, default_region: str) -> str:
    s = normalize_instance_string(raw_region).replace('_', '-')
    m = _REGION_ID_RE.search(s)
    rid = (m.group(1) if m else s).strip().lower()
    if rid in _ROW_REGION_ALIASES:
        return _ROW_REGION_ALIASES[rid]
    if rid in _SUPPORTED_REGION_IDS:
        return rid
    d = normalize_instance_string(default_region).replace('_', '-').strip().lower()
    return d if d in _SUPPORTED_REGION_IDS else DEFAULT_REGION


def _row_region_value(
    work: pd.DataFrame,
    row_i: int,
    cols: list,
    ins_idx: int,
    cc_idx: int | None,
) -> object | None:
    """
    Detect row region from any region-like column first, then from any cell token.
    """
    for j, c in enumerate(cols):
        if j == ins_idx or (cc_idx is not None and j == cc_idx):
            continue
        if 'region' not in _norm_header(c):
            continue
        v = work.iat[row_i, j]
        if _nonempty_cell(v):
            return v
    for j, _c in enumerate(cols):
        if j == ins_idx or (cc_idx is not None and j == cc_idx):
            continue
        v = work.iat[row_i, j]
        if _nonempty_cell(v) and _REGION_ID_RE.search(str(v)):
            return v
    return None


def _family_token_from_instance(api_name: str) -> str:
    s = normalize_instance_string(api_name)
    if not s:
        return ''
    body = s[3:] if s.startswith('db.') else s
    if '.' not in body:
        return ''
    return body.split('.', 1)[0]


def _is_graviton_instance_api(api_name: str) -> bool:
    fam = _family_token_from_instance(api_name)
    return bool(fam) and is_graviton_family(fam)


def _savings_from_hourly(current_hr: float | None, alt_hr: float | None) -> float | str:
    """Savings % from hourly list prices only; missing price → N/A; no hourly discount → No Savings."""
    if current_hr is None or alt_hr is None:
        return NA
    if not math.isfinite(current_hr) or not math.isfinite(alt_hr) or current_hr <= 0:
        return NA
    if alt_hr >= current_hr:
        return NO_SAVINGS
    pct = round((current_hr - alt_hr) / current_hr * 100, 1)
    return max(0.0, pct)


def _discount_pct_vs_list(act: object, list_hourly: object) -> float | str:
    """
    Discount % = ((Current list price/hr) - Actual cost) / (Current list price/hr) * 100.
    Both must be positive finite; actual >= list → No Discount; else 1 decimal.
    """
    a = _to_float(act)
    if a is None or not math.isfinite(a) or a <= 0:
        a = None
    c = None
    if list_hourly is not None:
        try:
            cf = float(list_hourly)
            if math.isfinite(cf) and cf > 0:
                c = cf
        except (TypeError, ValueError):
            pass
    if a is None or c is None:
        return NA
    if a >= c:
        return NO_DISCOUNT
    try:
        pct = round((c - a) / c * 100.0, 1)
        return pct if math.isfinite(pct) else NA
    except (ArithmeticError, ZeroDivisionError, TypeError, ValueError):
        return NA

def _to_float(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, Decimal):
        try:
            x = float(v)
            return x if math.isfinite(x) else None
        except (ArithmeticError, ValueError, TypeError):
            return None
    if hasattr(pd, 'Timestamp') and isinstance(v, pd.Timestamp):
        return None
    if hasattr(v, 'item') and not isinstance(v, (bytes, str)):
        try:
            v = v.item()
        except (AttributeError, ValueError):
            pass
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in ('nan', 'n/a', '-', ''):
            return None
        s = re.sub(r'^[\$€£]\s*', '', s)
        s = s.replace(',', '')
        s = s.strip()
        if not s:
            return None
        try:
            x = float(s)
        except ValueError:
            return None
        if pd.isna(x) or not math.isfinite(x):
            return None
        return x
    try:
        x = float(v)
        if pd.isna(x) or not math.isfinite(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _norm_header(name: object) -> str:
    s = str(name).strip().lower()
    s = re.sub(r'[_\-]+', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s


_MONTH_HDR_RE = re.compile(
    r'\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b|\b20\d{2}[-_/ ]?(?:0?[1-9]|1[0-2])\b',
    re.IGNORECASE,
)


def _cost_header_kind(name: object) -> str:
    """
    Lightweight cost-header classifier for row-wise actual cost fallback.
    Kinds: month, ri, on_demand, total, other.
    """
    n = _norm_header(name)
    if _MONTH_HDR_RE.search(n):
        return 'month'
    if 'on demand' in n or 'ondemand' in n:
        return 'on_demand'
    # Treat Savings Plan cost as RI-like committed spend for row-wise fallback.
    if (
        re.search(r'\bri\b', n)
        or 'reserved instance' in n
        or 'reservation' in n
        or 'savings plan' in n
        or 'savings_plan' in str(name).strip().lower()
        or 'sp cost' in n
    ):
        return 'ri'
    if 'total cost' in n or n.startswith('total '):
        return 'total'
    return 'other'


def _resolve_actual_cost_for_row(
    row: pd.Series,
    *,
    selected_cost_col: str | None,
    fallback_cost_cols: list[str],
) -> float | None:
    """
    Resolve row-wise actual cost with non-zero preference and safe fallback.

    Priority:
    1) Selected cost column (if > 0)
    2) Month-like columns
    3) If selected RI is zero/missing -> prefer On Demand (and vice versa)
    4) Remaining RI / On Demand / Total / other cost candidates
    """
    seen: set[str] = set()
    candidates: list[str] = []
    if selected_cost_col is not None:
        candidates.append(selected_cost_col)
        seen.add(selected_cost_col)
    for c in fallback_cost_cols:
        if c in seen:
            continue
        candidates.append(c)
        seen.add(c)

    def _pos_value(col: str) -> float | None:
        v = _to_float(row.get(col))
        if v is None or not math.isfinite(v) or v <= 0:
            return None
        return v

    if selected_cost_col is not None:
        sv = _pos_value(selected_cost_col)
        if sv is not None:
            return sv

    month_cols = [c for c in candidates if _cost_header_kind(c) == 'month']
    ri_cols = [c for c in candidates if _cost_header_kind(c) == 'ri']
    od_cols = [c for c in candidates if _cost_header_kind(c) == 'on_demand']
    total_cols = [c for c in candidates if _cost_header_kind(c) == 'total']
    other_cols = [c for c in candidates if _cost_header_kind(c) == 'other']

    selected_kind = _cost_header_kind(selected_cost_col) if selected_cost_col is not None else 'other'
    if selected_kind == 'ri':
        fallback_order = month_cols + od_cols + ri_cols + total_cols + other_cols
    elif selected_kind == 'on_demand':
        fallback_order = month_cols + ri_cols + od_cols + total_cols + other_cols
    else:
        fallback_order = month_cols + ri_cols + od_cols + total_cols + other_cols

    for c in fallback_order:
        if selected_cost_col is not None and c == selected_cost_col:
            continue
        v = _pos_value(c)
        if v is not None:
            return v
    return None

def process(df: pd.DataFrame, binding: ColumnBinding, region: str=DEFAULT_REGION, service: ServiceMode='both', cpu_filter: CPUFilterMode='both') -> pd.DataFrame:
    # CRITICAL: keep immutable baseline of the user input.
    original_df = df.copy()
    cols = list(original_df.columns)
    require_unique_column_names(cols)
    _reserved = frozenset(INSERT_COLS)
    _bad = [c for c in cols if c in _reserved]
    if _bad:
        raise ValueError(f'Input contains reserved enrichment column name(s): {sorted(set(_bad))!r}. Rename in source and re-upload to preserve data integrity.')
    if not any(c == binding.instance for c in cols):
        raise ValueError('Binding columns missing from DataFrame.')
    if binding.os is not None and not any(c == binding.os for c in cols):
        raise ValueError('Binding OS column missing from DataFrame.')
    try:
        ins_idx = _first_col_index(cols, binding.instance)
    except ValueError as exc:
        raise ValueError('Instance column not in DataFrame.') from exc
    work = original_df.copy()
    _finops_debug = os.environ.get('FINOPS_DEBUG', '').strip().lower() in ('1', 'true', 'yes')
    cc = binding.actual_cost
    cc_idx: int | None = _first_col_index(cols, cc) if (cc and any(c == cc for c in cols)) else None
    os_idx = _first_col_index(cols, binding.os) if binding.os is not None else None
    fallback_cost_cols: list[str] = []
    try:
        from data_loader import find_cost_columns_combined as _find_cost_columns_combined, _rank_cost_columns as _rank_cost_columns

        _skip: set[str] = {binding.instance}
        if binding.os is not None:
            _skip.add(binding.os)
        (det_cost_cols, _value_only) = _find_cost_columns_combined(work, _skip)
        fallback_cost_cols = _rank_cost_columns(det_cost_cols)
    except Exception:
        fallback_cost_cols = []
    if _finops_debug:
        logger.info(
            'FinOps enrichment (debug): rows=%s region=%s service=%s',
            len(work),
            region,
            service,
        )
    if work.empty:
        left = original_df.iloc[:, : ins_idx + 1].copy()
        right = original_df.iloc[:, ins_idx + 1 :].copy()
        finops_block = pd.DataFrame({c: pd.Series(index=original_df.index, dtype=object) for c in INSERT_COLS})
        final_df = pd.concat([left, finops_block, right], axis=1)
        _validate_final_integrity(
            original_df=original_df,
            final_df=final_df,
            ins_idx=ins_idx,
            new_cols=INSERT_COLS,
        )
        _raise_if_original_data_changed(original_df, df, context='processing')
        return final_df
    n = len(work)
    actual_vals: list[float | None] = [
        _resolve_actual_cost_for_row(
            work.iloc[i],
            selected_cost_col=cc,
            fallback_cost_cols=fallback_cost_cols,
        )
        for i in range(n)
    ]
    inst_series = work.iloc[:, ins_idx]
    cur_p: list = [None] * n
    a1i: list = [None] * n
    a1p: list = [None] * n
    a1s: list = [None] * n
    a2i: list = [None] * n
    a2p: list = [None] * n
    a2s: list = [None] * n
    act_out: list = [None] * n
    pricing_os_out: list[str] = [LINUX_FALLBACK_LABEL] * n
    cpu: CPUFilterMode = cpu_filter if cpu_filter in ('default', 'intel', 'amd', 'graviton', 'both') else 'both'
    row_na_fallback_count = 0
    # V2 perf guardrail: cache repeat lookups across identical row contexts.
    rec_cache: dict[tuple[str, str, CPUFilterMode], tuple[str | None, str | None]] = {}
    price_cache: dict[tuple[str, str, str, str], float | None] = {}
    for i in range(n):
        raw_inst = inst_series.iloc[i]
        raw_inst_norm = normalize_instance_string(raw_inst)
        raw_os_cell = _raw_os_cell_for_row(work, i, cols, ins_idx, cc_idx, os_idx)
        act = actual_vals[i]
        if act is not None and act <= 0:
            act = None
        act_out[i] = act
        disp_inst = raw_inst_norm
        os_engine = 'linux'
        try:
            pricing_os_out[i] = normalize_pricing_os_label(raw_os_cell)
            os_engine = normalize_os_engine_key(raw_os_cell)
            canon = canonicalize_instance_api_name(raw_inst_norm)
            if canon is not None:
                disp_inst = normalize_instance_string(canon)
            if canon is None:
                a1i[i] = a2i[i] = NA
                cur_p[i] = a1p[i] = a2p[i] = None
                a1s[i] = a2s[i] = NA
                continue
            inst = disp_inst
            if not _row_matches_service(inst, service):
                cur_p[i] = a1p[i] = a2p[i] = None
                a1i[i] = a2i[i] = NA
                a1s[i] = a2s[i] = NA
                continue
            backend = _pricing_backend(inst)
            rec_key = (inst, backend, cpu)
            if rec_key not in rec_cache:
                rec = get_rds_recommendations(inst, cpu_filter=cpu) if backend == 'rds' else get_recommendations(inst, cpu_filter=cpu)
                rec_cache[rec_key] = (rec.get('alt1'), rec.get('alt2'))
            (alt1, alt2) = rec_cache[rec_key]
            win_blocked_graviton_alt2 = False
            if pricing_os_out[i] == _PRICING_WINDOWS_LABEL:
                if alt1 and _is_graviton_instance_api(alt1):
                    alt1 = None
                if alt2 and _is_graviton_instance_api(alt2):
                    win_blocked_graviton_alt2 = True
                    alt2 = None
            if alt1 and alt2 and (alt1 == alt2):
                alt2 = None
            row_region_raw = _row_region_value(work, i, cols, ins_idx, cc_idx)
            row_region = _region_for_pricing(row_region_raw, default_region=region)
            cur_key = (inst, os_engine, backend, row_region)
            if cur_key not in price_cache:
                price_cache[cur_key] = _hourly_cur(inst, os_engine, backend, region=row_region)
            p_cur = price_cache[cur_key]
            if alt1 is not None:
                a1_key = (normalize_instance_string(alt1), os_engine, backend, row_region)
                if a1_key not in price_cache:
                    price_cache[a1_key] = _hourly_alt(alt1, os_engine, backend, region=row_region)
                p_a1 = price_cache[a1_key]
            else:
                p_a1 = None
            if alt2 is not None:
                a2_key = (normalize_instance_string(alt2), os_engine, backend, row_region)
                if a2_key not in price_cache:
                    price_cache[a2_key] = _hourly_alt(alt2, os_engine, backend, region=row_region)
                p_a2 = price_cache[a2_key]
            else:
                p_a2 = None
            cur_p[i] = p_cur
            a1i[i] = alt1 if alt1 is not None else NA
            if alt2 is not None:
                a2i[i] = alt2
            elif win_blocked_graviton_alt2:
                a2i[i] = ALT2_INCOMPATIBLE_OS
            elif alt1 is not None:
                a2i[i] = ALT2_NO_DISTINCT
            else:
                a2i[i] = NA
            a1p[i] = p_a1
            a2p[i] = p_a2
            a1s[i] = _savings_from_hourly(p_cur, p_a1)
            a2s[i] = _savings_from_hourly(p_cur, p_a2)
        except Exception as exc:
            row_na_fallback_count += 1
            if _finops_debug:
                logger.warning('Row %s: enrichment failed (%s) — filled N/A.', i, type(exc).__name__)
                logger.debug('Row enrichment detail', exc_info=True)
            try:
                pricing_os_out[i] = normalize_pricing_os_label(raw_os_cell)
            except Exception:
                pricing_os_out[i] = LINUX_FALLBACK_LABEL
            try:
                os_engine = normalize_os_engine_key(raw_os_cell)
            except Exception:
                os_engine = 'linux'
            cur_p[i] = a1p[i] = a2p[i] = None
            a1i[i] = a2i[i] = NA
            a1s[i] = a2s[i] = NA
    if _finops_debug:
        print(
            f'[FinOps DEBUG] row-region pricing rows={n} ui_region={region!r}',
            flush=True,
        )
        for j in range(min(5, n)):
            inst_j = normalize_instance_string(inst_series.iloc[j])
            os_j = pricing_os_out[j]
            pj = cur_p[j]
            p_txt = f'{pj:.6f}' if isinstance(pj, (int, float)) and math.isfinite(float(pj)) else 'N/A'
            row_region_raw_j = _row_region_value(work, j, cols, ins_idx, cc_idx)
            row_region_j = _region_for_pricing(row_region_raw_j, default_region=region)
            print(f'[FinOps DEBUG] {inst_j} {os_j} {row_region_j} {p_txt}', flush=True)
    discount_out = [_discount_pct_vs_list(act_out[i], cur_p[i]) for i in range(n)]
    _mid_lists = [
        pricing_os_out,
        act_out,
        discount_out,
        cur_p,
        a1i,
        a1p,
        a1s,
        a2i,
        a2p,
        a2s,
    ]
    if len(_mid_lists) != len(INSERT_COLS):
        raise RuntimeError('Data integrity violation: FinOps insert column mismatch.')
    finops_block = pd.DataFrame(dict(zip(INSERT_COLS, _mid_lists)), index=original_df.index)
    left = original_df.iloc[:, :ins_idx + 1].copy()
    right = original_df.iloc[:, ins_idx + 1:].copy()
    final_df = pd.concat([left, finops_block, right], axis=1)
    _validate_final_integrity(
        original_df=original_df,
        final_df=final_df,
        ins_idx=ins_idx,
        new_cols=INSERT_COLS,
    )
    _raise_if_original_data_changed(original_df, df, context='processing')
    if row_na_fallback_count:
        logger.info('FinOps enrichment: %s row(s) returned N/A fallback (invalid or unsupported row data).', row_na_fallback_count)
    if _finops_debug:
        logger.info('FinOps enrichment finished rows=%s', len(final_df))
    return final_df

def _na_like(x: object) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    if isinstance(x, str) and not x.strip():
        return True
    return False


def _raise_if_original_data_changed(original_df: pd.DataFrame, candidate_original: pd.DataFrame, *, context: str) -> None:
    """
    Fail fast if any original column/value changed (exact equality with dtype/column names).
    """
    try:
        assert_frame_equal(
            candidate_original,
            original_df,
            check_dtype=True,
            check_exact=True,
            check_names=True,
        )
    except AssertionError as exc:
        raise RuntimeError(
            f'Data integrity violation: original dataframe changed during {context}.'
        ) from exc


def _raise_if_original_column_changed(
    *,
    original_col: pd.Series,
    candidate_col: pd.Series,
    column_name: str,
    context: str,
) -> None:
    try:
        assert_series_equal(
            candidate_col,
            original_col,
            check_dtype=True,
            check_exact=True,
            check_names=True,
        )
    except AssertionError as exc:
        raise RuntimeError(
            f'Data integrity violation: original column {column_name!r} changed during {context}.'
        ) from exc


def _validate_final_integrity(
    *,
    original_df: pd.DataFrame,
    final_df: pd.DataFrame,
    ins_idx: int,
    new_cols: list[str],
) -> None:
    if len(final_df) != len(original_df):
        raise RuntimeError('Data integrity violation: row count changed.')
    expected_col_count = len(original_df.columns) + len(new_cols)
    if len(final_df.columns) != expected_col_count:
        raise RuntimeError(
            f'Data integrity violation: column count mismatch '
            f'({len(final_df.columns)} != {expected_col_count}).'
        )
    expected_cols = list(original_df.columns[: ins_idx + 1]) + new_cols + list(original_df.columns[ins_idx + 1 :])
    if list(final_df.columns) != expected_cols:
        raise RuntimeError(
            'Data integrity violation: original column order changed or FinOps insertion point is wrong.'
        )
    for orig_idx, col_name in enumerate(original_df.columns):
        final_idx = orig_idx if orig_idx <= ins_idx else orig_idx + len(new_cols)
        if final_df.columns[final_idx] != col_name:
            raise RuntimeError(
                f'Data integrity violation: original column {col_name!r} moved during final merge.'
            )
        _raise_if_original_column_changed(
            original_col=original_df.iloc[:, orig_idx],
            candidate_col=final_df.iloc[:, final_idx],
            column_name=str(col_name),
            context='final merge',
        )
    reconstructed_original = pd.concat(
        [
            final_df.iloc[:, : ins_idx + 1].copy(),
            final_df.iloc[:, ins_idx + 1 + len(new_cols) :].copy(),
        ],
        axis=1,
    )
    reconstructed_original.columns = original_df.columns
    _raise_if_original_data_changed(original_df, reconstructed_original, context='final merge')


def apply_na_fill(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in NA_FILL_COLS:
        if c not in df.columns:
            continue
        df[c] = df[c].apply(lambda x: NA if _na_like(x) else x)
    return df
