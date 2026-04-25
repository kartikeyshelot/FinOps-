from __future__ import annotations
import html
import inspect
import logging
import math
import re
import pandas as pd
import streamlit as st
from data_loader import OS_COLUMN_NONE_OPTION, LoadResult, analyze_load, finalize_binding, load_file
from excel_export import build_excel, sanitize_formula_injection_dataframe, savings_numeric
try:
    from instance_api import canonicalize_instance_api_name
except Exception:
    def canonicalize_instance_api_name(value: object) -> str | None:  # type: ignore[no-redef]
        return None
from processor import apply_na_fill, process
from pricing_engine import (
    DEFAULT_REGION, REGION_LABELS, SUPPORTED_REGIONS,
    list_known_instances,
)
from recommender import get_ec2_comparison

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger(__name__)
MAX_UI_TABLE_ROWS = 2000


# ── Display helpers ──────────────────────────────────────────────────────────

def _ui_stretch_kwargs(widget=st.dataframe) -> dict:
    if 'width' in inspect.signature(widget).parameters:
        return {'width': 'stretch'}
    return {'use_container_width': True}


def _dataframe_for_streamlit_arrow(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if out[c].dtype != object:
            continue

        def _scalar(v: object):
            if v is None:
                return pd.NA
            try:
                if pd.isna(v):
                    return pd.NA
            except (TypeError, ValueError):
                pass
            if isinstance(v, bytes):
                try:
                    return v.decode('utf-8', errors='replace')
                except Exception:
                    return str(v)
            return str(v)

        out[c] = out[c].map(_scalar).astype('string')
    return out


def _cell_display_generic(v: object) -> str:
    if v is None:
        return ''
    try:
        if pd.isna(v):
            return ''
    except (TypeError, ValueError):
        pass
    if isinstance(v, bytes):
        try:
            return v.decode('utf-8', errors='replace')
        except Exception:
            return str(v)
    return str(v)


def _format_display_money_cell(v: object, *, hourly: bool) -> str:
    if v is None:
        return 'N/A'
    try:
        if pd.isna(v):
            return 'N/A'
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        t = v.strip()
        if not t or t.upper() == 'N/A' or t.lower() in ('nan', 'none'):
            return 'N/A'
        if t.startswith('$'):
            return t
        try:
            x = float(t.replace(',', ''))
        except ValueError:
            return t
    else:
        try:
            x = float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return str(v)
    if not math.isfinite(x):
        return 'N/A'
    if hourly:
        return f'${x:.4f}'
    return f'${x:,.2f}'


def _format_display_discount_pct_cell(v: object) -> str:
    if v is None:
        return 'N/A'
    try:
        if pd.isna(v):
            return 'N/A'
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        t = v.strip()
        if not t or t.upper() == 'N/A' or t.lower() in ('nan', 'none'):
            return 'N/A'
        if t == 'No Discount':
            return 'No Discount'
        if t.endswith('%'):
            return t
        try:
            return f'{float(t.replace("%", "").replace(",", "").strip()):.1f}%'
        except ValueError:
            return t
    try:
        x = float(v)
        if math.isfinite(x):
            return f'{x:.1f}%'
    except (TypeError, ValueError):
        pass
    return str(v)


def _format_display_savings_cell(v: object) -> str:
    if v is None:
        return 'N/A'
    try:
        if pd.isna(v):
            return 'N/A'
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        t = v.strip()
        if not t or t.upper() == 'N/A' or t.lower() in ('nan', 'none'):
            return 'N/A'
        if t == 'No Savings':
            return 'No Savings'
        if t.endswith('%'):
            return t
        try:
            return f'{float(t.replace("%", "").replace(",", "").strip()):.1f}%'
        except ValueError:
            return t
    try:
        x = float(v)  # type: ignore[arg-type]
        if math.isfinite(x):
            return f'{x:.1f}%'
    except (TypeError, ValueError):
        pass
    return str(v)


def _enriched_table_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    parts: list[pd.Series] = []
    for j in range(df.shape[1]):
        name = df.columns[j]
        ser = df.iloc[:, j]
        cn = str(name)
        if cn == 'Actual Cost ($)':
            vals = [_format_display_money_cell(x, hourly=False) for x in ser]
        elif cn == 'Discount %':
            vals = [_format_display_discount_pct_cell(x) for x in ser]
        elif 'Price ($/hr)' in cn:
            vals = [_format_display_money_cell(x, hourly=True) for x in ser]
        elif 'Savings %' in cn:
            vals = [_format_display_savings_cell(x) for x in ser]
        else:
            vals = [_cell_display_generic(x) for x in ser]
        parts.append(pd.Series(vals, index=df.index, name=name, dtype=str))
    return pd.concat(parts, axis=1)


# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title='FinOps Optimizer',
    page_icon='◆',
    layout='wide',
    initial_sidebar_state='collapsed',
)

# ── CSS — Cherry Blossom palette ─────────────────────────────────────────────

FINOPS_CSS = """
<style>
/* Cherry Blossom palette
   --blossom  #EDAFB8  accent / highlights
   --petal    #F7E1D7  card backgrounds / soft fills
   --dust     #DEDBD2  borders / dividers
   --ash      #B0C4B1  secondary text / muted labels
   --iron     #4A5759  primary text / buttons
*/
:root {
  --blossom: #EDAFB8;
  --petal:   #F7E1D7;
  --dust:    #DEDBD2;
  --ash:     #B0C4B1;
  --iron:    #4A5759;
  --white:   #ffffff;
  --radius:  10px;
}

html { color-scheme: light; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
  color: var(--iron);
}
#MainMenu, [data-testid="stFooter"], [data-testid="stDecoration"] { visibility: hidden; }

section[data-testid="stApp"] { background: var(--white); }
section[data-testid="stApp"] .main .block-container {
  padding-top: 0.6rem;
  padding-bottom: 2rem;
  max-width: 1100px;
}

/* ── Buttons ── */
[data-testid="stButton"] button[kind="primary"],
[data-testid="stFormSubmitButton"] button {
  background: var(--iron) !important;
  border: none !important;
  color: #fff !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
  transition: background 0.18s ease !important;
}
[data-testid="stButton"] button[kind="primary"]:hover { background: #374445 !important; }
[data-testid="stButton"] button[kind="secondary"] {
  border: 1.5px solid var(--dust) !important;
  color: var(--iron) !important;
  background: var(--white) !important;
  border-radius: 8px !important;
}
[data-testid="stDownloadButton"] button {
  background: var(--iron) !important;
  border: none !important;
  color: #fff !important;
  border-radius: 8px !important;
  font-weight: 600 !important;
}

/* ── Inputs ── */
[data-testid="stTextInput"] input {
  border-radius: 8px !important;
  border-color: var(--dust) !important;
}
[data-testid="stTextInput"] input:focus {
  border-color: var(--blossom) !important;
  box-shadow: 0 0 0 3px rgba(237,175,184,0.22) !important;
}

/* ── Streamlit bordered containers ── */
section[data-testid="stApp"] div[data-testid="stVerticalBlockBorderWrapper"] {
  border-radius: var(--radius) !important;
  border: 1px solid var(--dust) !important;
  background: var(--white) !important;
  box-shadow: 0 1px 6px rgba(74,87,89,0.05) !important;
  padding-top: 0.75rem !important;
  padding-bottom: 1rem !important;
  margin-bottom: 1rem !important;
}

/* ── Hero ── */
.fo-hero {
  padding: 1.1rem 0 0.7rem;
  margin-bottom: 0.6rem;
  border-bottom: 1px solid var(--dust);
}
.fo-hero-row {
  display: flex;
  align-items: baseline;
  gap: 0.7rem;
  flex-wrap: wrap;
}
.fo-hero-title {
  font-size: 1.45rem;
  font-weight: 700;
  color: var(--iron);
  margin: 0;
  letter-spacing: -0.03em;
}
.fo-hero-accent { color: var(--blossom); }
.fo-hero-sub {
  font-size: 0.8rem;
  color: var(--ash);
  margin: 0.2rem 0 0;
}
.fo-pill {
  display: inline-block;
  font-size: 0.67rem;
  font-weight: 600;
  padding: 0.2rem 0.55rem;
  border-radius: 100px;
  background: var(--petal);
  border: 1px solid var(--dust);
  color: var(--iron);
}
.fo-pill-stale { background: #fff3e0; border-color: #f0b88c; color: #7a3e00; }

/* ── Section labels ── */
.fo-section {
  font-size: 0.6rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--ash);
  margin: 0 0 0.3rem;
}

/* ── Numbered step header ── */
.fo-step {
  display: flex;
  align-items: flex-start;
  gap: 0.6rem;
  margin-bottom: 0.8rem;
  padding-bottom: 0.65rem;
  border-bottom: 1px solid var(--dust);
}
.fo-step-num {
  width: 1.45rem;
  height: 1.45rem;
  border-radius: 50%;
  background: var(--iron);
  color: #fff;
  font-size: 0.7rem;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  margin-top: 0.05rem;
}
.fo-step-body { flex: 1; min-width: 0; }
.fo-step-title { font-size: 0.92rem; font-weight: 600; color: var(--iron); margin: 0; }
.fo-step-sub   { font-size: 0.76rem; color: var(--ash); margin: 0.12rem 0 0; }

/* ── Quick lookup section header ── */
.fo-lookup-head {
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--iron);
  margin: 0 0 0.3rem;
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
.fo-lookup-head-accent {
  font-size: 0.65rem;
  font-weight: 600;
  color: var(--ash);
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 0.18rem 0.5rem;
  border-radius: 100px;
  background: var(--petal);
  border: 1px solid var(--dust);
}
.fo-lookup-sub { font-size: 0.78rem; color: var(--ash); margin: 0 0 0.85rem; }

/* ── Pipeline ── */
.fo-pipeline {
  display: flex;
  gap: 0.35rem;
  align-items: center;
  margin: 0.4rem 0 0.85rem;
  flex-wrap: wrap;
}
.fo-pipe-step {
  font-size: 0.7rem;
  font-weight: 600;
  padding: 0.25rem 0.6rem;
  border-radius: 100px;
  border: 1px solid var(--dust);
  color: var(--ash);
  background: var(--white);
}
.fo-pipe-step.done   { background: var(--ash);  color: #fff; border-color: var(--ash); }
.fo-pipe-step.active { background: var(--iron); color: #fff; border-color: var(--iron); }
.fo-pipe-sep { color: var(--dust); font-size: 0.75rem; }

/* ── Current instance card ── */
.fo-cur-card {
  background: var(--petal);
  border: 1px solid var(--dust);
  border-radius: var(--radius);
  padding: 0.8rem 1rem;
  margin-bottom: 0.7rem;
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 0.5rem;
}
@media (max-width: 720px) { .fo-cur-card { grid-template-columns: repeat(2, 1fr); } }
.fo-cur-cell-label {
  font-size: 0.57rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ash);
  margin: 0 0 0.12rem;
}
.fo-cur-cell-val {
  font-size: 0.9rem;
  font-weight: 700;
  color: var(--iron);
  margin: 0;
  font-family: ui-monospace, 'SF Mono', monospace;
}
.fo-cur-cell-val.na {
  color: var(--ash);
  font-family: inherit;
  font-weight: 400;
  font-style: italic;
}

/* ── Alt recommendation cards ── */
.fo-alt-card {
  border: 1px solid var(--dust);
  border-radius: var(--radius);
  padding: 0.9rem 1rem 0.9rem 1.2rem;
  margin-bottom: 0.55rem;
  background: var(--white);
  position: relative;
  overflow: hidden;
}
.fo-alt-card::before {
  content: "";
  position: absolute;
  left: 0; top: 0; bottom: 0;
  width: 4px;
  background: var(--blossom);
  border-radius: var(--radius) 0 0 var(--radius);
}
.fo-alt-header {
  display: flex;
  align-items: baseline;
  gap: 0.55rem;
  margin-bottom: 0.6rem;
}
.fo-alt-rank {
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--blossom);
}
.fo-alt-name {
  font-size: 1rem;
  font-weight: 700;
  color: var(--iron);
  font-family: ui-monospace, 'SF Mono', monospace;
}
.fo-alt-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0.45rem;
  margin-bottom: 0.65rem;
}
@media (max-width: 600px) { .fo-alt-grid { grid-template-columns: repeat(2, 1fr); } }
.fo-alt-cell-label {
  font-size: 0.57rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  color: var(--ash);
  margin: 0 0 0.08rem;
}
.fo-alt-cell-val {
  font-size: 0.88rem;
  font-weight: 600;
  color: var(--iron);
  margin: 0;
}
.fo-alt-cell-val.save     { color: #2d6a4f; }
.fo-alt-cell-val.cost-more { color: #8b4c2a; }
.fo-reason {
  font-size: 0.78rem;
  line-height: 1.55;
  color: var(--iron);
  background: var(--petal);
  border-radius: 8px;
  padding: 0.45rem 0.7rem;
  margin: 0;
}
.fo-reason small { color: var(--ash); display: block; margin-top: 0.2rem; }

/* ── Alerts ── */
.fo-alert {
  padding: 0.6rem 0.85rem;
  border-radius: 8px;
  font-size: 0.81rem;
  line-height: 1.45;
  margin: 0.35rem 0;
  border: 1px solid var(--dust);
  color: var(--iron);
}
.fo-alert-warn { background: #fff8f0; border-color: #f0c090; }
.fo-alert-err  { background: #fff0f0; border-color: #f0a0a0; }
.fo-alert-ok   { background: #f0fff8; border-color: var(--ash); }

/* ── Metric cards ── */
.fo-metric {
  padding: 0.7rem 0.9rem;
  border-radius: var(--radius);
  border: 1px solid var(--dust);
  background: var(--white);
}
.fo-metric-label {
  font-size: 0.57rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--ash);
  margin: 0 0 0.18rem;
}
.fo-metric-val { font-size: 1.1rem; font-weight: 700; color: var(--iron); margin: 0; }
.fo-metric-val.good { color: #2d6a4f; }
.fo-metric-val.bad  { color: #8b4c2a; }

/* ── Trust / disclaimer block ── */
.fo-trust {
  background: var(--petal);
  border: 1px solid var(--dust);
  border-radius: var(--radius);
  padding: 0.8rem 1rem;
  font-size: 0.77rem;
  line-height: 1.55;
  color: var(--iron);
}

/* ── KPI labels ── */
.fo-kpi-label {
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--ash);
  margin: 0.35rem 0 0.5rem;
}

/* ── Divider ── */
.fo-divider { border: none; height: 1px; background: var(--dust); margin: 1.1rem 0; }

/* ── Footer ── */
.fo-footer {
  margin-top: 2rem;
  padding: 0.85rem 1rem;
  text-align: center;
  font-size: 0.73rem;
  color: var(--ash);
  border-top: 1px solid var(--dust);
}

/* ── Dataframe wrapper ── */
div[data-testid="stVerticalBlock"]:has(#fo-df-anchor) [data-testid="stDataFrame"] {
  max-height: 500px;
  overflow: auto;
  border-radius: 8px;
  border: 1px solid var(--dust);
}

/* ── Step-1 toolbar alignment ── */
#fo-toolbar-anchor { display: none; }
div[data-testid="stVerticalBlock"]:has(#fo-toolbar-anchor) [data-testid="stHorizontalBlock"] {
  align-items: flex-start !important;
}
div[data-testid="stVerticalBlock"]:has(#fo-toolbar-anchor) [data-testid="column"] {
  min-width: 0;
}
</style>
"""
st.markdown(FINOPS_CSS, unsafe_allow_html=True)


# ── UI helpers ────────────────────────────────────────────────────────────────

def _fo_step(num: int, title: str, subtitle: str = '') -> None:
    sub = f'<p class="fo-step-sub">{subtitle}</p>' if subtitle else ''
    st.markdown(
        f'<div class="fo-step"><span class="fo-step-num">{num}</span>'
        f'<div class="fo-step-body"><p class="fo-step-title">{title}</p>{sub}</div></div>',
        unsafe_allow_html=True,
    )


def _pipeline_html(active: int) -> str:
    labels = ['Upload', 'Map', 'Analyse', 'Export']
    here = html.escape(labels[active])
    parts = [f'<div class="fo-pipeline" title="You are here: {here}">']
    for i, lab in enumerate(labels):
        if i > 0:
            parts.append('<span class="fo-pipe-sep">›</span>')
        cls = 'fo-pipe-step done' if active > i else ('fo-pipe-step active' if active == i else 'fo-pipe-step')
        parts.append(f'<span class="{cls}">{html.escape(lab)}</span>')
    parts.append('</div>')
    return ''.join(parts)


def _sync_auto_binding(lr: LoadResult | None) -> None:
    if lr is None or st.session_state.get('binding') is not None:
        return
    if lr.needs_instance_pick or lr.needs_os_pick:
        return
    if lr.binding is None:
        return
    if len(lr.cost_candidates) > 1 and lr.binding.actual_cost is None:
        return
    st.session_state['binding'] = lr.binding


def _pipeline_step_index(lr: LoadResult | None) -> int:
    if st.session_state.get('result') is not None:
        return 3
    if lr is None:
        return 0
    if st.session_state.get('binding') is None:
        return 1
    return 2


_OLD_GEN_FAM_RE = re.compile(r'^(m[3-5]|c[3-5]|r[3-5]|t[1-3])([a-z][a-z0-9]*)?$', re.I)
_GRAV_ALT_RE = re.compile(r'[mcrtir]\d+g\.', re.I)


def _instance_family_token(cell: object) -> str:
    s = str(cell).strip().lower()
    if not s or s in ('nan', 'none', 'n/a'):
        return ''
    if s.startswith('db.') and s.count('.') >= 2:
        return s.split('.')[1]
    if '.' in s:
        return s.split('.')[0]
    return ''


def _resolve_instance_column_for_view(df: pd.DataFrame, bound_instance_col: str | None) -> str | None:
    if bound_instance_col and bound_instance_col in df.columns:
        return bound_instance_col
    if df.empty:
        return None
    best_col: str | None = None
    best_ratio = 0.0
    sample_n = min(len(df), 2000)
    for c in df.columns:
        ser = df[c].iloc[:sample_n]
        non_empty = 0
        valid = 0
        for v in ser:
            if v is None:
                continue
            try:
                if pd.isna(v):
                    continue
            except (TypeError, ValueError):
                pass
            s = str(v).strip()
            if not s or s.lower() in ('nan', 'none', 'n/a'):
                continue
            non_empty += 1
            if canonicalize_instance_api_name(v) is not None:
                valid += 1
        if non_empty == 0:
            continue
        ratio = valid / non_empty
        if ratio > best_ratio:
            best_ratio = ratio
            best_col = str(c)
    return best_col if best_ratio >= 0.2 else None


def _is_rds_instance_cell(cell: object) -> bool:
    canon = canonicalize_instance_api_name(cell)
    return bool(canon and canon.startswith('db.'))


def _is_old_gen_instance_cell(cell: object) -> bool:
    fam = _instance_family_token(cell)
    if not fam:
        return False
    if len(fam) >= 2 and fam[-1] == 'g' and fam[-2].isdigit():
        return False
    return _OLD_GEN_FAM_RE.match(fam) is not None


def _is_graviton_alt_cell(x: object) -> bool:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return False
    s = str(x).strip().lower()
    return bool(_GRAV_ALT_RE.search(s))


def _dashboard_strip_metrics(df: pd.DataFrame, inst_col: str | None) -> dict[str, float | int | None]:
    total_cost: float | None = None
    if 'Actual Cost ($)' in df.columns:
        ser = pd.to_numeric(df['Actual Cost ($)'], errors='coerce')
        s = float(ser.sum())
        if pd.notna(s) and s > 0:
            total_cost = s
        elif pd.notna(s):
            total_cost = 0.0
    avg_save: float | None = None
    if 'Alt1 Savings %' in df.columns:
        vals: list[float] = []
        for x in df['Alt1 Savings %']:
            v = _savings_for_kpi(x)
            if v is not None and v > 0:
                vals.append(v)
        if vals:
            avg_save = sum(vals) / len(vals)
    old_gen = 0
    grav = 0
    if inst_col and inst_col in df.columns:
        inst_ser = df[inst_col]
        old_gen = int(inst_ser.map(_is_old_gen_instance_cell).sum())
        a1c = 'Alt1 Instance' if 'Alt1 Instance' in df.columns else None
        a2c = 'Alt2 Instance' if 'Alt2 Instance' in df.columns else None
        a1_ser = df[a1c] if a1c else pd.Series(index=df.index, dtype=object)
        a2_ser = df[a2c] if a2c else pd.Series(index=df.index, dtype=object)
        grav = int((a1_ser.map(_is_graviton_alt_cell) | a2_ser.map(_is_graviton_alt_cell)).sum())
    return {'total_cost': total_cost, 'avg_save': avg_save, 'old_gen': old_gen, 'grav': grav}


def _render_dashboard_kpi_strip(m: dict[str, float | int | None]) -> None:
    (k1, k2, k3, k4) = st.columns(4)
    tc = m['total_cost']
    tc_s = f'${tc:,.2f}' if tc is not None else '—'
    av = m['avg_save']
    av_s = f'{av:.1f}%' if av is not None else '—'
    with k1:
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Total actual cost</p><p class="fo-metric-val">{tc_s}</p></div>', unsafe_allow_html=True)
    with k2:
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Avg Alt1 savings (&gt; 0)</p><p class="fo-metric-val good">{av_s}</p></div>', unsafe_allow_html=True)
    with k3:
        n = int(m['old_gen'] or 0)
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Older-gen families</p><p class="fo-metric-val bad">{n:,}</p></div>', unsafe_allow_html=True)
    with k4:
        g = int(m['grav'] or 0)
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Graviton in alts</p><p class="fo-metric-val">{g:,}</p></div>', unsafe_allow_html=True)


def _savings_for_kpi(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str) and v.strip() == 'No Savings':
        return None
    return savings_numeric(v)


def _series_savings_pct(col_name: str, df: pd.DataFrame) -> pd.Series:
    col = df.get(col_name)
    if col is None or len(col) == 0:
        return pd.Series(dtype=float)
    raw = [_savings_for_kpi(x) for x in col]
    return pd.to_numeric(pd.Series(raw), errors='coerce').dropna()


def _old_generation_detail_table(df: pd.DataFrame, inst_col: str | None) -> pd.DataFrame:
    if not inst_col or inst_col not in df.columns or df.empty:
        return pd.DataFrame()
    extra = [
        c for c in (
            'Pricing OS', 'Discount %', 'Current Price ($/hr)',
            'Alt1 Instance', 'Alt1 Savings %', 'Alt2 Instance', 'Alt2 Savings %',
        )
        if c in df.columns
    ]
    cols = [inst_col] + [c for c in extra if c != inst_col]
    sub = df.loc[:, cols].copy()
    mask = sub[inst_col].map(_is_old_gen_instance_cell)
    out = sub.loc[mask].copy()
    out.reset_index(drop=True, inplace=True)
    return out


def kpis(df: pd.DataFrame) -> dict:
    s1 = _series_savings_pct('Alt1 Savings %', df)
    s2 = _series_savings_pct('Alt2 Savings %', df)
    return {
        'total': len(df),
        'avg1': float(s1.mean()) if len(s1) else None,
        'max1': float(s1.max()) if len(s1) else None,
        'max2': float(s2.max()) if len(s2) else None,
        'act_col': 'Actual Cost ($)' in df.columns,
    }


def render_kpis(k: dict) -> None:
    (c1, c2, c3, c4, c5) = st.columns(5)
    with c1:
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Rows in view</p><p class="fo-metric-val">{k["total"]:,}</p></div>', unsafe_allow_html=True)
    with c2:
        v = f"{k['avg1']:.1f}%" if k['avg1'] is not None else '—'
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Avg Alt1 savings</p><p class="fo-metric-val good">{v}</p></div>', unsafe_allow_html=True)
    with c3:
        v = f"{k['max1']:.1f}%" if k['max1'] is not None else '—'
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Max Alt1 savings</p><p class="fo-metric-val good">{v}</p></div>', unsafe_allow_html=True)
    with c4:
        v = f"{k['max2']:.1f}%" if k['max2'] is not None else '—'
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Max Alt2 savings</p><p class="fo-metric-val good">{v}</p></div>', unsafe_allow_html=True)
    with c5:
        st.markdown(f'<div class="fo-metric"><p class="fo-metric-label">Actual cost col</p><p class="fo-metric-val">{"Yes" if k["act_col"] else "No"}</p></div>', unsafe_allow_html=True)


# ── Session state defaults ────────────────────────────────────────────────────

for (_k, _v) in (
    ('load_result', None), ('result', None), ('region_id', DEFAULT_REGION),
    ('service', 'both'), ('cpu_filter', 'both'), ('cost_pick', None),
):
    if _k not in st.session_state:
        st.session_state[_k] = _v

lr: LoadResult | None = st.session_state.get('load_result')
_sync_auto_binding(lr)

# ── EC2 Quick Lookup ──────────────────────────────────────────────────────────

with st.container(border=True):
    _ec2_all = [''] + list_known_instances(DEFAULT_REGION)

    with st.form(key='ql_form', border=False):
        (ql_inst_col, ql_reg_col, ql_os_col, ql_btn_col) = st.columns([3, 2.2, 1.5, 1.1])
        with ql_inst_col:
            ql_instance = st.selectbox(
                'instance', _ec2_all,
                format_func=lambda x: '— select or type to search —' if x == '' else x,
                label_visibility='collapsed', key='ql_instance',
            )
        with ql_reg_col:
            _region_opts = [f'{label}  [{rid}]' for (rid, label) in SUPPORTED_REGIONS]
            _def_idx = [r for (r, _) in SUPPORTED_REGIONS].index(DEFAULT_REGION)
            _ql_reg_sel = st.selectbox('region', _region_opts, index=_def_idx,
                                       label_visibility='collapsed', key='ql_region')
            ql_region = [r for (r, _) in SUPPORTED_REGIONS][_region_opts.index(_ql_reg_sel)]
        with ql_os_col:
            ql_os_sel = st.selectbox('os', ['Linux', 'Windows', 'RHEL', 'SUSE'],
                                     label_visibility='collapsed', key='ql_os')
            _os_map = {'Linux': 'linux', 'Windows': 'windows', 'RHEL': 'rhel', 'SUSE': 'suse'}
            ql_os_key = _os_map[ql_os_sel]
        with ql_btn_col:
            ql_go = st.form_submit_button('Analyse', type='primary', use_container_width=True)

    if ql_go and ql_instance:
        comp = get_ec2_comparison(ql_instance, region=ql_region, os_key=ql_os_key)
        cur = comp.get('current')
        alts = comp.get('alternatives', [])
        err = comp.get('error')

        if err and not cur:
            st.markdown(f'<div class="fo-alert fo-alert-err">{html.escape(err)}</div>', unsafe_allow_html=True)
        else:
            if cur:
                p = cur['price']
                p_s = f"${p:.4f}/hr" if p is not None else 'Not in dataset for this region / OS'
                p_cls = '' if p is not None else 'na'
                arch_label = {'intel': 'Intel Xeon', 'amd': 'AMD EPYC', 'graviton': 'AWS Graviton'}.get(
                    cur['arch'], cur['arch'].title()
                )
                cat_label = cur['category'].replace('_', ' ').title()
                eff = cur['efficiency_score']
                eff_s = f"{eff}/100"
                st.markdown(f'''
<div class="fo-cur-card">
  <div><p class="fo-cur-cell-label">Current instance</p><p class="fo-cur-cell-val">{html.escape(cur["instance"])}</p></div>
  <div><p class="fo-cur-cell-label">Price / hr</p><p class="fo-cur-cell-val {p_cls}">{html.escape(p_s)}</p></div>
  <div><p class="fo-cur-cell-label">Architecture</p><p class="fo-cur-cell-val">{html.escape(arch_label)}</p></div>
  <div><p class="fo-cur-cell-label">Generation</p><p class="fo-cur-cell-val">Gen {cur["gen"]}</p></div>
  <div><p class="fo-cur-cell-label">Category</p><p class="fo-cur-cell-val">{html.escape(cat_label)}</p></div>
</div>
''', unsafe_allow_html=True)

            if err:
                st.markdown(f'<div class="fo-alert fo-alert-warn">{html.escape(err)}</div>', unsafe_allow_html=True)

            if not alts:
                st.markdown(
                    '<div class="fo-alert fo-alert-warn">'
                    'No compatible Intel/AMD alternatives found for this instance in the selected region/OS.'
                    '</div>',
                    unsafe_allow_html=True,
                )
            else:
                rank_labels = ['Recommendation 1', 'Recommendation 2', 'Recommendation 3']
                for rank, alt in enumerate(alts, 1):
                    ap = alt['price']
                    ap_s = f"${ap:.4f}" if ap is not None else 'N/A'
                    sp = alt['savings_pct']
                    if sp is not None and sp > 0.05:
                        sp_s = f"+{sp:.1f}% cheaper"
                        sp_cls = 'save'
                    elif sp is not None and sp < -0.5:
                        sp_s = f"{sp:.1f}% more expensive"
                        sp_cls = 'cost-more'
                    else:
                        sp_s = 'Similar price'
                        sp_cls = ''
                    arch_l = {'intel': 'Intel Xeon', 'amd': 'AMD EPYC'}.get(alt['arch'], alt['arch'].title())
                    eff_s = f"Gen {alt['gen']} · {alt['efficiency_score']}/100"
                    # Price detail line
                    cur_p = cur['price'] if cur else None
                    if cur_p and ap:
                        hr_delta = cur_p - ap
                        monthly_delta = hr_delta * 730
                        if monthly_delta > 0:
                            price_detail = (
                                f"${ap:.4f}/hr  ·  save ${hr_delta:.4f}/hr  ·  "
                                f"~${monthly_delta:,.0f}/mo per instance"
                            )
                        elif monthly_delta < 0:
                            price_detail = (
                                f"${ap:.4f}/hr  ·  +${-hr_delta:.4f}/hr more  ·  "
                                f"~${-monthly_delta:,.0f}/mo extra per instance"
                            )
                        else:
                            price_detail = f"${ap:.4f}/hr  ·  same list price"
                    else:
                        price_detail = ap_s
                    rl = rank_labels[rank - 1] if rank <= 3 else f'Recommendation {rank}'
                    st.markdown(f'''
<div class="fo-alt-card">
  <div class="fo-alt-header">
    <span class="fo-alt-rank">{rl}</span>
    <span class="fo-alt-name">{html.escape(alt["instance"])}</span>
  </div>
  <div class="fo-alt-grid">
    <div>
      <p class="fo-alt-cell-label">Price / hr</p>
      <p class="fo-alt-cell-val">{html.escape(ap_s)}</p>
    </div>
    <div>
      <p class="fo-alt-cell-label">vs Current</p>
      <p class="fo-alt-cell-val {sp_cls}">{html.escape(sp_s)}</p>
    </div>
    <div>
      <p class="fo-alt-cell-label">Architecture</p>
      <p class="fo-alt-cell-val">{html.escape(arch_l)}</p>
    </div>
    <div>
      <p class="fo-alt-cell-label">Gen · Efficiency tier</p>
      <p class="fo-alt-cell-val">{html.escape(eff_s)}</p>
    </div>
  </div>
  <p class="fo-reason">{html.escape(alt["reason"])}<small>{html.escape(price_detail)}</small></p>
</div>
''', unsafe_allow_html=True)

    elif ql_go and not ql_instance:
        st.markdown(
            '<div class="fo-alert fo-alert-warn">Select an instance type from the dropdown first.</div>',
            unsafe_allow_html=True,
        )
st.markdown('<hr class="fo-divider">', unsafe_allow_html=True)

# ── Bulk file analysis ────────────────────────────────────────────────────────

with st.container(border=True):
    uploaded = st.file_uploader(
        'Drop your spreadsheet', type=['csv', 'xlsx', 'xls'], label_visibility='visible',
    )
    st.markdown('<div id="fo-toolbar-anchor"></div>', unsafe_allow_html=True)
    (reg_col, svc_col, cpu_col, go_col) = st.columns([2.5, 3.2, 2.0, 1.8], gap='medium')
    with reg_col:
        _reg_opts = [f'{label}  [{rid}]' for (rid, label) in SUPPORTED_REGIONS]
        _ridx = [r for (r, _) in SUPPORTED_REGIONS].index(DEFAULT_REGION)
        _sel_disp = st.selectbox('region', _reg_opts, index=_ridx, label_visibility='collapsed')
        sel_region = [r for (r, _) in SUPPORTED_REGIONS][_reg_opts.index(_sel_disp)]
        st.session_state['region_id'] = sel_region
    with svc_col:
        st.session_state['service'] = st.radio(
            'svc', ['both', 'ec2', 'rds'],
            format_func=lambda x: {'ec2': 'EC2', 'rds': 'RDS', 'both': 'Both'}[x],
            label_visibility='collapsed', horizontal=True,
        )
    with cpu_col:
        st.session_state['cpu_filter'] = st.selectbox(
            'cpu', ['both', 'default', 'intel', 'graviton'],
            format_func=lambda x: {'both': 'Both', 'default': 'Default', 'intel': 'Intel', 'graviton': 'Graviton'}[x],
            label_visibility='collapsed',
        )
    with go_col:
        run = st.button('Continue', type='primary', disabled=uploaded is None, use_container_width=True)

st.markdown('<hr class="fo-divider">', unsafe_allow_html=True)

# ── File load ─────────────────────────────────────────────────────────────────
if run and uploaded:
    with st.spinner('Loading…'):
        try:
            _lr_new = load_file(uploaded, uploaded.name)
            st.session_state['load_result'] = _lr_new
            st.session_state['result'] = None
            st.session_state['cost_pick'] = None
            st.session_state['binding'] = None
            st.session_state.pop('_enrich_svc', None)
            st.session_state.pop('_enrich_cpu', None)
            for w in _lr_new.warnings:
                st.markdown(f'<div class="fo-alert fo-alert-warn">{html.escape(str(w))}</div>', unsafe_allow_html=True)
        except ValueError as ve:
            st.session_state['load_result'] = None
            st.markdown(f'<div class="fo-alert fo-alert-err">{html.escape(str(ve))}</div>', unsafe_allow_html=True)
        except Exception as e:
            st.session_state['load_result'] = None
            log.error('load_file failed: %s', type(e).__name__)
            st.markdown('<div class="fo-alert fo-alert-err">Failed to read file.</div>', unsafe_allow_html=True)

lr = st.session_state.get('load_result')
_sync_auto_binding(lr)

binding_ready = False
chosen_binding = None
if st.session_state.get('binding') is not None:
    chosen_binding = st.session_state['binding']
    binding_ready = True

# ── Step 2: Map columns ───────────────────────────────────────────────────────
if lr is not None and not binding_ready:
    cols_all = list(lr.df.columns)
    if lr.needs_instance_pick or lr.needs_os_pick:
        with st.container(border=True):
            (mc1, mc2) = st.columns(2)
            with mc1:
                di = 0
                if lr.instance_candidates and lr.instance_candidates[0] in cols_all:
                    di = cols_all.index(lr.instance_candidates[0])
                inst_sel = st.selectbox('Instance / DB class (AWS API name)', cols_all, index=min(di, len(cols_all) - 1))
            with mc2:
                if lr.needs_os_pick:
                    os_sel = st.selectbox('OS / engine column', list(lr.os_candidates), index=0)
                else:
                    os_opts = [OS_COLUMN_NONE_OPTION] + cols_all
                    default_os = OS_COLUMN_NONE_OPTION
                    if lr.os_candidates and lr.os_candidates[0] in cols_all:
                        default_os = lr.os_candidates[0]
                    oi = os_opts.index(default_os) if default_os in os_opts else 0
                    os_sel = st.selectbox('OS / engine column (optional)', os_opts, index=min(oi, len(os_opts) - 1))
            cost_sel = None
            if len(lr.cost_candidates) >= 1:
                cost_sel = lr.cost_candidates[0]
            else:
                cost_sel = st.selectbox('Actual cost column (optional)', ['— None —'] + cols_all, key='cost_optional')
                if cost_sel == '— None —':
                    cost_sel = None
            if st.button('Save mapping', type='primary'):
                try:
                    os_final = None if os_sel == OS_COLUMN_NONE_OPTION else os_sel
                    b = finalize_binding(lr, inst_sel, os_final, cost_sel).binding
                    st.session_state['binding'] = b
                    st.session_state['cost_pick'] = cost_sel
                    st.rerun()
                except ValueError as e:
                    st.markdown(f'<div class="fo-alert fo-alert-err">{html.escape(str(e))}</div>', unsafe_allow_html=True)

if lr is not None and st.session_state.get('binding') is not None:
    chosen_binding = st.session_state['binding']
    binding_ready = True

# ── Step 3: Run enrichment ────────────────────────────────────────────────────
if lr is not None and binding_ready and st.session_state.get('result') is None:
    with st.container(border=True):
        if st.button('Run enrichment', type='primary', key='run_enrich'):
            try:
                svc = st.session_state['service']
                cpu = st.session_state['cpu_filter']
                reg = st.session_state.get('region_id', DEFAULT_REGION)
                out = process(lr.df, chosen_binding, region=reg, service=svc, cpu_filter=cpu)
                st.session_state['result'] = out
                st.session_state['_enrich_svc'] = svc
                st.session_state['_enrich_cpu'] = cpu
                st.success(f'Enriched {len(out):,} rows')
                st.rerun()
            except Exception as ex:
                st.markdown(f'<div class="fo-alert fo-alert-err">{html.escape(str(ex))}</div>', unsafe_allow_html=True)
                log.error('process failed: %s', type(ex).__name__)

# ── Step 4: Results ───────────────────────────────────────────────────────────
df_out: pd.DataFrame | None = st.session_state.get('result')
if df_out is not None:
    with st.container(border=True):
        if (st.session_state.get('_enrich_svc') != st.session_state.get('service') or
                st.session_state.get('_enrich_cpu') != st.session_state.get('cpu_filter')):
            st.warning('Service or CPU mode changed since last enrichment — click Run enrichment to refresh.')
        (f1, f2, f3, f4) = st.columns([1, 1, 1, 3])
        with f1:
            vf_svc = st.radio(
                'View', ['all', 'ec2', 'rds'],
                format_func=lambda x: {'all': 'Both', 'ec2': 'EC2 only', 'rds': 'RDS only'}[x],
                horizontal=True, key='vf_svc',
            )
        with f2:
            st.caption('CPU (enrichment)')
            st.write(str(st.session_state.get('cpu_filter', 'both')).title())
        with f3:
            vf_os = st.text_input('OS contains', placeholder='filter…', key='vf_os')
        with f4:
            q = st.text_input('Search', placeholder='any column…', key='vf_search')

        view = df_out.copy()
        bind = st.session_state.get('binding')
        bound_inst_col = bind.instance if bind else None
        inst_col = _resolve_instance_column_for_view(view, bound_inst_col)

        def _first_col_pos(frame: pd.DataFrame, name: str | None) -> int | None:
            if not name:
                return None
            try:
                return list(frame.columns).index(name)
            except ValueError:
                return None

        ii = _first_col_pos(view, inst_col)
        if ii is not None:
            inst_ser = view.iloc[:, ii].map(_is_rds_instance_cell)
            if vf_svc == 'ec2':
                view = view[~inst_ser]
            elif vf_svc == 'rds':
                view = view[inst_ser]
        os_col_name = bind.os if bind else None
        if vf_os:
            oi = _first_col_pos(view, os_col_name)
            pi = _first_col_pos(view, 'Pricing OS')
            oidx = oi if oi is not None else pi
            if oidx is not None:
                view = view[view.iloc[:, oidx].astype(str).str.contains(vf_os, case=False, na=False, regex=False)]
        if q:
            m = pd.Series(False, index=view.index)
            for j in range(view.shape[1]):
                m |= view.iloc[:, j].astype(str).str.contains(q, case=False, na=False, regex=False)
            view = view[m]

        st.caption(f'Showing **{len(view):,}** of **{len(df_out):,}** rows')
        if view.empty and len(df_out) > 0:
            st.warning('No rows match your filters. Clear Search and OS contains, or set View to Both.')

        try:
            _strip_m = _dashboard_strip_metrics(view, inst_col)
            _render_dashboard_kpi_strip(_strip_m)
            _og_n = int(_strip_m.get('old_gen') or 0)
            if _og_n > 0 and inst_col:
                _og_detail = _old_generation_detail_table(view, inst_col)
                with st.expander(f'Older-gen resources in current view ({_og_n})', expanded=False):
                    st.caption(
                        'Instance family matches older patterns (m3–m5, c3–c5, r3–r5, t1–t3 variants), '
                        'excluding Graviton (…g). RDS db. classes use the segment after db. as the family.'
                    )
                    if _og_detail.empty:
                        st.warning('Could not list rows — check that the instance column is mapped.')
                    else:
                        st.dataframe(
                            _dataframe_for_streamlit_arrow(_og_detail),
                            **_ui_stretch_kwargs(), hide_index=True,
                            height=min(420, 36 + 28 * len(_og_detail)),
                        )
            render_kpis(kpis(view))
        except Exception as ex:
            log.warning('KPI strip skipped: %s', type(ex).__name__)

        df_display = _enriched_table_for_display(view)
        st.markdown('<div id="fo-df-anchor"></div>', unsafe_allow_html=True)
        st.caption('Table shows $ for cost/hourly columns and % for savings; exports remain numeric.')
        st.dataframe(df_display, **_ui_stretch_kwargs(), hide_index=True, height=520)

        export_df = apply_na_fill(df_out)
        reg_id = st.session_state.get('region_id', DEFAULT_REGION)
        reg_lbl = REGION_LABELS.get(reg_id, '')
        (dx1, dx2) = st.columns(2)
        with dx1:
            st.download_button(
                'Download Excel', build_excel(export_df, reg_lbl, reg_id),
                'finops_recommendations.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                **_ui_stretch_kwargs(st.download_button),
            )
        with dx2:
            _csv_df = sanitize_formula_injection_dataframe(export_df.copy())
            st.download_button(
                'Download CSV', _csv_df.to_csv(index=False).encode(),
                'finops_recommendations.csv', 'text/csv',
                **_ui_stretch_kwargs(st.download_button),
            )

elif lr is None:
    st.markdown(
        '<div class="fo-alert" style="border-color:var(--dust);">'
        'Upload a spreadsheet above and click <strong>Continue</strong> to start bulk analysis.</div>',
        unsafe_allow_html=True,
    )
elif not binding_ready:
    st.markdown(
        '<div class="fo-alert" style="border-color:var(--dust);">'
        'Complete column mapping (step 2) then run enrichment (step 3).</div>',
        unsafe_allow_html=True,
    )

