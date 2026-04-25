"""Price-driven instance recommender.

Given an instance type, finds the cheapest alternative in the same workload
category at the same or newer generation, filtered by CPU architecture.

EC2 default: AMD + Intel candidates only.
RDS default: Graviton + AMD + Intel candidates.
"""
from __future__ import annotations

import logging
from typing import Literal

from instance_api import canonicalize_instance_api_name
from instance_families import FAMILY_META, Arch, FamilyInfo
from pricing_engine import EC2_ONDEMAND_BY_REGION_OS

logger = logging.getLogger(__name__)

CPUFilterMode = Literal['default', 'intel', 'amd', 'graviton', 'both']

# Reference prices for ranking (consistent across sizes).
_REF_REGION = 'eu-west-1'
_REF_OS = 'linux'

# ── Graviton detection (kept for processor.py backward compat) ─────────────

def is_graviton_family(family: str) -> bool:
    """Return True if *family* is Graviton-based per the metadata table."""
    info = FAMILY_META.get(family.lower().strip())
    if info is not None:
        return info.arch == 'graviton'
    # Fallback heuristic for families not in the table.
    fam = family.lower().strip()
    _GPU_G = frozenset({'g3', 'g3s', 'g4dn', 'g4ad', 'g5', 'g6', 'g6e', 'g5g'})
    if fam in _GPU_G:
        return False
    return fam.endswith('g') or fam.endswith('gd') or fam.endswith('gn')


# ── Size resolution ────────────────────────────────────────────────────────
# AWS size lineups differ across generations (e.g. c5 offers 9xlarge/18xlarge
# but c6a/c7i don't).  We map to the nearest LARGER available size so we never
# recommend a smaller instance than the current one.

SIZE_FALLBACK: dict[str, str] = {
    'nano': 'medium',
    'micro': 'medium',
    'small': 'medium',
}

# Canonical size order — used to find nearest-larger when exact match is absent.
_SIZE_ORDER: list[str] = [
    'nano', 'micro', 'small', 'medium', 'large',
    'xlarge', '2xlarge', '3xlarge', '4xlarge', '6xlarge',
    '8xlarge', '9xlarge', '10xlarge', '12xlarge', '16xlarge',
    '18xlarge', '24xlarge', '32xlarge', '48xlarge', '56xlarge',
    '96xlarge', '112xlarge', '224xlarge',
    'metal', 'metal-16xl', 'metal-24xl', 'metal-32xl', 'metal-48xl', 'metal-96xl',
]
_SIZE_RANK: dict[str, int] = {s: i for i, s in enumerate(_SIZE_ORDER)}


def _resolve_size_candidates(family: str, size: str, price_data: dict[str, float]) -> list[str]:
    """Return candidate sizes for *family*, ordered: exact, alias, nearest-larger, nearest-smaller.

    Returning multiple lets _find_alternatives pick the first one that is actually cheaper.
    Nearest-smaller is included so that families with changed size lineups (e.g. c5.18xlarge
    → c6a, which offers 16xlarge/24xlarge but not 18xlarge) can still produce a recommendation.
    """
    exact = f'{family}.{size}'
    if exact in price_data:
        return [size]

    fb = SIZE_FALLBACK.get(size)
    if fb and f'{family}.{fb}' in price_data:
        return [fb]

    cur_rank = _SIZE_RANK.get(size)
    if cur_rank is None:
        return []

    available = [
        (s, _SIZE_RANK[s])
        for inst in price_data
        if inst.startswith(f'{family}.')
        for s in [inst[len(family) + 1:]]
        if s in _SIZE_RANK
    ]
    if not available:
        return []

    larger = sorted([(s, r) for s, r in available if r > cur_rank], key=lambda x: x[1])
    smaller = sorted([(s, r) for s, r in available if r < cur_rank], key=lambda x: -x[1])
    return [s for s, _ in larger] + [s for s, _ in smaller]


def _resolve_size(family: str, size: str, price_data: dict[str, float]) -> str | None:
    """Return the first available size candidate (nearest-larger, then nearest-smaller)."""
    cands = _resolve_size_candidates(family, size, price_data)
    return cands[0] if cands else None


# ── Arch filter helpers ────────────────────────────────────────────────────

def _ec2_allowed_archs(cpu_filter: CPUFilterMode) -> frozenset[Arch]:
    """EC2: AMD + Intel by default; no Graviton unless explicitly requested."""
    if cpu_filter == 'intel':
        return frozenset({'intel'})
    if cpu_filter == 'amd':
        return frozenset({'amd'})
    if cpu_filter == 'graviton':
        return frozenset({'graviton'})
    # 'default' / 'both' -> AMD + Intel for EC2
    return frozenset({'amd', 'intel'})


def _rds_allowed_archs(cpu_filter: CPUFilterMode) -> frozenset[Arch]:
    """RDS: all three architectures by default."""
    if cpu_filter == 'intel':
        return frozenset({'intel'})
    if cpu_filter == 'amd':
        return frozenset({'amd'})
    if cpu_filter == 'graviton':
        return frozenset({'graviton'})
    # 'default' / 'both' -> all three for RDS
    return frozenset({'amd', 'intel', 'graviton'})


# ── Core recommendation engine ─────────────────────────────────────────────

def _parse_family_size(instance_type: str) -> tuple[str, str] | None:
    """Split 'm6a.large' -> ('m6a', 'large').  Returns None on failure."""
    if not instance_type or not isinstance(instance_type, str):
        return None
    s = instance_type.strip().lower()
    dot = s.rfind('.')
    if dot < 1:
        return None
    return (s[:dot], s[dot + 1:])


def _find_alternatives(
    family: str,
    size: str,
    allowed_archs: frozenset[Arch],
    price_data: dict[str, float],
) -> list[tuple[str, float]]:
    """Return [(instance_name, price)] sorted by price, cheapest first.

    Only includes candidates that are:
    - in the same category as *family*
    - at the same or newer generation
    - in the allowed architectures
    - a DIFFERENT family from the input
    - strictly cheaper than the current price
    """
    info = FAMILY_META.get(family)
    if info is None:
        return []

    cur_instance = f'{family}.{size}'
    cur_price = price_data.get(cur_instance)

    candidates: list[tuple[str, float]] = []

    for cand_fam, cand_info in FAMILY_META.items():
        # Same category, same or newer gen, allowed arch, different family.
        if cand_info.category != info.category:
            continue
        if cand_info.gen < info.gen:
            continue
        if cand_info.arch not in allowed_archs:
            continue
        if cand_fam == family:
            continue

        # Try all size candidates (exact, nearest-larger, nearest-smaller) and
        # take the first one that is strictly cheaper than the current price.
        for resolved in _resolve_size_candidates(cand_fam, size, price_data):
            cand_instance = f'{cand_fam}.{resolved}'
            cand_price = price_data.get(cand_instance)
            if cand_price is None:
                continue
            if cur_price is not None and cand_price >= cur_price:
                continue
            candidates.append((cand_instance, cand_price))
            break

    candidates.sort(key=lambda x: x[1])
    return candidates


def _pick_top_two(candidates: list[tuple[str, float]]) -> tuple[str | None, str | None]:
    """Pick Alt1 (cheapest) and Alt2 (next cheapest, different family)."""
    if not candidates:
        return (None, None)

    alt1 = candidates[0][0]
    alt1_fam = alt1.rsplit('.', 1)[0]

    alt2 = None
    for inst, _ in candidates[1:]:
        if inst.rsplit('.', 1)[0] != alt1_fam:
            alt2 = inst
            break

    return (alt1, alt2)


# ── Public API (backward-compatible signatures) ────────────────────────────

def get_recommendations(
    instance_type: str,
    region: str = _REF_REGION,
    cpu_filter: CPUFilterMode = 'both',
) -> dict[str, str | None]:
    """EC2 recommendations: AMD + Intel by default, price-ranked for *region*.

    Returns dict with keys: family, size, alt1, alt2.
    Falls back to _REF_REGION pricing if the requested region has no data.
    """
    result: dict[str, str | None] = {'family': None, 'size': None, 'alt1': None, 'alt2': None}

    canon = canonicalize_instance_api_name(instance_type)
    if canon is None or canon.startswith('db.'):
        return result

    parsed = _parse_family_size(canon)
    if parsed is None:
        return result

    family, size = parsed
    result['family'] = family
    result['size'] = size

    if family not in FAMILY_META:
        logger.debug('Family %r not in metadata table -- no recommendation.', family)
        return result

    price_data = (
        EC2_ONDEMAND_BY_REGION_OS.get(region, {}).get(_REF_OS)
        or EC2_ONDEMAND_BY_REGION_OS.get(_REF_REGION, {}).get(_REF_OS, {})
    )
    allowed = _ec2_allowed_archs(cpu_filter)
    candidates = _find_alternatives(family, size, allowed, price_data)
    alt1, alt2 = _pick_top_two(candidates)
    result['alt1'] = alt1
    result['alt2'] = alt2
    return result


# ── Efficiency proxy ────────────────────────────────────────────────────────

def _gen_efficiency_score(gen: int, arch: str) -> int:
    """Relative efficiency tier (0–100) — proxy based on AWS architectural generation.

    Not a benchmark — reflects expected per-generation improvement in IPC,
    memory bandwidth, and manufacturing process node.
    """
    base = min(90, max(10, (gen - 2) * 14))
    bonus = 3 if arch == 'amd' else (2 if arch == 'intel' else 0)
    return min(100, base + bonus)


def _find_alternatives_inclusive(
    family: str,
    size: str,
    allowed_archs: frozenset[Arch],
    price_data: dict[str, float],
) -> list[tuple[str, float]]:
    """Like _find_alternatives but without the must-be-cheaper price filter."""
    info = FAMILY_META.get(family)
    if info is None:
        return []
    candidates: list[tuple[str, float]] = []
    for cand_fam, cand_info in FAMILY_META.items():
        if cand_info.category != info.category:
            continue
        if cand_info.gen < info.gen:
            continue
        if cand_info.arch not in allowed_archs:
            continue
        if cand_fam == family:
            continue
        for resolved in _resolve_size_candidates(cand_fam, size, price_data):
            cand_instance = f'{cand_fam}.{resolved}'
            cand_price = price_data.get(cand_instance)
            if cand_price is not None:
                candidates.append((cand_instance, cand_price))
                break
    candidates.sort(key=lambda x: x[1])
    return candidates


def _pick_top_n(candidates: list[tuple[str, float]], n: int = 3) -> list[str]:
    """Pick top n distinct-family instances sorted by price."""
    picked: list[str] = []
    seen_fams: set[str] = set()
    for inst, _ in candidates:
        fam = inst.rsplit('.', 1)[0]
        if fam not in seen_fams:
            seen_fams.add(fam)
            picked.append(inst)
        if len(picked) >= n:
            break
    return picked


def _build_comparison_reason(
    cur_info: FamilyInfo,
    alt_info: FamilyInfo | None,
    savings_pct: float | None,
) -> str:
    if alt_info is None:
        return 'Alternative within the same workload category.'
    parts: list[str] = []
    if alt_info.gen > cur_info.gen:
        parts.append(
            f'Gen {alt_info.gen} vs Gen {cur_info.gen}: newer silicon — '
            'improved IPC, lower memory latency, and better throughput per vCPU'
        )
    elif alt_info.gen == cur_info.gen:
        parts.append(f'Same generation (Gen {alt_info.gen}) — different manufacturer')
    if alt_info.arch != cur_info.arch:
        if alt_info.arch == 'amd':
            parts.append(
                'AMD EPYC: high core density, NUMA-aware memory subsystem, '
                'competitive price-per-core at the same list tier'
            )
        elif alt_info.arch == 'intel':
            parts.append(
                'Intel Xeon: strong single-thread clock, broad ISA support '
                '(AVX-512 on Gen 6+), predictable latency profile'
            )
    else:
        arch_label = {'intel': 'Intel Xeon', 'amd': 'AMD EPYC'}.get(alt_info.arch, alt_info.arch.title())
        parts.append(f'{arch_label} — same manufacturer as your current instance')
    if savings_pct is not None:
        if savings_pct > 0.1:
            parts.append(f'{savings_pct:.1f}% lower AWS list price')
        elif savings_pct < -0.5:
            parts.append(
                f'{-savings_pct:.1f}% higher list price — newer generation trade-off; '
                'workloads with high CPU intensity typically recover cost through fewer instances'
            )
    return '. '.join(parts) + '.'


# ── Public API: EC2 quick-lookup comparison ────────────────────────────────

def get_ec2_comparison(
    instance_type: str,
    region: str = 'eu-west-1',
    os_key: str = 'linux',
) -> dict:
    """Return current instance metadata + up to 3 non-Graviton EC2 alternatives.

    Keys: 'current' (dict | None), 'alternatives' (list[dict]), 'error' (str | None).
    Never calls external APIs — uses the bundled EC2 pricing dataset.
    """
    result: dict = {'current': None, 'alternatives': [], 'error': None}

    canon = canonicalize_instance_api_name(instance_type)
    if canon is None or canon.startswith('db.'):
        result['error'] = f'"{instance_type}" is not a recognised EC2 instance type (example: m5.large).'
        return result

    parsed = _parse_family_size(canon)
    if parsed is None:
        result['error'] = f'Could not parse "{instance_type}".'
        return result

    family, size = parsed
    cur_info = FAMILY_META.get(family)
    price_data = EC2_ONDEMAND_BY_REGION_OS.get(region, {}).get(os_key, {})
    cur_price = price_data.get(canon)

    result['current'] = {
        'instance': canon,
        'family': family,
        'size': size,
        'price': cur_price,
        'arch': cur_info.arch if cur_info else 'unknown',
        'gen': cur_info.gen if cur_info else 0,
        'category': cur_info.category if cur_info else 'unknown',
        'efficiency_score': _gen_efficiency_score(cur_info.gen, cur_info.arch) if cur_info else 0,
    }

    if cur_info is None:
        result['error'] = (
            f'Instance family "{family}" is not in the pricing database. '
            'It may be a very new or uncommon family not yet in the bundled dataset.'
        )
        return result

    allowed: frozenset[Arch] = frozenset({'amd', 'intel'})  # No Graviton for EC2
    candidates = _find_alternatives_inclusive(family, size, allowed, price_data)
    top3 = _pick_top_n(candidates, 3)

    for alt_inst in top3:
        alt_fam = alt_inst.rsplit('.', 1)[0]
        alt_info = FAMILY_META.get(alt_fam)
        alt_price = price_data.get(alt_inst)

        savings_pct: float | None = None
        if cur_price is not None and alt_price is not None:
            savings_pct = (cur_price - alt_price) / cur_price * 100

        eff = _gen_efficiency_score(alt_info.gen, alt_info.arch) if alt_info else 0

        result['alternatives'].append({
            'instance': alt_inst,
            'family': alt_fam,
            'price': alt_price,
            'arch': alt_info.arch if alt_info else 'unknown',
            'gen': alt_info.gen if alt_info else 0,
            'savings_pct': savings_pct,
            'efficiency_score': eff,
            'reason': _build_comparison_reason(cur_info, alt_info, savings_pct),
        })

    if not result['alternatives']:
        result['error'] = (
            'No compatible Intel/AMD alternatives found in the same workload category '
            'at the same or newer generation for this region/OS combination.'
        )

    return result
