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


# ── Size fallback (some families don't offer nano/micro/small) ─────────────

SIZE_FALLBACK: dict[str, str] = {
    'nano': 'medium',
    'micro': 'medium',
    'small': 'medium',
}


def _resolve_size(family: str, size: str, price_data: dict[str, float]) -> str | None:
    """Return *size* if family.size exists in pricing, else try fallback."""
    candidate = f'{family}.{size}'
    if candidate in price_data:
        return size
    # Try common fallback: nano/micro/small -> medium
    fb = SIZE_FALLBACK.get(size)
    if fb and f'{family}.{fb}' in price_data:
        return fb
    return None


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

        resolved = _resolve_size(cand_fam, size, price_data)
        if resolved is None:
            continue

        cand_instance = f'{cand_fam}.{resolved}'
        cand_price = price_data.get(cand_instance)
        if cand_price is None:
            continue

        # Only recommend if strictly cheaper than current.
        if cur_price is not None and cand_price >= cur_price:
            continue

        candidates.append((cand_instance, cand_price))

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
    cpu_filter: CPUFilterMode = 'both',
) -> dict[str, str | None]:
    """EC2 recommendations: AMD + Intel by default, price-ranked.

    Returns dict with keys: family, size, alt1, alt2.
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

    price_data = EC2_ONDEMAND_BY_REGION_OS.get(_REF_REGION, {}).get(_REF_OS, {})
    allowed = _ec2_allowed_archs(cpu_filter)
    candidates = _find_alternatives(family, size, allowed, price_data)
    alt1, alt2 = _pick_top_two(candidates)
    result['alt1'] = alt1
    result['alt2'] = alt2
    return result
