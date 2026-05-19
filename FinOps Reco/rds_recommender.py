"""Price-driven RDS recommender.

Same logic as EC2 recommender but:
- Allows Graviton + AMD + Intel by default.
- Uses the RDS pricing dataset for ranking.
- Input/output use db.* prefixed API names.
"""
from __future__ import annotations

import logging

from instance_api import canonicalize_instance_api_name
from instance_families import FAMILY_META
from pricing_engine import get_rds_hourly, RDS_MYSQL_SA_HOURLY
from recommender import (
    CPUFilterMode,
    _find_alternatives,
    _parse_family_size,
    _pick_top_two,
    _rds_allowed_archs,
    _resolve_size,
)

logger = logging.getLogger(__name__)

_REF_REGION = 'eu-west-1'


def _build_rds_price_data(region: str = _REF_REGION) -> dict[str, float]:
    """Build a family.size -> price lookup from the RDS dataset (without db. prefix)."""
    raw = RDS_MYSQL_SA_HOURLY.get(region, {})
    result: dict[str, float] = {}
    for key, price in raw.items():
        # Keys are like 'db.m6g.large' — strip db. prefix for family matching.
        if key.startswith('db.'):
            result[key[3:]] = price
        else:
            result[key] = price
    return result


def get_rds_recommendations(
    db_class: str,
    region: str = _REF_REGION,
    cpu_filter: CPUFilterMode = 'both',
) -> dict[str, str | None]:
    """RDS recommendations: Graviton + AMD + Intel by default, price-ranked for *region*.

    Returns dict with keys: family, size, alt1, alt2.
    Falls back to _REF_REGION pricing if the requested region has no data.
    """
    out: dict[str, str | None] = {'family': None, 'size': None, 'alt1': None, 'alt2': None}

    canon = canonicalize_instance_api_name(db_class)
    if not canon or not canon.startswith('db.'):
        if db_class and str(db_class).strip():
            logger.warning('Invalid RDS API Name (value omitted for security)')
        return out

    # Strip db. prefix for internal processing.
    body = canon[3:]
    parsed = _parse_family_size(body)
    if parsed is None:
        return out

    family, size = parsed
    out['family'] = family
    out['size'] = size

    if family not in FAMILY_META:
        logger.debug('Family %r not in metadata table -- no RDS recommendation.', family)
        return out

    price_data = _build_rds_price_data(region) or _build_rds_price_data(_REF_REGION)
    allowed = _rds_allowed_archs(cpu_filter)
    candidates = _find_alternatives(family, size, allowed, price_data)
    alt1, alt2 = _pick_top_two(candidates)

    # Re-add db. prefix to results.
    def _to_db(x: str | None) -> str | None:
        if not x:
            return None
        return f'db.{x}' if not x.startswith('db.') else x

    out['alt1'] = _to_db(alt1)
    out['alt2'] = _to_db(alt2)
    return out
