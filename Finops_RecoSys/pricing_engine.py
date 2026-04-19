import logging
from datetime import datetime, timedelta, timezone
from rds_mysql_sa_prices import RDS_MYSQL_SA_HOURLY
from ec2_ondemand_public import EC2_ONDEMAND_BY_REGION_OS, EC2_ONDEMAND_PUBLIC_AS_OF

logger = logging.getLogger(__name__)
CACHE_TTL_DAYS = 7


def _manifest_as_datetime(iso_z: str) -> datetime:
    s = (iso_z or '').strip()
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return datetime.now(timezone.utc)


CACHE_METADATA: dict = {
    'last_updated': _manifest_as_datetime(EC2_ONDEMAND_PUBLIC_AS_OF),
    'source': 'AWS EC2 On-Demand — public meteredUnitMaps (per-OS SKU; same feed as aws.amazon.com)',
    'version': '2.0',
}
PRICING_SOURCE_LABEL: str = 'Bundled AWS EC2 meteredUnitMaps (on-demand)'
RDS_PRICING_NOTE: str = (
    'RDS hourly rates use a static MySQL Single-AZ–style class table where present. '
    'SQL Server / Oracle / BYOL differ — Windows-priced rows return N/A here; validate in the RDS console.'
)
DECISION_SUPPORT_NOTE: str = 'This tool is for decision support only. It is not a replacement for billing systems. Recommendations and indicative costs must be validated against actual invoices and engineering constraints before changes to production.'
DEFAULT_REGION = 'eu-west-1'
# On-demand hourly list lookups always use Ireland (matches AWS console for EU (Ireland)); UI region is for workflow only.
PRICING_LOOKUP_REGION: str = 'eu-west-1'
SUPPORTED_REGIONS: list[tuple[str, str]] = [('eu-west-1', 'EU (Ireland)'), ('us-east-1', 'US East (N. Virginia)'), ('ap-south-1', 'Asia Pacific (Mumbai)'), ('eu-central-1', 'EU (Frankfurt)')]
REGION_LABELS: dict[str, str] = dict(SUPPORTED_REGIONS)
_SUPPORTED_REGION_IDS: frozenset[str] = frozenset(r for r, _ in SUPPORTED_REGIONS)


def normalize_pricing_region(region: str | None) -> str:
    """Exact AWS region id (e.g. eu-west-1). Unsupported or blank → DEFAULT_REGION."""
    r = (region if region is not None else DEFAULT_REGION).strip().lower()
    return r if r in _SUPPORTED_REGION_IDS else DEFAULT_REGION

# Normalize free-text / engine labels to a pricing SKU bucket present in EC2_ONDEMAND_BY_REGION_OS
OS_ALIASES: dict[str, str] = {
    'amazon linux': 'linux',
    'amazon linux 2': 'linux',
    'al2': 'linux',
    'al2023': 'linux',
    'ubuntu': 'linux',
    'debian': 'linux',
    'linux': 'linux',
    'lin': 'linux',
    'red hat': 'rhel',
    'redhat': 'rhel',
    'red hat enterprise linux': 'rhel',
    'rhel': 'rhel',
    'win': 'windows',
    'windows': 'windows',
    'windows server': 'windows',
    'win2016': 'windows',
    'win2019': 'windows',
    'win2022': 'windows',
    'sles': 'suse',
    'suse linux': 'suse',
    'suse': 'suse',
}
OS_PRICING_KEYS: frozenset[str] = frozenset({'linux', 'windows', 'rhel', 'suse'})

# Backward compatibility: Linux on-demand only (tests and callers expecting PRICE_CACHE)
PRICE_CACHE: dict[str, dict[str, float]] = {r: EC2_ONDEMAND_BY_REGION_OS[r]['linux'].copy() for r in EC2_ONDEMAND_BY_REGION_OS}


def cost_disclaimer_text(pricing_region_id: str) -> str:
    rid = (pricing_region_id or DEFAULT_REGION).strip().lower()
    friendly = REGION_LABELS.get(rid, rid)
    ph = REGION_LABELS.get(PRICING_LOOKUP_REGION, PRICING_LOOKUP_REGION)
    return (
        f'Indicative hourly rates use AWS public on-demand SKUs for {ph} ({PRICING_LOOKUP_REGION}) only. '
        f'UI region ({friendly}, {rid}) does not change those lookups. '
        'Values are not invoices — validate against actual billing before decisions.'
    )


COST_DISCLAIMER_TEXT: str = cost_disclaimer_text(DEFAULT_REGION)


def _last_updated_utc() -> datetime:
    lu = CACHE_METADATA['last_updated']
    if lu.tzinfo is None:
        return lu.replace(tzinfo=timezone.utc)
    return lu.astimezone(timezone.utc)


def format_pricing_snapshot_line(pricing_region_id: str) -> str:
    rid = (pricing_region_id or DEFAULT_REGION).strip().lower()
    as_of = _last_updated_utc().strftime('%Y-%m-%d')
    return f'Pricing Snapshot: {rid} | Source: {PRICING_SOURCE_LABEL} | EC2 as of: {as_of} (manifest {EC2_ONDEMAND_PUBLIC_AS_OF})'


def cache_is_stale() -> bool:
    return datetime.now(timezone.utc) - _last_updated_utc() > timedelta(days=CACHE_TTL_DAYS)


def cache_age_days() -> int:
    return (datetime.now(timezone.utc) - _last_updated_utc()).days


def _normalize_os_key(os: str) -> str | None:
    os_key = (os or 'linux').lower().strip()
    os_key = OS_ALIASES.get(os_key, os_key)
    return os_key if os_key in OS_PRICING_KEYS else None


def get_rds_hourly(db_class: str, region: str=DEFAULT_REGION, os: str='linux') -> float | None:
    if not db_class or not isinstance(db_class, str):
        return None
    s = db_class.strip().lower()
    if not s.startswith('db.'):
        return None
    os_key = _normalize_os_key(os)
    if os_key is None or os_key != 'linux':
        return None
    rgn = (region or DEFAULT_REGION).lower().strip()
    table = RDS_MYSQL_SA_HOURLY.get(rgn)
    if not table:
        return None
    p = table.get(s)
    if p is None:
        return None
    return round(float(p), 6)


def get_price(instance_type: str, region: str=DEFAULT_REGION, os: str='linux') -> float | None:
    if not instance_type or not isinstance(instance_type, str):
        return None
    inst_key = instance_type.lower().strip()
    # RDS DB instance classes must use get_rds_hourly — never EC2 SKU tables (avoids silent N/A / wrong routing).
    if inst_key.startswith('db.'):
        return None
    rgn_key = (region or DEFAULT_REGION).lower().strip()
    os_key = _normalize_os_key(os)
    if os_key is None:
        return None
    region_bundle = EC2_ONDEMAND_BY_REGION_OS.get(rgn_key)
    if not region_bundle:
        return None
    price_table = region_bundle.get(os_key)
    if not price_table:
        return None
    p = price_table.get(inst_key)
    if p is None:
        return None
    return round(float(p), 6)


def list_known_instances(region: str=DEFAULT_REGION) -> list[str]:
    rgn = (region or DEFAULT_REGION).lower().strip()
    bundle = EC2_ONDEMAND_BY_REGION_OS.get(rgn)
    if not bundle:
        return []
    t = bundle.get('linux') or {}
    return sorted(t.keys())


def get_supported_regions() -> list[tuple[str, str]]:
    return SUPPORTED_REGIONS
