#!/usr/bin/env python3
"""
FinOps RecoSys — Pricing Data Fetcher

Downloads on-demand pricing from AWS's public Bulk Pricing API and writes
generated_pricing.py to the project root.

Usage:
    python scripts/fetch_pricing.py                      # all regions
    python scripts/fetch_pricing.py --region eu-west-1   # one region

AWS Bulk Pricing API base:
    https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/

No AWS credentials required — public, unauthenticated API.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "generated_pricing.py"

PRICING_BASE = "https://pricing.us-east-1.amazonaws.com"

TARGET_REGIONS = [
    "us-east-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "ca-central-1",
    "ca-west-1",
    "ap-south-1",
]

REGION_NAMES: dict[str, str] = {
    "us-east-1":    "US East (N. Virginia)",
    "eu-west-1":    "Europe (Ireland)",
    "eu-west-2":    "Europe (London)",
    "eu-west-3":    "Europe (Paris)",
    "eu-central-1": "Europe (Frankfurt)",
    "eu-central-2": "Europe (Zurich)",
    "eu-north-1":   "Europe (Stockholm)",
    "eu-south-1":   "Europe (Milan)",
    "eu-south-2":   "Europe (Spain)",
    "ca-central-1": "Canada (Central)",
    "ca-west-1":    "Canada West (Calgary)",
    "ap-south-1":   "Asia Pacific (Mumbai)",
}

OS_MAP: dict[str, str] = {
    "Linux": "linux",
    "Windows": "windows",
    "RHEL": "rhel",
    "SUSE": "suse",
    "Red Hat Enterprise Linux": "rhel",
    "Red Hat Enterprise Linux with HA": "rhel_ha",
    "SUSE Linux": "suse",
    "Ubuntu Pro": "ubuntu_pro",
}

RDS_ENGINE_MAP: dict[str, str] = {
    "MySQL": "mysql",
    "PostgreSQL": "postgres",
    "MariaDB": "mariadb",
    "Oracle": "oracle",
    "SQL Server": "sqlserver",
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _fetch_raw(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "finops-recosy-fetcher/1.0", "Accept-Encoding": "gzip, deflate"})
    with urlopen(req, timeout=300) as resp:
        raw = resp.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw


def _fetch_json(url: str) -> dict:
    return json.loads(_fetch_raw(url).decode("utf-8"))


def _parse_hourly_price(terms: dict, sku: str) -> float | None:
    """Extract the first on-demand USD hourly price for a SKU from the terms block."""
    on_demand = (terms or {}).get("OnDemand", {}).get(sku)
    if not on_demand:
        return None
    offer_key = next(iter(on_demand), None)
    if not offer_key:
        return None
    dims = on_demand[offer_key].get("priceDimensions", {})
    dim_key = next(iter(dims), None)
    if not dim_key:
        return None
    usd = dims[dim_key].get("pricePerUnit", {}).get("USD")
    try:
        v = float(usd)
        return None if v != v else v  # NaN guard
    except (TypeError, ValueError):
        return None


def _parse_memory(raw: str) -> float:
    try:
        return float(raw.replace(" GiB", "").replace(",", ""))
    except (ValueError, AttributeError):
        return 0.0


# ── Service fetchers ──────────────────────────────────────────────────────────

def fetch_ec2(region: str) -> dict[str, dict]:
    """
    Returns { instance_type: { vcpu, memory, prices: { os_key: hourly_usd } } }
    Filters: Shared tenancy, Compute Instance, on-demand, no pre-installed SW.
    """
    print(f"  [EC2] Fetching {region}...", flush=True)
    url = f"{PRICING_BASE}/offers/v1.0/aws/AmazonEC2/current/{region}/index.json"
    data = _fetch_json(url)
    terms = data.get("terms", {})
    instances: dict[str, dict] = {}

    for sku, product in (data.get("products") or {}).items():
        attr = product.get("attributes", {})
        if (
            attr.get("servicecode") != "AmazonEC2"
            or product.get("productFamily") != "Compute Instance"
            or attr.get("tenancy") != "Shared"
            or attr.get("capacitystatus") != "Used"
            or (attr.get("preInstalledSw") and attr.get("preInstalledSw") != "NA")
        ):
            continue

        instance_type = attr.get("instanceType")
        os_key = OS_MAP.get(attr.get("operatingSystem", ""))
        if not instance_type or not os_key:
            continue

        price = _parse_hourly_price(terms, sku)
        if price is None or price == 0:
            continue

        if instance_type not in instances:
            instances[instance_type] = {
                "vcpu": int(attr.get("vcpu") or 0),
                "memory": _parse_memory(attr.get("memory", "0 GiB")),
                "prices": {},
            }
        instances[instance_type]["prices"][os_key] = round(price, 6)

    print(f"    → {len(instances)} instance types", flush=True)
    return instances


def fetch_rds(region: str) -> dict:
    """
    Returns { instances: { db_class: { vcpu, memory, prices: { engine: hourly } } },
              storage: { vol_type: price_per_gb_month } }
    Single-AZ only; Aurora excluded.
    """
    print(f"  [RDS] Fetching {region}...", flush=True)
    url = f"{PRICING_BASE}/offers/v1.0/aws/AmazonRDS/current/{region}/index.json"
    data = _fetch_json(url)
    terms = data.get("terms", {})
    instances: dict[str, dict] = {}
    storage: dict[str, float] = {}

    for sku, product in (data.get("products") or {}).items():
        attr = product.get("attributes", {})
        price = _parse_hourly_price(terms, sku)
        if price is None:
            continue

        db_engine = attr.get("databaseEngine", "")

        if product.get("productFamily") == "Database Instance":
            if "Aurora" in db_engine:
                continue
            engine_key = RDS_ENGINE_MAP.get(db_engine)
            instance_type = attr.get("instanceType")
            if not instance_type or not engine_key:
                continue
            if attr.get("deploymentOption") != "Single-AZ":
                continue

            if instance_type not in instances:
                instances[instance_type] = {
                    "vcpu": int(attr.get("vcpu") or 0),
                    "memory": _parse_memory(attr.get("memory", "0 GiB")),
                    "prices": {},
                }
            instances[instance_type]["prices"][engine_key] = round(price, 6)

        elif product.get("productFamily") == "Database Storage":
            vol_type = attr.get("volumeType")
            if vol_type and price > 0:
                storage[vol_type] = round(price, 6)

    print(f"    → {len(instances)} DB instance types", flush=True)
    return {"instances": instances, "storage": storage}


# ── Orchestrator ──────────────────────────────────────────────────────────────

def fetch_all(regions: list[str]) -> dict:
    all_data: dict = {
        "meta": {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "regions": {r: REGION_NAMES.get(r, r) for r in regions},
            "type": "On-Demand",
        },
        "ec2": {},
        "rds": {},
    }

    for region in regions:
        label = REGION_NAMES.get(region, region)
        print(f"\n═══ {label} ({region}) ═══", flush=True)
        for svc, fn in (("ec2", fetch_ec2), ("rds", fetch_rds)):
            try:
                all_data[svc][region] = fn(region)
            except Exception as exc:
                print(f"  ✗ {svc}: {exc}", file=sys.stderr)
                all_data[svc][region] = {"error": str(exc)}

    return all_data


def generate_module(data: dict) -> None:
    fetched_at = data["meta"]["fetched_at"]
    code = (
        f'"""Auto-generated by scripts/fetch_pricing.py at {fetched_at}\n'
        f'Do not edit manually — re-run: python scripts/fetch_pricing.py\n"""\n'
        f"from __future__ import annotations\n\n"
        f"GENERATED_AS_OF: str = {fetched_at!r}\n\n"
        f"GENERATED_PRICING: dict = {data!r}\n"
    )
    # Write atomically: temp file in the same directory then rename.
    # Prevents a corrupt/truncated file if the process is killed mid-write,
    # which would cause a SyntaxError (not ImportError) and crash the app on startup.
    tmp_fd, tmp_path = tempfile.mkstemp(dir=OUTPUT_PATH.parent, suffix=".py.tmp")
    try:
        with open(tmp_fd, "w", encoding="utf-8") as f:
            f.write(code)
        Path(tmp_path).replace(OUTPUT_PATH)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\n✓ Wrote {OUTPUT_PATH} ({size_kb:.1f} KB)", flush=True)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch AWS on-demand pricing (EC2 + RDS) from the public Bulk Pricing API")
    parser.add_argument("--region", help="Fetch a single AWS region (e.g. eu-west-1)")
    args = parser.parse_args()

    regions = [args.region] if args.region else TARGET_REGIONS

    print("╔══════════════════════════════════════════════╗")
    print("║  FinOps RecoSys — Pricing Data Fetcher      ║")
    print("║  Services: EC2, RDS                         ║")
    print("║  Source: AWS Public Bulk Pricing API         ║")
    print("║  No credentials required                    ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"\nTarget regions : {', '.join(regions)}")
    print(f"Output         : {OUTPUT_PATH}\n")

    data = fetch_all(regions)
    generate_module(data)
    print("\n✅ Pricing fetch complete!")
    print("   Run `streamlit run app.py` to start the app.\n")


if __name__ == "__main__":
    main()
