# FinOps EC2 Optimizer · v2.0
**Production-grade AWS EC2 cost optimisation tool for enterprise datasets.**

Release v2.0 — version 2 ready to play (April 2026).

---

## Disclaimer (mandatory for internal use)

**Costs are based on a static AWS list-price snapshot (eu-west-1). Values are indicative and must be validated against actual billing before decision-making.**

- **Pricing snapshot** (region id, source, as-of date) is shown in the app header and repeated on the **Recommendations** sheet (top rows) and **Metadata** sheet in Excel exports.
- This tool is **decision support only**; it is **not** a replacement for billing systems (CUR, Cost Explorer, invoices). **Recommendations must be validated** by engineering and finance before production changes.

---

## What it does / does not do

| Does | Does not |
|------|----------|
| Enrich uploads with indicative alt instance classes, costs, and savings % from a **local** price dataset | Call the AWS Pricing API or send your data externally |
| Preserve original columns and insert enrichment **after** the instance column | Apply enterprise discounts, RIs, or Savings Plans automatically |
| Show **N/A** when a SKU or OS is unknown | Guarantee performance or Graviton compatibility |

**How to use:** Upload CSV/Excel → choose pricing **region** and **Service** (EC2 / RDS / Both) → map columns if needed → **Run enrichment** → filter → download **Excel** (includes disclaimer + metadata) or **CSV** (data table only).

---

## Interface (guided experience)

The Streamlit UI is designed for a **calm, product-style flow** (clarity-first, similar in spirit to Apple’s marketing sites—generous whitespace, system typography, soft cards, no external font CDNs):

- **Centered layout** (~1080px) with **numbered steps**: load file → optional merge → map columns → run enrichment → results.
- **SF / system font stack** (`-apple-system`, `BlinkMacSystemFont`, `Segoe UI`, …), **antialiased** type, **pill** primary actions, **rounded** inputs and file dropzones.
- **Light** theme by default in `config.toml` with **blue** primary accent; **dark mode** follows the OS (`prefers-color-scheme`) for backgrounds and cards.
- **Trust card** surfaces pricing snapshot, disclaimer, and expectation-setting in one readable block.

---

## Quick Start

### Local (Python)
```bash
# 1. Unzip and enter the project directory
cd finops_tool

# 2. Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run
streamlit run app.py
# → Opens at http://localhost:8501
```

### Docker
```bash
# Build
docker build -t finops-ec2-optimizer .

# Run
docker run -p 8501:8501 finops-ec2-optimizer

# Access
open http://localhost:8501
```

---

## Features

| Feature | Detail |
|---|---|
| **Guided UI** | Numbered steps, hero headline, trust card, pill buttons |
| File upload | CSV, XLSX, XLS |
| **Fix Your Sheet** | Optional merge on a common ID using strict **core_id** equality only: token pattern `[a-z]+[0-9]+` (e.g. `ab101` matches `aasss_ab101`, but not `ab10` / `xy101`); one output row per primary row (`sheet_merger.py`) |
| Auto column detection | Dynamic detection (no fixed names): instance via header + AWS API value shape, OS via value patterns; manual mapping when ambiguous |
| Pricing | Local static lists, 4 regions (no live Pricing API) |
| Service modes | EC2-only, RDS-only, or both |
| CPU modes | EC2: Default (AMD + Intel), AMD-only, Intel-only. RDS: Default (Graviton + AMD + Intel), or filter by arch |
| Recommendations | Price-driven: Alt1 / Alt2 are the **cheapest** same-category, same-or-newer-gen instances from the bundled price dataset. Only shows alternatives that are strictly cheaper than the current instance. |
| Table | Colour hints on savings columns; scrollable frame |
| Filters | View EC2/RDS subset, OS text filter, column search |
| KPI tiles | Portfolio strip + row stats (incl. max **Alt1** & **Alt2** savings, older-gen expander); table **$** / **%** display |
| **Discount %** | After **Actual Cost ($)** — compares actual to **Current Price ($/hr)** (N/A / No Discount / 1 decimal); use when actual is comparable to hourly list (e.g. effective $/hr) |
| Export | Excel (disclaimer + metadata rows) + CSV (table only) |
| Scale | Tested 10k+ rows |
| Security posture | Runtime pricing/enrichment uses **local** datasets only (no live AWS Pricing API calls) |

---

## Output columns (after enrichment)

New columns are inserted **immediately after** your mapped **instance** column (original columns otherwise **unchanged**), in this order:

| Column | Meaning |
|---|---|
| `Pricing OS` | Normalized **Linux** / **Windows** for list-price bucket |
| `Actual Cost ($)` | From your file (optional column) |
| `Discount %` | `(Current Price ($/hr) − Actual Cost ($)) / Current Price ($/hr) × 100` when both are valid and **> 0**; **`No Discount`** if actual ≥ list; else **`N/A`**. Assumes actual is comparable to **hourly** list (e.g. effective $/hr). |
| `Current Price ($/hr)` | Indicative on-demand list **hourly** (eu-west-1 bundled dataset) |
| `Alt1 Instance` / `Alt2 Instance` | Suggested API names (`db.*` for RDS) |
| `Alt1 Price ($/hr)` / `Alt2 Price ($/hr)` | Indicative list **hourly** for those classes |
| `Alt1 Savings %` / `Alt2 Savings %` | vs list current hourly, or **No Savings** / **N/A** |
| `Alt2 Instance` (edge cases) | May show **`N/A (No distinct alternative)`** or **`N/A (No compatible alternative)`** (e.g. Windows vs Graviton) |

Merge **flag** columns (`FinOps_Merge_*`) appear only when using **Fix Your Sheet**.

All **original** columns stay in place around this inserted block.

Data integrity is enforced with strict runtime validation:
- original columns remain present
- original column order remains unchanged
- original values/dtypes remain unchanged
- any mismatch raises an error (fail-fast)

---

## Pricing Regions

| Region ID | Label | Default |
|---|---|---|
| `eu-west-1` | EU (Ireland) | ✅ |
| `us-east-1` | US East (N. Virginia) | |
| `ap-south-1` | Asia Pacific (Mumbai) | |
| `eu-central-1` | EU (Frankfurt) | |

All list prices are resolved from bundled local datasets. Hourly lookups are pinned to `eu-west-1` (Ireland) in the enrichment engine.

---

## File Format

Minimum required input: a detectable **instance** column (AWS API-style values like `m5.large` / `db.r5.large`).

OS column is optional:
- if detected from values (`linux` / `unix` / `ubuntu` / `rhel` / `amazon linux` / `windows` / `win`), it is used
- if missing, pricing defaults to Linux

Optional (auto-detected): Cost, Usage, Region, Account, Application

Accepts varied column names (case-insensitive); detection is dynamic:
- instance headers commonly include keywords such as `api`, `instance`, `vm`, `type`, then validated by AWS API-style values
- OS is detected primarily from cell values (e.g. Product/System columns), not fixed header names
- `cost`, `monthly cost`, `spend`, `blended cost` → Cost
- `usage`, `hours`, `running hours` → Usage
- `region`, `location`, `aws region` → Region
- `account`, `account id`, `linked account` → Account
- `application`, `service`, `workload`, `project` → Application

If columns cannot be auto-detected → manual mapping UI appears.

---

## Project Structure

```
finops_tool/
├── app.py                # Streamlit UI
├── excel_export.py       # Excel download (disclaimer + metadata rows)
├── sheet_merger.py       # Fix Your Sheet: merge two uploads on a common key
├── data_loader.py        # File ingestion + column mapping
├── processor.py          # Enrichment pipeline
├── recommender.py        # Price-driven EC2 upgrade-path logic (AMD + Intel)
├── rds_recommender.py    # Price-driven RDS recommendations (Graviton + AMD + Intel)
├── instance_families.py  # Family metadata: category, arch, generation for all 118 families
├── pricing_engine.py     # Local price datasets + disclaimer constants
├── pricing_normalize.py  # String normalisation for pricing lookups
├── instance_api.py       # Strict API Name parsing (incl. flex / metal-Nxl)
├── os_resolve.py         # Value-based OS detection
├── ec2_ondemand_public.py# Bundled EC2 on-demand prices
├── rds_mysql_sa_prices.py# Bundled RDS MySQL SA prices
├── requirements.txt
├── Dockerfile
├── tests/
├── .streamlit/config.toml
└── README.md
```

---

## Notes

- Prices are indicative on-demand list prices from local bundled datasets.
  Validate against invoices / billing systems before making purchasing decisions.
- Graviton (ARM) recommendations assume workload compatibility. Validate
  OS and runtime support before migrating.
- The tool never guesses prices: unknown instance types return N/A.
