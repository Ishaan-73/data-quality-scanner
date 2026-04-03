# Data Quality Scanner (DQS)

A Python tool suite that connects to cloud data warehouses and runs 33 parameterized SQL quality checks across 13 dimensions, returning a weighted 0–100 quality score.

---

## What It Does

Given a YAML config pointing at a table (or set of tables), DQS will:

1. Connect to your data warehouse
2. Execute SQL-based quality checks against your data
3. Score each check on a 0–1 scale with partial credit
4. Aggregate into a single **0–100 quality score** weighted by research-based importance
5. Output results to console, JSON, or CSV

---

## Two Operating Modes

| | Mode A — Live Scan | Mode B — Synthetic Clone |
|---|---|---|
| **How it works** | Direct read-only connection to production DW | Extracts schema + stats from production, then disconnects. Generates synthetic data locally in DuckDB and runs checks there. |
| **Production access** | Throughout the scan | Brief profile pull only — no raw data leaves production |
| **Speed** | Fastest | Slower (synthetic generation step) |
| **Best for** | Teams comfortable with read-only prod access | Zero-risk environments, pre-sales demos, sandbox experimentation |

---

## Supported Connectors

Snowflake · BigQuery · Redshift · Databricks · PostgreSQL · DuckDB

```bash
pip install -e ".[snowflake]"   # or: bigquery, redshift, databricks, postgres, all
```

---

## The 33 Quality Checks

Organized across 13 dimensions, phased for rollout:

### Phase 1 — MVP (20 checks)

| Dimension | Checks |
|---|---|
| **Completeness** | Null ratio by column, null ratio in critical columns, empty row check, conditional completeness |
| **Uniqueness** | PK duplicate ratio, business key duplicate ratio, near-duplicate candidates |
| **Validity** | Data type conformance, domain/enum check, format validation (regex), range validation, negative value check |
| **Integrity** | Foreign key violations, orphan records, broken join coverage |
| **Freshness** | Freshness lag hours, stale table check, late-arriving data |
| **Volume** | Row count volume change, missing partition/date slice |

### Phase 2 (13 checks)
Consistency (4) · Accuracy (3) · Anomaly (2) · Schema drift (1) · Pipeline health (1)

### Later (2 checks)
Metadata completeness · Timestamp sequence integrity

---

## Weighting System

The original spec had flat weights that summed to **1.37** — making any composite score meaningless. DQS replaces this with a two-level system where all weights are independently normalized.

```
Overall Score = Σ ( Dimension Weight × Check-Within-Dimension Weight × Pass Score ) × 100
```

**Level 1 — Dimension weights** (sum = 1.00):

```
Completeness   20%   ← missing data breaks everything
Uniqueness     14%   ← duplicates silently corrupt aggregations
Validity       14%   ← type errors break pipelines
Integrity      12%   ← FK violations destroy join logic
Accuracy       10%
Freshness       9%
Consistency     8%
Volume          5%
Anomaly         3%
Schema          2%
Pipeline        1%
Time Series     1%
Metadata        1%
```

**Level 2 — Check weights within each dimension** (each sum = 1.00 per dimension). Example:

```
Completeness:   Critical Null (40%) · Null by Column (30%) · Conditional (20%) · Empty Row (10%)
Uniqueness:     PK Duplicate (55%) · Business Key (35%) · Near-Duplicate (10%)
Integrity:      FK Violation (50%) · Orphan Record (35%) · Join Coverage (15%)
```

**Effective weight** per check = dimension weight × within-dimension weight. All 33 effective weights sum to exactly **1.00**, validated programmatically on every run.

**Scoring** is not binary — a check that barely fails scores higher than one that fails badly:

```
pass_score = 1.0                                         if metric within threshold
pass_score = max(0,  1 − (actual − threshold) / threshold)  if failing (lower-is-better)
```

---

## CLI

```bash
# Generate a starter config
python -m dqs.cli init --dialect snowflake --output my_scan.yaml

# Run a live scan
python -m dqs.cli scan --config my_scan.yaml --mode live

# Run synthetic clone (no raw data leaves production)
python -m dqs.cli scan --config my_scan.yaml --mode synthetic

# Output formats
python -m dqs.cli scan --config my_scan.yaml --output json --output-file results.json
python -m dqs.cli scan --config my_scan.yaml --output csv  --output-file results.csv

# Explore checks
python -m dqs.cli list-checks
python -m dqs.cli list-checks --phase mvp
python -m dqs.cli list-checks --dimension completeness

# Run MVP checks only
python -m dqs.cli scan --config my_scan.yaml --mvp-only
```

---

## Score Interpretation

| Score | Rating | What it means |
|---|---|---|
| 90–100 | Excellent | Production-ready. Trusted for analytics and AI. |
| 70–89 | Good | Minor issues. Fine for most analytics; investigate before AI use. |
| 50–69 | Fair | Meaningful issues. Remediation recommended. |
| 0–49 | Poor | Significant problems. Do not use for critical decisions. |

---

## Project Structure

```
dqs/
├── cli.py                  # scan · list-checks · init · profile
├── scanner.py              # dispatch to live or synthetic mode
├── scorer.py               # weighted aggregation → ScanReport
├── reporter.py             # console (Rich) · JSON · CSV
├── config/
│   ├── models.py           # Pydantic: ScanConfig, CheckResult, ScanReport, ...
│   └── defaults.py         # all weights, thresholds, MVP phase map
├── connectors/             # Snowflake · BigQuery · Redshift · Databricks · Postgres · DuckDB
├── checks/                 # 13 modules, 33 BaseCheck subclasses
└── modes/
    ├── live_scan.py        # Mode A
    └── synthetic_clone.py  # Mode B — profile → synthesize → DuckDB
```

---

## Deliverables

| File | Description |
|---|---|
| `dqs/` | Full Python package — installable via `pip install -e .` |
| `config/example_scan.yaml` | Annotated example scan configuration |
| `DQS_Weights_Explainer.xlsx` | Management-ready Excel — weight rationale, before/after comparison, scoring logic, roadmap |
| `CLAUDE.md` | Developer reference — commands, architecture, how to add checks |
