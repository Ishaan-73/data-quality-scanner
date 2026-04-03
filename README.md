# Data Quality Scanner (DQS)

A Python tool suite that connects to cloud data warehouses and runs **33 parameterized SQL quality checks** across 13 dimensions, returning a weighted **0–100 quality score**.

---

## End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  1. CONFIGURE                                                   │
│                                                                 │
│     dqs init --dialect snowflake --output my_scan.yaml         │
│                                                                 │
│     Edit my_scan.yaml — point checks at your tables/columns    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. CHOOSE A MODE                                               │
│                                                                 │
│   Mode A — Live Scan          │   Mode B — Synthetic Clone      │
│   ──────────────────          │   ─────────────────────────     │
│   Direct read-only connection │   Connect briefly → extract     │
│   to production DW.           │   schema + stats → disconnect.  │
│   Fastest path to a score.    │   Generate synthetic rows in    │
│                               │   DuckDB → run checks there.    │
│                               │   No raw data leaves prod.      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. EXECUTE CHECKS                                              │
│                                                                 │
│   For each check in your config:                               │
│                                                                 │
│   CheckConfig  ──►  _build_sql()  ──►  connector.execute()    │
│                                              │                  │
│                                              ▼                  │
│                                      _extract_metric()         │
│                                              │                  │
│                                              ▼                  │
│                                       _evaluate()              │
│                                    metric vs threshold          │
│                                    → pass_score  (0.0–1.0)     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. SCORE                                                       │
│                                                                 │
│   overall_score =                                               │
│     Σ ( dim_weight × check_within_dim_weight × pass_score )    │
│     × 100                                                       │
│                                                                 │
│   All 33 effective weights sum to exactly 1.00                 │
│   Partial credit — a near-miss scores better than a hard fail  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  5. OUTPUT                                                      │
│                                                                 │
│   --output console  →  Rich table (dimension scores + failures) │
│   --output json     →  Full structured report                   │
│   --output csv      →  One row per check (load into BI / DW)   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Mode A — Live Scan (detailed)

```
YAML config
    │
    ▼
SnowflakeConnector.connect()          ← read-only session
    │
    ├── Check 2  (Critical Null)  →  SELECT (COUNT(*) - COUNT(col)) / COUNT(*) ...
    ├── Check 5  (PK Duplicates)  →  SELECT (COUNT(*) - COUNT(DISTINCT pk)) / COUNT(*) ...
    ├── Check 9  (Domain Check)   →  SELECT COUNT(CASE WHEN col NOT IN (...)) / COUNT(*) ...
    ├── Check 17 (FK Violations)  →  LEFT JOIN parent, WHERE parent.pk IS NULL ...
    ├── Check 20 (Freshness Lag)  →  SELECT DATEDIFF('hour', MAX(updated_at), NOW()) ...
    └── ...
    │
    ▼
compute_report()
    ├── Group results by dimension
    ├── Dimension score  = weighted avg of pass_scores × 100
    └── Overall score    = Σ(weighted_scores) × 100
    │
    ▼
render()  →  console / JSON / CSV
```

## Mode B — Synthetic Clone (detailed)

```
YAML config  (mode: synthetic)
    │
    ▼
Connect to production DW (briefly, read-only)
    │
    ▼
get_schema_profile(table)  ←  no raw data extracted, only statistics:
    ├── Column names + types
    ├── Null rates
    ├── Min / max / mean / stddev
    ├── Top value distributions (for categoricals)
    └── Cardinality ratios
    │
    ▼
connector.close()  ←  production disconnected here
    │
    ▼
Generate synthetic rows  (numpy + faker, seed=42)
    ├── Numeric cols   →  rng.normal(mean, stddev) clipped to [min, max]
    ├── Categorical    →  sampled from top_values distribution
    ├── Datetime cols  →  uniform between [min_ts, max_ts]
    ├── String cols    →  faker or from sample_values
    └── Nulls          →  injected at observed null_rate
    │
    ▼
DuckDBConnector  ←  in-memory, isolated
    └── load_dataframe(synthetic_df, table_name)
    │
    ▼
Same check execution as Mode A  (all SQL runs against DuckDB)
    │
    ▼
ScanReport  (flagged: is_synthetic = True)
```

---

## Quickstart

```bash
# Install
pip install -e ".[snowflake]"   # or: bigquery, redshift, databricks, postgres, all

# Generate a starter config
python -m dqs.cli init --dialect snowflake --output my_scan.yaml

# Run a live scan
python -m dqs.cli scan --config my_scan.yaml --mode live

# Run synthetic clone (no raw data leaves production)
python -m dqs.cli scan --config my_scan.yaml --mode synthetic

# Output formats
python -m dqs.cli scan --config my_scan.yaml --output json --output-file results.json
python -m dqs.cli scan --config my_scan.yaml --output csv  --output-file results.csv

# Explore the check catalog
python -m dqs.cli list-checks
python -m dqs.cli list-checks --phase mvp
python -m dqs.cli list-checks --dimension completeness

# Run MVP checks only (20 of 33)
python -m dqs.cli scan --config my_scan.yaml --mvp-only
```

---

## Supported Connectors

| Connector | Install extra |
|---|---|
| Snowflake | `pip install -e ".[snowflake]"` |
| BigQuery | `pip install -e ".[bigquery]"` |
| Redshift | `pip install -e ".[redshift]"` |
| Databricks | `pip install -e ".[databricks]"` |
| PostgreSQL | `pip install -e ".[postgres]"` |
| DuckDB | included in base install |

---

## The 33 Quality Checks

### Phase 1 — MVP (20 checks)

| Dimension | Weight | Checks |
|---|---|---|
| **Completeness** | 20% | Null ratio by column, null ratio in critical columns, empty row check, conditional completeness |
| **Uniqueness** | 14% | PK duplicate ratio, business key duplicate ratio, near-duplicate candidates |
| **Validity** | 14% | Data type conformance, domain/enum check, format validation, range validation, negative value check |
| **Integrity** | 12% | Foreign key violations, orphan records, broken join coverage |
| **Freshness** | 9% | Freshness lag hours, stale table check, late-arriving data |
| **Volume** | 5% | Row count volume change, missing partition/date slice |

### Phase 2 (13 checks)
Consistency (4) · Accuracy (3) · Anomaly (2) · Schema drift (1) · Pipeline health (1)

### Later (2 checks)
Metadata completeness · Timestamp sequence integrity

---

## Weighting System

Two-level hierarchy — both layers independently sum to 1.00:

```
Overall Score = Σ ( Dimension Weight × Check-Within-Dimension Weight × Pass Score ) × 100
```

**Scoring is not binary.** A check that barely fails gets partial credit:

```
pass_score = 1.0                                              ← within threshold
pass_score = max(0,  1 − (actual − threshold) / threshold)   ← failing, linear decay
```

| Score | Rating | Meaning |
|---|---|---|
| 90–100 | Excellent | Production-ready. Trusted for analytics and AI. |
| 70–89 | Good | Minor issues. Fine for most analytics. |
| 50–69 | Fair | Meaningful issues. Remediation recommended. |
| 0–49 | Poor | Do not use for critical decisions. |

---

## Project Structure

```
dqs/
├── cli.py                  # scan · list-checks · init · profile
├── scanner.py              # dispatch to live or synthetic mode
├── scorer.py               # weighted aggregation → ScanReport
├── reporter.py             # console (Rich) · JSON · CSV
├── config/
│   ├── models.py           # Pydantic: ScanConfig, CheckResult, ScanReport ...
│   └── defaults.py         # weights, thresholds, MVP phase map
├── connectors/             # Snowflake · BigQuery · Redshift · Databricks · Postgres · DuckDB
├── checks/                 # 13 modules, 33 BaseCheck subclasses
└── modes/
    ├── live_scan.py        # Mode A
    └── synthetic_clone.py  # Mode B — profile → synthesize → DuckDB
```