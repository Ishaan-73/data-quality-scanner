# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python tool suite (`dqs`) for running 33 parameterized SQL data quality checks across 13 dimensions against cloud data warehouses and lakehouses. Two operating modes:

- **Mode A — Live Scan** (`--mode live`): Direct read-only connection to production DW. Fastest path to a quality score.
- **Mode B — Synthetic Clone** (`--mode synthetic`): Extracts schema + statistical profile from production (brief, read-only), generates synthetic rows locally in DuckDB, runs all checks on the clone. No raw data values leave production.

## Commands

```bash
# Install (base deps only)
pip install -e .

# Install with a specific connector
pip install -e ".[snowflake]"   # or: bigquery, redshift, databricks, postgres, all

# List all 33 checks with weights and phases
python -m dqs.cli list-checks
python -m dqs.cli list-checks --phase mvp
python -m dqs.cli list-checks --dimension completeness

# Generate a starter scan config YAML
python -m dqs.cli init --dialect snowflake --output my_scan.yaml

# Run a scan
python -m dqs.cli scan --config config/example_scan.yaml --mode live
python -m dqs.cli scan --config config/example_scan.yaml --mode synthetic
python -m dqs.cli scan --config config/example_scan.yaml --mode live --output json
python -m dqs.cli scan --config config/example_scan.yaml --mode live --output csv --output-file results.csv
python -m dqs.cli scan --config config/example_scan.yaml --mvp-only   # run 20 MVP checks only

# Profile a single table
python -m dqs.cli profile --config my_scan.yaml --table schema.my_table

# Validate weights (should both print 1.0)
python -c "from dqs.config.defaults import DIMENSION_WEIGHTS, DIMENSION_CHECK_IDS, effective_weight; print(sum(DIMENSION_WEIGHTS.values())); print(sum(effective_weight(cid, dim) for dim, cids in DIMENSION_CHECK_IDS.items() for cid in cids))"
```

## Architecture

```
dqs/
├── cli.py              # Click CLI (scan, list-checks, init, profile)
├── scanner.py          # Entry point: scan_from_file(), scan() — dispatches to mode
├── scorer.py           # compute_report(): aggregates CheckResults → ScanReport
├── reporter.py         # render(): console (Rich), JSON, CSV output
├── config/
│   ├── models.py       # Pydantic: ConnectorConfig, CheckConfig, ScanConfig,
│   │                   #          CheckResult, DimensionResult, ScanReport,
│   │                   #          SchemaProfile, SchemaColumnProfile
│   └── defaults.py     # DIMENSION_WEIGHTS, CHECK_WITHIN_DIM_WEIGHTS,
│                       # DEFAULT_THRESHOLDS, MVP_PHASE, effective_weight()
├── connectors/
│   ├── base.py         # BaseConnector ABC: execute_query(), get_schema_profile(),
│   │                   #   get_table_rowcount(), test_connection(), _profile_column()
│   ├── __init__.py     # get_connector(config) factory
│   ├── snowflake_connector.py
│   ├── bigquery_connector.py
│   ├── redshift_connector.py
│   ├── databricks_connector.py
│   ├── postgres_connector.py
│   └── duckdb_connector.py   # also used for synthetic clone + local testing
├── checks/
│   ├── base.py         # BaseCheck ABC: run(), _build_sql(), _extract_metric(),
│   │                   #   _evaluate() — handles pass/fail + proximity scoring
│   ├── __init__.py     # CHECK_REGISTRY: {check_id: BaseCheck instance}
│   ├── completeness.py   # IDs 1–4
│   ├── uniqueness.py     # IDs 5–7
│   ├── validity.py       # IDs 8–12
│   ├── consistency.py    # IDs 13–16
│   ├── integrity.py      # IDs 17–19
│   ├── freshness.py      # IDs 20–22
│   ├── volume.py         # IDs 23–24
│   ├── accuracy.py       # IDs 25–27
│   ├── anomaly.py        # IDs 28–29
│   ├── schema_checks.py  # ID 30
│   ├── metadata_checks.py # ID 31
│   ├── pipeline_checks.py # ID 32
│   └── time_series.py     # ID 33
└── modes/
    ├── live_scan.py       # Mode A: connect → run checks → score
    └── synthetic_clone.py # Mode B: profile → disconnect → synthesize → DuckDB → run checks
```

## Weighting System

Two-level system (all weights validated to sum to 1.00):

**Level 1 — Dimension weights** (13 dimensions, sum = 1.00):
`completeness(0.20) > uniqueness(0.14) = validity(0.14) > integrity(0.12) > accuracy(0.10) > freshness(0.09) > consistency(0.08) > volume(0.05) > anomaly(0.03) > schema(0.02) > pipeline/time_series/metadata(0.01 each)`

**Level 2 — Check weights within dimension** (per-dimension, each sum = 1.00): defined in `dqs/config/defaults.py` `CHECK_WITHIN_DIM_WEIGHTS`.

**Effective weight** per check = `dim_weight × check_within_dim_weight`. Sum of all 33 effective weights = 1.00.

**Scoring**: `pass_score = 1.0` if check passes; otherwise proximity score = `max(0, 1 - (actual - threshold) / threshold)`. `overall_score = Σ(effective_weight × pass_score) × 100`.

## Adding a New Check

1. Create a class in the relevant `dqs/checks/<dimension>.py` inheriting `BaseCheck`
2. Set `check_id`, `name`, `dimension`, `metric_name`; implement `_build_sql()` and `_extract_metric()`
3. Register in `dqs/checks/__init__.py` `CHECK_REGISTRY`
4. Add its weight to `CHECK_WITHIN_DIM_WEIGHTS[dimension]` in `dqs/config/defaults.py` (ensure within-dimension weights still sum to 1.0)
5. Add default threshold to `DEFAULT_THRESHOLDS`
6. Assign phase in `MVP_PHASE`

## MVP Phase Classification

- **mvp** (phase 1, 20 checks): completeness (1–4), uniqueness (5–7), validity (8–12), integrity (17–19), freshness (20–22), volume (23–24)
- **phase2** (13 checks): consistency (13–16), accuracy (25–27), anomaly (28–29), schema (30), pipeline (32)
- **later** (2 checks): metadata (31), time_series (33)

## Key SQL Compatibility Notes

- `DATEDIFF('hour', ts1, ts2)` — Snowflake/DuckDB/Redshift. BigQuery needs `TIMESTAMP_DIFF`. Override `_build_sql()` in a BigQuery-specific subclass if needed.
- `TRY_CAST(col AS type)` — Snowflake/DuckDB. Redshift uses `pg_catalog` functions.
- `REGEXP_LIKE(col, pattern)` — Snowflake/DuckDB/Redshift. BigQuery uses `REGEXP_CONTAINS`.
- `STDDEV()` — standard across all dialects.
- `INFORMATION_SCHEMA.COLUMNS` — all dialects; BigQuery path differs (project.dataset.INFORMATION_SCHEMA.COLUMNS).
