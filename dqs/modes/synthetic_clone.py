"""
Mode B — Synthetic Clone

Steps:
  1. Connect briefly to production DW (read-only).
  2. Extract schema + statistical profile for each configured table.
  3. Disconnect from production.
  4. Generate synthetic rows matching the statistical profile (numpy + faker).
  5. Load synthetic data into in-memory DuckDB.
  6. Run all enabled checks against DuckDB.
  7. Return ScanReport flagged as is_synthetic=True.

No raw data values ever leave the production environment.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from dqs.checks import CHECK_REGISTRY
from dqs.config.defaults import MVP_PHASE
from dqs.config.models import (
    CheckConfig,
    CheckResult,
    ConnectorConfig,
    ScanConfig,
    ScanReport,
    SchemaColumnProfile,
    SchemaProfile,
)
from dqs.connectors import get_connector
from dqs.connectors.duckdb_connector import DuckDBConnector
from dqs.scorer import compute_report


def run_synthetic_clone(config: ScanConfig, pii_excluded: set | None = None) -> ScanReport:
    """
    Execute a synthetic clone scan.

    Uses config.profile_connector (or config.connector) to profile production,
    then runs checks on a synthetic DuckDB replica.
    """
    scan_start = datetime.utcnow()

    # Determine which connector to use for profiling
    profile_cfg = config.profile_connector or config.connector
    profile_connector = get_connector(profile_cfg)

    # Step 1-3: Profile all unique tables referenced in checks, then disconnect
    table_profiles: Dict[str, SchemaProfile] = {}
    table_names = _unique_tables(config)

    profile_connector.connect()
    try:
        for table in table_names:
            table_profiles[table] = profile_connector.get_schema_profile(table)
    finally:
        profile_connector.close()

    # Step 4-5: Generate synthetic data and load into DuckDB
    duck = DuckDBConnector(path=":memory:")
    duck.connect()

    for table, profile in table_profiles.items():
        synthetic_df = _generate_synthetic_data(profile, config.synthetic_row_count)
        safe_name = _safe_table_name(table)
        duck.load_dataframe(synthetic_df, safe_name)

    # Remap table names in check configs to DuckDB-safe names
    remapped_checks = _remap_check_tables(config.checks, table_names)

    # Step 6: Run checks against DuckDB
    results: List[CheckResult] = []
    checks = remapped_checks
    if config.mvp_only:
        checks = [c for c in checks if MVP_PHASE.get(c.check_id) == "mvp"]

    try:
        for check_cfg in checks:
            check = CHECK_REGISTRY.get(check_cfg.check_id)
            if check is None:
                continue
            if config.dimensions and check.dimension not in config.dimensions:
                continue
            result = check.run(duck, check_cfg, pii_excluded=pii_excluded)
            results.append(result)
    finally:
        duck.close()

    return compute_report(
        results=results,
        scan_name=config.scan_name,
        mode="synthetic",
        connector_dialect=profile_cfg.dialect,
        scan_start=scan_start,
        is_synthetic=True,
    )


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

def _generate_synthetic_data(profile: SchemaProfile, n_rows: int) -> pd.DataFrame:
    """Generate a synthetic DataFrame matching the column profiles."""
    data: Dict[str, Any] = {}
    rng = np.random.default_rng(seed=42)

    for col in profile.columns:
        data[col.name] = _generate_column(col, n_rows, rng)

    return pd.DataFrame(data)


def _generate_column(col: SchemaColumnProfile, n: int, rng: np.random.Generator) -> list:
    """Generate n synthetic values for a single column."""
    try:
        from faker import Faker  # type: ignore
        faker = Faker()
        faker.seed_instance(0)
    except ImportError:
        faker = None

    null_mask = rng.random(n) < col.null_rate

    # Use top_values distribution if available (categorical)
    if col.top_values and len(col.top_values) > 0:
        values_list = list(col.top_values.keys())
        probs = np.array(list(col.top_values.values()), dtype=float)
        probs = probs / probs.sum()
        values = rng.choice(values_list, size=n, p=probs).tolist()
    elif col.python_dtype == "int":
        mn = int(col.min_value) if col.min_value is not None else 0
        mx = int(col.max_value) if col.max_value is not None else 1_000_000
        if col.mean and col.stddev and col.stddev > 0:
            raw = rng.normal(col.mean, col.stddev, n)
            values = np.clip(raw, mn, mx).astype(int).tolist()
        else:
            values = rng.integers(mn, max(mx, mn + 1), n).tolist()
    elif col.python_dtype == "float":
        mn = float(col.min_value) if col.min_value is not None else 0.0
        mx = float(col.max_value) if col.max_value is not None else 1_000_000.0
        if col.mean and col.stddev and col.stddev > 0:
            raw = rng.normal(col.mean, col.stddev, n)
            values = np.clip(raw, mn, mx).tolist()
        else:
            values = rng.uniform(mn, mx, n).tolist()
    elif col.python_dtype == "bool":
        values = rng.choice([True, False], n).tolist()
    elif col.python_dtype == "datetime":
        try:
            base = datetime.fromisoformat(str(col.min_value)) if col.min_value else datetime(2020, 1, 1)
            end = datetime.fromisoformat(str(col.max_value)) if col.max_value else datetime.utcnow()
        except (ValueError, TypeError):
            base, end = datetime(2020, 1, 1), datetime.utcnow()
        span = max((end - base).total_seconds(), 1)
        offsets = rng.uniform(0, span, n)
        values = [base + timedelta(seconds=float(o)) for o in offsets]
    else:
        # String: use sample values if available, else faker
        if col.sample_values:
            values = rng.choice(col.sample_values, n).tolist()
        elif faker:
            values = [faker.word() for _ in range(n)]
        else:
            values = [f"val_{i}" for i in range(n)]

    # Apply nulls
    result = []
    for i, v in enumerate(values):
        result.append(None if null_mask[i] else v)

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_tables(config: ScanConfig) -> List[str]:
    """Collect all unique table names referenced across all checks."""
    tables: set[str] = set()
    table_fields = [
        "table", "source_table", "target_table", "reference_table",
        "parent_table", "child_table", "today_table", "prior_table",
        "expected_partitions_table", "actual_partitions_table",
        "metadata_catalog_table", "pipeline_runs_table",
    ]
    for chk in config.checks:
        for field in table_fields:
            val = getattr(chk, field, None)
            if val:
                tables.add(val)
    return list(tables)


def _safe_table_name(table: str) -> str:
    """Convert a fully-qualified table name to a DuckDB-safe identifier."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", table)


def _remap_check_tables(
    checks: List[CheckConfig], original_tables: List[str]
) -> List[CheckConfig]:
    """Return copies of checks with table names remapped to DuckDB-safe names."""
    mapping = {t: _safe_table_name(t) for t in original_tables}
    remapped = []
    table_fields = [
        "table", "source_table", "target_table", "reference_table",
        "parent_table", "child_table", "today_table", "prior_table",
        "expected_partitions_table", "actual_partitions_table",
        "metadata_catalog_table", "pipeline_runs_table",
    ]
    for chk in checks:
        chk_dict = chk.model_dump()
        for field in table_fields:
            val = chk_dict.get(field)
            if val and val in mapping:
                chk_dict[field] = mapping[val]
        remapped.append(CheckConfig(**chk_dict))
    return remapped
