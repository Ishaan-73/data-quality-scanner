"""Anomaly checks (IDs 28–29)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class OutlierDetection(BaseCheck):
    """
    Check 28 — Extreme values outside expected statistical bounds.

    Uses Z-score: |value - mean| / stddev > z_threshold.
    Advisory check (threshold = None).
    """

    check_id = 28
    name = "Outlier Detection"
    dimension = "anomaly"
    metric_name = "outlier_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        col = config.metric_column or config.column
        table = config.table
        z = config.outlier_z_threshold if config.outlier_z_threshold else 3.0
        if not col:
            raise ValueError("Check 28 requires 'column' or 'metric_column' in CheckConfig.")
        # STDDEV and AVG are standard SQL aggregates
        return (
            f"WITH stats AS ("
            f"  SELECT AVG(CAST({col} AS FLOAT)) AS mean_val, "
            f"         STDDEV(CAST({col} AS FLOAT)) AS std_val "
            f"  FROM {table} WHERE {col} IS NOT NULL"
            f") "
            f"SELECT COUNT(CASE WHEN ABS(CAST({col} AS FLOAT) - s.mean_val) / "
            f"NULLIF(s.std_val, 0) > {z} THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS outlier_ratio "
            f"FROM {table}, stats s WHERE {col} IS NOT NULL"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class DistributionDriftCheck(BaseCheck):
    """
    Check 29 — Distribution materially shifts from baseline period.

    Uses a simplified mean-shift signal: compares the current period mean
    against the baseline mean as a normalised deviation.

    For production PSI/KS statistics, subclass and override _build_sql.
    This check is advisory (threshold = None).
    """

    check_id = 29
    name = "Distribution Drift Check"
    dimension = "anomaly"
    metric_name = "distribution_drift_score"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        col = config.metric_column or config.column
        table = config.table
        baseline_table = config.reference_table
        if not col:
            raise ValueError("Check 29 requires 'column' or 'metric_column' in CheckConfig.")

        if baseline_table:
            # Compare current vs baseline mean as normalised absolute difference
            return (
                f"WITH cur AS ("
                f"  SELECT AVG(CAST({col} AS FLOAT)) AS mean_val FROM {table} "
                f"  WHERE {col} IS NOT NULL"
                f"), "
                f"base AS ("
                f"  SELECT AVG(CAST({col} AS FLOAT)) AS mean_val FROM {baseline_table} "
                f"  WHERE {col} IS NOT NULL"
                f") "
                f"SELECT ABS(cur.mean_val - base.mean_val) / "
                f"NULLIF(ABS(base.mean_val), 0) AS distribution_drift_score "
                f"FROM cur, base"
            )

        # Without baseline: report stddev / mean (coefficient of variation) as drift proxy
        return (
            f"SELECT STDDEV(CAST({col} AS FLOAT)) / "
            f"NULLIF(ABS(AVG(CAST({col} AS FLOAT))), 0) AS distribution_drift_score "
            f"FROM {table} WHERE {col} IS NOT NULL"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
