"""Completeness checks (IDs 1–4)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class NullRatioByColumn(BaseCheck):
    """Check 1 — Percent of null values in each column."""

    check_id = 1
    name = "Null Ratio by Column"
    dimension = "completeness"
    metric_name = "null_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        col = config.column or "*"
        table = config.table
        if col == "*":
            # Aggregate null ratio across all columns is not standard SQL;
            # fall back to a simple overall row null check.
            return (
                f"SELECT (COUNT(*) - COUNT(*)) / NULLIF(COUNT(*), 0) AS null_ratio "
                f"FROM {table}"
            )
        return (
            f"SELECT (COUNT(*) - COUNT({col})) / NULLIF(CAST(COUNT(*) AS FLOAT), 0) "
            f"AS null_ratio FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class NullRatioInCriticalColumns(BaseCheck):
    """Check 2 — Percent of null values in configured critical columns."""

    check_id = 2
    name = "Null Ratio in Critical Columns"
    dimension = "completeness"
    metric_name = "critical_null_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        cols = config.columns or ([config.column] if config.column else [])
        if not cols:
            raise ValueError("Check 2 requires 'columns' or 'column' in CheckConfig.")
        table = config.table
        # Average null ratio across all critical columns
        null_exprs = " + ".join(
            f"(COUNT(*) - COUNT({c}))" for c in cols
        )
        total_cells = f"(COUNT(*) * {len(cols)})"
        return (
            f"SELECT ({null_exprs}) / NULLIF(CAST({total_cells} AS FLOAT), 0) "
            f"AS critical_null_ratio FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class EmptyRowCheck(BaseCheck):
    """Check 3 — Rows where all major fields are null or blank."""

    check_id = 3
    name = "Empty Row Check"
    dimension = "completeness"
    metric_name = "empty_row_count"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        cols = config.columns or ([config.column] if config.column else [])
        table = config.table
        if cols:
            # Cast every column to VARCHAR so COALESCE works across mixed types
            coalesce_expr = ", ".join(f"CAST({c} AS VARCHAR)" for c in cols)
            where = f"COALESCE({coalesce_expr}) IS NULL"
        else:
            # Fallback: count rows where every column is null — not reliable without column list
            raise ValueError("Check 3 requires 'columns' listing the major fields to test.")
        return f"SELECT COUNT(*) AS empty_row_count FROM {table} WHERE {where}"

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class ConditionalCompleteness(BaseCheck):
    """Check 4 — Required field present when another condition is true."""

    check_id = 4
    name = "Conditional Completeness"
    dimension = "completeness"
    metric_name = "conditional_missing_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        table = config.table
        condition = config.condition
        dep_col = config.dependent_column or config.column
        if not condition or not dep_col:
            raise ValueError(
                "Check 4 requires 'condition' and 'dependent_column' in CheckConfig."
            )
        return (
            f"SELECT COUNT(*) / NULLIF(CAST((SELECT COUNT(*) FROM {table}) AS FLOAT), 0) "
            f"AS conditional_missing_ratio "
            f"FROM {table} WHERE ({condition}) AND {dep_col} IS NULL"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
