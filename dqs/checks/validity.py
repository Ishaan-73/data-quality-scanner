"""Validity checks (IDs 8–12)."""

from __future__ import annotations

from typing import Any, List, Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class DataTypeConformance(BaseCheck):
    """Check 8 — Values that do not conform to expected data type."""

    check_id = 8
    name = "Data Type Conformance"
    dimension = "validity"
    metric_name = "type_error_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        col = config.column
        table = config.table
        if not col:
            raise ValueError("Check 8 requires 'column' in CheckConfig.")
        # TRY_CAST / TRY_TO_NUMBER is Snowflake / DuckDB compatible.
        # For other dialects, subclass and override.
        expected_type = (config.extra or {}).get("expected_type", "DOUBLE")
        return (
            f"SELECT COUNT(CASE WHEN TRY_CAST({col} AS {expected_type}) IS NULL "
            f"AND {col} IS NOT NULL THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS type_error_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class AllowedValueDomainCheck(BaseCheck):
    """Check 9 — Values outside approved enum/domain list."""

    check_id = 9
    name = "Allowed Value / Domain Check"
    dimension = "validity"
    metric_name = "domain_violation_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        col = config.column
        table = config.table
        allowed = config.allowed_values
        if not col or not allowed:
            raise ValueError("Check 9 requires 'column' and 'allowed_values' in CheckConfig.")
        values_sql = ", ".join(f"'{v}'" for v in allowed)
        return (
            f"SELECT COUNT(CASE WHEN {col} NOT IN ({values_sql}) "
            f"AND {col} IS NOT NULL THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS domain_violation_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class FormatValidation(BaseCheck):
    """Check 10 — String format invalid (email, phone, postcode, etc.)."""

    check_id = 10
    name = "Format Validation"
    dimension = "validity"
    metric_name = "format_violation_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        col = config.column
        table = config.table
        pattern = config.pattern
        if not col or not pattern:
            raise ValueError("Check 10 requires 'column' and 'pattern' in CheckConfig.")
        # REGEXP_LIKE is widely supported (Snowflake, BigQuery, Redshift, DuckDB)
        return (
            f"SELECT COUNT(CASE WHEN {col} IS NOT NULL "
            f"AND NOT REGEXP_LIKE({col}, '{pattern}') THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS format_violation_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class RangeValidation(BaseCheck):
    """Check 11 — Numeric/date values outside expected min/max."""

    check_id = 11
    name = "Range Validation"
    dimension = "validity"
    metric_name = "range_violation_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        col = config.column
        table = config.table
        mn = config.min_value
        mx = config.max_value
        if not col or (mn is None and mx is None):
            raise ValueError(
                "Check 11 requires 'column' and at least one of 'min_value'/'max_value'."
            )
        conditions: list[str] = []
        if mn is not None:
            conditions.append(f"{col} < {mn}")
        if mx is not None:
            conditions.append(f"{col} > {mx}")
        out_of_range = " OR ".join(conditions)
        return (
            f"SELECT COUNT(CASE WHEN {col} IS NOT NULL "
            f"AND ({out_of_range}) THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS range_violation_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class NegativeValueCheck(BaseCheck):
    """Check 12 — Non-negative measures (revenue, quantity) found below zero."""

    check_id = 12
    name = "Negative Value Check"
    dimension = "validity"
    metric_name = "negative_value_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        col = config.column
        table = config.table
        if not col:
            raise ValueError("Check 12 requires 'column' in CheckConfig.")
        return (
            f"SELECT COUNT(CASE WHEN {col} < 0 THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS negative_value_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
