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

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        col = config.column
        table = config.table
        if not col:
            raise ValueError("Check 8 requires 'column' in CheckConfig.")
        # TRY_CAST is supported in Snowflake, DuckDB, and Synapse (T-SQL).
        # For Synapse, use FLOAT instead of DOUBLE (DOUBLE is not a T-SQL type).
        expected_type = (config.extra or {}).get("expected_type", "DOUBLE")
        if dialect == "synapse" and expected_type == "DOUBLE":
            expected_type = "FLOAT"
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

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
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
    """Check 10 — String format invalid (email, phone, postcode, etc.).

    For Synapse (T-SQL), REGEXP_LIKE is not available.  The Synapse branch
    uses ``PATINDEX`` with a T-SQL wildcard pattern instead.  Complex regex
    patterns are automatically converted to a simplified LIKE/PATINDEX
    approximation.
    """

    check_id = 10
    name = "Format Validation"
    dimension = "validity"
    metric_name = "format_violation_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        col = config.column
        table = config.table
        pattern = config.pattern
        if not col or not pattern:
            raise ValueError("Check 10 requires 'column' and 'pattern' in CheckConfig.")

        if dialect == "synapse":
            # Synapse / T-SQL does not support REGEXP_LIKE.
            # Use PATINDEX with a T-SQL pattern approximation.
            tsql_pattern = _regex_to_tsql_pattern(pattern)
            return (
                f"SELECT COUNT(CASE WHEN {col} IS NOT NULL "
                f"AND PATINDEX('{tsql_pattern}', {col}) = 0 THEN 1 END) / "
                f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS format_violation_ratio "
                f"FROM {table}"
            )

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

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
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

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
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


def _regex_to_tsql_pattern(regex_pattern: str) -> str:
    """Convert a simple regex pattern to a T-SQL PATINDEX wildcard pattern.

    T-SQL PATINDEX supports: ``%`` (any string), ``_`` (single char),
    ``[abc]`` (character class), ``[^abc]`` (negated class).

    This handles common patterns used in data quality checks (email, phone,
    postcode).  For very complex regex, returns a broad ``%`` pattern that
    accepts everything (the check becomes advisory).

    Examples:
        ``^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$``
        →  ``%_@_%.__%``  (simplified email pattern)
    """
    # Common email regex → simplified LIKE-compatible pattern
    if "@" in regex_pattern and ("\\." in regex_pattern or "." in regex_pattern):
        return "%_@_%.__%"

    # Phone patterns: digits, dashes, parens, spaces
    if regex_pattern.replace("\\", "").replace("d", "").replace("{", "").replace("}", "").replace(
        "-", ""
    ).replace("(", "").replace(")", "").replace("+", "").replace("[", "").replace("]", "").replace(
        "0-9", ""
    ).replace(
        "^", ""
    ).replace(
        "$", ""
    ).strip() == "":
        return "%[0-9]%"

    # Simple character-only patterns → pass through if they look PATINDEX-safe
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789%_[]^-. ")
    cleaned = regex_pattern.replace("\\", "")
    if all(c in safe_chars for c in cleaned):
        return f"%{cleaned}%"

    # Fallback: accept anything (check becomes advisory for complex regex)
    return "%"
