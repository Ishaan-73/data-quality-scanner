"""Consistency checks (IDs 13–16)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class CrossFieldConsistency(BaseCheck):
    """Check 13 — Logical consistency between fields in the same row."""

    check_id = 13
    name = "Cross-Field Consistency"
    dimension = "consistency"
    metric_name = "cross_field_error_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        table = config.table
        condition = config.condition  # e.g. "end_date < start_date"
        if not condition:
            raise ValueError(
                "Check 13 requires 'condition' (the cross-field violation expression) "
                "in CheckConfig."
            )
        return (
            f"SELECT COUNT(CASE WHEN {condition} THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS cross_field_error_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class CrossTableConsistency(BaseCheck):
    """Check 14 — Same business entity attributes conflict across tables."""

    check_id = 14
    name = "Cross-Table Consistency"
    dimension = "consistency"
    metric_name = "cross_table_mismatch_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        src = config.source_table or config.table
        ref = config.reference_table
        src_col = config.source_column or config.column
        ref_col = config.reference_column or config.column
        join_key = (config.extra or {}).get("join_key", "id")
        if not all([src, ref, src_col, ref_col]):
            raise ValueError(
                "Check 14 requires 'source_table', 'reference_table', "
                "'source_column', 'reference_column' in CheckConfig."
            )
        return (
            f"SELECT COUNT(CASE WHEN s.{src_col} <> r.{ref_col} THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS cross_table_mismatch_ratio "
            f"FROM {src} s JOIN {ref} r ON s.{join_key} = r.{join_key}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class UnitCurrencyConsistency(BaseCheck):
    """Check 15 — Unexpected mix of units, currencies, or measurement scales."""

    check_id = 15
    name = "Unit/Currency Consistency"
    dimension = "consistency"
    metric_name = "unit_mismatch_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        col = config.column
        table = config.table
        allowed = config.allowed_values
        if not col or not allowed:
            raise ValueError(
                "Check 15 requires 'column' (unit/currency column) and "
                "'allowed_values' (approved units) in CheckConfig."
            )
        values_sql = ", ".join(f"'{v}'" for v in allowed)
        return (
            f"SELECT COUNT(CASE WHEN {col} NOT IN ({values_sql}) "
            f"AND {col} IS NOT NULL THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS unit_mismatch_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class StandardizationCheck(BaseCheck):
    """Check 16 — Inconsistent labels or representations for the same concept."""

    check_id = 16
    name = "Standardization Check"
    dimension = "consistency"
    metric_name = "standardization_issue_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        col = config.column
        table = config.table
        canonical = config.allowed_values  # list of canonical/standard values
        if not col or not canonical:
            raise ValueError(
                "Check 16 requires 'column' and 'allowed_values' (canonical forms) "
                "in CheckConfig."
            )
        values_sql = ", ".join(f"UPPER(TRIM('{v}'))" for v in canonical)
        return (
            f"SELECT COUNT(CASE WHEN UPPER(TRIM(CAST({col} AS VARCHAR))) "
            f"NOT IN ({values_sql}) AND {col} IS NOT NULL THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS standardization_issue_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
