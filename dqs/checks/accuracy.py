"""Accuracy checks (IDs 25–27)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class SourceToTargetReconciliation(BaseCheck):
    """Check 25 — Target totals reconcile to source/control totals."""

    check_id = 25
    name = "Source-to-Target Reconciliation"
    dimension = "accuracy"
    metric_name = "reconciliation_variance_pct"

    def _build_sql(self, config: CheckConfig) -> str:
        src_table = config.source_table
        tgt_table = config.target_table or config.table
        src_col = config.source_column or config.column
        tgt_col = config.target_column or config.column
        if not all([src_table, tgt_table, src_col, tgt_col]):
            raise ValueError(
                "Check 25 requires 'source_table', 'target_table', "
                "'source_column', 'target_column' in CheckConfig."
            )
        return (
            f"WITH src AS (SELECT SUM({src_col}) AS total FROM {src_table}), "
            f"tgt AS (SELECT SUM({tgt_col}) AS total FROM {tgt_table}) "
            f"SELECT ABS(src.total - tgt.total) / NULLIF(ABS(CAST(src.total AS FLOAT)), 0) "
            f"AS reconciliation_variance_pct FROM src, tgt"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class BusinessRuleValidation(BaseCheck):
    """Check 26 — Derived business logic holds (e.g. revenue = price × qty)."""

    check_id = 26
    name = "Business Rule Validation"
    dimension = "accuracy"
    metric_name = "business_rule_error_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        table = config.table
        price_col = config.price_column
        qty_col = config.quantity_column
        rev_col = config.revenue_column
        # Allow a generic condition override for non-revenue rules
        condition = config.condition
        if condition:
            return (
                f"SELECT COUNT(CASE WHEN NOT ({condition}) THEN 1 END) / "
                f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS business_rule_error_ratio "
                f"FROM {table}"
            )
        if not all([price_col, qty_col, rev_col]):
            raise ValueError(
                "Check 26 requires either 'condition' (generic rule expression) "
                "or 'price_column', 'quantity_column', 'revenue_column' in CheckConfig."
            )
        return (
            f"SELECT COUNT(CASE WHEN ABS({rev_col} - ({price_col} * {qty_col})) > 0.01 "
            f"AND {rev_col} IS NOT NULL THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS business_rule_error_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class ReferenceDataValidation(BaseCheck):
    """Check 27 — Values align to trusted reference/master data."""

    check_id = 27
    name = "Reference Data Validation"
    dimension = "accuracy"
    metric_name = "reference_mismatch_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        fact_table = config.table
        ref_table = config.reference_table
        fact_col = config.column or config.source_column
        ref_col = config.reference_column or config.column
        join_key = (config.extra or {}).get("join_key", "code")
        if not all([fact_table, ref_table, fact_col, ref_col]):
            raise ValueError(
                "Check 27 requires 'table', 'reference_table', 'column', "
                "'reference_column' in CheckConfig."
            )
        return (
            f"SELECT COUNT(CASE WHEN f.{fact_col} <> r.{ref_col} THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS reference_mismatch_ratio "
            f"FROM {fact_table} f "
            f"JOIN {ref_table} r ON f.{join_key} = r.{join_key}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
