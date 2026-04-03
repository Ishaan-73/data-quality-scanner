"""Integrity checks (IDs 17–19)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class ForeignKeyViolationCheck(BaseCheck):
    """Check 17 — Child rows without a matching parent record."""

    check_id = 17
    name = "Foreign Key Violation Check"
    dimension = "integrity"
    metric_name = "fk_violation_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        child_table = config.child_table or config.table
        parent_table = config.parent_table or config.reference_table
        child_col = config.child_column or config.column
        parent_col = config.parent_column or config.reference_column
        if not all([child_table, parent_table, child_col, parent_col]):
            raise ValueError(
                "Check 17 requires 'child_table', 'parent_table', "
                "'child_column', 'parent_column' in CheckConfig."
            )
        return (
            f"SELECT COUNT(c.*) / NULLIF(CAST((SELECT COUNT(*) FROM {child_table}) AS FLOAT), 0) "
            f"AS fk_violation_ratio "
            f"FROM {child_table} c "
            f"LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col} "
            f"WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class OrphanRecordCheck(BaseCheck):
    """Check 18 — Unlinked rows that should map to a master/dimension."""

    check_id = 18
    name = "Orphan Record Check"
    dimension = "integrity"
    metric_name = "orphan_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        child_table = config.child_table or config.table
        parent_table = config.parent_table or config.reference_table
        child_col = config.child_column or config.column
        parent_col = config.parent_column or config.reference_column
        if not all([child_table, parent_table, child_col, parent_col]):
            raise ValueError(
                "Check 18 requires 'child_table', 'parent_table', "
                "'child_column', 'parent_column' in CheckConfig."
            )
        return (
            f"SELECT COUNT(*) / NULLIF(CAST((SELECT COUNT(*) FROM {child_table}) AS FLOAT), 0) "
            f"AS orphan_ratio "
            f"FROM {child_table} c "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {parent_table} p WHERE p.{parent_col} = c.{child_col}"
            f")"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class BrokenJoinCoverageCheck(BaseCheck):
    """
    Check 19 — Expected join coverage between two tables falls below target.

    direction = 'higher_is_better': passes when join_coverage_ratio >= threshold.
    Default threshold: 0.99 (99% coverage).
    """

    check_id = 19
    name = "Broken Join Coverage"
    dimension = "integrity"
    metric_name = "join_coverage_ratio"
    direction = "higher_is_better"

    def _build_sql(self, config: CheckConfig) -> str:
        left_table = config.source_table or config.table
        right_table = config.reference_table or config.parent_table
        left_col = config.source_column or config.column
        right_col = config.reference_column or config.parent_column
        if not all([left_table, right_table, left_col, right_col]):
            raise ValueError(
                "Check 19 requires 'source_table', 'reference_table', "
                "'source_column', 'reference_column' in CheckConfig."
            )
        return (
            f"SELECT COUNT(r.{right_col}) / NULLIF(CAST(COUNT(*) AS FLOAT), 0) "
            f"AS join_coverage_ratio "
            f"FROM {left_table} l "
            f"LEFT JOIN {right_table} r ON l.{left_col} = r.{right_col}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
