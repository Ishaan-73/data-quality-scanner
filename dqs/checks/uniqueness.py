"""Uniqueness checks (IDs 5–7)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class PrimaryKeyDuplicateRatio(BaseCheck):
    """Check 5 — Duplicate records based on primary key."""

    check_id = 5
    name = "Primary Key Duplicate Ratio"
    dimension = "uniqueness"
    metric_name = "pk_duplicate_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        pk = config.pk_column or config.column
        if not pk:
            raise ValueError("Check 5 requires 'pk_column' in CheckConfig.")
        table = config.table
        return (
            f"SELECT (COUNT(*) - COUNT(DISTINCT {pk})) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS pk_duplicate_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class BusinessKeyDuplicateRatio(BaseCheck):
    """Check 6 — Duplicate records based on business key."""

    check_id = 6
    name = "Business Key Duplicate Ratio"
    dimension = "uniqueness"
    metric_name = "business_duplicate_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        biz_keys = config.business_key_columns or (
            [config.column] if config.column else []
        )
        if not biz_keys:
            raise ValueError("Check 6 requires 'business_key_columns' in CheckConfig.")
        table = config.table
        key_expr = ", ".join(biz_keys)
        return (
            f"SELECT (COUNT(*) - COUNT(DISTINCT ({key_expr}))) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS business_duplicate_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class NearDuplicateCandidateCheck(BaseCheck):
    """
    Check 7 — Potential fuzzy/near-duplicates.

    Full fuzzy matching (e.g. Levenshtein) is expensive and dialect-specific.
    This implementation uses a deterministic phonetic/grouping heuristic:
    records sharing the same UPPER(TRIM(column)) are flagged as near-duplicate
    candidates. For true fuzzy matching, override _build_sql in a subclass.

    This check is advisory (threshold = None), so it never fails the score.
    """

    check_id = 7
    name = "Near-Duplicate Candidate Check"
    dimension = "uniqueness"
    metric_name = "near_duplicate_count"

    def _build_sql(self, config: CheckConfig) -> str:
        cols = config.columns or ([config.column] if config.column else [])
        if not cols:
            raise ValueError("Check 7 requires 'columns' in CheckConfig.")
        table = config.table
        key_expr = ", ".join(f"UPPER(TRIM(CAST({c} AS VARCHAR)))" for c in cols)
        return (
            f"SELECT COUNT(*) AS near_duplicate_count FROM ("
            f"  SELECT {key_expr}, COUNT(*) AS grp_cnt "
            f"  FROM {table} GROUP BY {key_expr} HAVING COUNT(*) > 1"
            f") sub"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
