"""Volume checks (IDs 23–24)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class RowCountVolumeChange(BaseCheck):
    """Check 23 — Unexpected day-over-day or run-over-run row count change."""

    check_id = 23
    name = "Row Count Volume Change"
    dimension = "volume"
    metric_name = "volume_change_pct"

    def _build_sql(self, config: CheckConfig) -> str:
        today_table = config.today_table or config.table
        prior_table = config.prior_table
        if not prior_table:
            # If only one table is provided, try to use a partition/date filter via condition
            if not config.condition:
                raise ValueError(
                    "Check 23 requires either 'prior_table' for a separate snapshot, "
                    "or 'today_table'/'prior_table'. "
                    "Alternatively, use 'condition' to compare date partitions."
                )
            # condition holds the date/partition logic for today vs prior
            return (
                f"WITH today AS (SELECT COUNT(*) AS cnt FROM {today_table} "
                f"WHERE {config.condition}), "
                f"prior AS (SELECT COUNT(*) AS cnt FROM {today_table} "
                f"WHERE NOT ({config.condition})) "
                f"SELECT ABS(t.cnt - p.cnt) / NULLIF(CAST(p.cnt AS FLOAT), 0) "
                f"AS volume_change_pct FROM today t, prior p"
            )
        return (
            f"WITH today AS (SELECT COUNT(*) AS cnt FROM {today_table}), "
            f"prior AS (SELECT COUNT(*) AS cnt FROM {prior_table}) "
            f"SELECT ABS(t.cnt - p.cnt) / NULLIF(CAST(p.cnt AS FLOAT), 0) "
            f"AS volume_change_pct FROM today t, prior p"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class MissingPartitionCheck(BaseCheck):
    """Check 24 — Expected partition/date slice not loaded."""

    check_id = 24
    name = "Missing Partition / Date Slice Check"
    dimension = "volume"
    metric_name = "missing_partition_count"

    def _build_sql(self, config: CheckConfig) -> str:
        expected_table = config.expected_partitions_table
        actual_table = config.actual_partitions_table or config.table
        pk = config.partition_key or config.pk_column or "partition_key"
        if not expected_table:
            raise ValueError(
                "Check 24 requires 'expected_partitions_table' (a table/CTE listing "
                "all expected partition keys) and 'actual_partitions_table'."
            )
        return (
            f"SELECT COUNT(*) AS missing_partition_count "
            f"FROM {expected_table} e "
            f"LEFT JOIN {actual_table} a ON e.{pk} = a.{pk} "
            f"WHERE a.{pk} IS NULL"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
