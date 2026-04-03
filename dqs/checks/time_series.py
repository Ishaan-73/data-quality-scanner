"""Time series checks (ID 33)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class TimestampSequenceIntegrity(BaseCheck):
    """
    Check 33 — Duplicate/missing/non-monotonic timestamps in time series.

    Counts rows where the timestamp is less than the previous row's timestamp
    within the same entity partition (i.e. non-monotonic ordering).
    """

    check_id = 33
    name = "Timestamp Sequence Integrity"
    dimension = "time_series"
    metric_name = "time_integrity_ratio"

    def _build_sql(self, config: CheckConfig) -> str:
        ts_col = config.timestamp_column
        table = config.table
        entity_col = config.entity_column
        if not ts_col:
            raise ValueError(
                "Check 33 requires 'timestamp_column' in CheckConfig."
            )
        partition_by = f"PARTITION BY {entity_col} " if entity_col else ""
        return (
            f"WITH ordered AS ("
            f"  SELECT {ts_col}, "
            f"         LAG({ts_col}) OVER ({partition_by}ORDER BY {ts_col}) AS prev_ts "
            f"  FROM {table}"
            f") "
            f"SELECT COUNT(CASE WHEN {ts_col} < prev_ts THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS time_integrity_ratio "
            f"FROM ordered"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
