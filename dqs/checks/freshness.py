"""Freshness checks (IDs 20–22)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class FreshnessLagHours(BaseCheck):
    """Check 20 — Hours since latest update versus SLA."""

    check_id = 20
    name = "Freshness Lag Hours"
    dimension = "freshness"
    metric_name = "freshness_lag_hours"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        ts_col = config.timestamp_column
        table = config.table
        if not ts_col:
            raise ValueError("Check 20 requires 'timestamp_column' in CheckConfig.")

        if dialect == "synapse":
            # Synapse T-SQL: DATEDIFF does not quote the datepart; uses GETDATE()
            return (
                f"SELECT DATEDIFF(hour, MAX({ts_col}), GETDATE()) "
                f"AS freshness_lag_hours FROM {table}"
            )

        # DATEDIFF with 'hour' works in Snowflake, DuckDB, and Redshift.
        # BigQuery uses TIMESTAMP_DIFF; override _build_sql for BigQuery.
        return (
            f"SELECT DATEDIFF('hour', MAX({ts_col}), CURRENT_TIMESTAMP) "
            f"AS freshness_lag_hours FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class StaleTableCheck(BaseCheck):
    """Check 21 — Table has not been updated within configured window.

    For Synapse, uses ``sys.objects.modify_date`` as an alternative stale
    detection mechanism when a timestamp column is provided alongside
    system metadata.
    """

    check_id = 21
    name = "Stale Table Check"
    dimension = "freshness"
    metric_name = "stale_table_flag"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        ts_col = config.timestamp_column
        table = config.table
        stale_days = config.stale_days if config.stale_days is not None else 1
        if not ts_col:
            raise ValueError("Check 21 requires 'timestamp_column' in CheckConfig.")

        if dialect == "synapse":
            # Synapse T-SQL: DATEADD does not quote the datepart; uses GETDATE()
            return (
                f"SELECT CASE WHEN MAX({ts_col}) < "
                f"DATEADD(day, -{stale_days}, GETDATE()) "
                f"THEN 1 ELSE 0 END AS stale_table_flag FROM {table}"
            )

        return (
            f"SELECT CASE WHEN MAX({ts_col}) < "
            f"DATEADD('day', -{stale_days}, CURRENT_TIMESTAMP) "
            f"THEN 1 ELSE 0 END AS stale_table_flag FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)


class LateArrivingDataCheck(BaseCheck):
    """Check 22 — Records arrived later than allowed delay after event time."""

    check_id = 22
    name = "Late-Arriving Data Check"
    dimension = "freshness"
    metric_name = "late_arrival_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        event_col = config.event_timestamp_column
        load_col = config.load_timestamp_column or config.timestamp_column
        table = config.table
        allowed_hours = config.late_arrival_hours if config.late_arrival_hours is not None else 24
        if not event_col or not load_col:
            raise ValueError(
                "Check 22 requires 'event_timestamp_column' and "
                "'load_timestamp_column' in CheckConfig."
            )

        if dialect == "synapse":
            # Synapse T-SQL: DATEDIFF without quoted datepart
            return (
                f"SELECT COUNT(CASE WHEN DATEDIFF(hour, {event_col}, {load_col}) > {allowed_hours} "
                f"THEN 1 END) / NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS late_arrival_ratio "
                f"FROM {table}"
            )

        return (
            f"SELECT COUNT(CASE WHEN DATEDIFF('hour', {event_col}, {load_col}) > {allowed_hours} "
            f"THEN 1 END) / NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS late_arrival_ratio "
            f"FROM {table}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)

