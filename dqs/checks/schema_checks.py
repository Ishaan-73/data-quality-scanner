"""Schema checks (ID 30)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class SchemaDriftDetection(BaseCheck):
    """
    Check 30 — Columns added/dropped/renamed or type changed unexpectedly.

    Compares current INFORMATION_SCHEMA columns against a baseline snapshot
    table that you maintain (e.g. dqs_schema_baseline).

    Baseline table schema:
        table_name      VARCHAR
        column_name     VARCHAR
        data_type       VARCHAR

    Returns the count of schema drift events (added + removed + type-changed columns).
    """

    check_id = 30
    name = "Schema Drift Detection"
    dimension = "schema"
    metric_name = "schema_drift_events"

    def _build_sql(self, config: CheckConfig) -> str:
        table = config.table
        baseline_table = config.reference_table
        if not baseline_table:
            raise ValueError(
                "Check 30 requires 'reference_table' (baseline schema snapshot table) "
                "in CheckConfig."
            )
        # Parse schema and table name
        parts = table.split(".")
        tbl_name = parts[-1].strip('"').strip("`")
        schema_filter = ""
        if len(parts) >= 2:
            schema = parts[-2].strip('"').strip("`")
            schema_filter = f" AND c.TABLE_SCHEMA = '{schema}'"

        return (
            f"WITH current_schema AS ("
            f"  SELECT LOWER(COLUMN_NAME) AS col_name, LOWER(DATA_TYPE) AS col_type "
            f"  FROM INFORMATION_SCHEMA.COLUMNS "
            f"  WHERE LOWER(TABLE_NAME) = LOWER('{tbl_name}'){schema_filter}"
            f"), "
            f"baseline AS ("
            f"  SELECT LOWER(column_name) AS col_name, LOWER(data_type) AS col_type "
            f"  FROM {baseline_table} "
            f"  WHERE LOWER(table_name) = LOWER('{tbl_name}')"
            f"), "
            f"added AS ("
            f"  SELECT c.col_name FROM current_schema c "
            f"  LEFT JOIN baseline b ON c.col_name = b.col_name WHERE b.col_name IS NULL"
            f"), "
            f"removed AS ("
            f"  SELECT b.col_name FROM baseline b "
            f"  LEFT JOIN current_schema c ON b.col_name = c.col_name WHERE c.col_name IS NULL"
            f"), "
            f"type_changed AS ("
            f"  SELECT c.col_name FROM current_schema c "
            f"  JOIN baseline b ON c.col_name = b.col_name WHERE c.col_type <> b.col_type"
            f") "
            f"SELECT "
            f"(SELECT COUNT(*) FROM added) + "
            f"(SELECT COUNT(*) FROM removed) + "
            f"(SELECT COUNT(*) FROM type_changed) AS schema_drift_events"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
