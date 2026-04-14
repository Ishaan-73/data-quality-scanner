"""Metadata checks (ID 31)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class MetadataCompleteness(BaseCheck):
    """
    Check 31 — Columns/tables missing description, owner, tags, or classification.

    Queries a metadata catalog table (e.g. dqs_metadata_catalog) that tracks
    documentation completeness.

    Expected catalog schema (minimum):
        table_name      VARCHAR
        column_name     VARCHAR   (NULL for table-level metadata)
        description     VARCHAR
        owner           VARCHAR
    """

    check_id = 31
    name = "Metadata Completeness"
    dimension = "metadata"
    metric_name = "metadata_gap_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        catalog_table = config.metadata_catalog_table
        if not catalog_table:
            raise ValueError(
                "Check 31 requires 'metadata_catalog_table' in CheckConfig."
            )
        # Optional: filter to specific table if provided
        where = ""
        if config.table:
            tbl_name = config.table.split(".")[-1].strip('"').strip("`")
            where = f" WHERE LOWER(table_name) = LOWER('{tbl_name}')"
        return (
            f"SELECT COUNT(CASE WHEN description IS NULL OR owner IS NULL THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS metadata_gap_ratio "
            f"FROM {catalog_table}{where}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
