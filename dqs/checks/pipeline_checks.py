"""Pipeline checks (ID 32)."""

from __future__ import annotations

from typing import Optional

from dqs.checks.base import BaseCheck, _ratio_metric
from dqs.config.models import CheckConfig


class PipelineFailureIndicator(BaseCheck):
    """
    Check 32 — Recent failed loads/jobs associated with this dataset.

    Queries a pipeline run summary table that your orchestrator populates.

    Expected table schema (minimum):
        dataset_name    VARCHAR
        run_status      VARCHAR   ('success' | 'failed' | 'running')
        run_ts          TIMESTAMP
    """

    check_id = 32
    name = "Pipeline Failure Indicator"
    dimension = "pipeline"
    metric_name = "pipeline_failure_ratio"

    def _build_sql(self, config: CheckConfig, dialect: str = "") -> str:
        runs_table = config.pipeline_runs_table
        dataset_name = config.dataset_name or (
            config.table.split(".")[-1] if config.table else None
        )
        if not runs_table:
            raise ValueError(
                "Check 32 requires 'pipeline_runs_table' in CheckConfig."
            )
        where = ""
        if dataset_name:
            where = f" WHERE LOWER(dataset_name) = LOWER('{dataset_name}')"
        return (
            f"SELECT COUNT(CASE WHEN LOWER(run_status) = 'failed' THEN 1 END) / "
            f"NULLIF(CAST(COUNT(*) AS FLOAT), 0) AS pipeline_failure_ratio "
            f"FROM {runs_table}{where}"
        )

    def _extract_metric(self, df) -> Optional[float]:
        return _ratio_metric(df)
