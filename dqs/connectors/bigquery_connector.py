"""BigQuery connector."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dqs.connectors.base import BaseConnector
from dqs.config.models import ConnectorConfig


class BigQueryConnector(BaseConnector):
    """Read-only BigQuery connector using google-cloud-bigquery."""

    dialect = "bigquery"

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._client: Any = None

    def connect(self) -> None:
        try:
            from google.cloud import bigquery  # type: ignore
            from google.oauth2 import service_account  # type: ignore
        except ImportError as e:
            raise ImportError(
                "google-cloud-bigquery is required. "
                "Install with: pip install 'dqs[bigquery]'"
            ) from e

        if self._config.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self._config.credentials_path
            )
            self._client = bigquery.Client(
                project=self._config.project, credentials=credentials
            )
        else:
            # Uses Application Default Credentials
            self._client = bigquery.Client(project=self._config.project)

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not self._client:
            self.connect()
        query_job = self._client.query(sql)
        return query_job.to_dataframe()

    def test_connection(self) -> bool:
        try:
            df = self.execute_query("SELECT 1 AS ok")
            return int(df.iloc[0]["ok"]) == 1
        except Exception:
            return False

    def _get_column_info(self, table: str) -> list[tuple[str, str]]:
        """Use BigQuery INFORMATION_SCHEMA (project.dataset.table format)."""
        parts = table.split(".")
        if len(parts) == 3:
            project, dataset, tbl = parts
        elif len(parts) == 2:
            project = self._config.project or ""
            dataset, tbl = parts
        else:
            project = self._config.project or ""
            dataset = self._config.schema_name or ""
            tbl = parts[0]

        sql = f"""
            SELECT column_name, data_type
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{tbl}'
            ORDER BY ordinal_position
        """
        try:
            df = self.execute_query(sql)
            return list(zip(df["column_name"].tolist(), df["data_type"].tolist()))
        except Exception:
            df = self.execute_query(f"SELECT * FROM `{table}` LIMIT 1")
            return [(c, "unknown") for c in df.columns]
