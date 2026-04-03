"""Databricks SQL connector."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dqs.connectors.base import BaseConnector
from dqs.config.models import ConnectorConfig


class DatabricksConnector(BaseConnector):
    """Read-only Databricks SQL connector using databricks-sql-connector."""

    dialect = "databricks"

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._conn: Any = None

    def connect(self) -> None:
        try:
            from databricks import sql as dbsql  # type: ignore
        except ImportError as e:
            raise ImportError(
                "databricks-sql-connector is required. "
                "Install with: pip install 'dqs[databricks]'"
            ) from e

        self._conn = dbsql.connect(
            server_hostname=self._config.server_hostname or self._config.host,
            http_path=self._config.http_path,
            access_token=self._config.access_token or self._config.password,
        )

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not self._conn:
            self.connect()
        with self._conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)

    def test_connection(self) -> bool:
        try:
            df = self.execute_query("SELECT 1 AS ok")
            return int(df.iloc[0]["ok"]) == 1
        except Exception:
            return False
