"""Redshift connector."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dqs.connectors.base import BaseConnector
from dqs.config.models import ConnectorConfig


class RedshiftConnector(BaseConnector):
    """Read-only Redshift connector using redshift-connector."""

    dialect = "redshift"

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._conn: Any = None

    def connect(self) -> None:
        try:
            import redshift_connector  # type: ignore
        except ImportError as e:
            raise ImportError(
                "redshift-connector is required. "
                "Install with: pip install 'dqs[redshift]'"
            ) from e

        self._conn = redshift_connector.connect(
            host=self._config.host,
            port=self._config.port or 5439,
            database=self._config.database,
            user=self._config.username,
            password=self._config.password,
        )

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        finally:
            cursor.close()

    def test_connection(self) -> bool:
        try:
            df = self.execute_query("SELECT 1 AS ok")
            return int(df.iloc[0]["ok"]) == 1
        except Exception:
            return False
