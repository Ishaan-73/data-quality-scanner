"""Snowflake connector."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dqs.connectors.base import BaseConnector
from dqs.config.models import ConnectorConfig


class SnowflakeConnector(BaseConnector):
    """Read-only Snowflake connector using snowflake-connector-python."""

    dialect = "snowflake"

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._conn: Any = None

    def connect(self) -> None:
        try:
            import snowflake.connector  # type: ignore
        except ImportError as e:
            raise ImportError(
                "snowflake-connector-python is required. "
                "Install with: pip install 'dqs[snowflake]'"
            ) from e

        params: dict[str, Any] = {
            "account": self._config.account,
            "user": self._config.username,
            "password": self._config.password,
            "database": self._config.database,
        }
        if self._config.schema_name:
            params["schema"] = self._config.schema_name
        if self._config.warehouse:
            params["warehouse"] = self._config.warehouse
        if self._config.role:
            params["role"] = self._config.role

        self._conn = snowflake.connector.connect(**params)

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
