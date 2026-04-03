"""PostgreSQL connector (also works for local Postgres and Redshift via psycopg2)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dqs.connectors.base import BaseConnector
from dqs.config.models import ConnectorConfig


class PostgresConnector(BaseConnector):
    """Read-only PostgreSQL connector using psycopg2."""

    dialect = "postgres"

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._conn: Any = None

    def connect(self) -> None:
        try:
            import psycopg2  # type: ignore
        except ImportError as e:
            raise ImportError(
                "psycopg2-binary is required. "
                "Install with: pip install 'dqs[postgres]'"
            ) from e

        self._conn = psycopg2.connect(
            host=self._config.host or "localhost",
            port=self._config.port or 5432,
            dbname=self._config.database,
            user=self._config.username,
            password=self._config.password,
        )
        # Read-only: set transaction to read-only
        self._conn.set_session(readonly=True, autocommit=True)

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
