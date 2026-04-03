"""DuckDB connector — used for synthetic clone mode and local testing."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dqs.connectors.base import BaseConnector
from dqs.config.models import ConnectorConfig


class DuckDBConnector(BaseConnector):
    """DuckDB connector (in-memory or file-based)."""

    dialect = "duckdb"

    def __init__(self, config: ConnectorConfig | None = None, path: str = ":memory:") -> None:
        self._path = (config.duckdb_path if config else None) or path
        self._conn: Any = None

    def connect(self) -> None:
        try:
            import duckdb  # type: ignore
        except ImportError as e:
            raise ImportError("duckdb is required: pip install duckdb") from e
        self._conn = duckdb.connect(self._path)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not self._conn:
            self.connect()
        return self._conn.execute(sql).df()

    def test_connection(self) -> bool:
        try:
            df = self.execute_query("SELECT 1 AS ok")
            return int(df.iloc[0]["ok"]) == 1
        except Exception:
            return False

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """Load a pandas DataFrame into DuckDB as a table (used by synthetic clone)."""
        if not self._conn:
            self.connect()
        self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self._conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
