"""Abstract base connector for all data warehouse dialects."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

import pandas as pd

from dqs.config.models import SchemaColumnProfile, SchemaProfile


class BaseConnector(ABC):
    """Read-only connector interface used by all quality checks."""

    dialect: str = ""

    # ------------------------------------------------------------------
    # Core interface — every connector must implement these
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self) -> None:
        """Establish the underlying connection."""

    @abstractmethod
    def close(self) -> None:
        """Release connection resources."""

    @abstractmethod
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL string and return results as a DataFrame."""

    @abstractmethod
    def test_connection(self) -> bool:
        """Return True if the connection is healthy."""

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "BaseConnector":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Convenience helpers (implemented here; overrideable)
    # ------------------------------------------------------------------

    def get_table_rowcount(self, table: str) -> int:
        df = self.execute_query(f"SELECT COUNT(*) AS cnt FROM {table}")
        return int(df.iloc[0]["cnt"])

    def get_schema_profile(self, table: str, sample_size: int = 10_000) -> SchemaProfile:
        """
        Extract lightweight schema + statistical profile from a table.
        Used by synthetic clone mode to profile before disconnecting.
        """
        row_count = self.get_table_rowcount(table)

        # Get column metadata
        col_info = self._get_column_info(table)

        columns: List[SchemaColumnProfile] = []
        for col_name, col_type in col_info:
            profile = self._profile_column(table, col_name, col_type, sample_size, row_count)
            columns.append(profile)

        return SchemaProfile(table_name=table, row_count=row_count, columns=columns)

    def _get_column_info(self, table: str) -> List[tuple[str, str]]:
        """Return list of (column_name, data_type) from INFORMATION_SCHEMA."""
        # Parse schema and table name
        parts = table.split(".")
        tbl = parts[-1].strip('"').strip("`")
        schema_filter = ""
        if len(parts) >= 2:
            schema = parts[-2].strip('"').strip("`")
            schema_filter = f" AND TABLE_SCHEMA = '{schema}'"

        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{tbl}'{schema_filter}
            ORDER BY ORDINAL_POSITION
        """
        try:
            df = self.execute_query(sql)
            return list(zip(df["COLUMN_NAME"].tolist(), df["DATA_TYPE"].tolist()))
        except Exception:
            # Fallback: infer from a sample row
            df = self.execute_query(f"SELECT * FROM {table} LIMIT 1")
            return [(c, "unknown") for c in df.columns]

    def _profile_column(
        self,
        table: str,
        col: str,
        dtype: str,
        sample_size: int,
        row_count: int,
    ) -> SchemaColumnProfile:
        """Run lightweight per-column stats queries."""
        python_dtype = _map_dtype(dtype)

        # Null rate
        null_df = self.execute_query(
            f"SELECT (COUNT(*) - COUNT({col})) / NULLIF(COUNT(*), 0) AS null_rate FROM {table}"
        )
        null_rate = float(null_df.iloc[0]["null_rate"] or 0)

        # Unique count
        unique_count: Optional[int] = None
        cardinality_ratio: Optional[float] = None
        try:
            u_df = self.execute_query(f"SELECT COUNT(DISTINCT {col}) AS uc FROM {table}")
            unique_count = int(u_df.iloc[0]["uc"] or 0)
            cardinality_ratio = unique_count / max(row_count, 1)
        except Exception:
            pass

        # Top values for low-cardinality columns
        top_values: Optional[dict[Any, float]] = None
        if unique_count is not None and unique_count <= 100 and row_count > 0:
            try:
                tv_df = self.execute_query(
                    f"""SELECT {col} AS val, COUNT(*) * 1.0 / {row_count} AS freq
                        FROM {table}
                        WHERE {col} IS NOT NULL
                        GROUP BY {col}
                        ORDER BY freq DESC
                        LIMIT 50"""
                )
                top_values = dict(zip(tv_df["val"].tolist(), tv_df["freq"].tolist()))
            except Exception:
                pass

        # Numeric stats
        mean: Optional[float] = None
        stddev: Optional[float] = None
        min_value: Optional[Any] = None
        max_value: Optional[Any] = None

        if python_dtype in ("int", "float"):
            try:
                stats_df = self.execute_query(
                    f"""SELECT MIN({col}) AS mn, MAX({col}) AS mx,
                               AVG(CAST({col} AS FLOAT)) AS avg_val,
                               STDDEV(CAST({col} AS FLOAT)) AS std_val
                        FROM {table}"""
                )
                row = stats_df.iloc[0]
                min_value = row["mn"]
                max_value = row["mx"]
                mean = float(row["avg_val"]) if row["avg_val"] is not None else None
                stddev = float(row["std_val"]) if row["std_val"] is not None else None
            except Exception:
                pass

        elif python_dtype == "datetime":
            try:
                stats_df = self.execute_query(
                    f"SELECT MIN({col}) AS mn, MAX({col}) AS mx FROM {table}"
                )
                min_value = str(stats_df.iloc[0]["mn"])
                max_value = str(stats_df.iloc[0]["mx"])
            except Exception:
                pass

        # Sample values
        sample_values: List[Any] = []
        try:
            s_df = self.execute_query(
                f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL LIMIT 20"
            )
            sample_values = s_df.iloc[:, 0].tolist()
        except Exception:
            pass

        return SchemaColumnProfile(
            name=col,
            dtype=dtype,
            python_dtype=python_dtype,
            null_rate=null_rate,
            unique_count=unique_count,
            cardinality_ratio=cardinality_ratio,
            sample_values=sample_values,
            min_value=min_value,
            max_value=max_value,
            mean=mean,
            stddev=stddev,
            top_values=top_values,
            is_nullable=null_rate > 0,
        )


def _map_dtype(sql_type: str) -> str:
    """Map SQL type string to a Python type category."""
    t = sql_type.upper()
    if any(x in t for x in ("INT", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "NUMBER", "NUMERIC", "DECIMAL")):
        return "int"
    if any(x in t for x in ("FLOAT", "DOUBLE", "REAL")):
        return "float"
    if any(x in t for x in ("BOOL",)):
        return "bool"
    if any(x in t for x in ("DATE", "TIME", "TIMESTAMP", "DATETIME")):
        return "datetime"
    return "str"
