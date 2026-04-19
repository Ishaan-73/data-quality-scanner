"""SQL Server on Azure connector.

Supports Azure SQL Database and SQL Server on Azure VMs via pyodbc
with ODBC Driver 18 for SQL Server.  Authentication methods:

  - service_principal: ClientSecretCredential (AAD token injection)
  - managed_identity:  ManagedIdentityCredential (AAD token injection)
  - sql_auth:          Username + password (standard SQL authentication)
  - interactive:       DefaultAzureCredential (browser / environment fallback)

T-SQL dialect is identical to Synapse for almost all DQS checks.
The dialect is registered as "sqlserver" which inherits all T-SQL
dialect patches (DATEDIFF without quotes, GETDATE(), FLOAT, PATINDEX).
"""

from __future__ import annotations

import struct
from typing import Any, Optional

import pandas as pd

from dqs.config.models import ConnectorConfig
from dqs.connectors.base import BaseConnector


class SqlServerConnector(BaseConnector):
    """Read-only Azure SQL Server connector using pyodbc."""

    dialect = "sqlserver"

    # SQL Server uses sys.dm_db_partition_stats (not the Synapse PDW DMV)
    _ROWCOUNT_SQL = (
        "SELECT SUM(p.rows) AS cnt "
        "FROM sys.dm_db_partition_stats p "
        "WHERE p.object_id = OBJECT_ID('{fqn}') "
        "AND p.index_id < 2"
    )

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._conn: Any = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Establish the ODBC connection to SQL Server."""
        try:
            import pyodbc  # type: ignore
        except ImportError as e:
            raise ImportError(
                "pyodbc is required for the SQL Server connector. "
                "Install with: pip install 'dqs[sqlserver]'"
            ) from e

        auth_config = getattr(self._config, "auth", None) or {}
        method = auth_config.get("method", "interactive")

        connection_string = self._build_connection_string()

        if method == "sql_auth":
            # Standard SQL auth — credentials in the connection string
            self._conn = pyodbc.connect(connection_string)
        else:
            # AAD token-based auth (service_principal, managed_identity, interactive)
            token = self._acquire_aad_token()
            token_bytes = self._encode_token_for_odbc(token)
            SQL_COPT_SS_ACCESS_TOKEN = 1256
            self._conn = pyodbc.connect(
                connection_string,
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_bytes},
            )

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Row count optimization using SQL Server partition stats DMV
    # ------------------------------------------------------------------

    def get_table_rowcount(self, table: str) -> int:
        """Return approximate row count from sys.dm_db_partition_stats.

        Avoids a full COUNT(*) scan on large tables. Falls back to
        COUNT(*) if the DMV query fails.
        """
        parts = table.split(".")
        tbl = parts[-1].strip('"').strip("`").strip("[").strip("]")
        schema = "dbo"
        if len(parts) >= 2:
            schema = parts[-2].strip('"').strip("`").strip("[").strip("]")
        fqn = f"{schema}.{tbl}"
        try:
            df = self.execute_query(self._ROWCOUNT_SQL.format(fqn=fqn))
            cnt = df.iloc[0]["cnt"]
            if cnt is not None:
                return int(cnt)
        except Exception:
            pass
        return super().get_table_rowcount(table)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_connection_string(self) -> str:
        host = self._config.host or ""
        port = self._config.port or 1433
        database = self._config.database or "master"

        auth_config = getattr(self._config, "auth", None) or {}
        method = auth_config.get("method", "interactive")

        base = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={host},{port};"
            f"Database={database};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )

        if method == "sql_auth":
            username = self._config.username or ""
            password = self._config.password or ""
            return base + f"UID={username};PWD={password};"

        # AAD methods — no UID/PWD, token injected via attrs_before
        return base

    def _acquire_aad_token(self) -> str:
        """Acquire an AAD access token for the Azure SQL resource."""
        try:
            from azure.identity import (  # type: ignore
                ClientSecretCredential,
                DefaultAzureCredential,
                ManagedIdentityCredential,
            )
        except ImportError as e:
            raise ImportError(
                "azure-identity is required for AAD auth. "
                "Install with: pip install 'dqs[sqlserver]'"
            ) from e

        auth_config = getattr(self._config, "auth", None) or {}
        method = auth_config.get("method", "interactive")
        scope = "https://database.windows.net/.default"

        if method == "service_principal":
            tenant_id = auth_config.get("tenant_id", "")
            client_id = auth_config.get("client_id", "")
            client_secret = auth_config.get("client_secret", "")
            if not all([tenant_id, client_id, client_secret]):
                raise ValueError(
                    "service_principal auth requires tenant_id, client_id, "
                    "and client_secret in the auth config block."
                )
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
        elif method == "managed_identity":
            credential = ManagedIdentityCredential()
        else:
            credential = DefaultAzureCredential()

        return credential.get_token(scope).token

    @staticmethod
    def _encode_token_for_odbc(token: str) -> bytes:
        """Encode an AAD bearer token into the binary format expected by
        ODBC Driver 18's SQL_COPT_SS_ACCESS_TOKEN attribute."""
        token_bytes = token.encode("UTF-16-LE")
        return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
