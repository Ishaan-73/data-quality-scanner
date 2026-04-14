"""Azure Synapse Analytics connector.

Supports both Dedicated SQL Pools and Serverless SQL Pools via pyodbc
with ODBC Driver 18 for SQL Server.  Authentication is handled through
Azure Active Directory (AAD) bearer tokens obtained via azure-identity.

Three auth methods are supported:
  - service_principal: ClientSecretCredential (tenant_id, client_id, client_secret)
  - managed_identity:  ManagedIdentityCredential
  - interactive:       DefaultAzureCredential (browser / environment fallback)
"""

from __future__ import annotations

import struct
from typing import Any, Optional

import pandas as pd

from dqs.config.models import ConnectorConfig
from dqs.connectors.base import BaseConnector


class SynapseConnector(BaseConnector):
    """Read-only Azure Synapse Analytics connector using pyodbc + AAD auth."""

    dialect = "synapse"

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._conn: Any = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Establish the ODBC connection to Synapse with AAD token injection."""
        try:
            import pyodbc  # type: ignore
        except ImportError as e:
            raise ImportError(
                "pyodbc is required for the Synapse connector. "
                "Install with: pip install 'dqs[synapse]'"
            ) from e

        token = self._acquire_aad_token()
        connection_string = self._build_connection_string()

        # Encode the AAD token for pyodbc SQL_COPT_SS_ACCESS_TOKEN attribute.
        # The token must be encoded as a sequence of bytes in a specific format
        # expected by the ODBC driver (UTF-16-LE with a leading length prefix).
        token_bytes = self._encode_token_for_odbc(token)

        SQL_COPT_SS_ACCESS_TOKEN = 1256
        self._conn = pyodbc.connect(
            connection_string,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_bytes},
        )

    def close(self) -> None:
        """Release the ODBC connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL string and return results as a DataFrame."""
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
        """Return True if the connection is healthy."""
        try:
            df = self.execute_query("SELECT 1 AS ok")
            return int(df.iloc[0]["ok"]) == 1
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Overrides for Synapse-specific behavior
    # ------------------------------------------------------------------

    def get_table_rowcount(self, table: str) -> int:
        """Return approximate row count.

        For dedicated pools, uses ``sys.dm_pdw_nodes_db_partition_stats``
        which avoids a full ``COUNT(*)`` scan on large distributed tables.
        For serverless pools, falls back to standard ``COUNT(*)``.
        """
        pool_type = getattr(self._config, "pool_type", None) or "dedicated"
        if pool_type == "dedicated":
            return self._rowcount_from_dmv(table)
        return super().get_table_rowcount(table)

    def _rowcount_from_dmv(self, table: str) -> int:
        """Use partition stats DMV for fast row count on Dedicated pools."""
        parts = table.split(".")
        tbl = parts[-1].strip('"').strip("`").strip("[").strip("]")
        schema = "dbo"
        if len(parts) >= 2:
            schema = parts[-2].strip('"').strip("`").strip("[").strip("]")

        sql = (
            "SELECT SUM(row_count) AS cnt "
            "FROM sys.dm_pdw_nodes_db_partition_stats "
            "WHERE object_id = OBJECT_ID(?) "
            "AND index_id < 2 "
            "AND pdw_node_id IN ("
            "  SELECT node_id FROM sys.dm_pdw_nodes WHERE type = 'COMPUTE'"
            ")"
        )
        # OBJECT_ID requires the two-part name
        fqn = f"{schema}.{tbl}"

        # Fall back to parameterized approach via direct SQL
        # (pyodbc parameters work here because it's a system DMV)
        try:
            df = self.execute_query(
                f"SELECT ISNULL(SUM(row_count), 0) AS cnt "
                f"FROM sys.dm_pdw_nodes_db_partition_stats "
                f"WHERE object_id = OBJECT_ID('{fqn}') "
                f"AND index_id < 2 "
                f"AND pdw_node_id IN ("
                f"  SELECT node_id FROM sys.dm_pdw_nodes WHERE type = 'COMPUTE'"
                f")"
            )
            return int(df.iloc[0]["cnt"])
        except Exception:
            # Fallback to COUNT(*)
            return super().get_table_rowcount(table)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_connection_string(self) -> str:
        """Construct the ODBC connection string for Synapse."""
        workspace = self._config.workspace or ""
        database = self._config.database or "master"
        pool_type = getattr(self._config, "pool_type", None) or "dedicated"

        if pool_type == "serverless":
            server = f"{workspace}-ondemand.sql.azuresynapse.net"
        else:
            server = f"{workspace}.sql.azuresynapse.net"

        return (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={server},1433;"
            f"Database={database};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )

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
                "azure-identity is required for Synapse AAD auth. "
                "Install with: pip install 'dqs[synapse]'"
            ) from e

        auth_config = getattr(self._config, "auth", None) or {}
        method = auth_config.get("method", "interactive")

        # The resource scope for Azure SQL / Synapse
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
            # 'interactive' or any other value → DefaultAzureCredential
            credential = DefaultAzureCredential()

        token = credential.get_token(scope)
        return token.token

    @staticmethod
    def _encode_token_for_odbc(token: str) -> bytes:
        """Encode an AAD bearer token into the binary format expected by
        ODBC Driver 18's ``SQL_COPT_SS_ACCESS_TOKEN`` connection attribute.

        The format is: 4-byte little-endian length prefix, followed by the
        token string encoded as UTF-16-LE.
        """
        token_bytes = token.encode("UTF-16-LE")
        return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
