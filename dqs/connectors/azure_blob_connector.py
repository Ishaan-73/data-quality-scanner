"""Azure Blob Storage / ADLS Gen2 connector.

Loads CSV or Parquet files from Azure Blob Storage or ADLS Gen2 into an
in-memory DuckDB instance, then exposes them as queryable tables.  This
enables pre-load quality gates — files can be validated before they are
ingested into Synapse by DataStream / ADF.

Supported file formats:  CSV, Parquet
Supported auth methods:  connection_string, sas_token, service_principal,
                          managed_identity, anonymous (public containers)

Usage in scan YAML
------------------
connector:
  dialect: azure_blob
  account_name: myadlsaccount
  container: raw-ingestion
  auth:
    method: service_principal
    tenant_id: "..."
    client_id: "..."
    client_secret: "..."
  # Files to load — each becomes a DuckDB table named by 'table_name'
  blobs:
    - path: ADP/2024-04-19/employees.csv
      table_name: adp_employees
      format: csv          # csv | parquet  (default: inferred from extension)
      csv_options:         # optional — passed to DuckDB read_csv_auto
        header: true
        delim: ","
    - path: FLOQAST/output/journal_entries.parquet
      table_name: floqast_journal
      format: parquet
"""

from __future__ import annotations

import io
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from dqs.config.models import ConnectorConfig
from dqs.connectors.base import BaseConnector


class AzureBlobConnector(BaseConnector):
    """Read-only connector that loads Azure Blob/ADLS files into DuckDB."""

    dialect = "azure_blob"

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._duckdb_conn: Any = None
        self._loaded_tables: List[str] = []

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Download blobs and register them as DuckDB tables."""
        try:
            import duckdb  # type: ignore
        except ImportError as e:
            raise ImportError(
                "duckdb is required for AzureBlobConnector: pip install duckdb"
            ) from e

        self._duckdb_conn = duckdb.connect(":memory:")

        extra = getattr(self._config, "extra", None) or {}
        blobs: List[Dict[str, Any]] = extra.get("blobs", [])
        if not blobs:
            # No blobs configured — connector is empty but functional
            return

        client = self._build_blob_service_client()
        container = extra.get("container") or getattr(self._config, "database", None) or ""

        for blob_spec in blobs:
            blob_path: str = blob_spec["path"]
            table_name: str = blob_spec.get("table_name") or blob_path.split("/")[-1].split(".")[0]
            fmt: str = blob_spec.get("format") or self._infer_format(blob_path)
            csv_opts: Dict[str, Any] = blob_spec.get("csv_options", {})

            df = self._download_blob_as_dataframe(client, container, blob_path, fmt, csv_opts)
            self._register_dataframe(df, table_name)
            self._loaded_tables.append(table_name)

    def close(self) -> None:
        if self._duckdb_conn:
            self._duckdb_conn.close()
            self._duckdb_conn = None
        self._loaded_tables.clear()

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not self._duckdb_conn:
            self.connect()
        return self._duckdb_conn.execute(sql).df()

    def test_connection(self) -> bool:
        try:
            df = self.execute_query("SELECT 1 AS ok")
            return int(df.iloc[0]["ok"]) == 1
        except Exception:
            return False

    def list_loaded_tables(self) -> List[str]:
        """Return the list of tables currently loaded into DuckDB."""
        return list(self._loaded_tables)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_blob_service_client(self) -> Any:
        """Construct an Azure BlobServiceClient based on configured auth."""
        try:
            from azure.storage.blob import BlobServiceClient  # type: ignore
        except ImportError as e:
            raise ImportError(
                "azure-storage-blob is required for AzureBlobConnector. "
                "Install with: pip install 'azure-storage-blob'"
            ) from e

        extra = getattr(self._config, "extra", None) or {}
        auth_config = getattr(self._config, "auth", None) or {}
        method = auth_config.get("method", "connection_string")
        account_name = extra.get("account_name") or getattr(self._config, "host", None) or ""

        if method == "connection_string":
            conn_str = auth_config.get("connection_string") or os.environ.get(
                "AZURE_STORAGE_CONNECTION_STRING", ""
            )
            if not conn_str:
                raise ValueError(
                    "connection_string auth requires 'connection_string' in auth config "
                    "or AZURE_STORAGE_CONNECTION_STRING environment variable."
                )
            return BlobServiceClient.from_connection_string(conn_str)

        if method == "sas_token":
            sas_token = auth_config.get("sas_token") or os.environ.get("AZURE_STORAGE_SAS_TOKEN", "")
            account_url = f"https://{account_name}.blob.core.windows.net"
            return BlobServiceClient(account_url=account_url, credential=sas_token)

        # AAD-based methods
        credential = self._get_aad_credential(auth_config)
        account_url = f"https://{account_name}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=credential)

    def _get_aad_credential(self, auth_config: Dict[str, Any]) -> Any:
        """Return an azure-identity credential object."""
        try:
            from azure.identity import (  # type: ignore
                ClientSecretCredential,
                DefaultAzureCredential,
                ManagedIdentityCredential,
            )
        except ImportError as e:
            raise ImportError(
                "azure-identity is required for AAD auth: pip install azure-identity"
            ) from e

        method = auth_config.get("method", "interactive")
        if method == "service_principal":
            return ClientSecretCredential(
                tenant_id=auth_config["tenant_id"],
                client_id=auth_config["client_id"],
                client_secret=auth_config["client_secret"],
            )
        if method == "managed_identity":
            return ManagedIdentityCredential()
        return DefaultAzureCredential()

    def _download_blob_as_dataframe(
        self,
        client: Any,
        container: str,
        blob_path: str,
        fmt: str,
        csv_opts: Dict[str, Any],
    ) -> pd.DataFrame:
        """Download a blob and parse it into a pandas DataFrame."""
        blob_client = client.get_blob_client(container=container, blob=blob_path)
        data = blob_client.download_blob().readall()

        if fmt == "parquet":
            return pd.read_parquet(io.BytesIO(data))

        # CSV — apply csv_options
        sep = csv_opts.get("delim", csv_opts.get("sep", ","))
        header = 0 if csv_opts.get("header", True) else None
        return pd.read_csv(io.BytesIO(data), sep=sep, header=header)

    def _register_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """Register a pandas DataFrame as a DuckDB table."""
        self._duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        # DuckDB can create a table directly from a pandas DataFrame in scope
        self._duckdb_conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM df"
        )

    @staticmethod
    def _infer_format(path: str) -> str:
        """Infer file format from path extension."""
        lower = path.lower()
        if lower.endswith(".parquet") or lower.endswith(".pq"):
            return "parquet"
        return "csv"
