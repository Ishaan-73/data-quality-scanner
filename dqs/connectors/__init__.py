"""Connector factory and registry."""

from __future__ import annotations

from dqs.config.models import ConnectorConfig
from dqs.connectors.base import BaseConnector


def get_connector(config: ConnectorConfig) -> BaseConnector:
    """Instantiate and return the appropriate connector for the given dialect."""
    dialect = config.dialect

    if dialect == "snowflake":
        from dqs.connectors.snowflake_connector import SnowflakeConnector
        return SnowflakeConnector(config)

    if dialect == "bigquery":
        from dqs.connectors.bigquery_connector import BigQueryConnector
        return BigQueryConnector(config)

    if dialect == "redshift":
        from dqs.connectors.redshift_connector import RedshiftConnector
        return RedshiftConnector(config)

    if dialect == "databricks":
        from dqs.connectors.databricks_connector import DatabricksConnector
        return DatabricksConnector(config)

    if dialect == "postgres":
        from dqs.connectors.postgres_connector import PostgresConnector
        return PostgresConnector(config)

    if dialect == "duckdb":
        from dqs.connectors.duckdb_connector import DuckDBConnector
        return DuckDBConnector(config)

    if dialect == "synapse":
        from dqs.connectors.synapse_connector import SynapseConnector
        return SynapseConnector(config)

    raise ValueError(
        f"Unsupported dialect '{dialect}'. "
        "Supported: snowflake, bigquery, redshift, databricks, postgres, duckdb, synapse"
    )


__all__ = ["get_connector", "BaseConnector"]
