"""Unit tests for the Azure Synapse Analytics connector.

All external dependencies (pyodbc, azure-identity) are mocked so that
tests run without any cloud resources or ODBC drivers installed.
"""

from __future__ import annotations

import struct
from unittest.mock import MagicMock, patch, PropertyMock
from typing import Any

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

def _make_config(**overrides: Any) -> Any:
    """Build a minimal ConnectorConfig for Synapse."""
    from dqs.config.models import ConnectorConfig

    defaults = {
        "dialect": "synapse",
        "workspace": "myworkspace",
        "database": "mydb",
        "pool_type": "dedicated",
        "auth": {
            "method": "service_principal",
            "tenant_id": "tenant-123",
            "client_id": "client-456",
            "client_secret": "secret-789",
        },
    }
    defaults.update(overrides)
    return ConnectorConfig(**defaults)


@pytest.fixture
def mock_pyodbc():
    """Mock pyodbc module."""
    with patch.dict("sys.modules", {"pyodbc": MagicMock()}) as mods:
        yield mods["pyodbc"]


@pytest.fixture
def mock_azure_identity():
    """Mock azure.identity module and its credential classes."""
    mock_module = MagicMock()

    # Create a token-like object
    mock_token = MagicMock()
    mock_token.token = "fake-aad-bearer-token-12345"

    # All credential classes return the same token
    mock_module.ClientSecretCredential.return_value.get_token.return_value = mock_token
    mock_module.ManagedIdentityCredential.return_value.get_token.return_value = mock_token
    mock_module.DefaultAzureCredential.return_value.get_token.return_value = mock_token

    with patch.dict("sys.modules", {
        "azure": MagicMock(),
        "azure.identity": mock_module,
    }):
        yield mock_module


# ---------------------------------------------------------------------------
# Test: Import & instantiation
# ---------------------------------------------------------------------------

class TestSynapseConnectorImport:
    """Verify the connector can be imported and instantiated."""

    def test_import(self):
        from dqs.connectors.synapse_connector import SynapseConnector
        assert SynapseConnector.dialect == "synapse"

    def test_factory_registration(self):
        """get_connector with dialect='synapse' returns SynapseConnector."""
        from dqs.connectors import get_connector
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        # We can't actually connect, but we can verify the type
        connector = get_connector(config)
        assert isinstance(connector, SynapseConnector)

    def test_dialect_in_config_model(self):
        """ConnectorConfig accepts 'synapse' as a valid dialect."""
        config = _make_config()
        assert config.dialect == "synapse"


# ---------------------------------------------------------------------------
# Test: Connection string construction
# ---------------------------------------------------------------------------

class TestConnectionString:
    """Verify ODBC connection string for both pool types."""

    def test_dedicated_pool_hostname(self):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(pool_type="dedicated")
        connector = SynapseConnector(config)
        conn_str = connector._build_connection_string()

        assert "myworkspace.sql.azuresynapse.net" in conn_str
        assert "-ondemand" not in conn_str
        assert "Database=mydb" in conn_str
        assert "Encrypt=yes" in conn_str

    def test_serverless_pool_hostname(self):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(pool_type="serverless")
        connector = SynapseConnector(config)
        conn_str = connector._build_connection_string()

        assert "myworkspace-ondemand.sql.azuresynapse.net" in conn_str
        assert "Database=mydb" in conn_str

    def test_default_pool_type_is_dedicated(self):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(pool_type=None)
        connector = SynapseConnector(config)
        conn_str = connector._build_connection_string()

        # When pool_type is None, defaults to dedicated
        assert "myworkspace.sql.azuresynapse.net" in conn_str
        assert "-ondemand" not in conn_str


# ---------------------------------------------------------------------------
# Test: AAD token acquisition (all three auth methods)
# ---------------------------------------------------------------------------

class TestAADTokenAcquisition:
    """Verify correct azure-identity credential is used per auth method."""

    def test_service_principal_auth(self, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(auth={
            "method": "service_principal",
            "tenant_id": "t1",
            "client_id": "c1",
            "client_secret": "s1",
        })
        connector = SynapseConnector(config)
        token = connector._acquire_aad_token()

        assert token == "fake-aad-bearer-token-12345"
        mock_azure_identity.ClientSecretCredential.assert_called_once_with(
            tenant_id="t1",
            client_id="c1",
            client_secret="s1",
        )

    def test_managed_identity_auth(self, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(auth={"method": "managed_identity"})
        connector = SynapseConnector(config)
        token = connector._acquire_aad_token()

        assert token == "fake-aad-bearer-token-12345"
        mock_azure_identity.ManagedIdentityCredential.assert_called_once()

    def test_interactive_auth(self, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(auth={"method": "interactive"})
        connector = SynapseConnector(config)
        token = connector._acquire_aad_token()

        assert token == "fake-aad-bearer-token-12345"
        mock_azure_identity.DefaultAzureCredential.assert_called_once()

    def test_default_auth_uses_interactive(self, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(auth={})
        connector = SynapseConnector(config)
        token = connector._acquire_aad_token()

        assert token == "fake-aad-bearer-token-12345"
        mock_azure_identity.DefaultAzureCredential.assert_called_once()

    def test_service_principal_missing_fields_raises(self, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(auth={
            "method": "service_principal",
            "tenant_id": "t1",
            # missing client_id and client_secret
        })
        connector = SynapseConnector(config)

        with pytest.raises(ValueError, match="service_principal auth requires"):
            connector._acquire_aad_token()

    def test_token_scope_is_azure_sql(self, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(auth={"method": "interactive"})
        connector = SynapseConnector(config)
        connector._acquire_aad_token()

        call_args = mock_azure_identity.DefaultAzureCredential.return_value.get_token.call_args
        assert call_args[0][0] == "https://database.windows.net/.default"


# ---------------------------------------------------------------------------
# Test: Token encoding for ODBC
# ---------------------------------------------------------------------------

class TestTokenEncoding:
    """Verify the binary encoding format expected by SQL_COPT_SS_ACCESS_TOKEN."""

    def test_encode_token_format(self):
        from dqs.connectors.synapse_connector import SynapseConnector

        token = "test-token"
        encoded = SynapseConnector._encode_token_for_odbc(token)

        # Should be: 4 bytes (little-endian length) + UTF-16-LE encoded token
        token_bytes = token.encode("UTF-16-LE")
        expected_len = len(token_bytes)
        assert len(encoded) == 4 + expected_len

        # Check the length prefix
        length_prefix = struct.unpack("<I", encoded[:4])[0]
        assert length_prefix == expected_len

        # Check the token content
        decoded_token = encoded[4:].decode("UTF-16-LE")
        assert decoded_token == token


# ---------------------------------------------------------------------------
# Test: connect() flow
# ---------------------------------------------------------------------------

class TestConnect:
    """Verify the full connect() path with mocked pyodbc and azure-identity."""

    def test_successful_connect(self, mock_pyodbc, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)

        # Mock pyodbc.connect
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn

        connector.connect()

        # Verify pyodbc.connect was called with correct args
        assert mock_pyodbc.connect.called
        call_args = mock_pyodbc.connect.call_args
        conn_str = call_args[0][0]

        assert "ODBC Driver 18" in conn_str
        assert "myworkspace.sql.azuresynapse.net" in conn_str
        assert "Database=mydb" in conn_str

        # Verify token was injected via attrs_before
        attrs = call_args[1]["attrs_before"]
        assert 1256 in attrs  # SQL_COPT_SS_ACCESS_TOKEN

    def test_connect_without_pyodbc_raises_import_error(self):
        """If pyodbc is not installed, connect() raises ImportError."""
        import sys
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)

        # Temporarily remove pyodbc from sys.modules if present
        with patch.dict("sys.modules", {"pyodbc": None}):
            with pytest.raises(ImportError, match="pyodbc is required"):
                connector.connect()


# ---------------------------------------------------------------------------
# Test: execute_query()
# ---------------------------------------------------------------------------

class TestExecuteQuery:
    """Verify execute_query returns a DataFrame."""

    def test_execute_returns_dataframe(self, mock_pyodbc, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)

        # Set up mock cursor
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        connector.connect()
        result = connector.execute_query("SELECT 1 AS col1, 'a' AS col2")

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["col1", "col2"]
        assert len(result) == 2
        assert result.iloc[0]["col1"] == 1

    def test_execute_auto_connects(self, mock_pyodbc, mock_azure_identity):
        """If not connected, execute_query should auto-connect."""
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)

        mock_cursor = MagicMock()
        mock_cursor.description = [("ok",)]
        mock_cursor.fetchall.return_value = [(1,)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        # Don't call connect() explicitly
        result = connector.execute_query("SELECT 1 AS ok")
        assert mock_pyodbc.connect.called
        assert result.iloc[0]["ok"] == 1


# ---------------------------------------------------------------------------
# Test: close()
# ---------------------------------------------------------------------------

class TestClose:
    """Verify close() releases the connection."""

    def test_close_calls_connection_close(self, mock_pyodbc, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)

        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn

        connector.connect()
        connector.close()

        mock_conn.close.assert_called_once()
        assert connector._conn is None

    def test_close_idempotent(self, mock_pyodbc, mock_azure_identity):
        """Calling close() when not connected does not raise."""
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)
        # Should not raise
        connector.close()


# ---------------------------------------------------------------------------
# Test: test_connection()
# ---------------------------------------------------------------------------

class TestTestConnection:
    """Verify test_connection() returns True/False."""

    def test_healthy_connection(self, mock_pyodbc, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)

        mock_cursor = MagicMock()
        mock_cursor.description = [("ok",)]
        mock_cursor.fetchall.return_value = [(1,)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        connector.connect()
        assert connector.test_connection() is True

    def test_broken_connection(self, mock_pyodbc, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config()
        connector = SynapseConnector(config)

        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("connection lost")
        mock_pyodbc.connect.return_value = mock_conn

        connector.connect()
        assert connector.test_connection() is False


# ---------------------------------------------------------------------------
# Test: get_table_rowcount() override for dedicated pools
# ---------------------------------------------------------------------------

class TestGetTableRowcount:
    """Verify DMV-based row count for dedicated, standard COUNT for serverless."""

    def test_dedicated_uses_dmv(self, mock_pyodbc, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(pool_type="dedicated")
        connector = SynapseConnector(config)

        mock_cursor = MagicMock()
        mock_cursor.description = [("cnt",)]
        mock_cursor.fetchall.return_value = [(1000000,)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        connector.connect()
        count = connector.get_table_rowcount("dbo.my_table")

        assert count == 1000000
        # Verify DMV was queried
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "dm_pdw_nodes_db_partition_stats" in executed_sql

    def test_serverless_uses_count(self, mock_pyodbc, mock_azure_identity):
        from dqs.connectors.synapse_connector import SynapseConnector

        config = _make_config(pool_type="serverless")
        connector = SynapseConnector(config)

        mock_cursor = MagicMock()
        mock_cursor.description = [("cnt",)]
        mock_cursor.fetchall.return_value = [(500,)]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        connector.connect()
        count = connector.get_table_rowcount("dbo.my_table")

        assert count == 500
        # Verify COUNT(*) was used, not DMV
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "COUNT(*)" in executed_sql


# ---------------------------------------------------------------------------
# Test: T-SQL dialect patches in checks
# ---------------------------------------------------------------------------

class TestSynapseDialectPatches:
    """Verify Synapse-specific SQL generation in check _build_sql methods."""

    def test_freshness_lag_synapse_sql(self):
        """Check 20 uses DATEDIFF without quotes and GETDATE() for Synapse."""
        from dqs.checks.freshness import FreshnessLagHours
        from dqs.config.models import CheckConfig

        check = FreshnessLagHours()
        config = CheckConfig(
            check_id=20,
            table="dbo.orders",
            timestamp_column="updated_at",
        )

        sql = check._build_sql(config, dialect="synapse")
        assert "DATEDIFF(hour," in sql
        assert "GETDATE()" in sql
        assert "'hour'" not in sql

    def test_freshness_lag_default_sql(self):
        """Check 20 uses quoted DATEDIFF for non-Synapse dialects."""
        from dqs.checks.freshness import FreshnessLagHours
        from dqs.config.models import CheckConfig

        check = FreshnessLagHours()
        config = CheckConfig(
            check_id=20,
            table="dbo.orders",
            timestamp_column="updated_at",
        )

        sql = check._build_sql(config, dialect="snowflake")
        assert "DATEDIFF('hour'" in sql
        assert "CURRENT_TIMESTAMP" in sql

    def test_stale_table_synapse_sql(self):
        """Check 21 uses DATEADD without quotes and GETDATE() for Synapse."""
        from dqs.checks.freshness import StaleTableCheck
        from dqs.config.models import CheckConfig

        check = StaleTableCheck()
        config = CheckConfig(
            check_id=21,
            table="dbo.orders",
            timestamp_column="updated_at",
            stale_days=3,
        )

        sql = check._build_sql(config, dialect="synapse")
        assert "DATEADD(day, -3" in sql
        assert "GETDATE()" in sql
        assert "'day'" not in sql

    def test_late_arriving_synapse_sql(self):
        """Check 22 uses DATEDIFF without quotes for Synapse."""
        from dqs.checks.freshness import LateArrivingDataCheck
        from dqs.config.models import CheckConfig

        check = LateArrivingDataCheck()
        config = CheckConfig(
            check_id=22,
            table="dbo.orders",
            event_timestamp_column="event_ts",
            load_timestamp_column="load_ts",
        )

        sql = check._build_sql(config, dialect="synapse")
        assert "DATEDIFF(hour," in sql
        assert "'hour'" not in sql

    def test_format_validation_synapse_uses_patindex(self):
        """Check 10 uses PATINDEX instead of REGEXP_LIKE for Synapse."""
        from dqs.checks.validity import FormatValidation
        from dqs.config.models import CheckConfig

        check = FormatValidation()
        config = CheckConfig(
            check_id=10,
            table="dbo.users",
            column="email",
            pattern="^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
        )

        sql = check._build_sql(config, dialect="synapse")
        assert "PATINDEX" in sql
        assert "REGEXP_LIKE" not in sql
        # Should use simplified email pattern
        assert "@" in sql

    def test_format_validation_default_uses_regexp(self):
        """Check 10 uses REGEXP_LIKE for non-Synapse dialects."""
        from dqs.checks.validity import FormatValidation
        from dqs.config.models import CheckConfig

        check = FormatValidation()
        config = CheckConfig(
            check_id=10,
            table="dbo.users",
            column="email",
            pattern="^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$",
        )

        sql = check._build_sql(config, dialect="snowflake")
        assert "REGEXP_LIKE" in sql
        assert "PATINDEX" not in sql

    def test_data_type_conformance_synapse_uses_float(self):
        """Check 8 uses FLOAT instead of DOUBLE for Synapse."""
        from dqs.checks.validity import DataTypeConformance
        from dqs.config.models import CheckConfig

        check = DataTypeConformance()
        config = CheckConfig(
            check_id=8,
            table="dbo.orders",
            column="amount",
        )

        sql = check._build_sql(config, dialect="synapse")
        # Default expected_type = DOUBLE, Synapse changes to FLOAT
        assert "TRY_CAST(amount AS FLOAT)" in sql

    def test_data_type_conformance_default_uses_double(self):
        """Check 8 uses DOUBLE for non-Synapse dialects."""
        from dqs.checks.validity import DataTypeConformance
        from dqs.config.models import CheckConfig

        check = DataTypeConformance()
        config = CheckConfig(
            check_id=8,
            table="dbo.orders",
            column="amount",
        )

        sql = check._build_sql(config, dialect="snowflake")
        assert "TRY_CAST(amount AS DOUBLE)" in sql


# ---------------------------------------------------------------------------
# Test: CLI init --dialect synapse
# ---------------------------------------------------------------------------

class TestCLIInit:
    """Verify the CLI init command produces Synapse starter YAML."""

    def test_synapse_starter_config(self):
        from dqs.cli import _build_example_config

        config = _build_example_config("synapse")

        connector = config["connector"]
        assert connector["dialect"] == "synapse"
        assert "workspace" in connector
        assert connector["pool_type"] == "dedicated"
        assert "auth" in connector
        assert connector["auth"]["method"] == "service_principal"
        assert "tenant_id" in connector["auth"]
        assert "client_id" in connector["auth"]
        assert "client_secret" in connector["auth"]


# ---------------------------------------------------------------------------
# Test: _regex_to_tsql_pattern helper
# ---------------------------------------------------------------------------

class TestRegexToTsqlPattern:
    """Verify regex → PATINDEX pattern conversion."""

    def test_email_pattern(self):
        from dqs.checks.validity import _regex_to_tsql_pattern

        pattern = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
        result = _regex_to_tsql_pattern(pattern)
        assert result == "%_@_%.__%"

    def test_fallback_complex_pattern(self):
        from dqs.checks.validity import _regex_to_tsql_pattern

        # Very complex regex that can't be converted
        result = _regex_to_tsql_pattern("(?=.*[A-Z])(?=.*\\d).{8,}")
        assert result == "%"  # Falls back to accept-all

    def test_simple_pattern_passthrough(self):
        from dqs.checks.validity import _regex_to_tsql_pattern

        result = _regex_to_tsql_pattern("ABC 123")
        assert "ABC 123" in result
