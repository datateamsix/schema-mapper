"""Pytest configuration and fixtures."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import pandas as pd
from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
        'active': [True, True, False, True, False]
    })


@pytest.fixture
def messy_dataframe():
    """Create a messy DataFrame for testing."""
    return pd.DataFrame({
        'User ID': [1, 2, 3],
        'First Name': ['Alice', 'Bob', 'Charlie'],
        'Created At': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Is Active?': ['yes', 'no', 'yes'],
        'Account Balance ($)': ['1000.50', '2000.75', '3000.00']
    })


@pytest.fixture
def dataframe_with_nulls():
    """Create a DataFrame with NULL values."""
    return pd.DataFrame({
        'required': [1, 2, 3, 4, 5],
        'nullable': [1, None, 3, None, 5],
        'sparse': [1] + [None] * 4
    })


# ==================== Connection Fixtures ====================


@pytest.fixture
def sample_canonical_schema():
    """Create a sample CanonicalSchema for testing."""
    return CanonicalSchema(
        table_name="users",
        dataset_name="analytics",
        database_name="production",
        columns=[
            ColumnDefinition(name="user_id", logical_type=LogicalType.BIGINT, nullable=False),
            ColumnDefinition(name="username", logical_type=LogicalType.STRING, nullable=False),
            ColumnDefinition(name="email", logical_type=LogicalType.STRING, nullable=True),
            ColumnDefinition(name="created_at", logical_type=LogicalType.TIMESTAMP, nullable=False),
            ColumnDefinition(name="is_active", logical_type=LogicalType.BOOLEAN, nullable=False),
            ColumnDefinition(name="balance", logical_type=LogicalType.DECIMAL, nullable=True, precision=10, scale=2),
        ]
    )


@pytest.fixture
def mock_connection_config():
    """Create a mock connection configuration dictionary."""
    return {
        'target': 'bigquery',
        'connections': {
            'bigquery': {
                'project': 'test-project',
                'credentials_path': '/path/to/credentials.json',
                'location': 'US'
            },
            'snowflake': {
                'account': 'test-account',
                'user': 'test-user',
                'password': 'test-password',
                'warehouse': 'TEST_WH',
                'database': 'TEST_DB',
                'schema': 'PUBLIC'
            },
            'postgresql': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password'
            },
            'redshift': {
                'host': 'test-cluster.redshift.amazonaws.com',
                'port': 5439,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password'
            },
            'sqlserver': {
                'server': 'test-server.database.windows.net',
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password',
                'driver': '{ODBC Driver 17 for SQL Server}'
            }
        },
        'pooling': {
            'enabled': False,
            'default': {
                'min_size': 2,
                'max_size': 10
            }
        }
    }


@pytest.fixture
def temp_config_file(mock_connection_config, tmp_path):
    """Create a temporary YAML config file."""
    import yaml
    config_path = tmp_path / "connections.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(mock_connection_config, f)
    return str(config_path)


@pytest.fixture
def temp_env_file(tmp_path):
    """Create a temporary .env file."""
    env_path = tmp_path / ".env"
    env_content = """
SNOWFLAKE_USER=test_user
SNOWFLAKE_PASSWORD=test_password
SNOWFLAKE_ACCOUNT=test_account
BQ_PROJECT_ID=test-project
BQ_CREDENTIALS_PATH=/path/to/credentials.json
PG_USER=test_user
PG_PASSWORD=test_password
REDSHIFT_USER=test_user
REDSHIFT_PASSWORD=test_password
MSSQL_USER=test_user
MSSQL_PASSWORD=test_password
"""
    with open(env_path, 'w') as f:
        f.write(env_content.strip())
    return str(env_path)


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    env_vars = {
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'BQ_PROJECT_ID': 'test-project',
        'BQ_CREDENTIALS_PATH': '/path/to/credentials.json',
        'PG_USER': 'test_user',
        'PG_PASSWORD': 'test_password',
        'REDSHIFT_USER': 'test_user',
        'REDSHIFT_PASSWORD': 'test_password',
        'MSSQL_USER': 'test_user',
        'MSSQL_PASSWORD': 'test_password',
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    mock_client = MagicMock()
    mock_client.project = "test-project"
    return mock_client


@pytest.fixture
def mock_snowflake_connection():
    """Create a mock Snowflake connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def mock_psycopg2_connection():
    """Create a mock psycopg2 connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def mock_pyodbc_connection():
    """Create a mock pyodbc connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn
