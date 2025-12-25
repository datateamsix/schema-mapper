"""
Unified database connection system for schema-mapper.

Provides a single interface for connecting to and introspecting databases across
BigQuery, Snowflake, Redshift, PostgreSQL, and SQL Server.

Main components:
- ConnectionFactory: Create platform-specific connections
- ConnectionConfig: Manage YAML + .env configuration
- BaseConnection: Abstract interface all connectors implement

Quick Start:
    >>> from schema_mapper.connections import ConnectionFactory, ConnectionConfig
    >>> config = ConnectionConfig('config/connections.yaml')
    >>> with ConnectionFactory.get_connection('bigquery', config) as conn:
    ...     if conn.table_exists('users', schema_name='public'):
    ...         schema = conn.get_target_schema('users', schema_name='public')
    ...         print(f"Found {len(schema.columns)} columns")

Examples:
    From YAML config:
    >>> config = ConnectionConfig('config/connections.yaml')
    >>> conn = ConnectionFactory.get_connection('snowflake', config)
    >>> schema = conn.get_target_schema('customers')

    From dictionary:
    >>> conn = ConnectionFactory.get_connection('bigquery', {
    ...     'project': 'my-project',
    ...     'credentials_path': '/path/to/key.json'
    ... })

    Using convenience function:
    >>> from schema_mapper.connections import get_connection
    >>> conn = get_connection('postgresql', 'config/connections.yaml')
"""

from .base import BaseConnection, ConnectionState
from .config import ConnectionConfig
from .factory import ConnectionFactory, get_connection
from .exceptions import (
    ConnectionError,
    ConfigurationError,
    AuthenticationError,
    NetworkError,
    TableNotFoundError,
    ExecutionError,
    TransactionError,
    PoolExhaustedError,
    IntrospectionError,
    ValidationError,
    RetryExhaustedError,
)

__all__ = [
    # Main classes
    'BaseConnection',
    'ConnectionState',
    'ConnectionConfig',
    'ConnectionFactory',
    'get_connection',
    # Exceptions
    'ConnectionError',
    'ConfigurationError',
    'AuthenticationError',
    'NetworkError',
    'TableNotFoundError',
    'ExecutionError',
    'TransactionError',
    'PoolExhaustedError',
    'IntrospectionError',
    'ValidationError',
    'RetryExhaustedError',
]
