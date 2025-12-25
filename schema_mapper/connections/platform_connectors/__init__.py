"""
Platform-specific database connectors.

Each platform connector implements the BaseConnection interface:
- BigQueryConnection - Google Cloud BigQuery
- SnowflakeConnection - Snowflake Data Warehouse
- RedshiftConnection - Amazon Redshift
- PostgreSQLConnection - PostgreSQL
- SQLServerConnection - Microsoft SQL Server

Note: Platform connectors are loaded lazily by the ConnectionFactory.
Install platform-specific dependencies to use:
    pip install schema-mapper[bigquery]
    pip install schema-mapper[snowflake]
    pip install schema-mapper[postgresql]
    pip install schema-mapper[redshift]
    pip install schema-mapper[sqlserver]
    pip install schema-mapper[all]  # All platforms
"""

# Platform connectors are imported lazily by ConnectionFactory
# to avoid requiring all dependencies when only using one platform

__all__ = []  # Lazy loaded
