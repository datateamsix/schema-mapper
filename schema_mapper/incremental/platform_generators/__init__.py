"""
Platform-specific incremental DDL generators.

This module contains the implementations of IncrementalDDLGenerator for each
supported platform:
    - BigQuery
    - Snowflake
    - Redshift
    - SQL Server
    - PostgreSQL

Each platform generator implements platform-specific:
    - MERGE/UPSERT syntax
    - Staging table patterns
    - Transaction handling
    - SCD Type 2 logic
    - CDC merge logic
    - Platform-specific optimizations (partitioning, clustering, etc.)

Note: Platform-specific generators will be implemented in subsequent prompts.
"""

__all__ = [
    'BigQueryIncrementalGenerator',
    'SnowflakeIncrementalGenerator',
    'RedshiftIncrementalGenerator',
    'SQLServerIncrementalGenerator',
    'PostgreSQLIncrementalGenerator',
]

# Platform generators will be imported here once implemented
# from .bigquery import BigQueryIncrementalGenerator
# from .snowflake import SnowflakeIncrementalGenerator
# from .redshift import RedshiftIncrementalGenerator
# from .sqlserver import SQLServerIncrementalGenerator
# from .postgresql import PostgreSQLIncrementalGenerator
