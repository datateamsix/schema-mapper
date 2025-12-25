"""
Unified DDL generation for different database platforms.

This module provides platform-specific DDL generators for creating CREATE TABLE
statements with support for clustering, partitioning, distribution, and other
platform-specific optimizations.

Consolidates functionality from generators.py and generators_enhanced.py into a
single, simplified API.
"""

from typing import List, Dict, Optional
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class DDLGenerator(ABC):
    """Abstract base class for DDL generators with enhanced features."""

    # Platform capabilities (override in subclasses)
    SUPPORTS_CLUSTERING = False
    SUPPORTS_PARTITIONING = False
    SUPPORTS_DISTRIBUTION = False
    SUPPORTS_SORT_KEYS = False
    SUPPORTS_CREATE_OR_REPLACE = False
    SUPPORTS_TRANSIENT = False
    MAX_CLUSTER_COLUMNS = 0

    @abstractmethod
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        # Clustering/Partitioning
        cluster_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        partition_type: Optional[str] = 'time',
        partition_expiration_days: Optional[int] = None,
        require_partition_filter: bool = False,
        # Range partitioning (BigQuery)
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        range_interval: Optional[int] = None,
        # Distribution/Sort (Redshift)
        distribution_style: Optional[str] = None,
        distribution_key: Optional[str] = None,
        sort_keys: Optional[List[str]] = None,
        interleaved_sort: bool = False,
        # SQL Server specific
        clustered_index: Optional[List[str]] = None,
        columnstore: bool = False,
        # Table options
        create_or_replace: bool = False,
        if_not_exists: bool = False,
        transient: bool = False,
        temporary: bool = False,
    ) -> str:
        """Generate DDL statement with platform-specific optimizations."""
        pass

    def _validate_cluster_by(self, cluster_by: Optional[List[str]]):
        """Validate clustering configuration."""
        if cluster_by:
            if not self.SUPPORTS_CLUSTERING:
                raise ValueError(f"{self.__class__.__name__} does not support clustering")
            if len(cluster_by) > self.MAX_CLUSTER_COLUMNS > 0:
                raise ValueError(
                    f"Maximum {self.MAX_CLUSTER_COLUMNS} clustering columns allowed"
                )

    def _validate_partition_by(self, partition_by: Optional[str]):
        """Validate partitioning configuration."""
        if partition_by and not self.SUPPORTS_PARTITIONING:
            raise ValueError(f"{self.__class__.__name__} does not support partitioning")

    def _validate_distribution(self, distribution_style: Optional[str], distribution_key: Optional[str]):
        """Validate distribution configuration."""
        if (distribution_style or distribution_key) and not self.SUPPORTS_DISTRIBUTION:
            raise ValueError(f"{self.__class__.__name__} does not support distribution")

    def _validate_sort_keys(self, sort_keys: Optional[List[str]]):
        """Validate sort key configuration."""
        if sort_keys and not self.SUPPORTS_SORT_KEYS:
            raise ValueError(f"{self.__class__.__name__} does not support sort keys")


class BigQueryDDLGenerator(DDLGenerator):
    """Generate DDL for BigQuery with partitioning and clustering support."""

    SUPPORTS_CLUSTERING = True
    SUPPORTS_PARTITIONING = True
    MAX_CLUSTER_COLUMNS = 4
    SUPPORTS_CREATE_OR_REPLACE = False

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        cluster_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        partition_type: Optional[str] = 'time',
        partition_expiration_days: Optional[int] = None,
        require_partition_filter: bool = False,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        range_interval: Optional[int] = None,
        distribution_style: Optional[str] = None,
        distribution_key: Optional[str] = None,
        sort_keys: Optional[List[str]] = None,
        interleaved_sort: bool = False,
        clustered_index: Optional[List[str]] = None,
        columnstore: bool = False,
        create_or_replace: bool = False,
        if_not_exists: bool = False,
        transient: bool = False,
        temporary: bool = False,
    ) -> str:
        """
        Generate BigQuery CREATE TABLE statement.

        Supports:
        - Partitioning by DATE, TIMESTAMP, or INTEGER RANGE
        - Clustering (up to 4 columns)
        - Partition expiration
        - Partition filter requirement

        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Dataset name
            project_id: GCP project ID
            cluster_by: Columns to cluster by (max 4)
            partition_by: Column to partition by
            partition_type: 'time', 'range', 'list', or 'hash'
            partition_expiration_days: Days until partition expires
            require_partition_filter: Require partition filter in queries
            range_start: Range partition start value
            range_end: Range partition end value
            range_interval: Range partition interval
            create_or_replace: Not supported for BigQuery (use if_not_exists)
            if_not_exists: Add IF NOT EXISTS clause

        Returns:
            DDL statement
        """
        # Validate
        self._validate_cluster_by(cluster_by)
        self._validate_partition_by(partition_by)

        # Build full table name
        if project_id and dataset_name:
            full_table = f"`{project_id}.{dataset_name}.{table_name}`"
        elif dataset_name:
            full_table = f"`{dataset_name}.{table_name}`"
        else:
            full_table = f"`{table_name}`"

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            if field.get('description'):
                desc = field['description'].replace('"', '\\"')
                col_def += f" OPTIONS(description=\"{desc}\")"
            col_defs.append(col_def)

        # Build DDL
        create_clause = "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
        ddl_parts = [f"{create_clause} {full_table} ("]
        ddl_parts.append(",\n".join(col_defs))
        ddl_parts.append(")")

        # Add partitioning
        if partition_by:
            partition_clause = self._build_partition_clause(
                partition_by, partition_type, schema,
                range_start, range_end, range_interval
            )
            if partition_clause:
                ddl_parts.append(partition_clause)

        # Add clustering
        if cluster_by:
            cluster_cols = ", ".join(cluster_by)
            ddl_parts.append(f"CLUSTER BY {cluster_cols}")

        # Add options
        options_parts = []
        if partition_expiration_days:
            options_parts.append(f"partition_expiration_days={partition_expiration_days}")
        if require_partition_filter:
            options_parts.append("require_partition_filter=true")

        if options_parts:
            ddl_parts.append(f"OPTIONS(\n  {',\n  '.join(options_parts)}\n)")

        ddl = "\n".join(ddl_parts) + ";"

        logger.info(f"Generated BigQuery DDL for {full_table}")
        return ddl

    def _build_partition_clause(
        self,
        partition_by: str,
        partition_type: str,
        schema: List[Dict],
        range_start: Optional[int],
        range_end: Optional[int],
        range_interval: Optional[int]
    ) -> Optional[str]:
        """Build PARTITION BY clause for BigQuery."""
        # Find column type
        col_type = None
        for field in schema:
            if field['name'] == partition_by:
                col_type = field['type']
                break

        if not col_type:
            logger.warning(f"Partition column {partition_by} not found in schema")
            return None

        # Build partition clause based on type
        if col_type == 'DATE':
            return f"PARTITION BY {partition_by}"
        elif col_type == 'TIMESTAMP':
            return f"PARTITION BY DATE({partition_by})"
        elif col_type == 'INT64' and partition_type == 'range':
            if range_start and range_end and range_interval:
                return f"PARTITION BY RANGE_BUCKET({partition_by}, GENERATE_ARRAY({range_start}, {range_end}, {range_interval}))"

        return f"PARTITION BY {partition_by}"


class SnowflakeDDLGenerator(DDLGenerator):
    """Generate DDL for Snowflake with clustering and transient table support."""

    SUPPORTS_CLUSTERING = True
    MAX_CLUSTER_COLUMNS = 4
    SUPPORTS_CREATE_OR_REPLACE = True
    SUPPORTS_TRANSIENT = True

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        cluster_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        partition_type: Optional[str] = 'time',
        partition_expiration_days: Optional[int] = None,
        require_partition_filter: bool = False,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        range_interval: Optional[int] = None,
        distribution_style: Optional[str] = None,
        distribution_key: Optional[str] = None,
        sort_keys: Optional[List[str]] = None,
        interleaved_sort: bool = False,
        clustered_index: Optional[List[str]] = None,
        columnstore: bool = False,
        create_or_replace: bool = False,
        if_not_exists: bool = False,
        transient: bool = False,
        temporary: bool = False,
    ) -> str:
        """
        Generate Snowflake CREATE TABLE statement.

        Supports:
        - Clustering (up to 4 columns)
        - CREATE OR REPLACE
        - TRANSIENT tables
        - TEMPORARY tables

        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name
            cluster_by: Columns to cluster by (max 4)
            create_or_replace: Use CREATE OR REPLACE
            if_not_exists: Add IF NOT EXISTS (mutually exclusive with create_or_replace)
            transient: Create TRANSIENT table
            temporary: Create TEMPORARY table

        Returns:
            DDL statement
        """
        # Validate
        self._validate_cluster_by(cluster_by)

        full_table = f"{dataset_name}.{table_name}" if dataset_name else table_name

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                col_def += f" COMMENT '{desc}'"
            col_defs.append(col_def)

        # Build CREATE clause
        create_parts = ["CREATE"]
        if create_or_replace:
            create_parts.append("OR REPLACE")
        if transient:
            create_parts.append("TRANSIENT")
        if temporary:
            create_parts.append("TEMPORARY")
        create_parts.append("TABLE")
        if if_not_exists and not create_or_replace:
            create_parts.append("IF NOT EXISTS")

        create_clause = " ".join(create_parts)

        # Build DDL
        ddl_parts = [f"{create_clause} {full_table} ("]
        ddl_parts.append(",\n".join(col_defs))
        ddl_parts.append(")")

        # Add clustering
        if cluster_by:
            cluster_cols = ", ".join(cluster_by)
            ddl_parts.append(f"CLUSTER BY ({cluster_cols})")

        ddl = "\n".join(ddl_parts) + ";"

        logger.info(f"Generated Snowflake DDL for {full_table}")
        return ddl


class RedshiftDDLGenerator(DDLGenerator):
    """Generate DDL for Redshift with distribution and sort key support."""

    SUPPORTS_DISTRIBUTION = True
    SUPPORTS_SORT_KEYS = True

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        cluster_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        partition_type: Optional[str] = 'time',
        partition_expiration_days: Optional[int] = None,
        require_partition_filter: bool = False,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        range_interval: Optional[int] = None,
        distribution_style: Optional[str] = None,
        distribution_key: Optional[str] = None,
        sort_keys: Optional[List[str]] = None,
        interleaved_sort: bool = False,
        clustered_index: Optional[List[str]] = None,
        columnstore: bool = False,
        create_or_replace: bool = False,
        if_not_exists: bool = False,
        transient: bool = False,
        temporary: bool = False,
    ) -> str:
        """
        Generate Redshift CREATE TABLE statement.

        Supports:
        - Distribution styles: KEY, ALL, EVEN, AUTO
        - Sort keys: COMPOUND (default) or INTERLEAVED
        - CREATE OR REPLACE (via DROP + CREATE)

        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name
            distribution_style: 'key', 'all', 'even', or 'auto'
            distribution_key: Column for KEY distribution
            sort_keys: Columns for sort key
            interleaved_sort: Use INTERLEAVED instead of COMPOUND sort
            create_or_replace: Drop table if exists then create
            if_not_exists: Add IF NOT EXISTS clause

        Returns:
            DDL statement
        """
        # Validate
        self._validate_distribution(distribution_style, distribution_key)
        self._validate_sort_keys(sort_keys)

        full_table = f"{dataset_name}.{table_name}" if dataset_name else table_name

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f'  "{field["name"]}" {field["type"]}'
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            col_defs.append(col_def)

        # Build DDL
        ddl = ""

        # Add DROP if create_or_replace
        if create_or_replace:
            ddl += f"DROP TABLE IF EXISTS {full_table};\n\n"

        # CREATE TABLE
        create_clause = "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
        ddl += f"{create_clause} {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n)"

        # Add distribution
        if distribution_style:
            style = distribution_style.upper()
            if style == 'KEY' and distribution_key:
                ddl += f"\nDISTSTYLE KEY\nDISTKEY ({distribution_key})"
            elif style in ['ALL', 'EVEN', 'AUTO']:
                ddl += f"\nDISTSTYLE {style}"

        # Add sort keys
        if sort_keys:
            sort_type = "INTERLEAVED" if interleaved_sort else "COMPOUND"
            sort_cols = ", ".join(sort_keys)
            ddl += f"\n{sort_type} SORTKEY ({sort_cols})"

        ddl += ";"

        # Add column comments separately
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"COMMENT ON COLUMN {full_table}.{field['name']} IS '{desc}';"
                comments.append(comment)

        if comments:
            ddl += "\n\n-- Column comments\n" + "\n".join(comments)

        logger.info(f"Generated Redshift DDL for {full_table}")
        return ddl


class SQLServerDDLGenerator(DDLGenerator):
    """Generate DDL for SQL Server with clustered indexes and columnstore support."""

    SUPPORTS_CLUSTERING = True  # Via clustered indexes
    MAX_CLUSTER_COLUMNS = 16

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        cluster_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        partition_type: Optional[str] = 'time',
        partition_expiration_days: Optional[int] = None,
        require_partition_filter: bool = False,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        range_interval: Optional[int] = None,
        distribution_style: Optional[str] = None,
        distribution_key: Optional[str] = None,
        sort_keys: Optional[List[str]] = None,
        interleaved_sort: bool = False,
        clustered_index: Optional[List[str]] = None,
        columnstore: bool = False,
        create_or_replace: bool = False,
        if_not_exists: bool = False,
        transient: bool = False,
        temporary: bool = False,
    ) -> str:
        """
        Generate SQL Server CREATE TABLE statement.

        Supports:
        - Clustered indexes (up to 16 columns)
        - Columnstore indexes for analytics
        - CREATE OR REPLACE (via DROP + CREATE)

        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name (defaults to 'dbo')
            clustered_index: Columns for clustered index
            columnstore: Create columnstore index for analytics workloads
            create_or_replace: Drop table if exists then create
            if_not_exists: Conditional creation

        Returns:
            DDL statement with extended properties
        """
        # Validate
        if clustered_index:
            if len(clustered_index) > self.MAX_CLUSTER_COLUMNS:
                raise ValueError(f"Maximum {self.MAX_CLUSTER_COLUMNS} clustered index columns allowed")

        schema_name = dataset_name or 'dbo'
        full_table = f"[{schema_name}].[{table_name}]"

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f"  [{field['name']}] {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            else:
                col_def += " NULL"
            col_defs.append(col_def)

        # Build DDL
        ddl = ""

        # Add DROP if create_or_replace
        if create_or_replace:
            ddl += f"DROP TABLE IF EXISTS {full_table};\nGO\n\n"
        elif if_not_exists:
            ddl += f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) = '{schema_name}' AND name = '{table_name}')\n"

        # CREATE TABLE
        ddl += f"CREATE TABLE {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"

        # Add clustered index
        if clustered_index:
            index_name = f"IX_{table_name}_clustered"
            cols = ", ".join(f"[{col}]" for col in clustered_index)
            ddl += f"\n\nCREATE CLUSTERED INDEX [{index_name}] ON {full_table} ({cols});"

        # Add columnstore index
        if columnstore:
            index_name = f"IX_{table_name}_columnstore"
            ddl += f"\n\nCREATE COLUMNSTORE INDEX [{index_name}] ON {full_table};"

        # Add extended properties for descriptions
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"""EXEC sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'{desc}',
    @level0type = N'SCHEMA', @level0name = N'{schema_name}',
    @level1type = N'TABLE', @level1name = N'{table_name}',
    @level2type = N'COLUMN', @level2name = N'{field['name']}';"""
                comments.append(comment)

        if comments:
            ddl += "\n\n-- Column descriptions\n" + "\n".join(comments)

        logger.info(f"Generated SQL Server DDL for {full_table}")
        return ddl


class PostgreSQLDDLGenerator(DDLGenerator):
    """Generate DDL for PostgreSQL with partitioning and index-based clustering."""

    SUPPORTS_CLUSTERING = True  # Via indexes
    SUPPORTS_PARTITIONING = True

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        cluster_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        partition_type: Optional[str] = 'time',
        partition_expiration_days: Optional[int] = None,
        require_partition_filter: bool = False,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
        range_interval: Optional[int] = None,
        distribution_style: Optional[str] = None,
        distribution_key: Optional[str] = None,
        sort_keys: Optional[List[str]] = None,
        interleaved_sort: bool = False,
        clustered_index: Optional[List[str]] = None,
        columnstore: bool = False,
        create_or_replace: bool = False,
        if_not_exists: bool = False,
        transient: bool = False,
        temporary: bool = False,
    ) -> str:
        """
        Generate PostgreSQL CREATE TABLE statement.

        Supports:
        - Partitioning: RANGE, LIST, HASH
        - Index-based clustering
        - CREATE OR REPLACE (via DROP + CREATE)

        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name
            partition_by: Column to partition by
            partition_type: 'range', 'list', or 'hash'
            cluster_by: Columns to create clustering index
            create_or_replace: Drop table if exists then create
            if_not_exists: Add IF NOT EXISTS clause
            temporary: Create TEMPORARY table

        Returns:
            DDL statement with COMMENT statements
        """
        full_table = f'"{dataset_name}"."{table_name}"' if dataset_name else f'"{table_name}"'

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f'  "{field["name"]}" {field["type"]}'
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            col_defs.append(col_def)

        # Build DDL
        ddl = ""

        # Add DROP if create_or_replace
        if create_or_replace:
            ddl += f"DROP TABLE IF EXISTS {full_table};\n\n"

        # CREATE TABLE
        create_parts = ["CREATE"]
        if temporary:
            create_parts.append("TEMPORARY")
        create_parts.append("TABLE")
        if if_not_exists:
            create_parts.append("IF NOT EXISTS")

        create_clause = " ".join(create_parts)
        ddl += f"{create_clause} {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n)"

        # Add partitioning
        if partition_by:
            partition_type_upper = partition_type.upper()
            if partition_type_upper in ['RANGE', 'LIST', 'HASH']:
                ddl += f"\nPARTITION BY {partition_type_upper} ({partition_by})"

        ddl += ";"

        # Add clustering index
        if cluster_by:
            index_name = f"idx_{table_name}_cluster"
            cols = ", ".join(f'"{col}"' for col in cluster_by)
            ddl += f"\n\nCREATE INDEX {index_name} ON {full_table} ({cols});"
            ddl += f"\nCLUSTER {full_table} USING {index_name};"

        # Add column comments
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"COMMENT ON COLUMN {full_table}.\"{field['name']}\" IS '{desc}';"
                comments.append(comment)

        if comments:
            ddl += "\n\n-- Column comments\n" + "\n".join(comments)

        logger.info(f"Generated PostgreSQL DDL for {full_table}")
        return ddl


# Factory function
def get_ddl_generator(platform: str) -> DDLGenerator:
    """
    Get the appropriate DDL generator for a platform.

    Args:
        platform: Target database platform

    Returns:
        DDLGenerator instance

    Raises:
        ValueError: If platform is not supported
    """
    generators = {
        'bigquery': BigQueryDDLGenerator,
        'snowflake': SnowflakeDDLGenerator,
        'redshift': RedshiftDDLGenerator,
        'sqlserver': SQLServerDDLGenerator,
        'postgresql': PostgreSQLDDLGenerator,
    }

    platform_lower = platform.lower()
    if platform_lower not in generators:
        raise ValueError(
            f"No DDL generator for platform: {platform}. "
            f"Supported: {', '.join(generators.keys())}"
        )

    return generators[platform_lower]()
