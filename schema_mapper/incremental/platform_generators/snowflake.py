"""
Snowflake-specific incremental DDL generator.

This module provides Snowflake-specific implementations of incremental load patterns
with transaction support, TRANSIENT tables, and COPY INTO statements.
"""

from typing import List, Dict, Optional, Any
from ..incremental_base import IncrementalDDLGenerator
from ..patterns import (
    LoadPattern,
    IncrementalConfig,
    MergeStrategy,
    DeleteStrategy
)


class SnowflakeIncrementalGenerator(IncrementalDDLGenerator):
    """
    Snowflake-specific implementation of incremental load patterns.

    Supports all standard load patterns with Snowflake-specific features:
    - Transaction support (BEGIN TRANSACTION/COMMIT)
    - TRANSIENT tables for staging
    - COPY INTO for stage-based loading
    - Session variables for dynamic values

    Example:
        >>> from schema_mapper.incremental import get_incremental_generator, IncrementalConfig, LoadPattern
        >>> generator = get_incremental_generator('snowflake')
        >>> config = IncrementalConfig(
        ...     load_pattern=LoadPattern.UPSERT,
        ...     primary_keys=['user_id']
        ... )
        >>> ddl = generator.generate_merge_ddl(
        ...     schema={'user_id': 'NUMBER', 'name': 'VARCHAR', 'updated_at': 'TIMESTAMP'},
        ...     table_name='users',
        ...     config=config,
        ...     database_name='mydb',
        ...     schema_name='public'
        ... )
    """

    def __init__(self):
        super().__init__('snowflake')

    def supports_pattern(self, pattern: LoadPattern) -> bool:
        """Check if Snowflake supports this load pattern."""
        supported = [
            LoadPattern.FULL_REFRESH,
            LoadPattern.APPEND_ONLY,
            LoadPattern.UPSERT,
            LoadPattern.DELETE_INSERT,
            LoadPattern.INCREMENTAL_TIMESTAMP,
            LoadPattern.INCREMENTAL_APPEND,
            LoadPattern.SCD_TYPE1,
            LoadPattern.SCD_TYPE2,
            LoadPattern.CDC_MERGE,
        ]
        return pattern in supported

    def generate_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate Snowflake MERGE statement with transaction support.

        Args:
            schema: Column name to data type mapping
            table_name: Target table name
            config: Incremental configuration
            database_name: Optional database name
            schema_name: Optional schema name (defaults to 'public')
            **kwargs: Additional options (staging_table, use_transaction)

        Returns:
            Complete MERGE DDL with transaction wrapper

        Example:
            >>> config = IncrementalConfig(
            ...     load_pattern=LoadPattern.UPSERT,
            ...     primary_keys=['id']
            ... )
            >>> ddl = generator.generate_merge_ddl(
            ...     schema={'id': 'NUMBER', 'name': 'VARCHAR'},
            ...     table_name='users',
            ...     config=config
            ... )
        """
        config.validate()

        target_table = self._build_table_ref(table_name, database_name, schema_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, database_name, schema_name)
        use_transaction = kwargs.get('use_transaction', True)

        # Build join condition on primary keys
        join_condition = self._build_join_condition(
            config.primary_keys,
            target_alias='target',
            source_alias='source'
        )

        # Get non-key columns for UPDATE
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        # Build UPDATE clause based on merge strategy
        update_clause = self._build_update_clause(non_key_columns, config)

        # Build INSERT clause
        all_columns = [field['name'] for field in schema]
        insert_columns = self._format_column_list(all_columns)
        insert_values = self._format_column_list(all_columns, prefix='source.')

        # Start building DDL
        ddl_parts = []

        if use_transaction:
            ddl_parts.append("BEGIN TRANSACTION;")
            ddl_parts.append("")

        ddl_parts.append(f"MERGE INTO {target_table} AS target")
        ddl_parts.append(f"USING {staging_ref} AS source")
        ddl_parts.append(f"ON {join_condition}")

        # WHEN MATCHED clause
        if config.merge_strategy != MergeStrategy.UPDATE_NONE:
            ddl_parts.append("WHEN MATCHED THEN")
            ddl_parts.append(f"  UPDATE SET {update_clause}")

        # WHEN NOT MATCHED clause
        ddl_parts.append("WHEN NOT MATCHED THEN")
        ddl_parts.append(f"  INSERT ({insert_columns})")
        ddl_parts.append(f"  VALUES ({insert_values});")

        if use_transaction:
            ddl_parts.append("")
            ddl_parts.append("COMMIT;")

        return "\n".join(ddl_parts)

    def generate_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate simple INSERT statement for append-only loads.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options (staging_table)

        Returns:
            INSERT statement
        """
        target_table = self._build_table_ref(table_name, database_name, schema_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, database_name, schema_name)

        all_columns = [field['name'] for field in schema]
        column_list = self._format_column_list(all_columns)

        ddl = f"""INSERT INTO {target_table} ({column_list})
SELECT {column_list}
FROM {staging_ref};"""

        return ddl

    def generate_full_refresh_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate full refresh DDL (CREATE OR REPLACE / TRUNCATE + INSERT).

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options (use_create_or_replace, staging_table)

        Returns:
            Full refresh DDL
        """
        target_table = self._build_table_ref(table_name, database_name, schema_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, database_name, schema_name)
        use_create_or_replace = kwargs.get('use_create_or_replace', False)

        all_columns = [field['name'] for field in schema]
        column_list = self._format_column_list(all_columns)

        if use_create_or_replace:
            # CREATE OR REPLACE approach
            ddl = f"""CREATE OR REPLACE TABLE {target_table} AS
SELECT {column_list}
FROM {staging_ref};"""
        else:
            # TRUNCATE + INSERT approach
            ddl = f"""BEGIN TRANSACTION;

TRUNCATE TABLE {target_table};

INSERT INTO {target_table} ({column_list})
SELECT {column_list}
FROM {staging_ref};

COMMIT;"""

        return ddl

    def generate_scd2_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SCD Type 2 DDL for maintaining historical records.

        Args:
            schema: Schema definition from SchemaMapper (should include SCD columns)
            table_name: Target table name
            config: Incremental configuration with SCD settings
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options (staging_table)

        Returns:
            Complete SCD Type 2 implementation
        """
        config.validate()

        target_table = self._build_table_ref(table_name, database_name, schema_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, database_name, schema_name)

        # Build hash comparison for change detection
        all_column_names = [field['name'] for field in schema]
        exclude_columns = config.primary_keys + [
            config.effective_date_column,
            config.expiration_date_column,
            config.is_current_column
        ]
        hash_columns = config.hash_columns or [col for col in all_column_names if col not in exclude_columns]
        hash_expr = self._build_hash_expression(hash_columns)

        # Build join on primary keys
        join_condition = self._build_join_condition(
            config.primary_keys,
            target_alias='target',
            source_alias='source'
        )

        # Get all non-SCD columns
        data_columns = [
            col for col in all_column_names
            if col not in [
                config.effective_date_column,
                config.expiration_date_column,
                config.is_current_column
            ]
        ]

        ddl = f"""BEGIN TRANSACTION;

-- Step 1: Expire changed records
UPDATE {target_table} AS target
SET {config.expiration_date_column} = CURRENT_TIMESTAMP(),
    {config.is_current_column} = FALSE
FROM {staging_ref} AS source
WHERE {join_condition}
  AND target.{config.is_current_column} = TRUE
  AND {hash_expr} != (
    SELECT {hash_expr.replace('target.', 'sub.')}
    FROM {target_table} AS sub
    WHERE {join_condition.replace('target.', 'sub.')}
      AND sub.{config.is_current_column} = TRUE
    LIMIT 1
  );

-- Step 2: Insert new versions for changed records
INSERT INTO {target_table} ({self._format_column_list(all_column_names)})
SELECT
  {self._format_column_list(data_columns, prefix='source.')},
  CURRENT_TIMESTAMP() AS {config.effective_date_column},
  '9999-12-31'::TIMESTAMP AS {config.expiration_date_column},
  TRUE AS {config.is_current_column}
FROM {staging_ref} AS source
WHERE EXISTS (
  SELECT 1
  FROM {target_table} AS target
  WHERE {join_condition}
    AND target.{config.expiration_date_column} = CURRENT_TIMESTAMP()
);

-- Step 3: Insert completely new records
INSERT INTO {target_table} ({self._format_column_list(all_column_names)})
SELECT
  {self._format_column_list(data_columns, prefix='source.')},
  CURRENT_TIMESTAMP() AS {config.effective_date_column},
  '9999-12-31'::TIMESTAMP AS {config.expiration_date_column},
  TRUE AS {config.is_current_column}
FROM {staging_ref} AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM {target_table} AS target
  WHERE {join_condition}
);

COMMIT;"""

        return ddl

    def generate_incremental_timestamp_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental load based on timestamp column.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration with incremental_column
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options (source_query)

        Returns:
            INSERT with timestamp filter
        """
        config.validate()

        target_table = self._build_table_ref(table_name, database_name, schema_name)
        source_query = kwargs.get('source_query', f"SELECT * FROM {table_name}_source")

        all_columns = [field['name'] for field in schema]
        column_list = self._format_column_list(all_columns)

        # Build lookback adjustment
        lookback_clause = ""
        if config.lookback_window:
            lookback_clause = f" - INTERVAL '{config.lookback_window}'"

        ddl = f"""-- Get max timestamp from target
SET max_ts = (
  SELECT NVL(MAX({config.incremental_column}), '1900-01-01'::TIMESTAMP)
  FROM {target_table}
);

-- Insert new records
INSERT INTO {target_table} ({column_list})
SELECT {column_list}
FROM ({source_query}) AS source
WHERE source.{config.incremental_column} > $max_ts{lookback_clause};"""

        return ddl

    def generate_cdc_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate CDC (Change Data Capture) MERGE with I/U/D operations.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration with operation_column
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options (staging_table)

        Returns:
            MERGE with DELETE support
        """
        config.validate()

        target_table = self._build_table_ref(table_name, database_name, schema_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, database_name, schema_name)

        # Build join condition
        join_condition = self._build_join_condition(
            config.primary_keys,
            target_alias='target',
            source_alias='source'
        )

        # Get non-key columns
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)
        # Exclude CDC metadata columns
        non_key_columns = [
            col for col in non_key_columns
            if col not in [config.operation_column, config.sequence_column]
        ]

        update_clause = self._build_update_clause(non_key_columns, config)

        # Build INSERT clause (exclude CDC columns)
        all_column_names = [field['name'] for field in schema]
        data_columns = [
            col for col in all_column_names
            if col not in [config.operation_column, config.sequence_column]
        ]
        insert_columns = self._format_column_list(data_columns)
        insert_values = self._format_column_list(data_columns, prefix='source.')

        ddl = f"""BEGIN TRANSACTION;

MERGE INTO {target_table} AS target
USING {staging_ref} AS source
ON {join_condition}
WHEN MATCHED AND source.{config.operation_column} = 'D' THEN
  DELETE
WHEN MATCHED AND source.{config.operation_column} IN ('U', 'I') THEN
  UPDATE SET {update_clause}
WHEN NOT MATCHED AND source.{config.operation_column} IN ('I', 'U') THEN
  INSERT ({insert_columns})
  VALUES ({insert_values});

COMMIT;"""

        return ddl

    def generate_staging_table_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        staging_name: Optional[str] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate CREATE TRANSIENT TABLE for staging.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Base table name
            staging_name: Optional staging table name
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options (transient, cluster_by)

        Returns:
            CREATE TABLE DDL for staging
        """
        staging_table = staging_name or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, database_name, schema_name)
        is_transient = kwargs.get('transient', True)
        cluster_by = kwargs.get('cluster_by')

        # Build column definitions
        column_defs = []
        for field in schema:
            column_defs.append(f"  {field['name']} {field['type']}")

        transient_keyword = "TRANSIENT " if is_transient else ""
        cluster_clause = ""
        if cluster_by:
            cluster_clause = f"\nCLUSTER BY ({', '.join(cluster_by)})"

        ddl = f"""CREATE OR REPLACE {transient_keyword}TABLE {staging_ref} (
{','.join(column_defs)}
){cluster_clause};"""

        return ddl

    def get_max_timestamp_query(
        self,
        table_name: str,
        timestamp_column: str,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate query to get max timestamp from table.

        Args:
            table_name: Target table name
            timestamp_column: Timestamp column name
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options

        Returns:
            SELECT query for max timestamp
        """
        table_ref = self._build_table_ref(table_name, database_name, schema_name)

        return f"""SELECT NVL(MAX({timestamp_column}), '1900-01-01'::TIMESTAMP) AS max_timestamp
FROM {table_ref};"""

    def generate_copy_into_ddl(
        self,
        table_name: str,
        stage_name: str,
        schema: Optional[Dict[str, str]] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate COPY INTO statement for loading from Snowflake stage.

        Args:
            table_name: Target table name
            stage_name: Snowflake stage name (e.g., '@my_stage/path/')
            schema: Optional column mapping for column specification
            database_name: Optional database name
            schema_name: Optional schema name
            **kwargs: Additional options (file_format, pattern, on_error)

        Returns:
            COPY INTO statement

        Example:
            >>> ddl = generator.generate_copy_into_ddl(
            ...     table_name='users',
            ...     stage_name='@my_stage/users/',
            ...     file_format='my_csv_format',
            ...     pattern='.*\\.csv'
            ... )
        """
        table_ref = self._build_table_ref(table_name, database_name, schema_name)
        file_format = kwargs.get('file_format', 'CSV')
        pattern = kwargs.get('pattern')
        on_error = kwargs.get('on_error', 'ABORT_STATEMENT')

        ddl_parts = [f"COPY INTO {table_ref}"]

        # Add column list if schema provided
        if schema:
            column_list = self._format_column_list(list(schema.keys()))
            ddl_parts.append(f"  ({column_list})")

        ddl_parts.append(f"FROM {stage_name}")
        ddl_parts.append(f"FILE_FORMAT = (TYPE = '{file_format}')")

        if pattern:
            ddl_parts.append(f"PATTERN = '{pattern}'")

        ddl_parts.append(f"ON_ERROR = '{on_error}';")

        return "\n".join(ddl_parts)

    def _build_table_ref(
        self,
        table_name: str,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None
    ) -> str:
        """
        Build fully-qualified table reference for Snowflake.

        Format: [database].[schema].table

        Args:
            table_name: Table name
            database_name: Optional database name
            schema_name: Optional schema name (defaults to 'public')

        Returns:
            Fully-qualified table reference
        """
        parts = []
        if database_name:
            parts.append(database_name)
        if schema_name:
            parts.append(schema_name)
        elif database_name:
            # If database provided but not schema, use public
            parts.append('public')
        parts.append(table_name)

        return '.'.join(parts)

    def _build_update_clause(
        self,
        columns: List[str],
        config: IncrementalConfig
    ) -> str:
        """Build UPDATE SET clause based on merge strategy."""
        if config.merge_strategy == MergeStrategy.UPDATE_SELECTIVE:
            columns = config.update_columns or columns

        updates = []
        for col in columns:
            # Skip the updated_by_column if it's in the list - we'll add it with CURRENT_TIMESTAMP
            if col != config.updated_by_column:
                updates.append(f"{col} = source.{col}")

        # Add updated_at timestamp if configured
        if config.updated_by_column:
            updates.append(f"{config.updated_by_column} = CURRENT_TIMESTAMP()")

        return ", ".join(updates)

    def _build_hash_expression(self, columns: List[str], alias: str = 'target') -> str:
        """
        Build HASH expression for change detection.

        Args:
            columns: Columns to include in hash
            alias: Table alias

        Returns:
            HASH expression
        """
        column_refs = [f"{alias}.{col}" for col in columns]
        return f"HASH({', '.join(column_refs)})"
