"""
SQL Server-specific incremental DDL generation.

SQL Server supports native MERGE statements (introduced in SQL Server 2008)
and provides robust transaction support. This module implements all
incremental load patterns using T-SQL syntax.
"""

from typing import List, Dict, Optional
from ..incremental_base import IncrementalDDLGenerator
from ..patterns import LoadPattern, IncrementalConfig, MergeStrategy, DeleteStrategy


class SQLServerIncrementalGenerator(IncrementalDDLGenerator):
    """
    SQL Server-specific implementation of incremental load patterns.

    Key SQL Server features:
    - Native MERGE statement (since SQL Server 2008)
    - T-SQL syntax and functions
    - Temporary tables (#temp_table) or table variables
    - Strong transaction support (BEGIN TRANSACTION/COMMIT)
    - Window functions and CTEs for complex logic
    - OUTPUT clause for auditing/logging

    Example:
        >>> from schema_mapper.incremental import get_incremental_generator, IncrementalConfig, LoadPattern
        >>> generator = get_incremental_generator('sqlserver')
        >>> config = IncrementalConfig(
        ...     load_pattern=LoadPattern.UPSERT,
        ...     primary_keys=['user_id']
        ... )
        >>> ddl = generator.generate_merge_ddl(
        ...     schema=[{'name': 'user_id', 'type': 'INT', 'mode': 'REQUIRED'}],
        ...     table_name='users',
        ...     config=config,
        ...     dataset_name='dbo'
        ... )
    """

    def __init__(self):
        super().__init__('sqlserver')

    def supports_pattern(self, pattern: LoadPattern) -> bool:
        """
        Check if SQL Server supports this load pattern.

        SQL Server supports all major patterns via native MERGE statement.
        """
        return True

    def generate_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SQL Server MERGE statement.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration
            dataset_name: Schema name (e.g., 'dbo', 'staging')
            **kwargs: Additional options (staging_table, use_transaction)

        Returns:
            Complete MERGE DDL with optional transaction wrapper
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name)
        use_transaction = kwargs.get('use_transaction', True)

        # Get all columns
        all_columns = [field['name'] for field in schema]
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        # Build join condition
        join_conditions = [
            f"target.[{key}] = source.[{key}]" for key in config.primary_keys
        ]
        join_clause = " AND ".join(join_conditions)

        ddl_parts = []

        if use_transaction:
            ddl_parts.append("BEGIN TRANSACTION;")
            ddl_parts.append("")

        ddl_parts.append(f"MERGE {target_table} AS target")
        ddl_parts.append(f"USING {staging_ref} AS source")
        ddl_parts.append(f"ON ({join_clause})")

        # WHEN MATCHED clause
        if config.merge_strategy != MergeStrategy.UPDATE_NONE:
            ddl_parts.append("WHEN MATCHED THEN")

            if config.merge_strategy == MergeStrategy.UPDATE_SELECTIVE:
                update_cols = config.update_columns or non_key_columns
            else:
                update_cols = non_key_columns

            # Add updated_at timestamp if configured
            if config.updated_by_column and config.updated_by_column in all_columns:
                update_cols = [col for col in update_cols if col != config.updated_by_column]
                set_statements = [f"    target.[{col}] = source.[{col}]" for col in update_cols]
                set_statements.append(f"    target.[{config.updated_by_column}] = GETDATE()")
            else:
                set_statements = [f"    target.[{col}] = source.[{col}]" for col in update_cols]

            ddl_parts.append("  UPDATE SET")
            ddl_parts.append(",\n".join(set_statements))

        # WHEN NOT MATCHED BY TARGET clause (INSERT)
        ddl_parts.append("WHEN NOT MATCHED BY TARGET THEN")
        ddl_parts.append("  INSERT (")
        ddl_parts.append("    " + ",\n    ".join([f"[{col}]" for col in all_columns]))
        ddl_parts.append("  )")
        ddl_parts.append("  VALUES (")
        ddl_parts.append("    " + ",\n    ".join([f"source.[{col}]" for col in all_columns]))
        ddl_parts.append("  );")

        if use_transaction:
            ddl_parts.append("")
            ddl_parts.append("COMMIT;")

        return "\n".join(ddl_parts)

    def generate_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SQL Server INSERT (APPEND) statement.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration
            dataset_name: Schema name
            **kwargs: Additional options (staging_table)

        Returns:
            INSERT statement
        """
        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name)

        columns = [field['name'] for field in schema]

        ddl_parts = []
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(f"FROM {staging_ref}")

        # Optional: Filter to only new records not in target (for INCREMENTAL_APPEND)
        if config.load_pattern == LoadPattern.INCREMENTAL_APPEND and config.primary_keys:
            join_conditions = [
                f"target.[{key}] = staging.[{key}]" for key in config.primary_keys
            ]
            join_clause = " AND ".join(join_conditions)

            ddl_parts.append("WHERE NOT EXISTS (")
            ddl_parts.append(f"  SELECT 1 FROM {target_table} AS target")
            ddl_parts.append(f"  WHERE {join_clause}")
            ddl_parts.append(")")

        ddl_parts.append(";")

        return "\n".join(ddl_parts)

    def generate_full_refresh_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SQL Server TRUNCATE + INSERT.

        For full refresh, TRUNCATE is faster than DELETE.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration
            dataset_name: Schema name
            **kwargs: Additional options (staging_table)

        Returns:
            TRUNCATE + INSERT DDL
        """
        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name)

        columns = [field['name'] for field in schema]

        ddl_parts = []
        ddl_parts.append("-- Full refresh: Truncate and reload")
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")
        ddl_parts.append(f"TRUNCATE TABLE {target_table};")
        ddl_parts.append("")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(f"FROM {staging_ref};")
        ddl_parts.append("")
        ddl_parts.append("COMMIT;")

        return "\n".join(ddl_parts)

    def generate_scd2_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SQL Server SCD Type 2 DDL.

        Uses MERGE with complex WHEN clauses to maintain historical versions.

        Args:
            schema: Schema definition from SchemaMapper (should include SCD columns)
            table_name: Target table name
            config: Incremental configuration with SCD settings
            dataset_name: Schema name
            **kwargs: Additional options (staging_table)

        Returns:
            Complete SCD Type 2 implementation
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name)

        # Get column names
        all_column_names = [field['name'] for field in schema]

        # Columns to track for changes
        exclude_columns = config.primary_keys + [
            config.effective_date_column,
            config.expiration_date_column,
            config.is_current_column
        ]
        hash_cols = config.hash_columns or [col for col in all_column_names if col not in exclude_columns]

        # Build join condition
        join_conditions = [
            f"target.[{key}] = source.[{key}]" for key in config.primary_keys
        ]
        join_clause = " AND ".join(join_conditions)

        # Get data columns (excluding SCD metadata)
        data_columns = [
            col for col in all_column_names
            if col not in [
                config.effective_date_column,
                config.expiration_date_column,
                config.is_current_column
            ]
        ]

        ddl_parts = []
        ddl_parts.append("-- SCD Type 2: Maintain historical versions")
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")

        # Step 1: Expire changed records
        ddl_parts.append("-- Step 1: Expire records that have changed")
        ddl_parts.append(f"UPDATE target")
        ddl_parts.append("SET")
        ddl_parts.append(f"  target.[{config.expiration_date_column}] = CAST(GETDATE() AS DATE),")
        ddl_parts.append(f"  target.[{config.is_current_column}] = 0")
        ddl_parts.append(f"FROM {target_table} AS target")
        ddl_parts.append(f"INNER JOIN {staging_ref} AS source")
        ddl_parts.append(f"  ON {join_clause}")
        ddl_parts.append(f"WHERE target.[{config.is_current_column}] = 1")

        # Check if any tracked columns changed
        change_conditions = []
        for col in hash_cols:
            change_conditions.append(
                f"  ISNULL(CAST(target.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL') != "
                f"ISNULL(CAST(source.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL')"
            )
        ddl_parts.append("  AND (")
        ddl_parts.append(" OR\n".join(change_conditions))
        ddl_parts.append("  );")
        ddl_parts.append("")

        # Step 2: Insert new and changed records
        ddl_parts.append("-- Step 2: Insert new and changed records")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in all_column_names]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"source.[{col}]" for col in data_columns]) + ",")
        ddl_parts.append(f"  CAST(GETDATE() AS DATE) AS [{config.effective_date_column}],")
        ddl_parts.append(f"  CAST('9999-12-31' AS DATE) AS [{config.expiration_date_column}],")
        ddl_parts.append(f"  1 AS [{config.is_current_column}]")
        ddl_parts.append(f"FROM {staging_ref} AS source")
        ddl_parts.append("WHERE NOT EXISTS (")
        ddl_parts.append(f"  SELECT 1 FROM {target_table} AS target")
        ddl_parts.append(f"  WHERE {join_clause}")
        ddl_parts.append(f"    AND target.[{config.is_current_column}] = 1")

        # Check all columns match (no change)
        match_conditions = []
        for col in hash_cols:
            match_conditions.append(
                f"    ISNULL(CAST(target.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL') = "
                f"ISNULL(CAST(source.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL')"
            )
        ddl_parts.append("    AND (")
        ddl_parts.append(" AND\n".join(match_conditions))
        ddl_parts.append("    )")
        ddl_parts.append(");")
        ddl_parts.append("")

        ddl_parts.append("COMMIT;")

        return "\n".join(ddl_parts)

    def generate_incremental_timestamp_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental load based on timestamp for SQL Server.

        Uses CTE or variable to store max timestamp.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration with incremental_column
            dataset_name: Schema name
            **kwargs: Additional options (staging_table)

        Returns:
            INSERT with timestamp filter using CTE
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name)

        columns = [field['name'] for field in schema]

        ddl_parts = []
        ddl_parts.append(f"-- Incremental load based on {config.incremental_column}")
        ddl_parts.append("")

        # Use CTE to get max timestamp
        ddl_parts.append("WITH MaxTimestamp AS (")
        ddl_parts.append(self.get_max_timestamp_query(table_name, config.incremental_column, dataset_name))
        ddl_parts.append(")")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(f"FROM {staging_ref}")
        ddl_parts.append("CROSS JOIN MaxTimestamp")

        # Build WHERE clause with optional lookback
        where_clause = f"WHERE [{config.incremental_column}] > MaxTimestamp.max_timestamp"
        if config.lookback_window:
            # Parse lookback window (e.g., "2 hours", "1 day")
            where_clause += f" - INTERVAL '{config.lookback_window}'"
        ddl_parts.append(where_clause)
        ddl_parts.append(";")

        return "\n".join(ddl_parts)

    def generate_cdc_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate CDC processing DDL for SQL Server.

        Processes I/U/D operations from CDC stream using MERGE or separate statements.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration with operation_column
            dataset_name: Schema name
            **kwargs: Additional options (staging_table)

        Returns:
            CDC processing with MERGE or separate DELETE/UPDATE/INSERT statements
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name)

        all_columns = [field['name'] for field in schema]
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        # Remove CDC metadata columns from data columns
        data_columns = [
            col for col in all_columns
            if col not in [config.operation_column, config.sequence_column]
        ]

        # Build join condition
        join_conditions = [
            f"target.[{key}] = source.[{key}]" for key in config.primary_keys
        ]
        join_clause = " AND ".join(join_conditions)

        ddl_parts = []
        ddl_parts.append("-- CDC Processing: Handle Insert/Update/Delete operations")
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")

        # Step 1: Handle DELETES
        if config.delete_strategy == DeleteStrategy.HARD_DELETE:
            ddl_parts.append("-- Step 1: Process DELETE operations")
            ddl_parts.append(f"DELETE target")
            ddl_parts.append(f"FROM {target_table} AS target")
            ddl_parts.append(f"INNER JOIN {staging_ref} AS source")
            ddl_parts.append(f"  ON {join_clause}")
            ddl_parts.append(f"WHERE source.[{config.operation_column}] = 'D';")
            ddl_parts.append("")
        elif config.delete_strategy == DeleteStrategy.SOFT_DELETE:
            ddl_parts.append("-- Step 1: Process DELETE operations (soft delete)")
            ddl_parts.append(f"UPDATE target")
            ddl_parts.append(f"SET target.[{config.soft_delete_column}] = 1")
            ddl_parts.append(f"FROM {target_table} AS target")
            ddl_parts.append(f"INNER JOIN {staging_ref} AS source")
            ddl_parts.append(f"  ON {join_clause}")
            ddl_parts.append(f"WHERE source.[{config.operation_column}] = 'D';")
            ddl_parts.append("")

        # Step 2: Handle UPDATES
        ddl_parts.append("-- Step 2: Process UPDATE operations")
        ddl_parts.append(f"UPDATE target")
        ddl_parts.append("SET")

        # Filter out CDC columns and primary keys from update
        update_columns = [col for col in non_key_columns if col not in [config.operation_column, config.sequence_column]]
        set_statements = [f"  target.[{col}] = source.[{col}]" for col in update_columns]
        ddl_parts.append(",\n".join(set_statements))

        ddl_parts.append(f"FROM {target_table} AS target")
        ddl_parts.append(f"INNER JOIN {staging_ref} AS source")
        ddl_parts.append(f"  ON {join_clause}")
        ddl_parts.append(f"WHERE source.[{config.operation_column}] = 'U';")
        ddl_parts.append("")

        # Step 3: Handle INSERTS
        ddl_parts.append("-- Step 3: Process INSERT operations")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in data_columns]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in data_columns]))
        ddl_parts.append(f"FROM {staging_ref}")
        ddl_parts.append(f"WHERE [{config.operation_column}] = 'I';")
        ddl_parts.append("")

        ddl_parts.append("COMMIT;")

        return "\n".join(ddl_parts)

    def generate_staging_table_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        staging_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate staging table DDL for SQL Server.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Base table name
            staging_name: Optional staging table name
            dataset_name: Schema name
            **kwargs: Additional options:
                - temporary: Create as temp table (#table) (default True)
                - clustered_index: Column(s) for clustered index

        Returns:
            CREATE TABLE DDL for staging
        """
        staging_name = staging_name or f"{table_name}_staging"
        temporary = kwargs.get('temporary', True)

        # Temp tables in SQL Server use # prefix
        if temporary:
            staging_table = f"#{staging_name}"
        else:
            staging_table = self._build_table_ref(staging_name, dataset_name)

        clustered_index = kwargs.get('clustered_index')

        ddl_parts = []
        ddl_parts.append("-- Create staging table")

        create_stmt = f"CREATE TABLE {staging_table} ("
        ddl_parts.append(create_stmt)

        # Add all columns
        column_defs = []
        for field in schema:
            col_def = f"  [{field['name']}] {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            column_defs.append(col_def)

        ddl_parts.append(",\n".join(column_defs))
        ddl_parts.append(");")

        # Add clustered index if specified
        if clustered_index:
            if isinstance(clustered_index, list):
                index_cols = ", ".join([f"[{col}]" for col in clustered_index])
            else:
                index_cols = f"[{clustered_index}]"

            ddl_parts.append("")
            ddl_parts.append(f"CREATE CLUSTERED INDEX IX_{staging_name}_Clustered")
            ddl_parts.append(f"ON {staging_table} ({index_cols});")

        return "\n".join(ddl_parts)

    def get_max_timestamp_query(
        self,
        table_name: str,
        timestamp_column: str,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate query to get max timestamp for SQL Server.

        Args:
            table_name: Target table name
            timestamp_column: Timestamp column name
            dataset_name: Schema name
            **kwargs: Additional options

        Returns:
            SELECT query for max timestamp (as CTE body)
        """
        table_ref = self._build_table_ref(table_name, dataset_name)

        return f"  SELECT ISNULL(MAX([{timestamp_column}]), '1970-01-01') AS max_timestamp\n  FROM {table_ref}"

    def _build_table_ref(
        self,
        table_name: str,
        dataset_name: Optional[str] = None
    ) -> str:
        """
        Build fully-qualified SQL Server table reference.

        Format: [schema].[table]

        Args:
            table_name: Table name
            dataset_name: Schema name (e.g., 'dbo', 'staging')

        Returns:
            Fully-qualified table reference
        """
        if dataset_name:
            return f"[{dataset_name}].[{table_name}]"
        else:
            return f"[{table_name}]"
