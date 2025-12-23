"""
Microsoft SQL Server-specific incremental DDL generation.
"""

from typing import List, Dict, Optional
from ..incremental_base import IncrementalDDLGenerator
from ..patterns import LoadPattern, IncrementalConfig, MergeStrategy, DeleteStrategy


class SQLServerIncrementalGenerator(IncrementalDDLGenerator):
    """
    Generate SQL Server-specific incremental load DDL.

    SQL Server specifics:
    - Native MERGE statement with OUTPUT clause
    - Square brackets for identifiers [schema].[table]
    - Temp tables use # prefix (#temp_table)
    - GETDATE() for current timestamp
    - IDENTITY columns for auto-increment
    - Transactional with BEGIN TRANSACTION/COMMIT
    - Clustered indexes for optimization
    """

    def __init__(self):
        super().__init__('sqlserver')

    def supports_pattern(self, pattern: LoadPattern) -> bool:
        """Check if SQL Server supports this pattern."""
        # SQL Server supports all patterns
        return True

    def generate_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        capture_output: bool = False,
        **kwargs
    ) -> str:
        """
        Generate SQL Server MERGE statement.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration
            dataset_name: Schema name (e.g., 'dbo', 'staging')
            database_name: Database name
            capture_output: Whether to include OUTPUT clause to capture changes
            **kwargs: Additional options (staging_table, use_transaction)

        Returns:
            Complete MERGE DDL with optional transaction wrapper

        Example output:
            BEGIN TRANSACTION;

            MERGE INTO [dbo].[users] AS target
            USING [dbo].[users_staging] AS source
            ON target.[user_id] = source.[user_id]
            WHEN MATCHED THEN
              UPDATE SET
                target.[name] = source.[name],
                target.[email] = source.[email],
                target.[updated_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN
              INSERT ([user_id], [name], [email], [created_at], [updated_at])
              VALUES (source.[user_id], source.[name], source.[email],
                      GETDATE(), GETDATE())
            OUTPUT $action, inserted.*, deleted.*;

            COMMIT TRANSACTION;
        """
        config.validate()

        # Build table references
        target_table = self._build_table_ref(table_name, dataset_name, database_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name, database_name)
        use_transaction = kwargs.get('use_transaction', True)

        # Get columns
        all_columns = [field['name'] for field in schema]
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        ddl_parts = []

        if use_transaction:
            ddl_parts.append("BEGIN TRANSACTION;")
            ddl_parts.append("")

        # Build MERGE statement
        ddl_parts.append(f"MERGE INTO {target_table} AS target")
        ddl_parts.append(f"USING {staging_ref} AS source")

        # ON condition
        join_condition = self._build_join_condition(config.primary_keys)
        ddl_parts.append(f"ON {join_condition}")

        # WHEN MATCHED
        if config.merge_strategy != MergeStrategy.UPDATE_NONE:
            ddl_parts.append(self._build_matched_clause(
                schema, config, non_key_columns
            ))

        # WHEN NOT MATCHED BY TARGET (insert new records)
        ddl_parts.append(self._build_not_matched_clause(
            schema, config, all_columns
        ))

        # Optional: WHEN NOT MATCHED BY SOURCE (handle deletes)
        if config.delete_strategy != DeleteStrategy.IGNORE:
            ddl_parts.append(self._build_not_matched_by_source_clause(config))

        # Optional: OUTPUT clause
        if capture_output:
            ddl_parts.append(self._build_output_clause(all_columns))

        ddl_parts.append(";")

        if use_transaction:
            ddl_parts.append("")
            ddl_parts.append("COMMIT TRANSACTION;")

        return "\n".join(ddl_parts)

    def _build_matched_clause(
        self,
        schema: List[Dict],
        config: IncrementalConfig,
        non_key_columns: List[str]
    ) -> str:
        """Build WHEN MATCHED clause for SQL Server."""

        clause_parts = []
        clause_parts.append("WHEN MATCHED")

        # Add condition for UPDATE_CHANGED strategy
        if config.merge_strategy == MergeStrategy.UPDATE_CHANGED:
            # Only update if values differ
            conditions = []
            check_cols = non_key_columns[:5]  # Limit for readability
            for col in check_cols:
                conditions.append(
                    f"  ISNULL(CAST(target.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL') != "
                    f"ISNULL(CAST(source.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL')"
                )
            if len(non_key_columns) > 5:
                conditions.append("  -- ... additional columns ...")

            clause_parts.append(" AND (")
            clause_parts.append(" OR\n".join(conditions))
            clause_parts.append(")")

        clause_parts.append(" THEN")
        clause_parts.append("  UPDATE SET")

        # Determine which columns to update
        if config.merge_strategy == MergeStrategy.UPDATE_SELECTIVE:
            update_cols = config.update_columns or non_key_columns
        else:
            update_cols = non_key_columns

        # Build SET clause
        set_statements = []
        for col in update_cols:
            set_statements.append(f"    target.[{col}] = source.[{col}]")

        # Add updated_at if configured
        if config.updated_by_column and config.updated_by_column not in update_cols:
            all_columns = [field['name'] for field in schema]
            if config.updated_by_column in all_columns:
                set_statements.append(
                    f"    target.[{config.updated_by_column}] = GETDATE()"
                )

        clause_parts.append(",\n".join(set_statements))

        return "\n".join(clause_parts)

    def _build_not_matched_clause(
        self,
        schema: List[Dict],
        config: IncrementalConfig,
        all_columns: List[str]
    ) -> str:
        """Build WHEN NOT MATCHED BY TARGET clause."""

        clause_parts = []
        clause_parts.append("WHEN NOT MATCHED BY TARGET THEN")
        clause_parts.append("  INSERT (")

        # Column list
        insert_cols = all_columns.copy()

        # Add created_at/updated_at if configured and not already in schema
        if config.created_by_column and config.created_by_column not in insert_cols:
            insert_cols.append(config.created_by_column)
        if config.updated_by_column and config.updated_by_column not in insert_cols:
            insert_cols.append(config.updated_by_column)

        clause_parts.append("    " + ",\n    ".join([f"[{col}]" for col in insert_cols]))
        clause_parts.append("  )")
        clause_parts.append("  VALUES (")

        # Values
        values = []
        for col in all_columns:
            values.append(f"source.[{col}]")

        # Add timestamps
        if config.created_by_column and config.created_by_column not in all_columns:
            values.append("GETDATE()")
        if config.updated_by_column and config.updated_by_column not in all_columns:
            values.append("GETDATE()")

        clause_parts.append("    " + ",\n    ".join(values))
        clause_parts.append("  )")

        return "\n".join(clause_parts)

    def _build_not_matched_by_source_clause(
        self,
        config: IncrementalConfig
    ) -> str:
        """
        Build WHEN NOT MATCHED BY SOURCE clause.

        Handles records in target that don't exist in source.
        """
        if config.delete_strategy == DeleteStrategy.HARD_DELETE:
            return "WHEN NOT MATCHED BY SOURCE THEN\n  DELETE"

        elif config.delete_strategy == DeleteStrategy.SOFT_DELETE:
            return (
                f"WHEN NOT MATCHED BY SOURCE THEN\n"
                f"  UPDATE SET target.[{config.soft_delete_column}] = 1"
            )

        return ""

    def _build_output_clause(self, columns: List[str]) -> str:
        """
        Build OUTPUT clause to capture merge results.

        The OUTPUT clause can capture:
        - $action: 'INSERT', 'UPDATE', or 'DELETE'
        - inserted.*: New row values
        - deleted.*: Old row values (for UPDATE and DELETE)
        """
        return (
            "OUTPUT\n"
            "  $action AS [MergeAction],\n"
            "  inserted.*,\n"
            "  deleted.*"
        )

    def generate_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate SQL Server INSERT (APPEND) statement."""

        target_table = self._build_table_ref(table_name, dataset_name, database_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name, database_name)

        columns = [field['name'] for field in schema]

        ddl_parts = []
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(f"FROM {staging_ref}")

        # Optional: Filter to only new records not in target
        if config.load_pattern == LoadPattern.INCREMENTAL_APPEND and config.primary_keys:
            join_condition = self._build_join_condition(config.primary_keys, 'target', staging_ref.split('.')[-1].strip(']'))
            ddl_parts.append("WHERE NOT EXISTS (")
            ddl_parts.append(f"  SELECT 1 FROM {target_table} AS target")
            ddl_parts.append(f"  WHERE {join_condition}")
            ddl_parts.append(")")

        ddl_parts.append(";")
        ddl_parts.append("")
        ddl_parts.append("COMMIT TRANSACTION;")

        return "\n".join(ddl_parts)

    def generate_full_refresh_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate SQL Server TRUNCATE + INSERT."""

        target_table = self._build_table_ref(table_name, dataset_name, database_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name, database_name)

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
        ddl_parts.append("COMMIT TRANSACTION;")

        return "\n".join(ddl_parts)

    def generate_scd2_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SQL Server SCD Type 2 DDL.

        Uses MERGE with multiple WHEN MATCHED clauses.
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name, database_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name, database_name)

        # Get column names
        all_column_names = [field['name'] for field in schema]

        # Columns to track for changes
        exclude_columns = config.primary_keys + [
            config.effective_date_column,
            config.expiration_date_column,
            config.is_current_column
        ]
        hash_cols = config.hash_columns or [col for col in all_column_names if col not in exclude_columns]

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

        # Use MERGE for SCD Type 2
        join_condition = self._build_join_condition(config.primary_keys)
        ddl_parts.append(f"MERGE INTO {target_table} AS target")
        ddl_parts.append(f"USING {staging_ref} AS source")
        ddl_parts.append(f"ON {join_condition}")
        ddl_parts.append(f"   AND target.[{config.is_current_column}] = 1")

        # WHEN MATCHED AND record changed THEN expire old version
        ddl_parts.append("WHEN MATCHED AND (")
        change_conditions = []
        for col in hash_cols:
            change_conditions.append(
                f"  ISNULL(CAST(target.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL') != "
                f"ISNULL(CAST(source.[{col}] AS NVARCHAR(MAX)), 'NULL_SENTINEL')"
            )
        ddl_parts.append(" OR\n".join(change_conditions))
        ddl_parts.append(") THEN")
        ddl_parts.append("  UPDATE SET")
        ddl_parts.append(f"    target.[{config.expiration_date_column}] = CAST(GETDATE() AS DATE),")
        ddl_parts.append(f"    target.[{config.is_current_column}] = 0")

        # WHEN NOT MATCHED THEN insert new record
        insert_cols = data_columns + [
            config.effective_date_column,
            config.expiration_date_column,
            config.is_current_column
        ]

        ddl_parts.append("WHEN NOT MATCHED BY TARGET THEN")
        ddl_parts.append("  INSERT (")
        ddl_parts.append("    " + ",\n    ".join([f"[{col}]" for col in insert_cols]))
        ddl_parts.append("  )")
        ddl_parts.append("  VALUES (")

        values = [f"source.[{col}]" for col in data_columns]
        values.append("CAST(GETDATE() AS DATE)")  # effective_date
        values.append("CAST('9999-12-31' AS DATE)")  # expiration_date
        values.append("1")  # is_current

        ddl_parts.append("    " + ",\n    ".join(values))
        ddl_parts.append("  );")
        ddl_parts.append("")

        # Insert new versions of changed records
        ddl_parts.append("-- Insert new versions of changed records")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in insert_cols]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"source.[{col}]" for col in data_columns]) + ",")
        ddl_parts.append(f"  CAST(GETDATE() AS DATE) AS [{config.effective_date_column}],")
        ddl_parts.append(f"  CAST('9999-12-31' AS DATE) AS [{config.expiration_date_column}],")
        ddl_parts.append(f"  1 AS [{config.is_current_column}]")
        ddl_parts.append(f"FROM {staging_ref} AS source")
        ddl_parts.append(f"INNER JOIN {target_table} AS target")

        target_join = self._build_join_condition(config.primary_keys, 'target', 'source')
        ddl_parts.append(f"  ON {target_join}")
        ddl_parts.append(f"WHERE target.[{config.is_current_column}] = 0")
        ddl_parts.append(f"  AND target.[{config.expiration_date_column}] = CAST(GETDATE() AS DATE);")
        ddl_parts.append("")

        ddl_parts.append("COMMIT TRANSACTION;")

        return "\n".join(ddl_parts)

    def generate_incremental_timestamp_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental load based on timestamp for SQL Server.

        Uses variable to store max timestamp.
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name, database_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name, database_name)

        columns = [field['name'] for field in schema]

        ddl_parts = []
        ddl_parts.append(f"-- Incremental load based on [{config.incremental_column}]")
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")

        # Step 1: Get max timestamp into variable
        ddl_parts.append("-- Get max timestamp from target")
        ddl_parts.append("DECLARE @max_ts DATETIME;")
        ddl_parts.append(f"SELECT @max_ts = ISNULL(MAX([{config.incremental_column}]), '1970-01-01')")
        ddl_parts.append(f"FROM {target_table};")
        ddl_parts.append("")

        # Step 2: Load only new records
        ddl_parts.append("-- Load only new records")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"[{col}]" for col in columns]))
        ddl_parts.append(f"FROM {staging_ref}")

        # Build WHERE clause with optional lookback
        where_clause = f"WHERE [{config.incremental_column}] > @max_ts"

        # Optional: Add lookback window using DATEADD
        if config.lookback_window:
            # Parse lookback window (e.g., "2 hours" -> DATEADD)
            parts = config.lookback_window.split()
            if len(parts) == 2:
                amount, unit = parts
                unit_map = {
                    'hour': 'HOUR', 'hours': 'HOUR',
                    'day': 'DAY', 'days': 'DAY',
                    'minute': 'MINUTE', 'minutes': 'MINUTE'
                }
                sql_unit = unit_map.get(unit.lower(), 'HOUR')
                where_clause = f"WHERE [{config.incremental_column}] > DATEADD({sql_unit}, -{amount}, @max_ts)"

        ddl_parts.append(where_clause)
        ddl_parts.append(";")
        ddl_parts.append("")

        ddl_parts.append("COMMIT TRANSACTION;")

        return "\n".join(ddl_parts)

    def generate_cdc_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate CDC MERGE DDL for SQL Server.

        SQL Server MERGE can handle all CDC operations (I/U/D) in one statement.
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name, database_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name, database_name)

        all_columns = [field['name'] for field in schema]
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        # Remove CDC metadata columns from data columns
        data_columns = [
            col for col in all_columns
            if col not in [config.operation_column, config.sequence_column]
        ]

        # Filter non-key columns to exclude CDC metadata
        update_columns = [
            col for col in non_key_columns
            if col not in [config.operation_column, config.sequence_column]
        ]

        ddl_parts = []
        ddl_parts.append("-- CDC Merge: Handle Insert/Update/Delete operations")
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")

        join_condition = self._build_join_condition(config.primary_keys)
        ddl_parts.append(f"MERGE INTO {target_table} AS target")
        ddl_parts.append(f"USING {staging_ref} AS source")
        ddl_parts.append(f"ON {join_condition}")

        # WHEN MATCHED AND operation = 'D' THEN DELETE
        if config.delete_strategy == DeleteStrategy.HARD_DELETE:
            ddl_parts.append(f"WHEN MATCHED AND source.[{config.operation_column}] = 'D' THEN")
            ddl_parts.append("  DELETE")
        elif config.delete_strategy == DeleteStrategy.SOFT_DELETE:
            ddl_parts.append(f"WHEN MATCHED AND source.[{config.operation_column}] = 'D' THEN")
            ddl_parts.append("  UPDATE SET")
            ddl_parts.append(f"    target.[{config.soft_delete_column}] = 1")

        # WHEN MATCHED AND operation = 'U' THEN UPDATE
        ddl_parts.append(f"WHEN MATCHED AND source.[{config.operation_column}] = 'U' THEN")
        ddl_parts.append("  UPDATE SET")
        set_statements = [f"    target.[{col}] = source.[{col}]" for col in update_columns]
        ddl_parts.append(",\n".join(set_statements))

        # WHEN NOT MATCHED BY TARGET AND operation = 'I' THEN INSERT
        ddl_parts.append(f"WHEN NOT MATCHED BY TARGET AND source.[{config.operation_column}] = 'I' THEN")
        ddl_parts.append("  INSERT (")
        ddl_parts.append("    " + ",\n    ".join([f"[{col}]" for col in data_columns]))
        ddl_parts.append("  )")
        ddl_parts.append("  VALUES (")
        ddl_parts.append("    " + ",\n    ".join([f"source.[{col}]" for col in data_columns]))
        ddl_parts.append("  );")
        ddl_parts.append("")

        ddl_parts.append("COMMIT TRANSACTION;")

        return "\n".join(ddl_parts)

    def generate_staging_table_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        staging_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        temporary: bool = True,
        **kwargs
    ) -> str:
        """
        Generate staging table DDL for SQL Server.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Base table name
            staging_name: Optional staging table name
            dataset_name: Schema name
            database_name: Database name
            temporary: Create as temp table with # prefix
            **kwargs: Additional options (clustered_index)
        """
        staging_name = staging_name or f"{table_name}_staging"

        if temporary:
            # SQL Server temp tables use # prefix
            staging_table = f"#{staging_name}"
        else:
            staging_table = self._build_table_ref(staging_name, dataset_name, database_name)

        clustered_index = kwargs.get('clustered_index')

        ddl_parts = []
        ddl_parts.append("-- Create staging table")
        ddl_parts.append(f"CREATE TABLE {staging_table} (")

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

    def generate_bulk_insert_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        file_path: str,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        staging_table: Optional[str] = None,
        field_terminator: str = ',',
        row_terminator: str = '\\n',
        first_row: int = 2,
        **kwargs
    ) -> str:
        """
        Generate SQL Server BULK INSERT statement.

        Args:
            schema: Schema definition (for reference, not used directly)
            table_name: Base table name
            file_path: Path to data file (local or UNC path)
            dataset_name: Schema name
            database_name: Database name
            staging_table: Explicit staging table name
            field_terminator: Column delimiter
            row_terminator: Row delimiter
            first_row: First row to import (2 = skip header)
            **kwargs: Additional options

        Returns:
            BULK INSERT statement

        Example:
            BULK INSERT [dbo].[users_staging]
            FROM 'C:\\data\\users.csv'
            WITH (
              FIELDTERMINATOR = ',',
              ROWTERMINATOR = '\\n',
              FIRSTROW = 2,
              TABLOCK
            );
        """
        target_table = self._build_table_ref(
            staging_table or f"{table_name}_staging",
            dataset_name,
            database_name
        )

        ddl_parts = []
        ddl_parts.append("-- Load data from file into staging table")
        ddl_parts.append(f"BULK INSERT {target_table}")
        ddl_parts.append(f"FROM '{file_path}'")
        ddl_parts.append("WITH (")
        ddl_parts.append(f"  FIELDTERMINATOR = '{field_terminator}',")
        ddl_parts.append(f"  ROWTERMINATOR = '{row_terminator}',")
        ddl_parts.append(f"  FIRSTROW = {first_row},")
        ddl_parts.append("  TABLOCK")
        ddl_parts.append(");")

        return "\n".join(ddl_parts)

    def generate_update_statistics_command(
        self,
        table_name: str,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate UPDATE STATISTICS command for SQL Server.

        Important for query performance after large loads.
        """
        table_ref = self._build_table_ref(table_name, dataset_name, database_name)

        return (
            "-- Update table statistics\n"
            f"UPDATE STATISTICS {table_ref} WITH FULLSCAN;"
        )

    def get_max_timestamp_query(
        self,
        table_name: str,
        timestamp_column: str,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate query to get max timestamp for SQL Server."""

        table_ref = self._build_table_ref(table_name, dataset_name, database_name)

        return (
            f"SELECT ISNULL(MAX([{timestamp_column}]), '1970-01-01') AS max_timestamp\n"
            f"FROM {table_ref}"
        )

    def _build_table_ref(
        self,
        table_name: str,
        dataset_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> str:
        """
        Build fully-qualified SQL Server table reference.

        Format: [database].[schema].[table] or [schema].[table] or [table]
        """
        parts = []

        if database_name:
            parts.append(f"[{database_name}]")

        if dataset_name:
            parts.append(f"[{dataset_name}]")

        parts.append(f"[{table_name}]")

        return ".".join(parts)

    def _build_join_condition(
        self,
        primary_keys: List[str],
        target_alias: str = "target",
        source_alias: str = "source"
    ) -> str:
        """Build JOIN ON condition for SQL Server with square brackets."""
        conditions = [
            f"{target_alias}.[{key}] = {source_alias}.[{key}]"
            for key in primary_keys
        ]
        return " AND ".join(conditions)
