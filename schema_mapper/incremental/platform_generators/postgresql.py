"""
PostgreSQL-specific incremental DDL generation.
"""

from typing import List, Dict, Optional
from ..incremental_base import IncrementalDDLGenerator
from ..patterns import LoadPattern, IncrementalConfig, MergeStrategy, DeleteStrategy


class PostgreSQLIncrementalGenerator(IncrementalDDLGenerator):
    """
    Generate PostgreSQL-specific incremental load DDL.

    PostgreSQL specifics:
    - INSERT ... ON CONFLICT (UPSERT) since version 9.5
    - CTEs (Common Table Expressions) with RETURNING
    - Strong transaction support (BEGIN/COMMIT)
    - TEMPORARY tables (not temp)
    - COPY command for efficient bulk loading
    - Double quotes for identifiers "schema"."table"
    - NOW() or CURRENT_TIMESTAMP for current time
    - Serial/Sequences for auto-increment
    - No native MERGE until PostgreSQL 15 (use ON CONFLICT)
    """

    def __init__(self):
        super().__init__('postgresql')

    def supports_pattern(self, pattern: LoadPattern) -> bool:
        """Check if PostgreSQL supports this pattern."""
        # PostgreSQL supports all patterns
        return True

    def generate_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate PostgreSQL UPSERT using INSERT ... ON CONFLICT.

        PostgreSQL doesn't have traditional MERGE until version 15.
        Uses INSERT ... ON CONFLICT DO UPDATE instead.

        Example output:
        ```sql
        BEGIN;

        -- Upsert using INSERT ... ON CONFLICT
        INSERT INTO "public"."users" (
          "user_id",
          "name",
          "email",
          "updated_at"
        )
        SELECT
          "user_id",
          "name",
          "email",
          CURRENT_TIMESTAMP
        FROM "public"."users_staging"
        ON CONFLICT ("user_id")
        DO UPDATE SET
          "name" = EXCLUDED."name",
          "email" = EXCLUDED."email",
          "updated_at" = CURRENT_TIMESTAMP;

        COMMIT;
        ```
        """
        config.validate()

        # Build table references
        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name
        )

        # Get columns
        all_columns = [field['name'] for field in schema]
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        sql = "BEGIN;\n\n"

        if config.merge_strategy == MergeStrategy.UPDATE_NONE:
            # INSERT ... ON CONFLICT DO NOTHING (insert only)
            sql += self._generate_insert_ignore(
                target_table, staging_table, all_columns, config.primary_keys
            )
        else:
            # INSERT ... ON CONFLICT DO UPDATE (upsert)
            sql += self._generate_upsert(
                target_table, staging_table, schema, config,
                all_columns, non_key_columns
            )

        sql += "\n\nCOMMIT;"

        return sql

    def _generate_upsert(
        self,
        target_table: str,
        staging_table: str,
        schema: List[Dict],
        config: IncrementalConfig,
        all_columns: List[str],
        non_key_columns: List[str]
    ) -> str:
        """Generate INSERT ... ON CONFLICT DO UPDATE."""

        sql = f"-- Upsert using INSERT ... ON CONFLICT\n"
        sql += f"INSERT INTO {target_table} (\n"

        # Insert columns
        insert_cols = all_columns.copy()
        if config.created_by_column and config.created_by_column not in insert_cols:
            insert_cols.append(config.created_by_column)
        if config.updated_by_column and config.updated_by_column not in insert_cols:
            insert_cols.append(config.updated_by_column)

        sql += "  " + ",\n  ".join([f'"{col}"' for col in insert_cols])
        sql += "\n)\n"

        # SELECT from staging
        sql += "SELECT\n"
        select_cols = [f'"{col}"' for col in all_columns]

        # Add timestamps
        if config.created_by_column and config.created_by_column not in all_columns:
            select_cols.append(f"CURRENT_TIMESTAMP")
        if config.updated_by_column and config.updated_by_column not in all_columns:
            select_cols.append(f"CURRENT_TIMESTAMP")

        sql += "  " + ",\n  ".join(select_cols)
        sql += f"\nFROM {staging_table}\n"

        # ON CONFLICT clause
        conflict_cols = ", ".join([f'"{key}"' for key in config.primary_keys])
        sql += f"ON CONFLICT ({conflict_cols})\n"

        # DO UPDATE SET
        sql += "DO UPDATE SET\n"

        # Determine which columns to update
        if config.merge_strategy == MergeStrategy.UPDATE_SELECTIVE:
            update_cols = config.update_columns
            sql += self._build_update_set_clause(update_cols, config)
        elif config.merge_strategy == MergeStrategy.UPDATE_CHANGED:
            # Only update if values differ
            update_cols = non_key_columns
            sql += self._build_update_set_clause(update_cols, config)
            sql += "\nWHERE "
            conditions = []
            check_cols = non_key_columns[:3]  # Limit for readability
            # Extract table name from fully qualified name
            table_only = target_table.split(".")[-1].strip('"')
            for col in check_cols:
                conditions.append(
                    f'"{table_only}"."{col}" IS DISTINCT FROM EXCLUDED."{col}"'
                )
            if len(non_key_columns) > 3:
                conditions.append("-- ... additional columns ...")
            sql += " OR\n  ".join(conditions)
        else:  # UPDATE_ALL
            update_cols = non_key_columns
            sql += self._build_update_set_clause(update_cols, config)

        sql += ";"

        return sql

    def _build_update_set_clause(
        self,
        update_cols: List[str],
        config: IncrementalConfig
    ) -> str:
        """Build SET clause for ON CONFLICT DO UPDATE."""

        set_statements = [f'  "{col}" = EXCLUDED."{col}"' for col in update_cols]

        # Add updated_at if configured
        if config.updated_by_column and config.updated_by_column not in update_cols:
            set_statements.append(
                f'  "{config.updated_by_column}" = CURRENT_TIMESTAMP'
            )

        return ",\n".join(set_statements)

    def _generate_insert_ignore(
        self,
        target_table: str,
        staging_table: str,
        all_columns: List[str],
        primary_keys: List[str]
    ) -> str:
        """Generate INSERT ... ON CONFLICT DO NOTHING."""

        sql = f"-- Insert new records only (ignore conflicts)\n"
        sql += f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in all_columns])
        sql += "\n)\n"
        sql += "SELECT\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in all_columns])
        sql += f"\nFROM {staging_table}\n"

        conflict_cols = ", ".join([f'"{key}"' for key in primary_keys])
        sql += f"ON CONFLICT ({conflict_cols}) DO NOTHING;"

        return sql

    def generate_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate PostgreSQL INSERT (APPEND) statement."""

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name
        )

        columns = [field['name'] for field in schema]

        sql = "BEGIN;\n\n"
        sql += f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in columns])
        sql += "\n)\n"
        sql += "SELECT\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in columns])
        sql += f"\nFROM {staging_table}"

        # Optional: Filter to only new records not in target
        if config.load_pattern == LoadPattern.INCREMENTAL_APPEND and config.primary_keys:
            sql += "\nWHERE NOT EXISTS (\n"
            sql += f"  SELECT 1 FROM {target_table} AS target\n"
            sql += f"  WHERE {self._build_join_condition(config.primary_keys, 'target', 'source')}\n"
            sql += ")"

        sql += ";\n\n"
        sql += "COMMIT;"

        return sql

    def generate_incremental_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental append DDL (append only new records).

        Same as INCREMENTAL_APPEND pattern - uses append logic.
        """
        # INCREMENTAL_APPEND is handled by generate_append_ddl with the flag check
        return self.generate_append_ddl(
            schema, table_name, config, dataset_name, project_id, **kwargs
        )

    def generate_full_refresh_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate PostgreSQL TRUNCATE + INSERT."""

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name
        )

        columns = [field['name'] for field in schema]

        sql = "-- Full refresh: Truncate and reload\n"
        sql += "BEGIN;\n\n"
        sql += f"TRUNCATE TABLE {target_table};\n\n"

        sql += f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in columns])
        sql += "\n)\n"
        sql += "SELECT\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in columns])
        sql += f"\nFROM {staging_table};\n\n"

        sql += "COMMIT;\n\n"
        sql += f"-- Recommended: ANALYZE {target_table};"

        return sql

    def generate_scd2_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate PostgreSQL SCD Type 2 DDL.

        Uses CTEs for clarity and efficiency.
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name
        )

        # Get column names
        all_column_names = [field['name'] for field in schema]

        # Columns to track for changes
        exclude_columns = config.primary_keys + [
            config.effective_date_column,
            config.expiration_date_column,
            config.is_current_column
        ]
        hash_cols = config.hash_columns or [col for col in all_column_names if col not in exclude_columns]

        sql = "-- SCD Type 2: Maintain historical versions\n"
        sql += "BEGIN;\n\n"

        # Step 1: Expire changed records using CTE
        sql += "-- Step 1: Expire records that have changed\n"
        sql += "WITH changed_records AS (\n"
        sql += f"  SELECT {', '.join([f'target.\"{key}\"' for key in config.primary_keys])}\n"
        sql += f"  FROM {target_table} AS target\n"
        sql += f"  INNER JOIN {staging_table} AS source\n"
        sql += f"    ON {self._build_join_condition(config.primary_keys, 'target', 'source')}\n"
        sql += f"  WHERE target.\"{config.is_current_column}\" = TRUE\n"

        # Check if any tracked columns changed
        sql += "    AND (\n"
        change_conditions = []
        for col in hash_cols:
            change_conditions.append(
                f"      target.\"{col}\" IS DISTINCT FROM source.\"{col}\""
            )
        sql += " OR\n".join(change_conditions)
        sql += "\n    )\n"
        sql += ")\n"

        sql += f"UPDATE {target_table}\n"
        sql += "SET\n"
        sql += f"  \"{config.expiration_date_column}\" = CURRENT_DATE,\n"
        sql += f"  \"{config.is_current_column}\" = FALSE\n"
        sql += "FROM changed_records\n"

        # Build WHERE for UPDATE
        # Extract table name from fully qualified name
        table_only = target_table.split(".")[-1].strip('"')
        where_conditions = [
            f'"{table_only}"."{key}" = changed_records."{key}"'
            for key in config.primary_keys
        ]
        sql += f"WHERE {' AND '.join(where_conditions)};\n\n"

        # Step 2: Insert new versions
        sql += "-- Step 2: Insert new and changed records\n"
        sql += f"INSERT INTO {target_table} (\n"

        insert_cols = all_column_names + [
            config.effective_date_column,
            config.expiration_date_column,
            config.is_current_column
        ]

        sql += "  " + ",\n  ".join([f'"{col}"' for col in insert_cols])
        sql += "\n)\n"
        sql += "SELECT\n"
        sql += "  " + ",\n  ".join([f'source."{col}"' for col in all_column_names]) + ",\n"
        sql += f"  CURRENT_DATE AS \"{config.effective_date_column}\",\n"
        sql += f"  '9999-12-31'::DATE AS \"{config.expiration_date_column}\",\n"
        sql += f"  TRUE AS \"{config.is_current_column}\"\n"
        sql += f"FROM {staging_table} AS source\n"
        sql += "WHERE NOT EXISTS (\n"
        sql += f"  SELECT 1 FROM {target_table} AS target\n"
        sql += f"  WHERE\n"
        sql += f"    {self._build_join_condition(config.primary_keys, 'target', 'source')}\n"
        sql += f"    AND target.\"{config.is_current_column}\" = TRUE\n"

        # Check all columns match (no change)
        sql += "    AND (\n"
        sql += "      " + " AND\n      ".join([
            f'target."{col}" IS NOT DISTINCT FROM source."{col}"'
            for col in hash_cols
        ])
        sql += "\n    )\n"
        sql += ");\n\n"

        sql += "COMMIT;\n\n"
        sql += f"-- Recommended: ANALYZE {target_table};"

        return sql

    def generate_staging_table_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        staging_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        temporary: bool = True,
        **kwargs
    ) -> str:
        """
        Generate staging table DDL for PostgreSQL.

        Args:
            temporary: Create as TEMPORARY table
        """
        staging_name = staging_name or f"{table_name}_staging"
        staging_table = self._build_table_ref(staging_name, dataset_name) if not temporary else f'"{staging_name}"'

        sql = "-- Create staging table\n"
        sql += "CREATE "

        if temporary:
            sql += "TEMPORARY "

        sql += f"TABLE {staging_table} (\n"

        # Add all columns
        column_defs = []
        for field in schema:
            col_def = f'  "{field["name"]}" {field["type"]}'
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            column_defs.append(col_def)

        sql += ",\n".join(column_defs)
        sql += "\n)"

        # Add ON COMMIT DROP for temporary tables
        if temporary:
            sql += " ON COMMIT DROP"

        sql += ";"

        return sql

    def generate_copy_from_csv_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        file_path: str,
        dataset_name: Optional[str] = None,
        staging_table: Optional[str] = None,
        delimiter: str = ',',
        header: bool = True,
        null_string: str = '',
        encoding: str = 'UTF8',
        **kwargs
    ) -> str:
        """
        Generate PostgreSQL COPY FROM statement.

        Args:
            file_path: Path to CSV file (absolute or relative)
            delimiter: Column delimiter
            header: Whether CSV has header row
            null_string: String representing NULL values
            encoding: File encoding

        Example output:
        ```sql
        COPY "public"."users_staging" (
          "user_id",
          "name",
          "email"
        )
        FROM '/path/to/users.csv'
        WITH (
          FORMAT csv,
          DELIMITER ',',
          HEADER true,
          NULL '',
          ENCODING 'UTF8'
        );
        ```
        """
        target_table = self._build_table_ref(
            staging_table or f"{table_name}_staging",
            dataset_name
        )

        columns = [field['name'] for field in schema]

        sql = f"-- Load data from CSV into staging table\n"
        sql += f"COPY {target_table} (\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in columns])
        sql += "\n)\n"
        sql += f"FROM '{file_path}'\n"
        sql += "WITH (\n"
        sql += "  FORMAT csv,\n"
        sql += f"  DELIMITER '{delimiter}',\n"
        sql += f"  HEADER {str(header).lower()},\n"
        sql += f"  NULL '{null_string}',\n"
        sql += f"  ENCODING '{encoding}'\n"
        sql += ");"

        return sql

    def get_max_timestamp_query(
        self,
        table_name: str,
        timestamp_column: str,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate query to get max timestamp for PostgreSQL."""

        table_ref = self._build_table_ref(table_name, dataset_name)

        return f"""SELECT COALESCE(MAX("{timestamp_column}"), '1970-01-01'::TIMESTAMP) AS max_timestamp
FROM {table_ref}""".strip()

    def generate_incremental_timestamp_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental load based on timestamp for PostgreSQL.

        Uses CTE to get max timestamp.
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name
        )

        columns = [field['name'] for field in schema]

        sql = f"-- Incremental load based on \"{config.incremental_column}\"\n"
        sql += "BEGIN;\n\n"

        # Use CTE to get max timestamp
        sql += "-- Load only new records\n"
        sql += "WITH max_timestamp AS (\n"
        sql += f"  {self.get_max_timestamp_query(table_name, config.incremental_column, dataset_name)}\n"
        sql += ")\n"

        sql += f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in columns])
        sql += "\n)\n"
        sql += "SELECT\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in columns])
        sql += f"\nFROM {staging_table}\n"
        sql += "CROSS JOIN max_timestamp\n"
        sql += f'WHERE "{config.incremental_column}" > max_timestamp.max_timestamp'

        # Optional: Add lookback window
        if config.lookback_window:
            # Parse lookback window (e.g., "2 hours" -> INTERVAL '2 hours')
            sql += f" - INTERVAL '{config.lookback_window}'"

        sql += ";\n\n"
        sql += "COMMIT;\n\n"
        sql += f"-- Recommended: ANALYZE {target_table};"

        return sql

    def generate_cdc_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate CDC processing DDL for PostgreSQL.

        Uses CTEs to handle each operation type separately.
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name
        )

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

        sql = "-- CDC Processing: Handle Insert/Update/Delete operations\n"
        sql += "BEGIN;\n\n"

        # Step 1: Handle DELETES
        if config.delete_strategy == DeleteStrategy.HARD_DELETE:
            sql += "-- Step 1: Process DELETE operations\n"
            sql += f"DELETE FROM {target_table}\n"
            sql += "WHERE ("
            sql += ", ".join([f'"{key}"' for key in config.primary_keys])
            sql += ") IN (\n"
            sql += f"  SELECT "
            sql += ", ".join([f'"{key}"' for key in config.primary_keys])
            sql += f"\n  FROM {staging_table}\n"
            sql += f'  WHERE "{config.operation_column}" = \'D\'\n'
            sql += ");\n\n"
        elif config.delete_strategy == DeleteStrategy.SOFT_DELETE:
            sql += "-- Step 1: Process DELETE operations (soft delete)\n"
            sql += f"UPDATE {target_table}\n"
            sql += f'SET "{config.soft_delete_column}" = TRUE\n'
            sql += "WHERE ("
            sql += ", ".join([f'"{key}"' for key in config.primary_keys])
            sql += ") IN (\n"
            sql += f"  SELECT "
            sql += ", ".join([f'"{key}"' for key in config.primary_keys])
            sql += f"\n  FROM {staging_table}\n"
            sql += f'  WHERE "{config.operation_column}" = \'D\'\n'
            sql += ");\n\n"

        # Step 2: Handle UPDATES using UPSERT
        sql += "-- Step 2: Process UPDATE operations\n"
        sql += f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in data_columns])
        sql += "\n)\n"
        sql += "SELECT\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in data_columns])
        sql += f"\nFROM {staging_table}\n"
        sql += f'WHERE "{config.operation_column}" = \'U\'\n'

        conflict_cols = ", ".join([f'"{key}"' for key in config.primary_keys])
        sql += f"ON CONFLICT ({conflict_cols})\n"
        sql += "DO UPDATE SET\n"
        set_statements = [f'  "{col}" = EXCLUDED."{col}"' for col in update_columns]
        sql += ",\n".join(set_statements)
        sql += ";\n\n"

        # Step 3: Handle INSERTS
        sql += "-- Step 3: Process INSERT operations\n"
        sql += f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in data_columns])
        sql += "\n)\n"
        sql += "SELECT\n"
        sql += "  " + ",\n  ".join([f'"{col}"' for col in data_columns])
        sql += f"\nFROM {staging_table}\n"
        sql += f'WHERE "{config.operation_column}" = \'I\'\n'
        sql += f"ON CONFLICT ({conflict_cols}) DO NOTHING;\n\n"

        sql += "COMMIT;\n\n"
        sql += f"-- Recommended: ANALYZE {target_table};"

        return sql

    def generate_analyze_command(
        self,
        table_name: str,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate ANALYZE command for PostgreSQL.

        Updates table statistics for query planner.
        """
        table_ref = self._build_table_ref(table_name, dataset_name)

        sql = "-- Update table statistics\n"
        sql += f"ANALYZE {table_ref};"

        return sql

    def generate_vacuum_command(
        self,
        table_name: str,
        dataset_name: Optional[str] = None,
        full: bool = False,
        analyze: bool = True,
        **kwargs
    ) -> str:
        """
        Generate VACUUM command for PostgreSQL.

        Args:
            full: Perform VACUUM FULL (rewrites entire table)
            analyze: Also update statistics (VACUUM ANALYZE)
        """
        table_ref = self._build_table_ref(table_name, dataset_name)

        sql = "-- Reclaim space and update statistics\n"
        sql += "VACUUM"

        if full:
            sql += " FULL"

        if analyze:
            sql += " ANALYZE"

        sql += f" {table_ref};"

        return sql

    def _build_table_ref(
        self,
        table_name: str,
        dataset_name: Optional[str] = None
    ) -> str:
        """Build fully-qualified PostgreSQL table reference."""

        if dataset_name:
            return f'"{dataset_name}"."{table_name}"'
        else:
            return f'"{table_name}"'

    def _build_join_condition(
        self,
        primary_keys: List[str],
        target_alias: str = "target",
        source_alias: str = "source"
    ) -> str:
        """Build JOIN ON condition for PostgreSQL with double quotes."""
        conditions = [
            f'{target_alias}."{key}" = {source_alias}."{key}"'
            for key in primary_keys
        ]
        return " AND ".join(conditions)
