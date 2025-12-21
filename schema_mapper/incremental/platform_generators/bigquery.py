"""
BigQuery-specific incremental DDL generation.

This module provides BigQuery-specific implementations for all incremental
load patterns including MERGE, SCD Type 2, CDC, and more.
"""

from typing import List, Dict, Optional
from ..incremental_base import IncrementalDDLGenerator
from ..patterns import LoadPattern, IncrementalConfig, MergeStrategy, DeleteStrategy


class BigQueryIncrementalGenerator(IncrementalDDLGenerator):
    """
    Generate BigQuery-specific incremental load DDL.

    BigQuery specifics:
    - Uses MERGE statement (fully supported)
    - Supports partition pruning for efficiency
    - Requires staging table for MERGE
    - No transactions (atomic at statement level)
    """

    def __init__(self):
        super().__init__('bigquery')

    def supports_pattern(self, pattern: LoadPattern) -> bool:
        """Check if BigQuery supports this pattern."""
        # BigQuery supports all patterns
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
        Generate BigQuery MERGE (UPSERT) statement.

        Example output:
        ```sql
        MERGE `project.dataset.table` AS target
        USING `project.dataset.table_staging` AS source
        ON target.user_id = source.user_id
        WHEN MATCHED THEN
          UPDATE SET
            target.name = source.name,
            target.email = source.email,
            target.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (user_id, name, email, created_at, updated_at)
          VALUES (source.user_id, source.name, source.email,
                  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        ```
        """
        # Build table references
        target_table = self._build_table_ref(table_name, dataset_name, project_id)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name,
            project_id
        )

        # Get columns
        all_columns = [field['name'] for field in schema]
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        # Build MERGE statement
        merge_sql = f"MERGE {target_table} AS target\n"
        merge_sql += f"USING {staging_table} AS source\n"

        # ON condition
        merge_sql += f"ON {self._build_join_condition(config.primary_keys)}\n"

        # WHEN MATCHED
        if config.merge_strategy != MergeStrategy.UPDATE_NONE:
            merge_sql += self._build_matched_clause(
                schema, config, non_key_columns
            )

        # WHEN NOT MATCHED
        merge_sql += self._build_not_matched_clause(
            schema, config, all_columns
        )

        merge_sql += ";"

        return merge_sql

    def _build_matched_clause(
        self,
        schema: List[Dict],
        config: IncrementalConfig,
        non_key_columns: List[str]
    ) -> str:
        """Build WHEN MATCHED clause."""

        # Determine which columns to update
        if config.merge_strategy == MergeStrategy.UPDATE_SELECTIVE:
            update_cols = config.update_columns
        elif config.merge_strategy == MergeStrategy.UPDATE_CHANGED:
            # Update only if values differ (requires hash comparison)
            update_cols = non_key_columns
            # Build WHEN MATCHED with change detection
            clause = "WHEN MATCHED AND (\n"
            conditions = []
            for col in non_key_columns:
                conditions.append(
                    f"  target.{col} != source.{col} OR "
                    f"(target.{col} IS NULL AND source.{col} IS NOT NULL) OR "
                    f"(target.{col} IS NOT NULL AND source.{col} IS NULL)"
                )
            clause += " OR\n".join(conditions)
            clause += "\n) THEN\n  UPDATE SET\n"
        else:  # UPDATE_ALL
            update_cols = non_key_columns
            clause = "WHEN MATCHED THEN\n  UPDATE SET\n"

        if config.merge_strategy != MergeStrategy.UPDATE_CHANGED:
            clause = "WHEN MATCHED THEN\n  UPDATE SET\n"

        # Build SET clause
        set_statements = []
        for col in update_cols:
            set_statements.append(f"    target.{col} = source.{col}")

        # Add updated_at if configured
        if config.updated_by_column and config.updated_by_column not in update_cols:
            set_statements.append(
                f"    target.{config.updated_by_column} = CURRENT_TIMESTAMP()"
            )

        clause += ",\n".join(set_statements)
        clause += "\n"

        return clause

    def _build_not_matched_clause(
        self,
        schema: List[Dict],
        config: IncrementalConfig,
        all_columns: List[str]
    ) -> str:
        """Build WHEN NOT MATCHED clause."""

        clause = "WHEN NOT MATCHED THEN\n"
        clause += "  INSERT ("

        # Column list
        insert_cols = all_columns.copy()

        # Add created_at/updated_at if configured and not in schema
        if config.created_by_column and config.created_by_column not in insert_cols:
            insert_cols.append(config.created_by_column)
        if config.updated_by_column and config.updated_by_column not in insert_cols:
            insert_cols.append(config.updated_by_column)

        clause += ", ".join(insert_cols)
        clause += ")\n  VALUES ("

        # Values
        values = []
        for col in all_columns:
            values.append(f"source.{col}")

        # Add timestamps
        if config.created_by_column and config.created_by_column not in all_columns:
            values.append("CURRENT_TIMESTAMP()")
        if config.updated_by_column and config.updated_by_column not in all_columns:
            values.append("CURRENT_TIMESTAMP()")

        clause += ", ".join(values)
        clause += ")\n"

        return clause

    def generate_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate BigQuery INSERT (APPEND) statement."""

        target_table = self._build_table_ref(table_name, dataset_name, project_id)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name,
            project_id
        )

        columns = [field['name'] for field in schema]

        sql = f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join(columns)
        sql += "\n)\n"
        sql += f"SELECT\n"
        sql += "  " + ",\n  ".join(columns)
        sql += f"\nFROM {staging_table}"

        sql += ";"

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
        """Generate incremental append DDL (append only new records)."""

        target_table = self._build_table_ref(table_name, dataset_name, project_id)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name,
            project_id
        )

        columns = [field['name'] for field in schema]

        sql = f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join(columns)
        sql += "\n)\n"
        sql += f"SELECT\n"
        sql += "  " + ",\n  ".join(columns)
        sql += f"\nFROM {staging_table}"

        # Filter to only new records not in target
        sql += f"\nWHERE NOT EXISTS (\n"
        sql += f"  SELECT 1 FROM {target_table} AS target\n"
        sql += f"  WHERE {self._build_join_condition(config.primary_keys, 'target', 'source')}\n"
        sql += ")"

        sql += ";"

        return sql

    def generate_full_refresh_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate BigQuery TRUNCATE + INSERT or CREATE OR REPLACE."""

        target_table = self._build_table_ref(table_name, dataset_name, project_id)

        # Option 1: TRUNCATE + INSERT (preserves table structure)
        sql = f"-- Full refresh: Truncate and reload\n"
        sql += f"TRUNCATE TABLE {target_table};\n\n"
        sql += self.generate_append_ddl(
            schema, table_name, config, dataset_name, project_id
        )

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
        Generate BigQuery SCD Type 2 DDL.

        SCD Type 2 maintains history by:
        1. Expiring old records (set effective_to, is_current = FALSE)
        2. Inserting new versions of changed records
        """
        target_table = self._build_table_ref(table_name, dataset_name, project_id)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name,
            project_id
        )

        # Columns to track for changes
        hash_cols = config.hash_columns or self._get_non_key_columns(schema, config.primary_keys)

        sql = f"-- SCD Type 2: Maintain historical versions\n\n"

        # Step 1: Expire changed records
        sql += f"-- Step 1: Expire records that have changed\n"
        sql += f"UPDATE {target_table} AS target\n"
        sql += f"SET\n"
        sql += f"  {config.expiration_date_column} = CURRENT_DATE(),\n"
        sql += f"  {config.is_current_column} = FALSE\n"
        sql += f"FROM {staging_table} AS source\n"
        sql += f"WHERE\n"
        sql += f"  {self._build_join_condition(config.primary_keys)}\n"
        sql += f"  AND target.{config.is_current_column} = TRUE\n"

        # Check if any tracked columns changed
        sql += "  AND (\n"
        change_conditions = []
        for col in hash_cols:
            change_conditions.append(
                f"    target.{col} != source.{col} OR "
                f"(target.{col} IS NULL AND source.{col} IS NOT NULL) OR "
                f"(target.{col} IS NOT NULL AND source.{col} IS NULL)"
            )
        sql += " OR\n".join(change_conditions)
        sql += "\n  );\n\n"

        # Step 2: Insert new versions
        sql += f"-- Step 2: Insert new and changed records\n"
        sql += f"INSERT INTO {target_table} (\n"

        all_columns = [field['name'] for field in schema]
        insert_cols = all_columns + [
            config.effective_date_column,
            config.expiration_date_column,
            config.is_current_column
        ]

        sql += "  " + ",\n  ".join(insert_cols)
        sql += "\n)\n"
        sql += f"SELECT\n"
        sql += "  " + ",\n  ".join([f"source.{col}" for col in all_columns]) + ",\n"
        sql += f"  CURRENT_DATE() AS {config.effective_date_column},\n"
        sql += f"  DATE('9999-12-31') AS {config.expiration_date_column},\n"
        sql += f"  TRUE AS {config.is_current_column}\n"
        sql += f"FROM {staging_table} AS source\n"
        sql += f"WHERE NOT EXISTS (\n"
        sql += f"  SELECT 1 FROM {target_table} AS target\n"
        sql += f"  WHERE\n"
        sql += f"    {self._build_join_condition(config.primary_keys)}\n"
        sql += f"    AND target.{config.is_current_column} = TRUE\n"

        # Check all columns match (no change)
        sql += "    AND (\n"
        sql += "      " + " AND\n      ".join([
            f"(target.{col} = source.{col} OR (target.{col} IS NULL AND source.{col} IS NULL))"
            for col in hash_cols
        ])
        sql += "\n    )\n"
        sql += ");"

        return sql

    def generate_scd1_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SCD Type 1 DDL (overwrite changes).

        SCD Type 1 is essentially an UPSERT - just update the current values.
        """
        # SCD1 is just a regular UPSERT
        return self.generate_merge_ddl(
            schema, table_name, config, dataset_name, project_id, **kwargs
        )

    def generate_staging_table_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        staging_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate staging table DDL for BigQuery."""

        staging_name = staging_name or f"{table_name}_staging"
        staging_table = self._build_table_ref(staging_name, dataset_name, project_id)

        sql = f"-- Create staging table\n"
        sql += f"CREATE OR REPLACE TABLE {staging_table} (\n"

        # Add all columns
        column_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            column_defs.append(col_def)

        sql += ",\n".join(column_defs)
        sql += "\n);"

        return sql

    def get_max_timestamp_query(
        self,
        table_name: str,
        timestamp_column: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Generate query to get max timestamp."""

        table_ref = self._build_table_ref(table_name, dataset_name, project_id)

        return f"""SELECT COALESCE(MAX({timestamp_column}), TIMESTAMP('1970-01-01')) AS max_timestamp
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
        Generate incremental load based on timestamp.

        Only loads records newer than max timestamp in target.
        """
        target_table = self._build_table_ref(table_name, dataset_name, project_id)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name,
            project_id
        )

        columns = [field['name'] for field in schema]

        sql = f"-- Incremental load based on {config.incremental_column}\n\n"

        # Step 1: Get max timestamp (using DECLARE for scripting)
        sql += f"-- Get max timestamp from target\n"
        sql += f"DECLARE max_ts TIMESTAMP DEFAULT (\n"
        sql += f"  {self.get_max_timestamp_query(table_name, config.incremental_column, dataset_name, project_id)}\n"
        sql += f");\n\n"

        # Step 2: Load only new records
        sql += f"-- Load only new records\n"
        sql += f"INSERT INTO {target_table} (\n"
        sql += "  " + ",\n  ".join(columns)
        sql += "\n)\n"
        sql += f"SELECT\n"
        sql += "  " + ",\n  ".join(columns)
        sql += f"\nFROM {staging_table}\n"

        # Add WHERE clause for incremental filter
        if config.lookback_window:
            sql += f"WHERE {config.incremental_column} > TIMESTAMP_SUB(max_ts, INTERVAL {config.lookback_window})"
        else:
            sql += f"WHERE {config.incremental_column} > max_ts"

        sql += ";"

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
        Generate CDC MERGE DDL.

        Handles I/U/D operations from CDC stream.
        """
        target_table = self._build_table_ref(table_name, dataset_name, project_id)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name,
            project_id
        )

        all_columns = [field['name'] for field in schema]
        non_key_columns = self._get_non_key_columns(schema, config.primary_keys)

        sql = f"-- CDC Merge: Handle Insert/Update/Delete operations\n\n"

        sql += f"MERGE {target_table} AS target\n"
        sql += f"USING {staging_table} AS source\n"
        sql += f"ON {self._build_join_condition(config.primary_keys)}\n"

        # WHEN MATCHED AND operation = 'D' THEN DELETE
        if config.delete_strategy == DeleteStrategy.HARD_DELETE:
            sql += f"WHEN MATCHED AND source.{config.operation_column} = 'D' THEN\n"
            sql += "  DELETE\n"
        elif config.delete_strategy == DeleteStrategy.SOFT_DELETE:
            sql += f"WHEN MATCHED AND source.{config.operation_column} = 'D' THEN\n"
            sql += "  UPDATE SET\n"
            sql += f"    target.{config.soft_delete_column} = TRUE\n"

        # WHEN MATCHED AND operation = 'U' THEN UPDATE
        sql += f"WHEN MATCHED AND source.{config.operation_column} = 'U' THEN\n"
        sql += "  UPDATE SET\n"
        set_statements = [f"    target.{col} = source.{col}" for col in non_key_columns]
        sql += ",\n".join(set_statements)
        sql += "\n"

        # WHEN NOT MATCHED AND operation = 'I' THEN INSERT
        sql += f"WHEN NOT MATCHED AND source.{config.operation_column} = 'I' THEN\n"
        sql += "  INSERT ("
        sql += ", ".join(all_columns)
        sql += ")\n  VALUES ("
        sql += ", ".join([f"source.{col}" for col in all_columns])
        sql += ");"

        return sql

    def generate_delete_insert_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate DELETE + INSERT DDL.

        Alternative to MERGE for platforms that prefer it.
        """
        target_table = self._build_table_ref(table_name, dataset_name, project_id)
        staging_table = self._build_table_ref(
            config.staging_table or f"{table_name}_staging",
            dataset_name,
            project_id
        )

        columns = [field['name'] for field in schema]

        sql = f"-- Delete-Insert pattern\n\n"

        # Step 1: Delete matching records
        sql += f"-- Step 1: Delete matching records\n"
        sql += f"DELETE FROM {target_table} AS target\n"
        sql += f"WHERE EXISTS (\n"
        sql += f"  SELECT 1 FROM {staging_table} AS source\n"
        sql += f"  WHERE {self._build_join_condition(config.primary_keys)}\n"
        sql += ");\n\n"

        # Step 2: Insert from staging
        sql += f"-- Step 2: Insert from staging\n"
        sql += self.generate_append_ddl(
            schema, table_name, config, dataset_name, project_id
        )

        return sql

    def _build_table_ref(
        self,
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """Build fully-qualified BigQuery table reference."""

        if project_id and dataset_name:
            return f"`{project_id}.{dataset_name}.{table_name}`"
        elif dataset_name:
            return f"`{dataset_name}.{table_name}`"
        else:
            return f"`{table_name}`"
