"""
Redshift-specific incremental DDL generation.

Redshift does not support MERGE statements, so we simulate MERGE using
DELETE + INSERT pattern within transactions. This module also supports
COPY commands for efficient S3 loading, distribution/sort keys, and
VACUUM/ANALYZE maintenance commands.
"""

from typing import List, Dict, Optional
from ..incremental_base import IncrementalDDLGenerator
from ..patterns import LoadPattern, IncrementalConfig, MergeStrategy, DeleteStrategy


class RedshiftIncrementalGenerator(IncrementalDDLGenerator):
    """
    Redshift-specific implementation of incremental load patterns.

    Key Redshift features:
    - NO MERGE statement (uses DELETE + INSERT pattern)
    - COPY command for efficient S3 loading
    - Distribution keys (DISTKEY) for data distribution across nodes
    - Sort keys (SORTKEY) for query performance
    - TEMPORARY tables commonly used for staging
    - VACUUM and ANALYZE for maintenance
    - Transactional support (BEGIN/COMMIT)

    Example:
        >>> from schema_mapper.incremental import get_incremental_generator, IncrementalConfig, LoadPattern
        >>> generator = get_incremental_generator('redshift')
        >>> config = IncrementalConfig(
        ...     load_pattern=LoadPattern.UPSERT,
        ...     primary_keys=['user_id']
        ... )
        >>> ddl = generator.generate_merge_ddl(
        ...     schema=[{'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'}],
        ...     table_name='users',
        ...     config=config,
        ...     dataset_name='public'
        ... )
    """

    def __init__(self):
        super().__init__('redshift')

    def supports_pattern(self, pattern: LoadPattern) -> bool:
        """
        Check if Redshift supports this load pattern.

        Note: Redshift doesn't have native MERGE, but we simulate it
        with DELETE + INSERT, so UPSERT is supported.
        """
        # All patterns supported, MERGE simulated with DELETE+INSERT
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
        Generate Redshift "MERGE" using DELETE + INSERT pattern.

        Redshift doesn't have native MERGE, so we use:
        1. BEGIN TRANSACTION
        2. DELETE matching records from target
        3. INSERT all records from staging
        4. COMMIT

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration
            dataset_name: Schema name (e.g., 'public', 'analytics')
            **kwargs: Additional options (staging_table, use_transaction)

        Returns:
            Complete DELETE + INSERT DDL with transaction wrapper
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
        join_condition = self._build_join_condition(
            config.primary_keys,
            target_table.split('.')[-1],
            'staging'
        )

        ddl_parts = []

        ddl_parts.append("-- Redshift MERGE simulation using DELETE + INSERT")
        ddl_parts.append("-- Note: Redshift does not support MERGE statement")

        if use_transaction:
            ddl_parts.append("BEGIN TRANSACTION;")
            ddl_parts.append("")

        # Step 1: Delete matching records
        ddl_parts.append("-- Step 1: Delete records that will be updated")
        ddl_parts.append(f"DELETE FROM {target_table}")
        ddl_parts.append(f"USING {staging_ref} AS staging")
        ddl_parts.append(f"WHERE {join_condition};")
        ddl_parts.append("")

        # Step 2: Insert all records from staging
        ddl_parts.append("-- Step 2: Insert all records from staging")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join(all_columns))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join(all_columns))
        ddl_parts.append(f"FROM {staging_ref};")

        if use_transaction:
            ddl_parts.append("")
            ddl_parts.append("COMMIT;")

        # Add performance recommendations
        ddl_parts.append("")
        ddl_parts.append("-- Performance recommendations:")
        ddl_parts.append(f"-- ANALYZE {target_table};  -- Update table statistics")
        ddl_parts.append(f"-- VACUUM {target_table};   -- Reclaim space and sort rows (if needed)")

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
        Generate Redshift INSERT (APPEND) statement.

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
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join(columns))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join(columns))
        ddl_parts.append(f"FROM {staging_ref}")

        # Optional: Filter to only new records not in target (for INCREMENTAL_APPEND)
        if config.load_pattern == LoadPattern.INCREMENTAL_APPEND:
            join_condition = self._build_join_condition(
                config.primary_keys,
                'target',
                staging_ref.split('.')[-1]
            )
            ddl_parts.append("WHERE NOT EXISTS (")
            ddl_parts.append(f"  SELECT 1 FROM {target_table} AS target")
            ddl_parts.append(f"  WHERE {join_condition}")
            ddl_parts.append(")")

        ddl_parts.append(";")
        ddl_parts.append("")
        ddl_parts.append("COMMIT;")
        ddl_parts.append("")
        ddl_parts.append(f"-- Recommended: ANALYZE {target_table};")

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
        Generate Redshift TRUNCATE + INSERT.

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
        ddl_parts.append("  " + ",\n  ".join(columns))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join(columns))
        ddl_parts.append(f"FROM {staging_ref};")
        ddl_parts.append("")
        ddl_parts.append("COMMIT;")
        ddl_parts.append("")
        ddl_parts.append(f"-- Recommended: ANALYZE {target_table};")

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
        Generate Redshift SCD Type 2 DDL.

        Uses UPDATE + INSERT pattern to maintain historical versions.

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
        join_condition = self._build_join_condition(
            config.primary_keys,
            target_table.split('.')[-1],
            'staging'
        )

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
        ddl_parts.append(f"UPDATE {target_table}")
        ddl_parts.append("SET")
        ddl_parts.append(f"  {config.expiration_date_column} = GETDATE()::DATE,")
        ddl_parts.append(f"  {config.is_current_column} = FALSE")
        ddl_parts.append(f"FROM {staging_ref} AS staging")
        ddl_parts.append("WHERE")
        ddl_parts.append(f"  {join_condition}")
        ddl_parts.append(f"  AND {target_table.split('.')[-1]}.{config.is_current_column} = TRUE")

        # Check if any tracked columns changed
        ddl_parts.append("  AND (")
        change_conditions = []
        for col in hash_cols:
            change_conditions.append(
                f"    COALESCE({target_table.split('.')[-1]}.{col}::VARCHAR, 'NULL_SENTINEL') != "
                f"COALESCE(staging.{col}::VARCHAR, 'NULL_SENTINEL')"
            )
        ddl_parts.append(" OR\n".join(change_conditions))
        ddl_parts.append("  );")
        ddl_parts.append("")

        # Step 2: Insert new and changed records
        ddl_parts.append("-- Step 2: Insert new and changed records")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join(all_column_names))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join([f"staging.{col}" for col in data_columns]) + ",")
        ddl_parts.append(f"  GETDATE()::DATE AS {config.effective_date_column},")
        ddl_parts.append(f"  '9999-12-31'::DATE AS {config.expiration_date_column},")
        ddl_parts.append(f"  TRUE AS {config.is_current_column}")
        ddl_parts.append(f"FROM {staging_ref} AS staging")
        ddl_parts.append("WHERE NOT EXISTS (")
        ddl_parts.append(f"  SELECT 1 FROM {target_table} AS target")
        ddl_parts.append("  WHERE")

        # Join on primary keys
        target_join_condition = self._build_join_condition(
            config.primary_keys,
            'target',
            'staging'
        )
        ddl_parts.append(f"    {target_join_condition}")
        ddl_parts.append(f"    AND target.{config.is_current_column} = TRUE")

        # Check all columns match (no change)
        ddl_parts.append("    AND (")
        match_conditions = []
        for col in hash_cols:
            match_conditions.append(
                f"      COALESCE(target.{col}::VARCHAR, 'NULL_SENTINEL') = COALESCE(staging.{col}::VARCHAR, 'NULL_SENTINEL')"
            )
        ddl_parts.append(" AND\n".join(match_conditions))
        ddl_parts.append("    )")
        ddl_parts.append(");")
        ddl_parts.append("")

        ddl_parts.append("COMMIT;")
        ddl_parts.append("")
        ddl_parts.append(f"-- Recommended: ANALYZE {target_table};")

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
        Generate incremental load based on timestamp for Redshift.

        Uses temporary table to store max timestamp.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration with incremental_column
            dataset_name: Schema name
            **kwargs: Additional options (staging_table)

        Returns:
            INSERT with timestamp filter using temp table
        """
        config.validate()

        target_table = self._build_table_ref(table_name, dataset_name)
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        staging_ref = self._build_table_ref(staging_table, dataset_name)

        columns = [field['name'] for field in schema]

        ddl_parts = []
        ddl_parts.append(f"-- Incremental load based on {config.incremental_column}")
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")

        # Step 1: Get max timestamp into temp table
        ddl_parts.append("-- Get max timestamp from target")
        ddl_parts.append("CREATE TEMP TABLE max_ts_temp AS")
        ddl_parts.append(self.get_max_timestamp_query(table_name, config.incremental_column, dataset_name))
        ddl_parts.append(";")
        ddl_parts.append("")

        # Step 2: Load only new records
        ddl_parts.append("-- Load only new records")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join(columns))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join(columns))
        ddl_parts.append(f"FROM {staging_ref}")
        ddl_parts.append("CROSS JOIN max_ts_temp")

        # Build WHERE clause with optional lookback
        where_clause = f"WHERE {config.incremental_column} > max_ts_temp.max_timestamp"
        if config.lookback_window:
            where_clause += f" - INTERVAL '{config.lookback_window}'"
        ddl_parts.append(where_clause)
        ddl_parts.append(";")
        ddl_parts.append("")

        # Cleanup
        ddl_parts.append("DROP TABLE max_ts_temp;")
        ddl_parts.append("")

        ddl_parts.append("COMMIT;")
        ddl_parts.append("")
        ddl_parts.append(f"-- Recommended: ANALYZE {target_table};")

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
        Generate CDC processing DDL for Redshift.

        Processes I/U/D operations from CDC stream.
        Since Redshift has no MERGE, we handle each operation type separately.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental configuration with operation_column
            dataset_name: Schema name
            **kwargs: Additional options (staging_table)

        Returns:
            CDC processing with separate DELETE/UPDATE/INSERT statements
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
        join_condition = self._build_join_condition(
            config.primary_keys,
            target_table.split('.')[-1],
            'staging'
        )

        ddl_parts = []
        ddl_parts.append("-- CDC Processing: Handle Insert/Update/Delete operations")
        ddl_parts.append("BEGIN TRANSACTION;")
        ddl_parts.append("")

        # Step 1: Handle DELETES
        if config.delete_strategy == DeleteStrategy.HARD_DELETE:
            ddl_parts.append("-- Step 1: Process DELETE operations")
            ddl_parts.append(f"DELETE FROM {target_table}")
            ddl_parts.append(f"USING {staging_ref} AS staging")
            ddl_parts.append("WHERE")
            ddl_parts.append(f"  {join_condition}")
            ddl_parts.append(f"  AND staging.{config.operation_column} = 'D';")
            ddl_parts.append("")
        elif config.delete_strategy == DeleteStrategy.SOFT_DELETE:
            ddl_parts.append("-- Step 1: Process DELETE operations (soft delete)")
            ddl_parts.append(f"UPDATE {target_table}")
            ddl_parts.append(f"SET {config.soft_delete_column} = TRUE")
            ddl_parts.append(f"FROM {staging_ref} AS staging")
            ddl_parts.append("WHERE")
            ddl_parts.append(f"  {join_condition}")
            ddl_parts.append(f"  AND staging.{config.operation_column} = 'D';")
            ddl_parts.append("")

        # Step 2: Handle UPDATES
        ddl_parts.append("-- Step 2: Process UPDATE operations")
        ddl_parts.append(f"UPDATE {target_table}")
        ddl_parts.append("SET")

        # Filter out CDC columns and primary keys from update
        update_columns = [col for col in non_key_columns if col not in [config.operation_column, config.sequence_column]]
        set_statements = [f"  {col} = staging.{col}" for col in update_columns]
        ddl_parts.append(",\n".join(set_statements))

        ddl_parts.append(f"FROM {staging_ref} AS staging")
        ddl_parts.append("WHERE")
        ddl_parts.append(f"  {join_condition}")
        ddl_parts.append(f"  AND staging.{config.operation_column} = 'U';")
        ddl_parts.append("")

        # Step 3: Handle INSERTS
        ddl_parts.append("-- Step 3: Process INSERT operations")
        ddl_parts.append(f"INSERT INTO {target_table} (")
        ddl_parts.append("  " + ",\n  ".join(data_columns))
        ddl_parts.append(")")
        ddl_parts.append("SELECT")
        ddl_parts.append("  " + ",\n  ".join(data_columns))
        ddl_parts.append(f"FROM {staging_ref}")
        ddl_parts.append(f"WHERE {config.operation_column} = 'I';")
        ddl_parts.append("")

        ddl_parts.append("COMMIT;")
        ddl_parts.append("")
        ddl_parts.append(f"-- Recommended: ANALYZE {target_table};")

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
        Generate staging table DDL for Redshift.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Base table name
            staging_name: Optional staging table name
            dataset_name: Schema name
            **kwargs: Additional options:
                - temporary: Create as TEMPORARY table (default True)
                - distribution_style: 'KEY', 'ALL', 'EVEN', or 'AUTO'
                - distribution_key: Column for KEY distribution
                - sort_key: List of columns for sort key

        Returns:
            CREATE TABLE DDL for staging
        """
        staging_name = staging_name or f"{table_name}_staging"
        staging_table = self._build_table_ref(staging_name, dataset_name)

        temporary = kwargs.get('temporary', True)
        distribution_style = kwargs.get('distribution_style')
        distribution_key = kwargs.get('distribution_key')
        sort_key = kwargs.get('sort_key')

        ddl_parts = []
        ddl_parts.append("-- Create staging table")

        create_stmt = "CREATE "
        if temporary:
            create_stmt += "TEMPORARY "
        create_stmt += f"TABLE {staging_table} ("
        ddl_parts.append(create_stmt)

        # Add all columns
        column_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            column_defs.append(col_def)

        ddl_parts.append(",\n".join(column_defs))
        ddl_parts.append(")")

        # Add distribution key
        if distribution_style:
            if distribution_style.upper() == 'KEY' and distribution_key:
                ddl_parts.append(f"DISTKEY({distribution_key})")
            elif distribution_style.upper() in ['ALL', 'EVEN', 'AUTO']:
                ddl_parts.append(f"DISTSTYLE {distribution_style.upper()}")

        # Add sort key
        if sort_key:
            ddl_parts.append(f"SORTKEY({', '.join(sort_key)})")

        ddl_parts.append(";")

        return "\n".join(ddl_parts)

    def get_max_timestamp_query(
        self,
        table_name: str,
        timestamp_column: str,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate query to get max timestamp for Redshift.

        Args:
            table_name: Target table name
            timestamp_column: Timestamp column name
            dataset_name: Schema name
            **kwargs: Additional options

        Returns:
            SELECT query for max timestamp
        """
        table_ref = self._build_table_ref(table_name, dataset_name)

        return f"SELECT COALESCE(MAX({timestamp_column}), '1970-01-01'::TIMESTAMP) AS max_timestamp\nFROM {table_ref}"

    def generate_copy_from_s3_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        s3_path: str,
        iam_role: str,
        file_format: str = 'CSV',
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate Redshift COPY command for loading from S3.

        Args:
            schema: Schema definition (for reference, not used in COPY)
            table_name: Target table name (usually staging table)
            s3_path: S3 path (e.g., 's3://bucket/path/')
            iam_role: IAM role ARN for S3 access
            file_format: 'CSV', 'JSON', 'PARQUET', 'AVRO', 'ORC'
            dataset_name: Schema name
            **kwargs: Additional options:
                - delimiter: Column delimiter for CSV (default ',')
                - ignore_header: Number of header rows to skip (default 1)
                - region: AWS region (optional)
                - staging_table: Explicit staging table name

        Returns:
            COPY command DDL statement

        Example:
            >>> ddl = generator.generate_copy_from_s3_ddl(
            ...     schema=schema,
            ...     table_name='events',
            ...     s3_path='s3://my-bucket/events/',
            ...     iam_role='arn:aws:iam::123456789:role/RedshiftCopyRole',
            ...     file_format='CSV',
            ...     dataset_name='raw'
            ... )
        """
        staging_table = kwargs.get('staging_table') or f"{table_name}_staging"
        target_table = self._build_table_ref(staging_table, dataset_name)

        delimiter = kwargs.get('delimiter', ',')
        ignore_header = kwargs.get('ignore_header', 1)
        region = kwargs.get('region')

        ddl_parts = []
        ddl_parts.append("-- Load data from S3 into staging table")
        ddl_parts.append(f"COPY {target_table}")
        ddl_parts.append(f"FROM '{s3_path}'")
        ddl_parts.append(f"IAM_ROLE '{iam_role}'")

        # File format
        file_format_upper = file_format.upper()
        if file_format_upper == 'CSV':
            ddl_parts.append("FORMAT AS CSV")
            ddl_parts.append(f"DELIMITER '{delimiter}'")
            ddl_parts.append(f"IGNOREHEADER {ignore_header}")
            ddl_parts.append("DATEFORMAT 'auto'")
            ddl_parts.append("TIMEFORMAT 'auto'")
        elif file_format_upper == 'JSON':
            ddl_parts.append("FORMAT AS JSON 'auto'")
        elif file_format_upper in ['PARQUET', 'AVRO', 'ORC']:
            ddl_parts.append(f"FORMAT AS {file_format_upper}")

        # Region
        if region:
            ddl_parts.append(f"REGION '{region}'")

        # Error handling
        ddl_parts.append("MAXERROR 0;  -- Fail on any error")

        return "\n".join(ddl_parts)

    def generate_vacuum_analyze_commands(
        self,
        table_name: str,
        dataset_name: Optional[str] = None,
        vacuum_type: str = 'FULL',
        **kwargs
    ) -> str:
        """
        Generate VACUUM and ANALYZE commands for maintenance.

        Args:
            table_name: Table name
            dataset_name: Schema name
            vacuum_type: 'FULL', 'DELETE ONLY', 'SORT ONLY', or 'REINDEX'
            **kwargs: Additional options

        Returns:
            VACUUM and ANALYZE commands
        """
        table_ref = self._build_table_ref(table_name, dataset_name)

        ddl_parts = []
        ddl_parts.append("-- Post-load maintenance commands")
        ddl_parts.append("")
        ddl_parts.append("-- Reclaim space and sort rows")
        ddl_parts.append(f"VACUUM {vacuum_type} {table_ref};")
        ddl_parts.append("")
        ddl_parts.append("-- Update table statistics for query planner")
        ddl_parts.append(f"ANALYZE {table_ref};")

        return "\n".join(ddl_parts)

    def _build_table_ref(
        self,
        table_name: str,
        dataset_name: Optional[str] = None
    ) -> str:
        """
        Build fully-qualified Redshift table reference.

        Format: [schema].table

        Args:
            table_name: Table name
            dataset_name: Schema name (e.g., 'public', 'analytics')

        Returns:
            Fully-qualified table reference
        """
        if dataset_name:
            return f"{dataset_name}.{table_name}"
        else:
            return table_name
