"""
Unit tests for RedshiftIncrementalGenerator.

This module tests all incremental load patterns for Amazon Redshift,
including MERGE simulation (DELETE + INSERT), COPY FROM S3, distribution keys,
sort keys, and maintenance commands.
"""

import pytest
from schema_mapper.incremental import (
    LoadPattern,
    IncrementalConfig,
    get_incremental_generator,
)
from schema_mapper.incremental.patterns import DeleteStrategy
from schema_mapper.incremental.platform_generators.redshift import (
    RedshiftIncrementalGenerator,
)


@pytest.fixture
def redshift_generator():
    """Create a Redshift incremental generator instance."""
    return RedshiftIncrementalGenerator()


@pytest.fixture
def basic_schema():
    """Basic schema for testing (List[Dict] format)."""
    return [
        {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'VARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'VARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ]


@pytest.fixture
def scd2_schema():
    """Schema for SCD2 testing."""
    return [
        {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'VARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'VARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': 'valid_from', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'valid_to', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'is_current', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
    ]


@pytest.fixture
def cdc_schema():
    """Schema for CDC testing."""
    return [
        {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'VARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'VARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': '_op', 'type': 'VARCHAR(1)', 'mode': 'REQUIRED'},
        {'name': '_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    ]


# ============================================================================
# Factory Tests
# ============================================================================

class TestRedshiftFactory:
    """Test Redshift generator factory function."""

    def test_factory_returns_redshift_generator(self):
        """Test that factory returns correct Redshift generator."""
        generator = get_incremental_generator('redshift')
        assert isinstance(generator, RedshiftIncrementalGenerator)
        assert generator.platform == 'redshift'


# ============================================================================
# MERGE Tests (DELETE + INSERT Pattern)
# ============================================================================

class TestRedshiftMerge:
    """Test Redshift MERGE (DELETE + INSERT) pattern."""

    def test_basic_merge_with_transaction(self, redshift_generator, basic_schema):
        """Test basic MERGE with transaction wrapper."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column=None  # Disable auto-timestamp
        )

        ddl = redshift_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='public'
        )

        # Check transaction wrapper
        assert 'BEGIN TRANSACTION;' in ddl
        assert 'COMMIT;' in ddl

        # Check DELETE statement
        assert 'DELETE FROM public.users' in ddl
        assert 'USING public.users_staging' in ddl
        assert 'WHERE users.user_id = staging.user_id' in ddl

        # Check INSERT statement
        assert 'INSERT INTO public.users' in ddl
        assert 'FROM public.users_staging' in ddl

    def test_merge_with_composite_keys(self, redshift_generator, basic_schema):
        """Test MERGE with composite primary keys."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id', 'email'],
            updated_by_column=None
        )

        ddl = redshift_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        # Check composite key join
        assert 'users.user_id = staging.user_id' in ddl
        assert 'users.email = staging.email' in ddl
        assert 'AND' in ddl

    def test_merge_with_custom_staging_name(self, redshift_generator, basic_schema):
        """Test MERGE with custom staging table name."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column=None
        )

        ddl = redshift_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            staging_table='users_temp'
        )

        assert 'users_temp' in ddl


# ============================================================================
# Append Tests
# ============================================================================

class TestRedshiftAppend:
    """Test Redshift append pattern."""

    def test_basic_append(self, redshift_generator, basic_schema):
        """Test basic append without deduplication."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY,
            primary_keys=[],
            updated_by_column=None
        )

        ddl = redshift_generator.generate_append_ddl(
            schema=basic_schema,
            table_name='events',
            config=config,
            dataset_name='analytics'
        )

        # Check simple INSERT
        assert 'INSERT INTO analytics.events' in ddl
        assert 'FROM analytics.events_staging' in ddl
        assert 'WHERE NOT EXISTS' not in ddl

    def test_append_with_deduplication(self, redshift_generator, basic_schema):
        """Test append with deduplication filter."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            primary_keys=['user_id'],
            updated_by_column=None
        )

        ddl = redshift_generator.generate_append_ddl(
            schema=basic_schema,
            table_name='events',
            config=config,
            dataset_name='analytics'
        )

        # Check NOT EXISTS clause
        assert 'WHERE NOT EXISTS' in ddl
        assert 'SELECT 1 FROM analytics.events' in ddl


# ============================================================================
# Full Refresh Tests
# ============================================================================

class TestRedshiftFullRefresh:
    """Test Redshift full refresh pattern."""

    def test_full_refresh_truncate(self, redshift_generator, basic_schema):
        """Test full refresh with TRUNCATE."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.FULL_REFRESH,
            primary_keys=[],
            updated_by_column=None
        )

        ddl = redshift_generator.generate_full_refresh_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='public'
        )

        # Check TRUNCATE and INSERT
        assert 'TRUNCATE TABLE public.users;' in ddl
        assert 'INSERT INTO public.users' in ddl
        assert 'FROM public.users_staging' in ddl


# ============================================================================
# SCD2 Tests
# ============================================================================

class TestRedshiftSCD2:
    """Test Redshift SCD Type 2 pattern."""

    def test_scd2_basic(self, redshift_generator, scd2_schema):
        """Test basic SCD2 with UPDATE + INSERT."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            effective_date_column='valid_from',
            expiration_date_column='valid_to',
            is_current_column='is_current',
            hash_columns=['name', 'email'],  # Required for SCD_TYPE2
            updated_by_column=None
        )

        ddl = redshift_generator.generate_scd2_ddl(
            schema=scd2_schema,
            table_name='users',
            config=config,
            dataset_name='dim'
        )

        # Check transaction
        assert 'BEGIN' in ddl
        assert 'COMMIT' in ddl

        # Check UPDATE to expire existing records
        assert 'UPDATE' in ddl
        assert 'valid_to' in ddl.lower() or 'expiration' in ddl.lower()
        assert 'is_current' in ddl.lower() or 'current' in ddl.lower()

        # Check INSERT for new versions
        assert 'INSERT INTO' in ddl

    def test_scd2_with_change_detection(self, redshift_generator, scd2_schema):
        """Test SCD2 with change detection columns."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            effective_date_column='valid_from',
            expiration_date_column='valid_to',
            is_current_column='is_current',
            hash_columns=['name', 'email'],
            updated_by_column=None
        )

        ddl = redshift_generator.generate_scd2_ddl(
            schema=scd2_schema,
            table_name='users',
            config=config
        )

        # Check that hash columns are referenced
        assert 'name' in ddl.lower() or 'email' in ddl.lower()


# ============================================================================
# Incremental Timestamp Tests
# ============================================================================

class TestRedshiftIncrementalTimestamp:
    """Test Redshift incremental timestamp pattern."""

    def test_incremental_timestamp_basic(self, redshift_generator, basic_schema):
        """Test incremental timestamp with temp table."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=[],
            incremental_column='updated_at',
            updated_by_column=None
        )

        ddl = redshift_generator.generate_incremental_timestamp_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='public'
        )

        # Check temp table for max timestamp
        assert 'CREATE TEMPORARY TABLE' in ddl or 'CREATE TEMP TABLE' in ddl
        assert 'max_timestamp' in ddl.lower()
        assert 'COALESCE(MAX(updated_at)' in ddl

        # Check INSERT with timestamp filter
        assert 'INSERT INTO public.users' in ddl
        assert 'WHERE' in ddl
        assert 'updated_at' in ddl


# ============================================================================
# CDC Tests
# ============================================================================

class TestRedshiftCDC:
    """Test Redshift CDC pattern."""

    def test_cdc_merge_basic(self, redshift_generator, cdc_schema):
        """Test CDC merge with DELETE/UPDATE/INSERT operations."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            delete_strategy=DeleteStrategy.HARD_DELETE,
            updated_by_column=None
        )

        ddl = redshift_generator.generate_cdc_merge_ddl(
            schema=cdc_schema,
            table_name='users',
            config=config,
            dataset_name='public'
        )

        # Check transaction
        assert 'BEGIN' in ddl
        assert 'COMMIT' in ddl

        # Check DELETE operation
        assert "DELETE FROM" in ddl
        assert "_op = 'D'" in ddl

        # Check UPDATE operation
        assert "UPDATE" in ddl
        assert "_op = 'U'" in ddl

        # Check INSERT operation
        assert "INSERT INTO" in ddl
        assert "_op = 'I'" in ddl

    def test_cdc_with_sequence_column(self, redshift_generator, cdc_schema):
        """Test CDC accepts sequence_column config without errors."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            sequence_column='_timestamp',
            delete_strategy=DeleteStrategy.HARD_DELETE,
            updated_by_column=None
        )

        ddl = redshift_generator.generate_cdc_merge_ddl(
            schema=cdc_schema,
            table_name='users',
            config=config
        )

        # Verify CDC operations are generated correctly
        assert 'BEGIN' in ddl
        assert 'COMMIT' in ddl
        assert "DELETE FROM" in ddl
        assert "UPDATE" in ddl
        assert "INSERT INTO" in ddl
        # Sequence column should be excluded from data columns (CDC metadata)
        assert 'user_id' in ddl
        assert 'name' in ddl
        assert 'email' in ddl


# ============================================================================
# Staging Table Tests
# ============================================================================

class TestRedshiftStagingTable:
    """Test Redshift staging table generation."""

    def test_staging_table_basic(self, redshift_generator, basic_schema):
        """Test basic temporary staging table."""
        ddl = redshift_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            dataset_name='public'
        )

        # Check TEMPORARY table
        assert 'CREATE TEMPORARY TABLE' in ddl or 'CREATE TEMP TABLE' in ddl
        assert 'users_staging' in ddl

        # Check columns
        assert 'user_id BIGINT' in ddl
        assert 'name VARCHAR(100)' in ddl
        assert 'email VARCHAR(255)' in ddl

    def test_staging_table_with_distkey(self, redshift_generator, basic_schema):
        """Test staging table with distribution key."""
        ddl = redshift_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            distribution_style='KEY',
            distribution_key='user_id'
        )

        # Check DISTKEY
        assert 'DISTKEY(user_id)' in ddl

    def test_staging_table_with_sortkey(self, redshift_generator, basic_schema):
        """Test staging table with sort key."""
        ddl = redshift_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            sort_key=['updated_at']
        )

        # Check SORTKEY
        assert 'SORTKEY(updated_at)' in ddl

    def test_staging_table_with_compound_sortkey(self, redshift_generator, basic_schema):
        """Test staging table with compound sort key."""
        ddl = redshift_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            sort_key=['user_id', 'updated_at']
        )

        # Check compound SORTKEY
        assert 'SORTKEY(user_id, updated_at)' in ddl

    def test_staging_table_with_dist_style(self, redshift_generator, basic_schema):
        """Test staging table with distribution style."""
        ddl = redshift_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            distribution_style='ALL'
        )

        # Check DISTSTYLE
        assert 'DISTSTYLE ALL' in ddl


# ============================================================================
# COPY FROM S3 Tests
# ============================================================================

class TestRedshiftCopyFromS3:
    """Test Redshift COPY FROM S3 command."""

    def test_copy_csv_basic(self, redshift_generator, basic_schema):
        """Test basic CSV COPY from S3."""
        ddl = redshift_generator.generate_copy_from_s3_ddl(
            schema=basic_schema,
            table_name='users',
            s3_path='s3://my-bucket/users/',
            iam_role='arn:aws:iam::123456789012:role/RedshiftRole',
            file_format='CSV',
            dataset_name='public'
        )

        # Check COPY command structure
        assert 'COPY public.users' in ddl
        assert "FROM 's3://my-bucket/users/'" in ddl
        assert "IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole'" in ddl
        assert 'FORMAT AS CSV' in ddl

    def test_copy_json(self, redshift_generator, basic_schema):
        """Test JSON COPY from S3."""
        ddl = redshift_generator.generate_copy_from_s3_ddl(
            schema=basic_schema,
            table_name='events',
            s3_path='s3://my-bucket/events/',
            iam_role='arn:aws:iam::123456789012:role/RedshiftRole',
            file_format='JSON',
            json_paths='auto'
        )

        # Check JSON format
        assert 'FORMAT AS JSON' in ddl
        assert "'auto'" in ddl or 'auto' in ddl

    def test_copy_parquet(self, redshift_generator, basic_schema):
        """Test Parquet COPY from S3."""
        ddl = redshift_generator.generate_copy_from_s3_ddl(
            schema=basic_schema,
            table_name='analytics',
            s3_path='s3://my-bucket/analytics/',
            iam_role='arn:aws:iam::123456789012:role/RedshiftRole',
            file_format='PARQUET'
        )

        # Check Parquet format
        assert 'FORMAT AS PARQUET' in ddl

    def test_copy_with_delimiter(self, redshift_generator, basic_schema):
        """Test COPY with custom delimiter."""
        ddl = redshift_generator.generate_copy_from_s3_ddl(
            schema=basic_schema,
            table_name='users',
            s3_path='s3://my-bucket/users/',
            iam_role='arn:aws:iam::123456789012:role/RedshiftRole',
            file_format='CSV',
            delimiter='|'
        )

        # Check delimiter
        assert "DELIMITER '|'" in ddl

    def test_copy_with_ignore_header(self, redshift_generator, basic_schema):
        """Test COPY with header row skip."""
        ddl = redshift_generator.generate_copy_from_s3_ddl(
            schema=basic_schema,
            table_name='users',
            s3_path='s3://my-bucket/users/',
            iam_role='arn:aws:iam::123456789012:role/RedshiftRole',
            file_format='CSV',
            ignore_header=1
        )

        # Check ignore header
        assert 'IGNOREHEADER 1' in ddl


# ============================================================================
# Maintenance Tests
# ============================================================================

class TestRedshiftMaintenance:
    """Test Redshift VACUUM and ANALYZE commands."""

    def test_vacuum_full_with_analyze(self, redshift_generator):
        """Test full VACUUM with ANALYZE."""
        ddl = redshift_generator.generate_vacuum_analyze_commands(
            table_name='users',
            dataset_name='public',
            vacuum_type='FULL'
        )

        # Check VACUUM FULL
        assert 'VACUUM FULL public.users;' in ddl
        # Check ANALYZE
        assert 'ANALYZE public.users;' in ddl

    def test_vacuum_delete_only(self, redshift_generator):
        """Test DELETE ONLY VACUUM."""
        ddl = redshift_generator.generate_vacuum_analyze_commands(
            table_name='events',
            vacuum_type='DELETE ONLY'
        )

        # Check VACUUM DELETE ONLY
        assert 'VACUUM DELETE ONLY events;' in ddl
        # Check ANALYZE is included
        assert 'ANALYZE events;' in ddl

    def test_vacuum_sort_only(self, redshift_generator):
        """Test SORT ONLY VACUUM."""
        ddl = redshift_generator.generate_vacuum_analyze_commands(
            table_name='logs',
            dataset_name='analytics',
            vacuum_type='SORT ONLY'
        )

        # Check VACUUM SORT ONLY
        assert 'VACUUM SORT ONLY analytics.logs;' in ddl

    def test_vacuum_reindex(self, redshift_generator):
        """Test REINDEX VACUUM."""
        ddl = redshift_generator.generate_vacuum_analyze_commands(
            table_name='users',
            vacuum_type='REINDEX'
        )

        # Check VACUUM REINDEX
        assert 'VACUUM REINDEX users;' in ddl


# ============================================================================
# Utility Tests
# ============================================================================

class TestRedshiftUtilities:
    """Test Redshift utility methods."""

    def test_get_max_timestamp_query(self, redshift_generator):
        """Test max timestamp query generation."""
        query = redshift_generator.get_max_timestamp_query(
            table_name='users',
            timestamp_column='updated_at',
            dataset_name='public'
        )

        # Check query structure
        assert 'SELECT COALESCE(MAX(updated_at)' in query
        assert 'FROM public.users' in query
        assert "'1970-01-01'::TIMESTAMP" in query or '1970-01-01' in query

    def test_build_table_ref_with_schema(self, redshift_generator):
        """Test table reference building with schema."""
        ref = redshift_generator._build_table_ref('users', 'public')
        assert ref == 'public.users'

    def test_build_table_ref_without_schema(self, redshift_generator):
        """Test table reference building without schema."""
        ref = redshift_generator._build_table_ref('users', None)
        assert ref == 'users'


# ============================================================================
# Integration Tests
# ============================================================================

class TestRedshiftIntegration:
    """Integration tests for end-to-end Redshift workflows."""

    def test_complete_upsert_workflow(self, redshift_generator, basic_schema):
        """Test complete UPSERT workflow with staging and COPY."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column='updated_at'
        )

        # Generate staging table
        staging_ddl = redshift_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            dataset_name='public',
            dist_key='user_id'
        )
        assert 'CREATE TEMPORARY TABLE' in staging_ddl or 'CREATE TEMP TABLE' in staging_ddl

        # Generate COPY command
        copy_ddl = redshift_generator.generate_copy_from_s3_ddl(
            schema=basic_schema,
            table_name='users_staging',
            s3_path='s3://bucket/users/',
            iam_role='arn:aws:iam::123:role/Role',
            file_format='CSV'
        )
        assert 'COPY' in copy_ddl

        # Generate MERGE
        merge_ddl = redshift_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='public'
        )
        assert 'DELETE FROM' in merge_ddl
        assert 'INSERT INTO' in merge_ddl

        # Generate maintenance
        maint_ddl = redshift_generator.generate_vacuum_analyze_commands(
            table_name='users',
            dataset_name='public'
        )
        assert 'VACUUM' in maint_ddl
        assert 'ANALYZE' in maint_ddl

    def test_complete_cdc_workflow(self, redshift_generator, cdc_schema):
        """Test complete CDC workflow."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            sequence_column='_timestamp',
            delete_strategy=DeleteStrategy.HARD_DELETE,
            updated_by_column=None
        )

        # Generate CDC merge
        cdc_ddl = redshift_generator.generate_cdc_merge_ddl(
            schema=cdc_schema,
            table_name='users',
            config=config,
            dataset_name='public'
        )

        # Check all operations are present
        assert 'DELETE FROM' in cdc_ddl  # Delete operations
        assert 'UPDATE' in cdc_ddl  # Update operations
        assert 'INSERT INTO' in cdc_ddl  # Insert operations
        assert 'BEGIN' in cdc_ddl
        assert 'COMMIT' in cdc_ddl
