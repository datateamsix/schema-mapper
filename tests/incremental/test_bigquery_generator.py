"""
Unit tests for BigQuery incremental DDL generator.
"""

import pytest
import pandas as pd
from schema_mapper import SchemaMapper
from schema_mapper.incremental import (
    LoadPattern,
    IncrementalConfig,
    MergeStrategy,
    DeleteStrategy,
    get_incremental_generator
)


@pytest.fixture
def sample_df():
    """Sample DataFrame for testing."""
    return pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['a@ex.com', 'b@ex.com', 'c@ex.com'],
        'updated_at': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
    })


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return [
        {'name': 'user_id', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]


class TestBigQueryGenerator:
    """Test BigQuery incremental DDL generator."""

    def test_generator_initialization(self):
        """Test BigQuery generator initialization."""
        generator = get_incremental_generator('bigquery')
        assert generator.platform == 'bigquery'
        assert generator.supports_pattern(LoadPattern.UPSERT)
        assert generator.supports_pattern(LoadPattern.SCD_TYPE2)
        assert generator.supports_pattern(LoadPattern.CDC_MERGE)

    def test_all_patterns_supported(self):
        """Test that BigQuery supports all patterns."""
        generator = get_incremental_generator('bigquery')

        for pattern in LoadPattern:
            assert generator.supports_pattern(pattern), \
                f"BigQuery should support {pattern.value}"


class TestBigQueryMerge:
    """Test BigQuery MERGE DDL generation."""

    def test_basic_merge_ddl(self, sample_df):
        """Test basic BigQuery MERGE DDL generation."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics',
            project_id='my-project'
        )

        assert 'MERGE `my-project.analytics.users`' in ddl
        assert 'USING `my-project.analytics.users_staging`' in ddl
        assert 'ON target.user_id = source.user_id' in ddl
        assert 'WHEN MATCHED THEN' in ddl
        assert 'WHEN NOT MATCHED THEN' in ddl
        assert 'UPDATE SET' in ddl
        assert 'INSERT' in ddl

    def test_merge_with_composite_key(self, sample_schema):
        """Test MERGE with composite primary key."""
        generator = get_incremental_generator('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id', 'email']
        )

        ddl = generator.generate_merge_ddl(
            sample_schema,
            'users',
            config,
            dataset_name='analytics'
        )

        assert 'target.user_id = source.user_id AND target.email = source.email' in ddl

    def test_merge_update_selective(self, sample_schema):
        """Test MERGE with selective column updates."""
        generator = get_incremental_generator('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_SELECTIVE,
            update_columns=['name', 'email']
        )

        ddl = generator.generate_merge_ddl(
            sample_schema,
            'users',
            config,
            dataset_name='analytics'
        )

        assert 'target.name = source.name' in ddl
        assert 'target.email = source.email' in ddl
        # updated_at should not be in UPDATE SET since it's not in update_columns
        assert 'target.updated_at = source.updated_at' not in ddl

    def test_merge_update_changed(self, sample_schema):
        """Test MERGE with UPDATE_CHANGED strategy."""
        generator = get_incremental_generator('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_CHANGED
        )

        ddl = generator.generate_merge_ddl(
            sample_schema,
            'users',
            config,
            dataset_name='analytics'
        )

        # Should have change detection in WHEN MATCHED clause
        assert 'WHEN MATCHED AND (' in ddl
        assert 'target.name != source.name' in ddl or 'IS NULL' in ddl

    def test_merge_update_none(self, sample_schema):
        """Test MERGE with UPDATE_NONE (insert only)."""
        generator = get_incremental_generator('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_NONE
        )

        ddl = generator.generate_merge_ddl(
            sample_schema,
            'users',
            config,
            dataset_name='analytics'
        )

        # Should NOT have WHEN MATCHED clause
        assert 'WHEN MATCHED' not in ddl
        assert 'UPDATE SET' not in ddl
        assert 'WHEN NOT MATCHED THEN' in ddl

    def test_merge_with_timestamps(self, sample_schema):
        """Test MERGE with created_at/updated_at columns."""
        generator = get_incremental_generator('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            created_by_column='created_at',
            updated_by_column='updated_at'
        )

        ddl = generator.generate_merge_ddl(
            sample_schema,
            'users',
            config,
            dataset_name='analytics'
        )

        # Should have timestamp logic
        assert 'CURRENT_TIMESTAMP()' in ddl


class TestBigQueryAppend:
    """Test BigQuery APPEND DDL generation."""

    def test_basic_append(self, sample_df):
        """Test basic BigQuery APPEND DDL."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY,
            primary_keys=[]
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'events',
            config,
            dataset_name='analytics'
        )

        assert 'INSERT INTO `analytics.events`' in ddl
        assert 'SELECT' in ddl
        assert 'FROM `analytics.events_staging`' in ddl
        assert 'user_id' in ddl
        assert 'name' in ddl

    def test_incremental_append(self, sample_df):
        """Test incremental append (only new records)."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'events',
            config,
            dataset_name='analytics'
        )

        assert 'INSERT INTO' in ddl
        assert 'WHERE NOT EXISTS' in ddl
        assert 'target.user_id = source.user_id' in ddl


class TestBigQueryFullRefresh:
    """Test BigQuery FULL REFRESH DDL generation."""

    def test_full_refresh(self, sample_df):
        """Test BigQuery FULL REFRESH DDL."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.FULL_REFRESH,
            primary_keys=[]
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics',
            project_id='my-project'
        )

        assert 'TRUNCATE TABLE `my-project.analytics.users`' in ddl
        assert 'INSERT INTO' in ddl


class TestBigQuerySCD2:
    """Test BigQuery SCD Type 2 DDL generation."""

    def test_scd2_ddl(self, sample_df):
        """Test BigQuery SCD Type 2 DDL generation."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            hash_columns=['name', 'email'],
            effective_date_column='effective_from',
            expiration_date_column='effective_to',
            is_current_column='is_current'
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'customers',
            config,
            dataset_name='analytics'
        )

        # Step 1: Expire changed records
        assert 'UPDATE `analytics.customers`' in ddl
        assert 'effective_to = CURRENT_DATE()' in ddl
        assert 'is_current = FALSE' in ddl

        # Step 2: Insert new versions
        assert 'INSERT INTO' in ddl
        assert 'effective_from' in ddl
        assert "DATE('9999-12-31')" in ddl
        assert 'TRUE AS is_current' in ddl

        # Change detection
        assert 'target.name != source.name' in ddl or 'IS NULL' in ddl

    def test_scd2_with_all_columns_tracked(self, sample_schema):
        """Test SCD2 tracking all non-key columns."""
        generator = get_incremental_generator('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            hash_columns=None,  # Will use all non-key columns
            effective_date_column='effective_from',
            expiration_date_column='effective_to',
            is_current_column='is_current'
        )

        ddl = generator.generate_scd2_ddl(
            sample_schema,
            'customers',
            config,
            dataset_name='analytics'
        )

        # Should track name, email, updated_at (all non-key columns)
        assert 'target.name != source.name' in ddl or 'IS NULL' in ddl
        assert 'target.email != source.email' in ddl or 'IS NULL' in ddl


class TestBigQuerySCD1:
    """Test BigQuery SCD Type 1 DDL generation."""

    def test_scd1_ddl(self, sample_df):
        """Test BigQuery SCD Type 1 DDL (should be same as UPSERT)."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE1,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'customers',
            config,
            dataset_name='analytics'
        )

        # SCD1 should generate MERGE (same as UPSERT)
        assert 'MERGE' in ddl
        assert 'WHEN MATCHED THEN' in ddl
        assert 'UPDATE SET' in ddl


class TestBigQueryIncrementalTimestamp:
    """Test BigQuery incremental timestamp load."""

    def test_incremental_timestamp(self, sample_df):
        """Test BigQuery incremental timestamp load."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=['user_id'],
            incremental_column='updated_at'
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics'
        )

        assert 'DECLARE max_ts TIMESTAMP' in ddl
        assert 'MAX(updated_at)' in ddl
        assert "TIMESTAMP('1970-01-01')" in ddl
        assert 'WHERE updated_at > max_ts' in ddl

    def test_incremental_timestamp_with_lookback(self, sample_df):
        """Test incremental timestamp with lookback window."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=['user_id'],
            incremental_column='updated_at',
            lookback_window='2 HOUR'
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics'
        )

        assert 'TIMESTAMP_SUB(max_ts, INTERVAL 2 HOUR)' in ddl


class TestBigQueryCDC:
    """Test BigQuery CDC MERGE DDL generation."""

    def test_cdc_merge_hard_delete(self, sample_df):
        """Test CDC MERGE with hard delete."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            delete_strategy=DeleteStrategy.HARD_DELETE
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics'
        )

        assert 'MERGE' in ddl
        assert "source._op = 'D'" in ddl
        assert 'DELETE' in ddl
        assert "source._op = 'U'" in ddl
        assert 'UPDATE SET' in ddl
        assert "source._op = 'I'" in ddl
        assert 'INSERT' in ddl

    def test_cdc_merge_soft_delete(self, sample_df):
        """Test CDC MERGE with soft delete."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            delete_strategy=DeleteStrategy.SOFT_DELETE,
            soft_delete_column='is_deleted'
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics'
        )

        assert "source._op = 'D'" in ddl
        assert 'is_deleted = TRUE' in ddl
        assert 'DELETE' not in ddl  # Should not have hard DELETE


class TestBigQueryDeleteInsert:
    """Test BigQuery DELETE+INSERT pattern."""

    def test_delete_insert(self, sample_df):
        """Test DELETE+INSERT pattern."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.DELETE_INSERT,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics'
        )

        assert 'DELETE FROM `analytics.users`' in ddl
        assert 'WHERE EXISTS' in ddl
        assert 'target.user_id = source.user_id' in ddl
        assert 'INSERT INTO' in ddl


class TestBigQueryStagingTable:
    """Test BigQuery staging table DDL generation."""

    def test_staging_table_ddl(self, sample_schema):
        """Test staging table DDL generation."""
        generator = get_incremental_generator('bigquery')

        ddl = generator.generate_staging_table_ddl(
            sample_schema,
            'users',
            dataset_name='analytics',
            project_id='my-project'
        )

        assert 'CREATE OR REPLACE TABLE `my-project.analytics.users_staging`' in ddl
        assert 'user_id INT64' in ddl
        assert 'name STRING' in ddl
        assert 'email STRING' in ddl

    def test_staging_table_with_custom_name(self, sample_schema):
        """Test staging table with custom name."""
        generator = get_incremental_generator('bigquery')

        ddl = generator.generate_staging_table_ddl(
            sample_schema,
            'users',
            staging_name='temp_users',
            dataset_name='analytics'
        )

        assert 'CREATE OR REPLACE TABLE `analytics.temp_users`' in ddl


class TestBigQueryHelpers:
    """Test BigQuery helper methods."""

    def test_build_table_ref_with_project(self):
        """Test building table reference with project ID."""
        generator = get_incremental_generator('bigquery')

        ref = generator._build_table_ref(
            'users',
            dataset_name='analytics',
            project_id='my-project'
        )

        assert ref == '`my-project.analytics.users`'

    def test_build_table_ref_without_project(self):
        """Test building table reference without project ID."""
        generator = get_incremental_generator('bigquery')

        ref = generator._build_table_ref(
            'users',
            dataset_name='analytics'
        )

        assert ref == '`analytics.users`'

    def test_build_table_ref_table_only(self):
        """Test building table reference with only table name."""
        generator = get_incremental_generator('bigquery')

        ref = generator._build_table_ref('users')

        assert ref == '`users`'

    def test_get_max_timestamp_query(self):
        """Test max timestamp query generation."""
        generator = get_incremental_generator('bigquery')

        query = generator.get_max_timestamp_query(
            'users',
            'updated_at',
            dataset_name='analytics',
            project_id='my-project'
        )

        assert 'SELECT COALESCE(MAX(updated_at)' in query
        assert "TIMESTAMP('1970-01-01')" in query
        assert '`my-project.analytics.users`' in query


class TestBigQueryIntegration:
    """Test BigQuery integration with SchemaMapper."""

    def test_via_schema_mapper(self, sample_df):
        """Test generating BigQuery DDL via SchemaMapper."""
        mapper = SchemaMapper('bigquery')

        # Test UPSERT
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics',
            project_id='my-project'
        )

        assert 'MERGE' in ddl
        assert 'my-project.analytics.users' in ddl

    def test_generate_merge_ddl_shortcut(self, sample_df):
        """Test generate_merge_ddl shortcut method."""
        mapper = SchemaMapper('bigquery')

        ddl = mapper.generate_merge_ddl(
            sample_df,
            'users',
            primary_keys=['user_id'],
            dataset_name='analytics',
            project_id='my-project'
        )

        assert 'MERGE `my-project.analytics.users`' in ddl
        assert 'WHEN MATCHED THEN' in ddl
        assert 'WHEN NOT MATCHED THEN' in ddl


class TestBigQueryEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_primary_keys_for_upsert(self, sample_df):
        """Test that UPSERT requires primary keys."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=[]
        )

        with pytest.raises(ValueError, match="primary_keys cannot be empty"):
            config.validate()

    def test_missing_incremental_column(self, sample_df):
        """Test that INCREMENTAL_TIMESTAMP requires incremental_column."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=['user_id'],
            incremental_column=None
        )

        with pytest.raises(ValueError, match="incremental_column required"):
            config.validate()

    def test_scd2_requires_columns(self, sample_df):
        """Test that SCD2 requires effective/expiration/current columns."""
        mapper = SchemaMapper('bigquery')

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            effective_date_column=None  # Missing required field
        )

        with pytest.raises(ValueError):
            config.validate()
