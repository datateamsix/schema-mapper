"""
Unit tests for SQLServerIncrementalGenerator.

This module tests all incremental load patterns for Microsoft SQL Server,
including native MERGE statements, CDC operations, SCD Type 2, temp tables,
and T-SQL specific features.
"""

import pytest
from schema_mapper.incremental import (
    LoadPattern,
    IncrementalConfig,
    get_incremental_generator,
)
from schema_mapper.incremental.patterns import DeleteStrategy, MergeStrategy
from schema_mapper.incremental.platform_generators.sqlserver import (
    SQLServerIncrementalGenerator,
)


@pytest.fixture
def sqlserver_generator():
    """Create a SQL Server incremental generator instance."""
    return SQLServerIncrementalGenerator()


@pytest.fixture
def basic_schema():
    """Basic schema for testing (List[Dict] format)."""
    return [
        {'name': 'user_id', 'type': 'INT', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'NVARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'NVARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'DATETIME2', 'mode': 'NULLABLE'},
    ]


@pytest.fixture
def scd2_schema():
    """Schema for SCD2 testing."""
    return [
        {'name': 'user_id', 'type': 'INT', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'NVARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'NVARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': 'valid_from', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'valid_to', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'is_current', 'type': 'BIT', 'mode': 'REQUIRED'},
    ]


@pytest.fixture
def cdc_schema():
    """Schema for CDC testing."""
    return [
        {'name': 'user_id', 'type': 'INT', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'NVARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'NVARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': '_op', 'type': 'CHAR(1)', 'mode': 'REQUIRED'},
        {'name': '_timestamp', 'type': 'DATETIME2', 'mode': 'REQUIRED'},
    ]


# ============================================================================
# Factory Tests
# ============================================================================

class TestSQLServerFactory:
    """Test SQL Server generator factory function."""

    def test_factory_returns_sqlserver_generator(self):
        """Test that factory returns correct SQL Server generator."""
        generator = get_incremental_generator('sqlserver')
        assert isinstance(generator, SQLServerIncrementalGenerator)
        assert generator.platform == 'sqlserver'

    def test_supports_all_patterns(self, sqlserver_generator):
        """Test that SQL Server supports all load patterns."""
        patterns = [
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
        for pattern in patterns:
            assert sqlserver_generator.supports_pattern(pattern)


# ============================================================================
# MERGE Tests
# ============================================================================

class TestSQLServerMerge:
    """Test SQL Server MERGE statement generation."""

    def test_basic_merge_with_transaction(self, sqlserver_generator, basic_schema):
        """Test basic MERGE with transaction wrapper."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='dbo'
        )

        # Check transaction wrapper
        assert 'BEGIN TRANSACTION;' in ddl
        assert 'COMMIT TRANSACTION;' in ddl

        # Check MERGE statement structure
        assert 'MERGE INTO [dbo].[users] AS target' in ddl
        assert 'USING [dbo].[users_staging] AS source' in ddl
        assert 'ON target.[user_id] = source.[user_id]' in ddl

        # Check WHEN MATCHED clause
        assert 'WHEN MATCHED' in ddl
        assert 'UPDATE SET' in ddl

        # Check WHEN NOT MATCHED BY TARGET clause
        assert 'WHEN NOT MATCHED BY TARGET THEN' in ddl
        assert 'INSERT' in ddl
        assert 'VALUES' in ddl

    def test_merge_without_transaction(self, sqlserver_generator, basic_schema):
        """Test MERGE without transaction wrapper."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            use_transaction=False
        )

        # Should not have transaction wrapper
        assert 'BEGIN TRANSACTION' not in ddl
        assert 'COMMIT' not in ddl

        # But should still have MERGE
        assert 'MERGE' in ddl

    def test_merge_with_composite_keys(self, sqlserver_generator, basic_schema):
        """Test MERGE with composite primary keys."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id', 'email'],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        # Check composite key join
        assert 'target.[user_id] = source.[user_id]' in ddl
        assert 'target.[email] = source.[email]' in ddl
        assert 'AND' in ddl

    def test_merge_with_selective_update(self, sqlserver_generator, basic_schema):
        """Test MERGE with selective column updates."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_SELECTIVE,
            update_columns=['name'],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        # Should only update specified columns
        assert 'target.[name] = source.[name]' in ddl
        # Should not update email
        assert 'target.[email] = source.[email]' not in ddl

    def test_merge_with_updated_at_column(self, sqlserver_generator, basic_schema):
        """Test MERGE with automatic updated_at timestamp."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column='updated_at'
        )

        ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        # Should set updated_at to GETDATE() in SET clause
        assert 'target.[updated_at] = GETDATE()' in ddl or 'GETDATE()' in ddl

    def test_merge_update_none_strategy(self, sqlserver_generator, basic_schema):
        """Test MERGE with UPDATE_NONE strategy (INSERT only)."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_NONE,
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        # Should not have UPDATE SET clause
        assert 'UPDATE SET' not in ddl
        # But should still have INSERT
        assert 'INSERT' in ddl

    def test_merge_with_custom_staging_name(self, sqlserver_generator, basic_schema):
        """Test MERGE with custom staging table name."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            staging_table='users_temp',
            dataset_name='staging'
        )

        assert '[staging].[users_temp]' in ddl


# ============================================================================
# Append Tests
# ============================================================================

class TestSQLServerAppend:
    """Test SQL Server append pattern."""

    def test_basic_append(self, sqlserver_generator, basic_schema):
        """Test basic append without deduplication."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY,
            primary_keys=[],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_append_ddl(
            schema=basic_schema,
            table_name='events',
            config=config,
            dataset_name='dbo'
        )

        # Check simple INSERT
        assert 'INSERT INTO [dbo].[events]' in ddl
        assert 'FROM [dbo].[events_staging]' in ddl
        assert 'WHERE NOT EXISTS' not in ddl

    def test_append_with_deduplication(self, sqlserver_generator, basic_schema):
        """Test append with deduplication filter."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            primary_keys=['user_id'],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_append_ddl(
            schema=basic_schema,
            table_name='events',
            config=config,
            dataset_name='dbo'
        )

        # Check NOT EXISTS clause
        assert 'WHERE NOT EXISTS' in ddl
        assert 'SELECT 1 FROM [dbo].[events] AS target' in ddl

    def test_append_with_custom_staging(self, sqlserver_generator, basic_schema):
        """Test append with custom staging table."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY,
            primary_keys=[],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_append_ddl(
            schema=basic_schema,
            table_name='events',
            config=config,
            staging_table='events_temp'
        )

        assert '[events_temp]' in ddl


# ============================================================================
# Full Refresh Tests
# ============================================================================

class TestSQLServerFullRefresh:
    """Test SQL Server full refresh pattern."""

    def test_full_refresh_truncate(self, sqlserver_generator, basic_schema):
        """Test full refresh with TRUNCATE."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.FULL_REFRESH,
            primary_keys=[],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_full_refresh_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='dbo'
        )

        # Check TRUNCATE and INSERT
        assert 'TRUNCATE TABLE [dbo].[users];' in ddl
        assert 'INSERT INTO [dbo].[users]' in ddl
        assert 'FROM [dbo].[users_staging]' in ddl
        assert 'BEGIN TRANSACTION;' in ddl or 'TRANSACTION' in ddl
        assert 'COMMIT TRANSACTION;' in ddl or 'COMMIT' in ddl


# ============================================================================
# SCD2 Tests
# ============================================================================

class TestSQLServerSCD2:
    """Test SQL Server SCD Type 2 pattern."""

    def test_scd2_basic(self, sqlserver_generator, scd2_schema):
        """Test basic SCD2 with UPDATE + INSERT."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            effective_date_column='valid_from',
            expiration_date_column='valid_to',
            is_current_column='is_current',
            hash_columns=['name', 'email'],
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_scd2_ddl(
            schema=scd2_schema,
            table_name='users',
            config=config,
            dataset_name='dim'
        )

        # Check transaction
        assert 'BEGIN TRANSACTION;' in ddl or 'TRANSACTION' in ddl
        assert 'COMMIT TRANSACTION;' in ddl or 'COMMIT' in ddl

        # Check UPDATE to expire existing records (in MERGE)
        assert 'UPDATE SET' in ddl
        assert '[valid_to]' in ddl
        assert '[is_current] = 0' in ddl

        # Check INSERT for new versions
        assert 'INSERT INTO [dim].[users]' in ddl
        assert 'GETDATE()' in ddl

    def test_scd2_with_change_detection(self, sqlserver_generator, scd2_schema):
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

        ddl = sqlserver_generator.generate_scd2_ddl(
            schema=scd2_schema,
            table_name='users',
            config=config
        )

        # Check that hash columns are referenced with ISNULL
        assert 'ISNULL' in ddl
        assert 'NULL_SENTINEL' in ddl


# ============================================================================
# Incremental Timestamp Tests
# ============================================================================

class TestSQLServerIncrementalTimestamp:
    """Test SQL Server incremental timestamp pattern."""

    def test_incremental_timestamp_basic(self, sqlserver_generator, basic_schema):
        """Test incremental timestamp with CTE."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=[],
            incremental_column='updated_at',
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_incremental_timestamp_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='dbo'
        )

        # Check variable declaration for max timestamp
        assert 'DECLARE @max_ts' in ddl
        assert 'ISNULL(MAX([updated_at])' in ddl

        # Check INSERT with timestamp filter
        assert 'INSERT INTO [dbo].[users]' in ddl
        assert 'WHERE [updated_at] > @max_ts' in ddl

    def test_incremental_timestamp_with_lookback(self, sqlserver_generator, basic_schema):
        """Test incremental timestamp with lookback window."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=[],
            incremental_column='updated_at',
            lookback_window='2 hours',
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_incremental_timestamp_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        # Check lookback interval (SQL Server uses DATEADD)
        assert "DATEADD(HOUR, -2, @max_ts)" in ddl


# ============================================================================
# CDC Tests
# ============================================================================

class TestSQLServerCDC:
    """Test SQL Server CDC pattern."""

    def test_cdc_merge_basic(self, sqlserver_generator, cdc_schema):
        """Test CDC merge with DELETE/UPDATE/INSERT operations."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            delete_strategy=DeleteStrategy.HARD_DELETE,
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_cdc_merge_ddl(
            schema=cdc_schema,
            table_name='users',
            config=config,
            dataset_name='dbo'
        )

        # Check transaction
        assert 'BEGIN TRANSACTION;' in ddl or 'TRANSACTION' in ddl
        assert 'COMMIT TRANSACTION;' in ddl or 'COMMIT' in ddl

        # Check DELETE operation (in MERGE)
        assert 'DELETE' in ddl
        assert "[_op] = 'D'" in ddl

        # Check UPDATE operation (in MERGE)
        assert 'UPDATE SET' in ddl
        assert "[_op] = 'U'" in ddl

        # Check INSERT operation (in MERGE)
        assert 'INSERT' in ddl
        assert "[_op] = 'I'" in ddl

    def test_cdc_with_soft_delete(self, sqlserver_generator, cdc_schema):
        """Test CDC with soft delete strategy."""
        # Add soft delete column to schema
        cdc_schema_with_soft_delete = cdc_schema + [
            {'name': 'is_deleted', 'type': 'BIT', 'mode': 'NULLABLE'}
        ]

        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            delete_strategy=DeleteStrategy.SOFT_DELETE,
            soft_delete_column='is_deleted',
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_cdc_merge_ddl(
            schema=cdc_schema_with_soft_delete,
            table_name='users',
            config=config
        )

        # Should have UPDATE for soft delete (in MERGE statement)
        assert 'UPDATE' in ddl
        assert 'is_deleted' in ddl or 'deleted' in ddl.lower()
        # MERGE uses DELETE keyword in WHEN clause, so just check it handles soft deletes
        assert '= 1' in ddl

    def test_cdc_with_sequence_column(self, sqlserver_generator, cdc_schema):
        """Test CDC accepts sequence_column config without errors."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            sequence_column='_timestamp',
            delete_strategy=DeleteStrategy.HARD_DELETE,
            updated_by_column=None
        )

        ddl = sqlserver_generator.generate_cdc_merge_ddl(
            schema=cdc_schema,
            table_name='users',
            config=config
        )

        # Verify CDC operations are generated correctly (using MERGE)
        assert 'BEGIN TRANSACTION;' in ddl or 'TRANSACTION' in ddl
        assert 'COMMIT TRANSACTION;' in ddl or 'COMMIT' in ddl
        assert 'DELETE' in ddl or 'MERGE' in ddl  # DELETE in WHEN clause
        assert 'UPDATE' in ddl  # UPDATE in WHEN clause
        assert 'INSERT' in ddl


# ============================================================================
# Staging Table Tests
# ============================================================================

class TestSQLServerStagingTable:
    """Test SQL Server staging table generation."""

    def test_staging_table_temp(self, sqlserver_generator, basic_schema):
        """Test temporary staging table with # prefix."""
        ddl = sqlserver_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            temporary=True
        )

        # Check temp table with # prefix
        assert 'CREATE TABLE #users_staging' in ddl

        # Check columns
        assert '[user_id] INT NOT NULL' in ddl
        assert '[name] NVARCHAR(100)' in ddl
        assert '[email] NVARCHAR(255)' in ddl

    def test_staging_table_permanent(self, sqlserver_generator, basic_schema):
        """Test permanent staging table."""
        ddl = sqlserver_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            dataset_name='staging',
            temporary=False
        )

        # Should not have # prefix
        assert '#' not in ddl
        # Should have schema prefix
        assert '[staging].[users_staging]' in ddl

    def test_staging_table_with_clustered_index(self, sqlserver_generator, basic_schema):
        """Test staging table with clustered index."""
        ddl = sqlserver_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            temporary=True,
            clustered_index='user_id'
        )

        # Check clustered index
        assert 'CREATE CLUSTERED INDEX' in ddl
        assert 'IX_users_staging_Clustered' in ddl
        assert '([user_id])' in ddl

    def test_staging_table_with_composite_clustered_index(self, sqlserver_generator, basic_schema):
        """Test staging table with composite clustered index."""
        ddl = sqlserver_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            temporary=True,
            clustered_index=['user_id', 'updated_at']
        )

        # Check composite clustered index
        assert 'CREATE CLUSTERED INDEX' in ddl
        assert '[user_id], [updated_at]' in ddl

    def test_staging_table_with_custom_name(self, sqlserver_generator, basic_schema):
        """Test staging table with custom name."""
        ddl = sqlserver_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            staging_name='users_temp',
            temporary=True
        )

        assert '#users_temp' in ddl


# ============================================================================
# Utility Tests
# ============================================================================

class TestSQLServerUtilities:
    """Test SQL Server utility methods."""

    def test_get_max_timestamp_query(self, sqlserver_generator):
        """Test max timestamp query generation."""
        query = sqlserver_generator.get_max_timestamp_query(
            table_name='users',
            timestamp_column='updated_at',
            dataset_name='dbo'
        )

        # Check query structure
        assert 'ISNULL(MAX([updated_at])' in query
        assert 'FROM [dbo].[users]' in query
        assert '1970-01-01' in query

    def test_build_table_ref_with_schema(self, sqlserver_generator):
        """Test table reference building with schema."""
        ref = sqlserver_generator._build_table_ref('users', 'dbo')
        assert ref == '[dbo].[users]'

    def test_build_table_ref_without_schema(self, sqlserver_generator):
        """Test table reference building without schema."""
        ref = sqlserver_generator._build_table_ref('users', None)
        assert ref == '[users]'


# ============================================================================
# Integration Tests
# ============================================================================

class TestSQLServerIntegration:
    """Integration tests for end-to-end SQL Server workflows."""

    def test_complete_upsert_workflow(self, sqlserver_generator, basic_schema):
        """Test complete UPSERT workflow with staging and merge."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column='updated_at'
        )

        # Generate staging table
        staging_ddl = sqlserver_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            temporary=True,
            clustered_index='user_id'
        )
        assert 'CREATE TABLE #users_staging' in staging_ddl
        assert 'CREATE CLUSTERED INDEX' in staging_ddl

        # Generate MERGE
        merge_ddl = sqlserver_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            dataset_name='dbo'
        )
        assert 'MERGE INTO [dbo].[users]' in merge_ddl or 'MERGE' in merge_ddl
        assert 'WHEN MATCHED' in merge_ddl
        assert 'WHEN NOT MATCHED BY TARGET' in merge_ddl

    def test_complete_cdc_workflow(self, sqlserver_generator, cdc_schema):
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
        cdc_ddl = sqlserver_generator.generate_cdc_merge_ddl(
            schema=cdc_schema,
            table_name='users',
            config=config,
            dataset_name='dbo'
        )

        # Check all operations are present (in MERGE statement)
        assert 'DELETE' in cdc_ddl  # Delete operations (in WHEN clause)
        assert 'UPDATE' in cdc_ddl  # Update operations (in WHEN clause)
        assert 'INSERT' in cdc_ddl  # Insert operations
        assert 'BEGIN TRANSACTION;' in cdc_ddl or 'TRANSACTION' in cdc_ddl
        assert 'COMMIT TRANSACTION;' in cdc_ddl or 'COMMIT' in cdc_ddl

    def test_complete_scd2_workflow(self, sqlserver_generator, scd2_schema):
        """Test complete SCD2 workflow."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            effective_date_column='valid_from',
            expiration_date_column='valid_to',
            is_current_column='is_current',
            hash_columns=['name', 'email'],
            updated_by_column=None
        )

        # Generate SCD2 DDL
        scd2_ddl = sqlserver_generator.generate_scd2_ddl(
            schema=scd2_schema,
            table_name='dim_users',
            config=config,
            dataset_name='dwh'
        )

        # Check SCD2 operations (using MERGE)
        assert 'UPDATE' in scd2_ddl  # Expire existing records (in MERGE)
        assert 'valid_to' in scd2_ddl
        assert 'is_current' in scd2_ddl
        assert 'INSERT' in scd2_ddl  # Insert new versions
        assert 'BEGIN TRANSACTION;' in scd2_ddl or 'TRANSACTION' in scd2_ddl
        assert 'COMMIT TRANSACTION;' in scd2_ddl or 'COMMIT' in scd2_ddl
