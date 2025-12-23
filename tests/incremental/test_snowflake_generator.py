"""
Unit tests for Snowflake incremental DDL generator.
"""

import pytest
from schema_mapper.incremental import (
    get_incremental_generator,
    IncrementalConfig,
    LoadPattern,
    MergeStrategy,
    DeleteStrategy,
)


@pytest.fixture
def snowflake_generator():
    """Create Snowflake generator instance."""
    return get_incremental_generator('snowflake')


@pytest.fixture
def basic_schema():
    """Basic schema for testing."""
    return [
        {'name': 'user_id', 'type': 'NUMBER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'VARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'VARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]


@pytest.fixture
def scd2_schema():
    """Schema with SCD Type 2 columns."""
    return [
        {'name': 'user_id', 'type': 'NUMBER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'VARCHAR(100)', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'VARCHAR(255)', 'mode': 'NULLABLE'},
        {'name': 'effective_from', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'effective_to', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'is_current', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}
    ]


@pytest.fixture
def copy_into_schema():
    """Schema for COPY INTO tests (Dict format)."""
    return {
        'user_id': 'NUMBER',
        'name': 'VARCHAR(100)',
        'email': 'VARCHAR(255)',
        'created_at': 'TIMESTAMP'
    }


class TestSnowflakeGenerator:
    """Test Snowflake generator initialization and capabilities."""

    def test_generator_creation(self, snowflake_generator):
        """Test that Snowflake generator is created correctly."""
        assert snowflake_generator is not None
        assert snowflake_generator.platform == 'snowflake'

    def test_supports_all_patterns(self, snowflake_generator):
        """Test that Snowflake supports all expected patterns."""
        supported_patterns = [
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
        for pattern in supported_patterns:
            assert snowflake_generator.supports_pattern(pattern)


class TestMergeDDL:
    """Test MERGE statement generation."""

    def test_basic_merge_with_transaction(self, snowflake_generator, basic_schema):
        """Test basic MERGE with transaction wrapper."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column=None  # Disable auto-timestamp for basic test
        )

        ddl = snowflake_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        assert 'BEGIN TRANSACTION;' in ddl
        assert 'COMMIT;' in ddl
        assert 'MERGE INTO users AS target' in ddl
        assert 'USING users_staging AS source' in ddl
        assert 'ON target.user_id = source.user_id' in ddl
        assert 'WHEN MATCHED THEN' in ddl
        assert 'UPDATE SET' in ddl
        assert 'name = source.name' in ddl
        assert 'email = source.email' in ddl
        assert 'updated_at = source.updated_at' in ddl
        assert 'WHEN NOT MATCHED THEN' in ddl
        assert 'INSERT' in ddl

    def test_merge_without_transaction(self, snowflake_generator, basic_schema):
        """Test MERGE without transaction wrapper."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        ddl = snowflake_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            use_transaction=False
        )

        assert 'BEGIN TRANSACTION;' not in ddl
        assert 'COMMIT;' not in ddl
        assert 'MERGE INTO users AS target' in ddl

    def test_merge_with_database_and_schema(self, snowflake_generator, basic_schema):
        """Test MERGE with fully-qualified table names."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        ddl = snowflake_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            database_name='mydb',
            schema_name='public'
        )

        assert 'MERGE INTO mydb.public.users AS target' in ddl
        assert 'USING mydb.public.users_staging AS source' in ddl

    def test_merge_with_composite_keys(self, snowflake_generator):
        """Test MERGE with composite primary keys."""
        schema = [
            {'name': 'user_id', 'type': 'NUMBER', 'mode': 'REQUIRED'},
            {'name': 'order_id', 'type': 'NUMBER', 'mode': 'REQUIRED'},
            {'name': 'amount', 'type': 'DECIMAL(10,2)', 'mode': 'NULLABLE'}
        ]
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id', 'order_id']
        )

        ddl = snowflake_generator.generate_merge_ddl(
            schema=schema,
            table_name='orders',
            config=config
        )

        assert 'target.user_id = source.user_id AND target.order_id = source.order_id' in ddl

    def test_merge_with_selective_update(self, snowflake_generator, basic_schema):
        """Test MERGE with selective column updates."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_SELECTIVE,
            update_columns=['name']
        )

        ddl = snowflake_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        assert 'UPDATE SET name = source.name' in ddl
        # Should not update other columns
        assert 'email = source.email' not in ddl

    def test_merge_with_updated_at_column(self, snowflake_generator, basic_schema):
        """Test MERGE with automatic updated_at timestamp."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            updated_by_column='updated_at'
        )

        ddl = snowflake_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        assert 'updated_at = CURRENT_TIMESTAMP()' in ddl

    def test_merge_update_none_strategy(self, snowflake_generator, basic_schema):
        """Test MERGE with UPDATE_NONE strategy (INSERT only)."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_NONE
        )

        ddl = snowflake_generator.generate_merge_ddl(
            schema=basic_schema,
            table_name='users',
            config=config
        )

        assert 'WHEN MATCHED THEN' not in ddl
        assert 'UPDATE SET' not in ddl
        assert 'WHEN NOT MATCHED THEN' in ddl


class TestAppendDDL:
    """Test append-only INSERT generation."""

    def test_basic_append(self, snowflake_generator, basic_schema):
        """Test simple append INSERT."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY,
            primary_keys=[]
        )

        ddl = snowflake_generator.generate_append_ddl(
            schema=basic_schema,
            table_name='events',
            config=config
        )

        assert 'INSERT INTO events' in ddl
        assert 'SELECT' in ddl
        assert 'FROM events_staging' in ddl
        assert 'user_id, name, email, updated_at' in ddl

    def test_append_with_custom_staging(self, snowflake_generator, basic_schema):
        """Test append with custom staging table."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY,
            primary_keys=[]
        )

        ddl = snowflake_generator.generate_append_ddl(
            schema=basic_schema,
            table_name='events',
            config=config,
            staging_table='custom_staging'
        )

        assert 'FROM custom_staging' in ddl


class TestFullRefreshDDL:
    """Test full refresh DDL generation."""

    def test_full_refresh_with_create_or_replace(self, snowflake_generator, basic_schema):
        """Test full refresh using CREATE OR REPLACE."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.FULL_REFRESH,
            primary_keys=[]
        )

        ddl = snowflake_generator.generate_full_refresh_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            use_create_or_replace=True
        )

        assert 'CREATE OR REPLACE TABLE users AS' in ddl
        assert 'SELECT' in ddl
        assert 'FROM users_staging' in ddl
        assert 'TRUNCATE' not in ddl

    def test_full_refresh_with_truncate(self, snowflake_generator, basic_schema):
        """Test full refresh using TRUNCATE + INSERT."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.FULL_REFRESH,
            primary_keys=[]
        )

        ddl = snowflake_generator.generate_full_refresh_ddl(
            schema=basic_schema,
            table_name='users',
            config=config,
            use_create_or_replace=False
        )

        assert 'BEGIN TRANSACTION;' in ddl
        assert 'TRUNCATE TABLE users;' in ddl
        assert 'INSERT INTO users' in ddl
        assert 'COMMIT;' in ddl


class TestSCD2DDL:
    """Test SCD Type 2 DDL generation."""

    def test_scd2_basic(self, snowflake_generator, scd2_schema):
        """Test basic SCD Type 2 implementation."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            hash_columns=['name', 'email'],
            effective_date_column='effective_from',
            expiration_date_column='effective_to',
            is_current_column='is_current'
        )

        ddl = snowflake_generator.generate_scd2_ddl(
            schema=scd2_schema,
            table_name='users',
            config=config
        )

        assert 'BEGIN TRANSACTION;' in ddl
        assert '-- Step 1: Expire changed records' in ddl
        assert 'UPDATE' in ddl
        assert 'effective_to = CURRENT_TIMESTAMP()' in ddl
        assert 'is_current = FALSE' in ddl
        assert '-- Step 2: Insert new versions for changed records' in ddl
        assert '-- Step 3: Insert completely new records' in ddl
        assert 'HASH(' in ddl
        assert 'COMMIT;' in ddl

    def test_scd2_validation_requires_hash_columns(self, snowflake_generator, scd2_schema):
        """Test that SCD2 requires hash_columns."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            # Missing hash_columns
            effective_date_column='effective_from',
            expiration_date_column='effective_to',
            is_current_column='is_current'
        )

        with pytest.raises(ValueError, match='hash_columns required'):
            snowflake_generator.generate_scd2_ddl(
                schema=scd2_schema,
                table_name='users',
                config=config
            )


class TestIncrementalTimestampDDL:
    """Test incremental timestamp-based loads."""

    def test_incremental_timestamp_basic(self, snowflake_generator, basic_schema):
        """Test basic incremental timestamp load."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=[],
            incremental_column='updated_at'
        )

        ddl = snowflake_generator.generate_incremental_timestamp_ddl(
            schema=basic_schema,
            table_name='events',
            config=config
        )

        assert 'SET max_ts = (' in ddl
        assert 'NVL(MAX(updated_at)' in ddl
        assert '1900-01-01' in ddl
        assert 'INSERT INTO events' in ddl
        assert 'WHERE source.updated_at > $max_ts' in ddl

    def test_incremental_timestamp_with_lookback(self, snowflake_generator, basic_schema):
        """Test incremental timestamp with lookback window."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=[],
            incremental_column='updated_at',
            lookback_window='2 hours'
        )

        ddl = snowflake_generator.generate_incremental_timestamp_ddl(
            schema=basic_schema,
            table_name='events',
            config=config
        )

        assert "- INTERVAL '2 hours'" in ddl

    def test_incremental_timestamp_validation(self, snowflake_generator, basic_schema):
        """Test that incremental timestamp requires incremental_column."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=[]
            # Missing incremental_column
        )

        with pytest.raises(ValueError, match='incremental_column required'):
            snowflake_generator.generate_incremental_timestamp_ddl(
                schema=basic_schema,
                table_name='events',
                config=config
            )


class TestCDCMergeDDL:
    """Test CDC (Change Data Capture) MERGE generation."""

    def test_cdc_merge_basic(self, snowflake_generator):
        """Test CDC MERGE with I/U/D operations."""
        schema = [
            {'name': 'user_id', 'type': 'NUMBER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'VARCHAR(100)', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'VARCHAR(255)', 'mode': 'NULLABLE'},
            {'name': '_op', 'type': 'VARCHAR(1)', 'mode': 'NULLABLE'},
            {'name': '_seq', 'type': 'NUMBER', 'mode': 'NULLABLE'}
        ]
        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            sequence_column='_seq'
        )

        ddl = snowflake_generator.generate_cdc_merge_ddl(
            schema=schema,
            table_name='users',
            config=config
        )

        assert 'BEGIN TRANSACTION;' in ddl
        assert 'MERGE INTO users AS target' in ddl
        assert "WHEN MATCHED AND source._op = 'D' THEN" in ddl
        assert 'DELETE' in ddl
        assert "WHEN MATCHED AND source._op IN ('U', 'I') THEN" in ddl
        assert 'UPDATE SET' in ddl
        assert "WHEN NOT MATCHED AND source._op IN ('I', 'U') THEN" in ddl
        assert 'INSERT' in ddl
        # CDC columns should not be in UPDATE or INSERT
        assert '_op = source._op' not in ddl
        assert '_seq = source._seq' not in ddl


class TestStagingTableDDL:
    """Test staging table DDL generation."""

    def test_staging_table_transient(self, snowflake_generator, basic_schema):
        """Test TRANSIENT staging table creation."""
        ddl = snowflake_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            transient=True
        )

        assert 'CREATE OR REPLACE TRANSIENT TABLE users_staging' in ddl
        assert 'user_id NUMBER' in ddl
        assert 'name VARCHAR(100)' in ddl
        assert 'email VARCHAR(255)' in ddl
        assert 'updated_at TIMESTAMP' in ddl

    def test_staging_table_permanent(self, snowflake_generator, basic_schema):
        """Test permanent (non-TRANSIENT) staging table."""
        ddl = snowflake_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            transient=False
        )

        assert 'CREATE OR REPLACE TABLE users_staging' in ddl
        assert 'TRANSIENT' not in ddl.replace('CREATE OR REPLACE TABLE', '')

    def test_staging_table_with_cluster(self, snowflake_generator, basic_schema):
        """Test staging table with clustering."""
        ddl = snowflake_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            cluster_by=['user_id', 'updated_at']
        )

        assert 'CLUSTER BY (user_id, updated_at)' in ddl

    def test_staging_table_with_custom_name(self, snowflake_generator, basic_schema):
        """Test staging table with custom name."""
        ddl = snowflake_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            staging_name='my_staging_table'
        )

        assert 'my_staging_table' in ddl

    def test_staging_table_with_database_and_schema(self, snowflake_generator, basic_schema):
        """Test staging table with fully-qualified name."""
        ddl = snowflake_generator.generate_staging_table_ddl(
            schema=basic_schema,
            table_name='users',
            database_name='mydb',
            schema_name='staging'
        )

        assert 'CREATE OR REPLACE TRANSIENT TABLE mydb.staging.users_staging' in ddl


class TestMaxTimestampQuery:
    """Test max timestamp query generation."""

    def test_max_timestamp_basic(self, snowflake_generator):
        """Test basic max timestamp query."""
        query = snowflake_generator.get_max_timestamp_query(
            table_name='events',
            timestamp_column='event_time'
        )

        assert 'SELECT NVL(MAX(event_time)' in query
        assert '1900-01-01' in query
        assert 'FROM events' in query

    def test_max_timestamp_with_database_and_schema(self, snowflake_generator):
        """Test max timestamp query with fully-qualified table."""
        query = snowflake_generator.get_max_timestamp_query(
            table_name='events',
            timestamp_column='event_time',
            database_name='analytics',
            schema_name='raw'
        )

        assert 'FROM analytics.raw.events' in query


class TestCopyIntoDDL:
    """Test COPY INTO statement generation."""

    def test_copy_into_basic(self, snowflake_generator, copy_into_schema):
        """Test basic COPY INTO statement."""
        ddl = snowflake_generator.generate_copy_into_ddl(
            table_name='users',
            stage_name='@my_stage/users/',
            schema=copy_into_schema
        )

        assert 'COPY INTO users' in ddl
        assert 'user_id, name, email, created_at' in ddl
        assert 'FROM @my_stage/users/' in ddl
        assert "FILE_FORMAT = (TYPE = 'CSV')" in ddl
        assert "ON_ERROR = 'ABORT_STATEMENT'" in ddl

    def test_copy_into_with_pattern(self, snowflake_generator, copy_into_schema):
        """Test COPY INTO with file pattern."""
        ddl = snowflake_generator.generate_copy_into_ddl(
            table_name='users',
            stage_name='@my_stage/users/',
            schema=copy_into_schema,
            pattern='.*\\.csv'
        )

        assert "PATTERN = '.*\\.csv'" in ddl

    def test_copy_into_with_custom_file_format(self, snowflake_generator, copy_into_schema):
        """Test COPY INTO with custom file format."""
        ddl = snowflake_generator.generate_copy_into_ddl(
            table_name='users',
            stage_name='@my_stage/users/',
            schema=copy_into_schema,
            file_format='my_json_format'
        )

        assert "FILE_FORMAT = (TYPE = 'my_json_format')" in ddl

    def test_copy_into_with_on_error(self, snowflake_generator, copy_into_schema):
        """Test COPY INTO with custom error handling."""
        ddl = snowflake_generator.generate_copy_into_ddl(
            table_name='users',
            stage_name='@my_stage/users/',
            schema=copy_into_schema,
            on_error='CONTINUE'
        )

        assert "ON_ERROR = 'CONTINUE'" in ddl

    def test_copy_into_without_schema(self, snowflake_generator):
        """Test COPY INTO without explicit schema (no column list)."""
        ddl = snowflake_generator.generate_copy_into_ddl(
            table_name='users',
            stage_name='@my_stage/users/'
        )

        assert 'COPY INTO users' in ddl
        assert 'FROM @my_stage/users/' in ddl
        # Should not have column list
        assert '(' not in ddl.split('\n')[0]

    def test_copy_into_with_database_and_schema(self, snowflake_generator, copy_into_schema):
        """Test COPY INTO with fully-qualified table name."""
        ddl = snowflake_generator.generate_copy_into_ddl(
            table_name='users',
            stage_name='@my_stage/users/',
            schema=copy_into_schema,
            database_name='mydb',
            schema_name='public'
        )

        assert 'COPY INTO mydb.public.users' in ddl


class TestTableReferenceBuilding:
    """Test Snowflake-specific table reference building."""

    def test_table_only(self, snowflake_generator):
        """Test table name only."""
        ref = snowflake_generator._build_table_ref('users')
        assert ref == 'users'

    def test_schema_and_table(self, snowflake_generator):
        """Test schema.table reference."""
        ref = snowflake_generator._build_table_ref('users', schema_name='public')
        assert ref == 'public.users'

    def test_database_schema_table(self, snowflake_generator):
        """Test database.schema.table reference."""
        ref = snowflake_generator._build_table_ref('users', database_name='mydb', schema_name='public')
        assert ref == 'mydb.public.users'

    def test_database_without_schema_defaults_to_public(self, snowflake_generator):
        """Test that database without schema defaults to public."""
        ref = snowflake_generator._build_table_ref('users', database_name='mydb')
        assert ref == 'mydb.public.users'


class TestIntegrationWithSchemaMapper:
    """Test integration with SchemaMapper."""

    def test_generate_snowflake_copy_into_via_mapper(self):
        """Test COPY INTO generation through SchemaMapper."""
        from schema_mapper import SchemaMapper

        mapper = SchemaMapper('snowflake')
        schema = {
            'user_id': 'NUMBER',
            'name': 'VARCHAR(100)',
            'email': 'VARCHAR(255)'
        }

        ddl = mapper.generate_snowflake_copy_into(
            table_name='users',
            stage_name='@s3_stage/users/',
            schema=schema,
            file_format='my_csv_format'
        )

        assert 'COPY INTO users' in ddl
        assert 'FROM @s3_stage/users/' in ddl
        assert "FILE_FORMAT = (TYPE = 'my_csv_format')" in ddl

    def test_generate_snowflake_copy_into_wrong_platform(self):
        """Test that COPY INTO raises error for non-Snowflake mapper."""
        from schema_mapper import SchemaMapper

        mapper = SchemaMapper('bigquery')

        with pytest.raises(ValueError, match='only works with Snowflake mapper'):
            mapper.generate_snowflake_copy_into(
                table_name='users',
                stage_name='@stage/',
                schema={}
            )
