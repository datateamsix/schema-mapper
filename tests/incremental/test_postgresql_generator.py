"""Tests for PostgreSQL incremental DDL generator."""

import pytest
import pandas as pd
from schema_mapper import SchemaMapper
from schema_mapper.incremental import LoadPattern, IncrementalConfig, MergeStrategy, DeleteStrategy


@pytest.fixture
def sample_df():
    """Sample DataFrame for testing."""
    return pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['a@ex.com', 'b@ex.com', 'c@ex.com'],
        'updated_at': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
    })


class TestPostgreSQLUpsertDDL:
    """Test PostgreSQL INSERT ... ON CONFLICT (UPSERT)."""

    def test_basic_upsert(self, sample_df):
        """Test INSERT ... ON CONFLICT DO UPDATE."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='public'
        )

        # Check transaction wrapper
        assert 'BEGIN;' in ddl
        assert 'COMMIT;' in ddl

        # Check INSERT ... ON CONFLICT structure
        assert 'INSERT INTO "public"."users"' in ddl
        assert 'FROM "public"."users_staging"' in ddl
        assert 'ON CONFLICT ("user_id")' in ddl
        assert 'DO UPDATE SET' in ddl
        assert 'EXCLUDED' in ddl

    def test_upsert_with_schema(self, sample_df):
        """Test UPSERT with schema qualification."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config,
            dataset_name='analytics'
        )

        assert '"analytics"."users"' in ddl
        assert '"analytics"."users_staging"' in ddl

    def test_insert_ignore(self, sample_df):
        """Test INSERT ... ON CONFLICT DO NOTHING."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_NONE
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config
        )

        assert 'ON CONFLICT' in ddl
        assert 'DO NOTHING' in ddl

    def test_upsert_composite_key(self, sample_df):
        """Test UPSERT with composite primary key."""
        df = pd.DataFrame({
            'order_id': [1, 1, 2],
            'line_number': [1, 2, 1],
            'product': ['A', 'B', 'C']
        })

        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['order_id', 'line_number']
        )

        ddl = mapper.generate_incremental_ddl(
            df,
            'order_lines',
            config
        )

        assert 'ON CONFLICT ("order_id", "line_number")' in ddl

    def test_upsert_selective_update(self, sample_df):
        """Test UPSERT with selective column updates."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_SELECTIVE,
            update_columns=['email']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config
        )

        assert 'DO UPDATE SET' in ddl
        assert '"email" = EXCLUDED."email"' in ddl


class TestPostgreSQLAppendDDL:
    """Test PostgreSQL APPEND DDL generation."""

    def test_basic_append(self, sample_df):
        """Test basic INSERT (APPEND) DDL."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY,
            primary_keys=[]
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'events',
            config,
            dataset_name='public'
        )

        assert 'BEGIN;' in ddl
        assert 'INSERT INTO "public"."events"' in ddl
        assert 'SELECT' in ddl
        assert 'FROM "public"."events_staging"' in ddl
        assert 'COMMIT;' in ddl

    def test_incremental_append(self, sample_df):
        """Test incremental append (skip existing records)."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'events',
            config
        )

        assert 'WHERE NOT EXISTS' in ddl


class TestPostgreSQLFullRefresh:
    """Test PostgreSQL FULL_REFRESH DDL generation."""

    def test_truncate_and_load(self, sample_df):
        """Test TRUNCATE + INSERT pattern."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.FULL_REFRESH,
            primary_keys=[]
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'dim_products',
            config,
            dataset_name='public'
        )

        assert 'TRUNCATE TABLE "public"."dim_products"' in ddl
        assert 'INSERT INTO "public"."dim_products"' in ddl
        assert 'ANALYZE' in ddl


class TestPostgreSQLSCD2:
    """Test PostgreSQL SCD Type 2 DDL generation."""

    def test_scd2_with_cte(self, sample_df):
        """Test SCD Type 2 using CTEs."""
        mapper = SchemaMapper('postgresql')

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
            'dim_customers',
            config,
            dataset_name='public'
        )

        # Should use CTE
        assert 'WITH changed_records AS' in ddl

        # Should expire changed records
        assert 'UPDATE "public"."dim_customers"' in ddl
        assert 'effective_to' in ddl
        assert 'is_current' in ddl

        # Should insert new versions
        assert 'INSERT INTO "public"."dim_customers"' in ddl
        assert "'9999-12-31'" in ddl

        # Check IS DISTINCT FROM for NULL handling
        assert 'IS DISTINCT FROM' in ddl

    def test_scd2_all_columns(self, sample_df):
        """Test SCD Type 2 tracking all columns."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['user_id'],
            hash_columns=['name', 'email', 'updated_at'],  # Specify all data columns
            effective_date_column='effective_from',
            expiration_date_column='effective_to',
            is_current_column='is_current'
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'dim_customers',
            config
        )

        assert 'WITH changed_records AS' in ddl
        assert 'IS DISTINCT FROM' in ddl


class TestPostgreSQLIncrementalTimestamp:
    """Test PostgreSQL incremental timestamp loads."""

    def test_incremental_timestamp_with_cte(self, sample_df):
        """Test incremental load using CTE for max timestamp."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=['user_id'],
            incremental_column='updated_at'
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'events',
            config,
            dataset_name='public'
        )

        # Should use CTE
        assert 'WITH max_timestamp AS' in ddl

        # Should filter on timestamp
        assert 'WHERE "updated_at" > max_timestamp.max_timestamp' in ddl

    def test_incremental_timestamp_with_lookback(self, sample_df):
        """Test incremental load with lookback window."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=['user_id'],
            incremental_column='updated_at',
            lookback_window='2 hours'
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'events',
            config
        )

        # Should use INTERVAL for lookback
        assert "INTERVAL '2 hours'" in ddl


class TestPostgreSQLCDC:
    """Test PostgreSQL CDC processing DDL generation."""

    def test_cdc_with_hard_delete(self, sample_df):
        """Test CDC with hard deletes."""
        df_cdc = sample_df.copy()
        df_cdc['_op'] = ['I', 'U', 'D']

        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            delete_strategy=DeleteStrategy.HARD_DELETE
        )

        ddl = mapper.generate_incremental_ddl(
            df_cdc,
            'users',
            config
        )

        # Should handle all three operations
        assert 'DELETE FROM' in ddl
        assert "WHERE \"_op\" = 'D'" in ddl or "'D'" in ddl
        assert "WHERE \"_op\" = 'U'" in ddl or "'U'" in ddl
        assert "WHERE \"_op\" = 'I'" in ddl or "'I'" in ddl
        assert 'ON CONFLICT' in ddl

    def test_cdc_with_soft_delete(self, sample_df):
        """Test CDC with soft deletes."""
        df_cdc = sample_df.copy()
        df_cdc['_op'] = ['I', 'U', 'D']

        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['user_id'],
            operation_column='_op',
            delete_strategy=DeleteStrategy.SOFT_DELETE,
            soft_delete_column='is_deleted'
        )

        ddl = mapper.generate_incremental_ddl(
            df_cdc,
            'users',
            config
        )

        # Should set is_deleted flag
        assert 'is_deleted' in ddl
        assert 'UPDATE' in ddl


class TestPostgreSQLStagingTable:
    """Test PostgreSQL staging table generation."""

    def test_staging_table_temporary(self, sample_df):
        """Test creating TEMPORARY staging table."""
        mapper = SchemaMapper('postgresql')

        schema, _ = mapper.generate_schema(sample_df)

        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        staging_ddl = generator.generate_staging_table_ddl(
            schema,
            'users',
            temporary=True
        )

        assert 'CREATE TEMPORARY TABLE' in staging_ddl
        assert 'ON COMMIT DROP' in staging_ddl

    def test_staging_table_permanent(self, sample_df):
        """Test creating permanent staging table."""
        mapper = SchemaMapper('postgresql')

        schema, _ = mapper.generate_schema(sample_df)

        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        staging_ddl = generator.generate_staging_table_ddl(
            schema,
            'users',
            dataset_name='staging',
            temporary=False
        )

        assert 'CREATE TABLE "staging"."users_staging"' in staging_ddl
        assert 'TEMPORARY' not in staging_ddl


class TestPostgreSQLCopyFromCSV:
    """Test PostgreSQL COPY FROM CSV command generation."""

    def test_copy_csv_basic(self, sample_df):
        """Test COPY FROM CSV."""
        mapper = SchemaMapper('postgresql')

        copy_ddl = mapper.generate_postgresql_copy_from_csv(
            sample_df,
            'events',
            '/data/events.csv',
            schema_name='staging'
        )

        assert 'COPY "staging"."events_staging"' in copy_ddl
        assert "FROM '/data/events.csv'" in copy_ddl
        assert 'FORMAT csv' in copy_ddl
        assert "DELIMITER ','" in copy_ddl
        assert 'HEADER true' in copy_ddl

    def test_copy_csv_custom_delimiter(self, sample_df):
        """Test COPY with custom delimiter."""
        mapper = SchemaMapper('postgresql')

        schema, _ = mapper.generate_schema(sample_df)

        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        copy_ddl = generator.generate_copy_from_csv_ddl(
            schema,
            'events',
            '/data/events.txt',
            delimiter='|',
            header=False
        )

        assert "DELIMITER '|'" in copy_ddl
        assert 'HEADER false' in copy_ddl

    def test_copy_csv_with_null_string(self, sample_df):
        """Test COPY with custom NULL string."""
        mapper = SchemaMapper('postgresql')

        schema, _ = mapper.generate_schema(sample_df)

        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        copy_ddl = generator.generate_copy_from_csv_ddl(
            schema,
            'events',
            '/data/events.csv',
            null_string='NULL'
        )

        assert "NULL 'NULL'" in copy_ddl

    def test_copy_csv_with_encoding(self, sample_df):
        """Test COPY with custom encoding."""
        mapper = SchemaMapper('postgresql')

        schema, _ = mapper.generate_schema(sample_df)

        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        copy_ddl = generator.generate_copy_from_csv_ddl(
            schema,
            'events',
            '/data/events.csv',
            encoding='LATIN1'
        )

        assert "ENCODING 'LATIN1'" in copy_ddl


class TestPostgreSQLMaintenance:
    """Test PostgreSQL maintenance command generation."""

    def test_analyze_command(self, sample_df):
        """Test ANALYZE command."""
        mapper = SchemaMapper('postgresql')

        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        analyze_cmd = generator.generate_analyze_command(
            'events',
            dataset_name='public'
        )

        assert 'ANALYZE "public"."events"' in analyze_cmd

    def test_vacuum_analyze(self, sample_df):
        """Test VACUUM ANALYZE command."""
        mapper = SchemaMapper('postgresql')

        maintenance = mapper.generate_postgresql_maintenance(
            'events',
            schema_name='public',
            vacuum_full=False
        )

        assert 'VACUUM ANALYZE "public"."events"' in maintenance

    def test_vacuum_full(self, sample_df):
        """Test VACUUM FULL ANALYZE command."""
        mapper = SchemaMapper('postgresql')

        maintenance = mapper.generate_postgresql_maintenance(
            'events',
            schema_name='public',
            vacuum_full=True
        )

        assert 'VACUUM FULL ANALYZE "public"."events"' in maintenance

    def test_vacuum_only(self, sample_df):
        """Test VACUUM without ANALYZE."""
        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        vacuum_cmd = generator.generate_vacuum_command(
            'events',
            dataset_name='public',
            full=False,
            analyze=False
        )

        assert 'VACUUM "public"."events"' in vacuum_cmd
        assert 'ANALYZE' not in vacuum_cmd


class TestPostgreSQLEdgeCases:
    """Test edge cases and error handling."""

    def test_without_schema_name(self, sample_df):
        """Test without schema qualification."""
        mapper = SchemaMapper('postgresql')

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        ddl = mapper.generate_incremental_ddl(
            sample_df,
            'users',
            config
        )

        # Should still work without schema
        assert 'INSERT INTO "users"' in ddl
        assert 'ON CONFLICT' in ddl

    def test_non_postgresql_copy_error(self, sample_df):
        """Test that COPY method errors on non-PostgreSQL platform."""
        mapper = SchemaMapper('bigquery')

        with pytest.raises(ValueError, match="only works with PostgreSQL"):
            mapper.generate_postgresql_copy_from_csv(
                sample_df,
                'events',
                '/data/events.csv'
            )

    def test_non_postgresql_maintenance_error(self, sample_df):
        """Test that maintenance method errors on non-PostgreSQL platform."""
        mapper = SchemaMapper('bigquery')

        with pytest.raises(ValueError, match="only works with PostgreSQL"):
            mapper.generate_postgresql_maintenance(
                'events',
                schema_name='public'
            )


class TestPostgreSQLPatternSupport:
    """Test PostgreSQL pattern support."""

    def test_supports_all_patterns(self):
        """Test that PostgreSQL supports all patterns."""
        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        all_patterns = [
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

        for pattern in all_patterns:
            assert generator.supports_pattern(pattern)


class TestPostgreSQLIntegration:
    """Test end-to-end PostgreSQL integration scenarios."""

    def test_complete_workflow(self, sample_df):
        """Test complete CSV to PostgreSQL workflow."""
        mapper = SchemaMapper('postgresql')

        # Step 1: Create staging table
        schema, _ = mapper.generate_schema(sample_df)

        from schema_mapper.incremental import get_incremental_generator
        generator = get_incremental_generator('postgresql')

        staging_ddl = generator.generate_staging_table_ddl(
            schema,
            'events',
            temporary=True
        )

        assert 'CREATE TEMPORARY TABLE' in staging_ddl

        # Step 2: Load CSV
        copy_ddl = mapper.generate_postgresql_copy_from_csv(
            sample_df,
            'events',
            '/data/events.csv',
            schema_name='public'
        )

        assert 'COPY' in copy_ddl

        # Step 3: UPSERT to final table
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        upsert_ddl = mapper.generate_incremental_ddl(
            sample_df,
            'events',
            config,
            dataset_name='public'
        )

        assert 'ON CONFLICT' in upsert_ddl

        # Step 4: Update statistics
        maintenance = mapper.generate_postgresql_maintenance(
            'events',
            schema_name='public'
        )

        assert 'VACUUM ANALYZE' in maintenance
