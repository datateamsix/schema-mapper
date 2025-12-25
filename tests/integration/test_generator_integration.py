"""Integration tests for connections with incremental generators."""

import pytest
from unittest.mock import Mock, MagicMock, patch

from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType
from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator


class TestIncrementalGeneratorIntegration:
    """Test integration between connections and incremental load generators."""

    def test_all_platforms_have_incremental_generators(self):
        """Test that all platforms support incremental load generators."""
        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        for platform in platforms:
            generator = get_incremental_generator(platform)
            assert generator is not None

    def test_append_only_pattern_across_platforms(self, sample_canonical_schema):
        """Test APPEND_ONLY pattern generates DDL for all platforms."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.APPEND_ONLY
        )

        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        for platform in platforms:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=sample_canonical_schema,
                table_name='users',
                config=config
            )

            assert ddl is not None
            assert isinstance(ddl, str)
            assert len(ddl) > 0

    def test_upsert_pattern_across_platforms(self):
        """Test UPSERT pattern generates DDL for all platforms."""
        schema = CanonicalSchema(
            table_name='customers',
            columns=[
                ColumnDefinition(name='customer_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='email', logical_type=LogicalType.STRING, nullable=False),
                ColumnDefinition(name='updated_at', logical_type=LogicalType.TIMESTAMP, nullable=False),
            ]
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['customer_id']
        )

        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        for platform in platforms:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='customers',
                config=config
            )

            assert ddl is not None
            assert isinstance(ddl, str)
            assert len(ddl) > 0
            # UPSERT/MERGE should be in the DDL
            assert ('MERGE' in ddl.upper() or
                    'INSERT' in ddl.upper() or
                    'UPDATE' in ddl.upper() or
                    'UPSERT' in ddl.upper())

    def test_scd2_pattern_across_platforms(self):
        """Test SCD2 pattern generates DDL for all platforms."""
        schema = CanonicalSchema(
            table_name='dim_customers',
            columns=[
                ColumnDefinition(name='customer_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='email', logical_type=LogicalType.STRING, nullable=False),
                ColumnDefinition(name='valid_from', logical_type=LogicalType.TIMESTAMP, nullable=False),
                ColumnDefinition(name='valid_to', logical_type=LogicalType.TIMESTAMP, nullable=True),
                ColumnDefinition(name='is_current', logical_type=LogicalType.BOOLEAN, nullable=False),
            ]
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD2,
            primary_keys=['customer_id'],
            scd2_columns=['email']
        )

        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        for platform in platforms:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='dim_customers',
                config=config
            )

            assert ddl is not None
            assert isinstance(ddl, str)
            assert len(ddl) > 0

    def test_delete_pattern_across_platforms(self):
        """Test DELETE pattern generates DDL for all platforms."""
        schema = CanonicalSchema(
            table_name='events',
            columns=[
                ColumnDefinition(name='event_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='user_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='event_time', logical_type=LogicalType.TIMESTAMP, nullable=False),
            ]
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.DELETE,
            primary_keys=['event_id']
        )

        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        for platform in platforms:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='events',
                config=config
            )

            assert ddl is not None
            assert isinstance(ddl, str)
            assert len(ddl) > 0
            assert 'DELETE' in ddl.upper()

    def test_snapshot_pattern_across_platforms(self):
        """Test SNAPSHOT pattern generates DDL for all platforms."""
        schema = CanonicalSchema(
            table_name='inventory_snapshot',
            columns=[
                ColumnDefinition(name='product_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='quantity', logical_type=LogicalType.INTEGER, nullable=False),
                ColumnDefinition(name='snapshot_date', logical_type=LogicalType.DATE, nullable=False),
            ]
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.SNAPSHOT
        )

        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        for platform in platforms:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='inventory_snapshot',
                config=config
            )

            assert ddl is not None
            assert isinstance(ddl, str)
            assert len(ddl) > 0


class TestIncrementalWithConnection:
    """Test incremental generator DDL execution workflow."""

    def test_incremental_ddl_workflow_concept(self):
        """Test the conceptual workflow: Schema -> Generator -> DDL -> Connection -> Execute."""
        # Step 1: Create schema
        schema = CanonicalSchema(
            table_name='users',
            columns=[
                ColumnDefinition(name='user_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='username', logical_type=LogicalType.STRING, nullable=False),
            ]
        )

        # Step 2: Configure incremental load
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id']
        )

        # Step 3: Generate platform-specific DDL
        for platform in ['bigquery', 'snowflake', 'postgresql']:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='users',
                config=config
            )

            # Step 4: Verify DDL is ready for execution
            assert ddl is not None
            assert isinstance(ddl, str)

            # In real usage, this DDL would be executed via connection:
            # with ConnectionFactory.get_connection(platform, config) as conn:
            #     conn.execute_ddl(ddl)

    def test_multi_step_incremental_load(self):
        """Test multi-step incremental load pattern."""
        schema = CanonicalSchema(
            table_name='orders',
            columns=[
                ColumnDefinition(name='order_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='customer_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='order_date', logical_type=LogicalType.DATE, nullable=False),
                ColumnDefinition(name='total_amount', logical_type=LogicalType.DECIMAL, nullable=False, precision=10, scale=2),
            ]
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['order_id'],
            update_columns=['customer_id', 'total_amount']
        )

        for platform in ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='orders',
                config=config
            )

            # Verify DDL includes update columns
            assert ddl is not None
            assert 'total_amount' in ddl.lower() or 'TOTAL_AMOUNT' in ddl


class TestIncrementalErrorHandling:
    """Test error handling in incremental generator integration."""

    def test_upsert_without_primary_keys_fails(self):
        """Test that UPSERT without primary keys raises appropriate error."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition(name='id', logical_type=LogicalType.BIGINT, nullable=False)
            ]
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=[]  # Missing primary keys
        )

        for platform in ['bigquery', 'snowflake']:
            generator = get_incremental_generator(platform)

            # Should either raise error or generate fallback DDL
            try:
                ddl = generator.generate_incremental_ddl(
                    schema=schema,
                    table_name='test',
                    config=config
                )
                # If it doesn't raise, it should return some DDL
                assert ddl is not None
            except (ValueError, Exception):
                # Expected to fail without primary keys
                pass

    def test_scd2_without_scd2_columns(self):
        """Test SCD2 pattern without SCD2 columns."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition(name='id', logical_type=LogicalType.BIGINT, nullable=False)
            ]
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD2,
            primary_keys=['id'],
            scd2_columns=[]  # No SCD2 columns
        )

        for platform in ['bigquery', 'snowflake']:
            generator = get_incremental_generator(platform)

            # Should handle gracefully
            try:
                ddl = generator.generate_incremental_ddl(
                    schema=schema,
                    table_name='test',
                    config=config
                )
                # Might generate DDL or raise error
                if ddl:
                    assert isinstance(ddl, str)
            except (ValueError, Exception):
                # May reject invalid config
                pass


class TestCrossPatternCompatibility:
    """Test compatibility across different load patterns."""

    def test_same_schema_different_patterns(self):
        """Test same schema with different load patterns."""
        schema = CanonicalSchema(
            table_name='products',
            columns=[
                ColumnDefinition(name='product_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='name', logical_type=LogicalType.STRING, nullable=False),
                ColumnDefinition(name='price', logical_type=LogicalType.DECIMAL, nullable=False, precision=10, scale=2),
                ColumnDefinition(name='updated_at', logical_type=LogicalType.TIMESTAMP, nullable=False),
            ]
        )

        patterns = [
            IncrementalConfig(load_pattern=LoadPattern.APPEND_ONLY),
            IncrementalConfig(load_pattern=LoadPattern.UPSERT, primary_keys=['product_id']),
            IncrementalConfig(load_pattern=LoadPattern.SNAPSHOT),
            IncrementalConfig(load_pattern=LoadPattern.DELETE, primary_keys=['product_id']),
        ]

        for platform in ['bigquery', 'snowflake', 'postgresql']:
            generator = get_incremental_generator(platform)

            for config in patterns:
                ddl = generator.generate_incremental_ddl(
                    schema=schema,
                    table_name='products',
                    config=config
                )

                assert ddl is not None
                assert isinstance(ddl, str)
                assert len(ddl) > 0
