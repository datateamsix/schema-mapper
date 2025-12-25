"""End-to-end integration tests for the complete workflow."""

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch

from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType, infer_canonical_schema
from schema_mapper.renderers.factory import RendererFactory
from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator
from schema_mapper.connections.config import ConnectionConfig
from schema_mapper.connections.factory import ConnectionFactory


class TestDataFrameToConnection:
    """Test complete workflow: DataFrame -> Schema -> Renderer -> DDL -> (Connection)."""

    def test_dataframe_to_canonical_to_ddl(self, sample_dataframe):
        """Test: DataFrame -> CanonicalSchema -> DDL."""
        # Step 1: Infer schema from DataFrame
        schema = infer_canonical_schema(
            sample_dataframe,
            table_name='employees',
            dataset_name='hr',
            project_id='company'
        )

        assert schema is not None
        assert schema.table_name == 'employees'
        assert len(schema.columns) == 5

        # Step 2: Render to DDL for each platform
        for platform in ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            assert ddl is not None
            assert 'employees' in ddl.lower()
            assert 'id' in ddl.lower()
            assert 'name' in ddl.lower()

    def test_dataframe_to_incremental_ddl(self, sample_dataframe):
        """Test: DataFrame -> CanonicalSchema -> Incremental DDL."""
        # Step 1: Infer schema
        schema = infer_canonical_schema(
            sample_dataframe,
            table_name='employees'
        )

        # Step 2: Configure incremental load
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id']
        )

        # Step 3: Generate incremental DDL
        for platform in ['bigquery', 'snowflake', 'postgresql']:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='employees',
                config=config
            )

            assert ddl is not None
            assert isinstance(ddl, str)

    def test_csv_to_connection_workflow_concept(self, sample_dataframe):
        """Test conceptual workflow: CSV -> DataFrame -> Schema -> DDL -> Connection."""
        # Simulate CSV to DataFrame (already have DataFrame)
        df = sample_dataframe

        # Infer schema
        schema = infer_canonical_schema(
            df,
            table_name='sales',
            partition_columns=['id']
        )

        # Render to BigQuery DDL
        renderer = RendererFactory.get_renderer('bigquery', schema)
        ddl = renderer.to_ddl()

        # In real usage with connection:
        # config = ConnectionConfig('connections.yaml')
        # with ConnectionFactory.get_connection('bigquery', config) as conn:
        #     conn.execute_ddl(ddl)
        #     conn.load_dataframe(df, 'sales')

        assert ddl is not None


class TestCrossPlatformMigration:
    """Test cross-platform schema migration workflows."""

    def test_schema_migration_concept(self):
        """Test conceptual workflow: Introspect Platform A -> Migrate to Platform B."""
        # Simulate introspecting from Snowflake
        snowflake_schema = CanonicalSchema(
            table_name='customers',
            dataset_name='public',
            database_name='analytics',
            columns=[
                ColumnDefinition(name='customer_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='email', logical_type=LogicalType.STRING, nullable=False),
                ColumnDefinition(name='created_at', logical_type=LogicalType.TIMESTAMP, nullable=False),
                ColumnDefinition(name='metadata', logical_type=LogicalType.JSON, nullable=True),
            ]
        )

        # Render for BigQuery
        bq_renderer = RendererFactory.get_renderer('bigquery', snowflake_schema)
        bq_ddl = bq_renderer.to_ddl()

        # Render for PostgreSQL
        pg_renderer = RendererFactory.get_renderer('postgresql', snowflake_schema)
        pg_ddl = pg_renderer.to_ddl()

        # Both should generate valid DDL
        assert bq_ddl is not None
        assert pg_ddl is not None
        assert 'customers' in bq_ddl.lower()
        assert 'customers' in pg_ddl.lower()

    def test_multi_platform_deployment(self):
        """Test deploying same schema to multiple platforms."""
        schema = CanonicalSchema(
            table_name='events',
            columns=[
                ColumnDefinition(name='event_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='user_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='event_type', logical_type=LogicalType.STRING, nullable=False),
                ColumnDefinition(name='event_time', logical_type=LogicalType.TIMESTAMP, nullable=False),
                ColumnDefinition(name='properties', logical_type=LogicalType.JSON, nullable=True),
            ]
        )

        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']
        ddls = {}

        for platform in platforms:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddls[platform] = renderer.to_ddl()

        # Verify all platforms have DDL
        assert len(ddls) == 5
        for platform, ddl in ddls.items():
            assert ddl is not None
            assert 'events' in ddl.lower()


class TestIncrementalLoadWorkflows:
    """Test complete incremental load workflows."""

    def test_daily_snapshot_workflow(self, sample_dataframe):
        """Test daily snapshot incremental load workflow."""
        # Add partition column
        df = sample_dataframe.copy()
        df['snapshot_date'] = pd.to_datetime('2024-01-01')

        schema = infer_canonical_schema(
            df,
            table_name='daily_metrics',
            partition_columns=['snapshot_date']
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.SNAPSHOT
        )

        for platform in ['bigquery', 'snowflake', 'postgresql']:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='daily_metrics',
                config=config
            )

            assert ddl is not None

    def test_upsert_with_updated_records(self):
        """Test UPSERT workflow for updating records."""
        # Original records
        original_df = pd.DataFrame({
            'user_id': [1, 2, 3],
            'username': ['alice', 'bob', 'charlie'],
            'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
            'updated_at': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01'])
        })

        # Updated records
        updated_df = pd.DataFrame({
            'user_id': [2, 3, 4],  # 2 and 3 updated, 4 is new
            'username': ['bob_updated', 'charlie', 'david'],
            'email': ['bob_new@example.com', 'charlie@example.com', 'david@example.com'],
            'updated_at': pd.to_datetime(['2024-01-02', '2024-01-01', '2024-01-02'])
        })

        schema = infer_canonical_schema(
            updated_df,
            table_name='users'
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            update_columns=['username', 'email', 'updated_at']
        )

        for platform in ['bigquery', 'snowflake', 'postgresql']:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='users',
                config=config
            )

            assert ddl is not None
            assert ('MERGE' in ddl.upper() or
                    'INSERT' in ddl.upper() or
                    'UPDATE' in ddl.upper())

    def test_scd2_dimension_tracking(self):
        """Test SCD2 workflow for dimension tracking."""
        df = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'customer_name': ['Alice Corp', 'Bob Inc', 'Charlie LLC'],
            'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
            'valid_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'valid_to': pd.to_datetime([None, None, None]),
            'is_current': [True, True, True]
        })

        schema = infer_canonical_schema(
            df,
            table_name='dim_customers'
        )

        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD2,
            primary_keys=['customer_id'],
            scd2_columns=['customer_name', 'address']
        )

        for platform in ['bigquery', 'snowflake', 'postgresql']:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='dim_customers',
                config=config
            )

            assert ddl is not None


class TestConfigurationWorkflows:
    """Test configuration-driven workflows."""

    def test_yaml_config_to_connection_concept(self, mock_connection_config):
        """Test YAML config workflow concept."""
        # Step 1: Load configuration
        config = ConnectionConfig.from_dict(mock_connection_config)

        # Step 2: Verify configuration
        assert config.get_default_target() == 'bigquery'
        assert config.has_target('snowflake')
        assert config.has_target('postgresql')

        # Step 3: Get platform-specific config
        bq_config = config.get_connection_config('bigquery')
        assert bq_config['project'] == 'test-project'

        # In real usage:
        # with ConnectionFactory.get_connection('bigquery', config) as conn:
        #     conn.test_connection()
        #     schema = conn.get_target_schema('table_name')

    def test_multi_environment_concept(self):
        """Test multi-environment deployment concept."""
        # Development config
        dev_config = {
            'target': 'postgresql',
            'connections': {
                'postgresql': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'dev_db'
                }
            }
        }

        # Production config
        prod_config = {
            'target': 'bigquery',
            'connections': {
                'bigquery': {
                    'project': 'prod-project'
                }
            }
        }

        dev = ConnectionConfig.from_dict(dev_config)
        prod = ConnectionConfig.from_dict(prod_config)

        assert dev.get_default_target() == 'postgresql'
        assert prod.get_default_target() == 'bigquery'


class TestComplexDataTypes:
    """Test workflows with complex data types."""

    def test_nested_json_schema(self):
        """Test schema with nested JSON data."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'user_data': [
                '{"name": "Alice", "age": 30}',
                '{"name": "Bob", "age": 25}',
                '{"name": "Charlie", "age": 35}'
            ]
        })

        schema = infer_canonical_schema(
            df,
            table_name='users_with_json'
        )

        # Render for platforms that support JSON
        for platform in ['bigquery', 'snowflake', 'postgresql']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            assert ddl is not None
            assert 'users_with_json' in ddl.lower()

    def test_decimal_precision_workflow(self):
        """Test schema with specific decimal precision."""
        df = pd.DataFrame({
            'product_id': [1, 2, 3],
            'price': [19.99, 29.95, 39.50],
            'tax_rate': [0.0825, 0.0725, 0.0925]
        })

        schema = infer_canonical_schema(
            df,
            table_name='products'
        )

        # Verify decimal columns can be rendered
        for platform in ['bigquery', 'snowflake', 'postgresql']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            assert ddl is not None
            assert ('DECIMAL' in ddl.upper() or
                    'NUMERIC' in ddl.upper() or
                    'NUMBER' in ddl.upper() or
                    'FLOAT' in ddl.upper())

    def test_timestamp_timezone_handling(self):
        """Test schema with timestamp timezone handling."""
        df = pd.DataFrame({
            'event_id': [1, 2, 3],
            'event_time': pd.to_datetime([
                '2024-01-01 12:00:00+00:00',
                '2024-01-01 13:30:00+00:00',
                '2024-01-01 15:45:00+00:00'
            ])
        })

        schema = infer_canonical_schema(
            df,
            table_name='events'
        )

        # Verify timestamp handling
        for platform in ['bigquery', 'snowflake', 'postgresql']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            assert ddl is not None
            assert ('TIMESTAMP' in ddl.upper() or 'DATETIME' in ddl.upper())


class TestErrorRecoveryWorkflows:
    """Test error recovery and validation workflows."""

    def test_schema_validation_before_deployment(self, sample_dataframe):
        """Test schema validation before deployment."""
        # Infer schema
        schema = infer_canonical_schema(
            sample_dataframe,
            table_name='validated_table'
        )

        # Validate schema can be rendered for all platforms
        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']
        validation_results = {}

        for platform in platforms:
            try:
                renderer = RendererFactory.get_renderer(platform, schema)
                ddl = renderer.to_ddl()
                validation_results[platform] = True if ddl else False
            except Exception as e:
                validation_results[platform] = False

        # At least some platforms should validate successfully
        assert any(validation_results.values())

    def test_fallback_strategy(self):
        """Test fallback strategy when primary platform fails."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition(name='id', logical_type=LogicalType.BIGINT, nullable=False)
            ]
        )

        # Try primary platform, fall back to secondary
        primary_platform = 'bigquery'
        fallback_platform = 'postgresql'

        try:
            renderer = RendererFactory.get_renderer(primary_platform, schema)
            ddl = renderer.to_ddl()
        except Exception:
            # Fallback to secondary platform
            renderer = RendererFactory.get_renderer(fallback_platform, schema)
            ddl = renderer.to_ddl()

        assert ddl is not None
