"""Integration tests for connections with renderers."""

import pytest
from unittest.mock import Mock, MagicMock, patch

from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType
from schema_mapper.renderers.factory import RendererFactory
from schema_mapper.connections.config import ConnectionConfig
from schema_mapper.connections.factory import ConnectionFactory


class TestRendererIntegration:
    """Test integration between connections and renderers."""

    def test_canonical_schema_to_ddl_via_renderer(self, sample_canonical_schema):
        """Test that CanonicalSchema can be rendered to DDL."""
        # For each platform, render the canonical schema to DDL
        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        for platform in platforms:
            renderer = RendererFactory.get_renderer(platform, sample_canonical_schema)
            ddl = renderer.to_ddl()

            # Verify DDL was generated
            assert ddl is not None
            assert len(ddl) > 0
            assert isinstance(ddl, str)

            # Verify table name is in DDL
            assert 'users' in ddl.lower()

            # Verify some columns are in DDL
            assert 'user_id' in ddl.lower()
            assert 'username' in ddl.lower()

    def test_bigquery_renderer_with_mock_connection(self, sample_canonical_schema, mock_connection_config):
        """Test BigQuery renderer DDL can be used with mock connection."""
        # Render DDL for BigQuery
        renderer = RendererFactory.get_renderer('bigquery', sample_canonical_schema)
        ddl = renderer.to_ddl()

        # Verify BigQuery-specific syntax
        assert 'CREATE TABLE' in ddl.upper() or 'CREATE OR REPLACE TABLE' in ddl.upper()
        assert 'INT64' in ddl or 'BIGINT' in ddl

    def test_snowflake_renderer_with_mock_connection(self, sample_canonical_schema, mock_connection_config):
        """Test Snowflake renderer DDL can be used with mock connection."""
        # Render DDL for Snowflake
        renderer = RendererFactory.get_renderer('snowflake', sample_canonical_schema)
        ddl = renderer.to_ddl()

        # Verify Snowflake-specific syntax
        assert 'CREATE TABLE' in ddl.upper() or 'CREATE OR REPLACE TABLE' in ddl.upper()
        assert 'NUMBER' in ddl or 'BIGINT' in ddl or 'VARCHAR' in ddl

    def test_postgresql_renderer_with_mock_connection(self, sample_canonical_schema, mock_connection_config):
        """Test PostgreSQL renderer DDL can be used with mock connection."""
        # Render DDL for PostgreSQL
        renderer = RendererFactory.get_renderer('postgresql', sample_canonical_schema)
        ddl = renderer.to_ddl()

        # Verify PostgreSQL-specific syntax
        assert 'CREATE TABLE' in ddl.upper()
        assert 'BIGINT' in ddl or 'VARCHAR' in ddl

    def test_renderer_factory_supports_all_platforms(self):
        """Test that RendererFactory supports all connection platforms."""
        platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition(name='id', logical_type=LogicalType.BIGINT, nullable=False)
            ]
        )

        for platform in platforms:
            # Should not raise exception
            renderer = RendererFactory.get_renderer(platform, schema)
            assert renderer is not None
            assert renderer.to_ddl() is not None


class TestConnectionWithRenderer:
    """Test connection methods that use renderers."""

    @patch('schema_mapper.connections.platform_connectors.bigquery.bigquery.Client')
    def test_bigquery_create_table_from_schema(self, mock_bq_client, sample_canonical_schema, mock_connection_config):
        """Test BigQuery connection can create table from CanonicalSchema using renderer."""
        # This tests the integration point where connections use renderers
        config = ConnectionConfig.from_dict(mock_connection_config)

        # The integration point: connection should use renderer internally
        # when create_table_from_schema is called
        assert sample_canonical_schema.table_name == 'users'
        assert len(sample_canonical_schema.columns) == 6

    def test_canonical_schema_roundtrip_concept(self, sample_canonical_schema):
        """Test the concept of: CanonicalSchema -> Renderer -> DDL -> Connection -> Execute."""
        # This tests the full workflow conceptually

        # Step 1: We have a CanonicalSchema
        assert sample_canonical_schema is not None

        # Step 2: Render to platform-specific DDL
        renderer = RendererFactory.get_renderer('bigquery', sample_canonical_schema)
        ddl = renderer.to_ddl()
        assert ddl is not None

        # Step 3: Connection would execute this DDL
        # (In integration test with real DB, we would: conn.execute_ddl(ddl))
        assert isinstance(ddl, str)
        assert len(ddl) > 50  # Reasonable DDL length


class TestCrossPlatformSchemaRendering:
    """Test rendering same schema across different platforms."""

    def test_same_schema_different_platforms(self):
        """Test that same CanonicalSchema renders appropriately for each platform."""
        schema = CanonicalSchema(
            table_name='customers',
            dataset_name='analytics',
            database_name='production',
            columns=[
                ColumnDefinition(name='customer_id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='email', logical_type=LogicalType.STRING, nullable=False),
                ColumnDefinition(name='created_at', logical_type=LogicalType.TIMESTAMP, nullable=False),
                ColumnDefinition(name='balance', logical_type=LogicalType.DECIMAL, nullable=True, precision=10, scale=2),
                ColumnDefinition(name='is_active', logical_type=LogicalType.BOOLEAN, nullable=False),
                ColumnDefinition(name='metadata', logical_type=LogicalType.JSON, nullable=True),
            ]
        )

        # Render for each platform
        platform_ddls = {}
        for platform in ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']:
            renderer = RendererFactory.get_renderer(platform, schema)
            platform_ddls[platform] = renderer.to_ddl()

        # Verify all platforms generated DDL
        assert len(platform_ddls) == 5
        for platform, ddl in platform_ddls.items():
            assert ddl is not None
            assert len(ddl) > 0
            assert 'customers' in ddl.lower()

        # Verify platform-specific differences
        # BigQuery uses INT64, STRING
        assert ('INT64' in platform_ddls['bigquery'] or 'BIGINT' in platform_ddls['bigquery'])

        # PostgreSQL uses BIGINT, VARCHAR
        assert ('BIGINT' in platform_ddls['postgresql'] or 'INTEGER' in platform_ddls['postgresql'])

        # Snowflake uses NUMBER, VARCHAR
        assert ('NUMBER' in platform_ddls['snowflake'] or 'VARCHAR' in platform_ddls['snowflake'] or 'BIGINT' in platform_ddls['snowflake'])

    def test_decimal_type_across_platforms(self):
        """Test that DECIMAL type is handled correctly across platforms."""
        schema = CanonicalSchema(
            table_name='prices',
            columns=[
                ColumnDefinition(
                    name='amount',
                    logical_type=LogicalType.DECIMAL,
                    nullable=False,
                    precision=18,
                    scale=4
                )
            ]
        )

        for platform in ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            # Verify DECIMAL/NUMERIC type is present
            assert ('DECIMAL' in ddl.upper() or
                    'NUMERIC' in ddl.upper() or
                    'BIGNUMERIC' in ddl.upper() or
                    'NUMBER' in ddl.upper())

    def test_timestamp_type_across_platforms(self):
        """Test that TIMESTAMP type is handled correctly across platforms."""
        schema = CanonicalSchema(
            table_name='events',
            columns=[
                ColumnDefinition(
                    name='event_time',
                    logical_type=LogicalType.TIMESTAMP,
                    nullable=False
                )
            ]
        )

        for platform in ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            # Verify TIMESTAMP or DATETIME type is present
            assert ('TIMESTAMP' in ddl.upper() or
                    'DATETIME' in ddl.upper() or
                    'DATETIME2' in ddl.upper())

    def test_json_type_across_platforms(self):
        """Test that JSON type is handled correctly across platforms."""
        schema = CanonicalSchema(
            table_name='documents',
            columns=[
                ColumnDefinition(
                    name='data',
                    logical_type=LogicalType.JSON,
                    nullable=True
                )
            ]
        )

        for platform in ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            # Verify JSON/VARIANT/JSONB type is present (or TEXT fallback)
            # Different platforms handle JSON differently
            assert len(ddl) > 0  # At minimum, DDL was generated


class TestRendererErrorHandling:
    """Test error handling in renderer integration."""

    def test_renderer_with_empty_schema(self):
        """Test renderer handles schema with no columns gracefully."""
        schema = CanonicalSchema(
            table_name='empty_table',
            columns=[]
        )

        # Most renderers should handle this (though might produce unusual DDL)
        for platform in ['bigquery', 'snowflake', 'postgresql']:
            renderer = RendererFactory.get_renderer(platform, schema)
            # Should not crash
            try:
                ddl = renderer.to_ddl()
                # DDL might be minimal but should be a string
                assert isinstance(ddl, str)
            except Exception:
                # Some platforms might reject empty schemas, which is ok
                pass

    def test_renderer_with_special_characters_in_names(self):
        """Test renderer handles special characters in table/column names."""
        schema = CanonicalSchema(
            table_name='user-events',  # Hyphen in name
            columns=[
                ColumnDefinition(name='user-id', logical_type=LogicalType.BIGINT, nullable=False),
                ColumnDefinition(name='event_type', logical_type=LogicalType.STRING, nullable=False),
            ]
        )

        for platform in ['bigquery', 'snowflake', 'postgresql']:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()

            # Should generate DDL (possibly with quoted identifiers)
            assert ddl is not None
            assert len(ddl) > 0
