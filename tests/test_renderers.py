"""Tests for renderer modules."""

import pytest
import json
from schema_mapper.canonical import (
    CanonicalSchema,
    ColumnDefinition,
    LogicalType,
    OptimizationHints
)
from schema_mapper.renderers import (
    RendererFactory,
    BigQueryRenderer,
    SnowflakeRenderer,
    RedshiftRenderer,
    PostgreSQLRenderer
)


@pytest.fixture
def basic_schema():
    """Create a basic test schema."""
    return CanonicalSchema(
        table_name='events',
        dataset_name='analytics',
        columns=[
            ColumnDefinition('event_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('user_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('event_type', LogicalType.STRING),
            ColumnDefinition('event_date', LogicalType.DATE, nullable=False),
            ColumnDefinition('revenue', LogicalType.FLOAT)
        ]
    )


@pytest.fixture
def optimized_schema():
    """Create an optimized test schema with both partitioning and clustering (BigQuery)."""
    return CanonicalSchema(
        table_name='events',
        dataset_name='analytics',
        project_id='my-project',
        columns=[
            ColumnDefinition('event_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('user_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('event_type', LogicalType.STRING),
            ColumnDefinition('event_date', LogicalType.DATE, nullable=False)
        ],
        optimization=OptimizationHints(
            partition_columns=['event_date'],
            cluster_columns=['user_id', 'event_type']
        )
    )


@pytest.fixture
def clustered_schema():
    """Create a schema with clustering only (Snowflake)."""
    return CanonicalSchema(
        table_name='events',
        dataset_name='analytics',
        columns=[
            ColumnDefinition('event_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('user_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('event_type', LogicalType.STRING),
            ColumnDefinition('event_date', LogicalType.DATE, nullable=False)
        ],
        optimization=OptimizationHints(
            cluster_columns=['user_id', 'event_type']
        )
    )


@pytest.fixture
def partitioned_schema():
    """Create a schema with partitioning only (PostgreSQL)."""
    return CanonicalSchema(
        table_name='events',
        dataset_name='analytics',
        columns=[
            ColumnDefinition('event_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('user_id', LogicalType.BIGINT, nullable=False),
            ColumnDefinition('event_type', LogicalType.STRING),
            ColumnDefinition('event_date', LogicalType.DATE, nullable=False)
        ],
        optimization=OptimizationHints(
            partition_columns=['event_date']
        )
    )


class TestRendererFactory:
    """Test RendererFactory."""

    def test_get_bigquery_renderer(self, basic_schema):
        """Test getting BigQuery renderer."""
        renderer = RendererFactory.get_renderer('bigquery', basic_schema)
        assert isinstance(renderer, BigQueryRenderer)

    def test_get_snowflake_renderer(self, basic_schema):
        """Test getting Snowflake renderer."""
        renderer = RendererFactory.get_renderer('snowflake', basic_schema)
        assert isinstance(renderer, SnowflakeRenderer)

    def test_get_redshift_renderer(self, basic_schema):
        """Test getting Redshift renderer."""
        renderer = RendererFactory.get_renderer('redshift', basic_schema)
        assert isinstance(renderer, RedshiftRenderer)

    def test_get_postgresql_renderer(self, basic_schema):
        """Test getting PostgreSQL renderer."""
        renderer = RendererFactory.get_renderer('postgresql', basic_schema)
        assert isinstance(renderer, PostgreSQLRenderer)

    def test_invalid_platform(self, basic_schema):
        """Test error with invalid platform."""
        with pytest.raises(ValueError):
            RendererFactory.get_renderer('invalid', basic_schema)

    def test_supported_platforms(self):
        """Test supported platforms list."""
        platforms = RendererFactory.supported_platforms()
        assert 'bigquery' in platforms
        assert 'snowflake' in platforms
        assert 'redshift' in platforms
        assert 'postgresql' in platforms

    def test_supports_json_schema(self):
        """Test JSON schema support check."""
        assert RendererFactory.supports_json_schema('bigquery') == True
        assert RendererFactory.supports_json_schema('snowflake') == False
        assert RendererFactory.supports_json_schema('redshift') == False


class TestBigQueryRenderer:
    """Test BigQueryRenderer."""

    def test_platform_name(self, basic_schema):
        """Test platform name."""
        renderer = BigQueryRenderer(basic_schema)
        assert renderer.platform_name() == 'bigquery'

    def test_supports_json_schema(self, basic_schema):
        """Test JSON schema support."""
        renderer = BigQueryRenderer(basic_schema)
        assert renderer.supports_json_schema() == True

    def test_to_physical_types(self, basic_schema):
        """Test logical to physical type conversion."""
        renderer = BigQueryRenderer(basic_schema)
        types = renderer.to_physical_types()

        assert types['event_id'] == 'INT64'
        assert types['user_id'] == 'INT64'
        assert types['event_type'] == 'STRING'
        assert types['event_date'] == 'DATE'
        assert types['revenue'] == 'FLOAT64'

    def test_to_ddl_basic(self, basic_schema):
        """Test basic DDL generation."""
        renderer = BigQueryRenderer(basic_schema)
        ddl = renderer.to_ddl()

        assert 'CREATE TABLE' in ddl
        assert 'analytics.events' in ddl
        assert 'event_id INT64 NOT NULL' in ddl
        assert 'event_type STRING' in ddl

    def test_to_ddl_with_optimization(self, optimized_schema):
        """Test DDL with partitioning and clustering."""
        renderer = BigQueryRenderer(optimized_schema)
        ddl = renderer.to_ddl()

        assert 'CREATE TABLE' in ddl
        assert 'PARTITION BY' in ddl
        assert 'CLUSTER BY' in ddl
        assert 'event_date' in ddl
        assert 'user_id' in ddl

    def test_to_schema_json(self, basic_schema):
        """Test JSON schema generation."""
        renderer = BigQueryRenderer(basic_schema)
        json_str = renderer.to_schema_json()

        schema = json.loads(json_str)
        assert isinstance(schema, list)
        assert len(schema) == 5
        assert schema[0]['name'] == 'event_id'
        assert schema[0]['type'] == 'INT64'
        assert schema[0]['mode'] == 'REQUIRED'

    def test_validate_too_many_cluster_columns(self):
        """Test validation fails with too many cluster columns."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition('a', LogicalType.INTEGER),
                ColumnDefinition('b', LogicalType.INTEGER),
                ColumnDefinition('c', LogicalType.INTEGER),
                ColumnDefinition('d', LogicalType.INTEGER),
                ColumnDefinition('e', LogicalType.INTEGER)
            ],
            optimization=OptimizationHints(
                cluster_columns=['a', 'b', 'c', 'd', 'e']  # Too many (max 4)
            )
        )

        with pytest.raises(ValueError) as exc_info:
            BigQueryRenderer(schema)
        assert 'max 4' in str(exc_info.value).lower()

    def test_to_cli_create(self, optimized_schema):
        """Test CLI create command generation."""
        renderer = BigQueryRenderer(optimized_schema)
        cmd = renderer.to_cli_create()

        assert 'bq' in cmd.lower()
        assert 'query' in cmd.lower() or 'mk' in cmd.lower()

    def test_to_cli_load(self, basic_schema):
        """Test CLI load command generation."""
        renderer = BigQueryRenderer(basic_schema)
        cmd = renderer.to_cli_load('data.csv')

        assert 'bq load' in cmd
        assert 'data.csv' in cmd


class TestSnowflakeRenderer:
    """Test SnowflakeRenderer."""

    def test_platform_name(self, basic_schema):
        """Test platform name."""
        renderer = SnowflakeRenderer(basic_schema)
        assert renderer.platform_name() == 'snowflake'

    def test_no_json_schema_support(self, basic_schema):
        """Test Snowflake doesn't support JSON schemas."""
        renderer = SnowflakeRenderer(basic_schema)
        assert renderer.supports_json_schema() == False
        assert renderer.to_schema_json() is None

    def test_to_physical_types(self, basic_schema):
        """Test type conversion."""
        renderer = SnowflakeRenderer(basic_schema)
        types = renderer.to_physical_types()

        assert types['event_id'] == 'NUMBER(38,0)'
        assert types['event_type'] == 'VARCHAR(16777216)'
        assert types['revenue'] == 'FLOAT'

    def test_to_ddl_basic(self, basic_schema):
        """Test basic DDL generation."""
        renderer = SnowflakeRenderer(basic_schema)
        ddl = renderer.to_ddl()

        assert 'CREATE TABLE' in ddl
        assert 'analytics.events' in ddl
        assert 'NUMBER(38,0)' in ddl

    def test_to_ddl_with_clustering(self, clustered_schema):
        """Test DDL with clustering."""
        renderer = SnowflakeRenderer(clustered_schema)
        ddl = renderer.to_ddl()

        assert 'CLUSTER BY' in ddl
        assert 'user_id' in ddl

    def test_validate_no_partitioning(self):
        """Test validation fails with partitioning (not supported)."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[ColumnDefinition('id', LogicalType.INTEGER)],
            optimization=OptimizationHints(partition_columns=['id'])
        )

        with pytest.raises(ValueError) as exc_info:
            SnowflakeRenderer(schema)
        assert 'partition' in str(exc_info.value).lower()


class TestRedshiftRenderer:
    """Test RedshiftRenderer."""

    def test_platform_name(self, basic_schema):
        """Test platform name."""
        renderer = RedshiftRenderer(basic_schema)
        assert renderer.platform_name() == 'redshift'

    def test_to_physical_types(self, basic_schema):
        """Test type conversion."""
        renderer = RedshiftRenderer(basic_schema)
        types = renderer.to_physical_types()

        assert types['event_id'] == 'BIGINT'
        assert types['event_type'] == 'VARCHAR(256)'
        assert types['revenue'] == 'DOUBLE PRECISION'

    def test_to_ddl_basic(self, basic_schema):
        """Test basic DDL generation."""
        renderer = RedshiftRenderer(basic_schema)
        ddl = renderer.to_ddl()

        assert 'CREATE TABLE' in ddl
        assert 'analytics.events' in ddl
        assert 'BIGINT' in ddl

    def test_to_ddl_with_distribution_and_sort(self):
        """Test DDL with distribution and sort keys."""
        schema = CanonicalSchema(
            table_name='events',
            dataset_name='analytics',
            columns=[
                ColumnDefinition('user_id', LogicalType.BIGINT),
                ColumnDefinition('event_date', LogicalType.DATE)
            ],
            optimization=OptimizationHints(
                distribution_column='user_id',
                sort_columns=['event_date']
            )
        )

        renderer = RedshiftRenderer(schema)
        ddl = renderer.to_ddl()

        assert 'DISTSTYLE KEY' in ddl
        assert 'DISTKEY' in ddl
        assert 'SORTKEY' in ddl
        assert 'user_id' in ddl
        assert 'event_date' in ddl

    def test_validate_no_clustering(self):
        """Test validation fails with clustering (not supported)."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[ColumnDefinition('id', LogicalType.INTEGER)],
            optimization=OptimizationHints(cluster_columns=['id'])
        )

        with pytest.raises(ValueError) as exc_info:
            RedshiftRenderer(schema)
        assert 'clustering' in str(exc_info.value).lower() or 'sort' in str(exc_info.value).lower()


class TestPostgreSQLRenderer:
    """Test PostgreSQLRenderer."""

    def test_platform_name(self, basic_schema):
        """Test platform name."""
        renderer = PostgreSQLRenderer(basic_schema)
        assert renderer.platform_name() == 'postgresql'

    def test_to_physical_types(self, basic_schema):
        """Test type conversion."""
        renderer = PostgreSQLRenderer(basic_schema)
        types = renderer.to_physical_types()

        assert types['event_id'] == 'BIGINT'
        assert types['event_type'] == 'VARCHAR(255)'
        assert types['revenue'] == 'DOUBLE PRECISION'

    def test_to_ddl_basic(self, basic_schema):
        """Test basic DDL generation."""
        renderer = PostgreSQLRenderer(basic_schema)
        ddl = renderer.to_ddl()

        assert 'CREATE TABLE' in ddl
        assert 'events' in ddl
        assert 'BIGINT' in ddl

    def test_to_ddl_with_partitioning(self, partitioned_schema):
        """Test DDL with partitioning."""
        renderer = PostgreSQLRenderer(partitioned_schema)
        ddl = renderer.to_ddl()

        assert 'PARTITION BY' in ddl
        assert 'event_date' in ddl

    def test_to_cli_create(self, basic_schema):
        """Test CLI create command generation."""
        renderer = PostgreSQLRenderer(basic_schema)
        cmd = renderer.to_cli_create()

        assert 'psql' in cmd


class TestMultiPlatformRendering:
    """Test rendering same schema across all platforms."""

    def test_all_platforms_generate_ddl(self, basic_schema):
        """Test that all platforms can generate DDL."""
        platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

        for platform in platforms:
            renderer = RendererFactory.get_renderer(platform, basic_schema)
            ddl = renderer.to_ddl()

            assert 'CREATE TABLE' in ddl
            assert 'events' in ddl
            assert len(ddl) > 0

    def test_all_platforms_physical_types(self, basic_schema):
        """Test physical type mappings for all platforms."""
        platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

        for platform in platforms:
            renderer = RendererFactory.get_renderer(platform, basic_schema)
            types = renderer.to_physical_types()

            assert len(types) == 5
            assert 'event_id' in types
            assert 'user_id' in types

    def test_all_platforms_cli_commands(self, basic_schema):
        """Test CLI command generation for all platforms."""
        platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

        for platform in platforms:
            renderer = RendererFactory.get_renderer(platform, basic_schema)

            create_cmd = renderer.to_cli_create()
            assert len(create_cmd) > 0

            load_cmd = renderer.to_cli_load('data.csv')
            assert len(load_cmd) > 0
            assert 'data.csv' in load_cmd
