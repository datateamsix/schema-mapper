"""
Examples demonstrating DDL generation with clustering, partitioning, and distribution.

This script shows how to use the enhanced DDL generators with various
platform-specific optimization features.
"""

from schema_mapper.ddl_mappings import (
    DDLOptions,
    ClusteringConfig,
    PartitionConfig,
    DistributionConfig,
    SortKeyConfig,
    PartitionType,
    DistributionStyle,
)
from schema_mapper.generators_enhanced import get_enhanced_ddl_generator


# Sample schema (from CSV analysis)
SAMPLE_SCHEMA = [
    {'name': 'event_id', 'type': 'INT64', 'mode': 'REQUIRED'},
    {'name': 'user_id', 'type': 'INT64', 'mode': 'REQUIRED'},
    {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'event_ts', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'revenue', 'type': 'FLOAT', 'mode': 'NULLABLE'},
]


def example_bigquery_partitioned_clustered():
    """BigQuery: Partitioned by date, clustered by user_id and event_type."""
    print("=" * 80)
    print("BigQuery - Partitioned + Clustered Table")
    print("=" * 80)

    generator = get_enhanced_ddl_generator('bigquery')

    options = DDLOptions(
        partitioning=PartitionConfig(
            column='event_date',
            partition_type=PartitionType.TIME,
            expiration_days=365,
            require_partition_filter=True
        ),
        clustering=ClusteringConfig(
            columns=['user_id', 'event_type']
        )
    )

    ddl = generator.generate(
        schema=SAMPLE_SCHEMA,
        table_name='events',
        dataset_name='analytics',
        project_id='my-project',
        ddl_options=options
    )

    print(ddl)
    print("\n")


def example_bigquery_timestamp_partition():
    """BigQuery: Partitioned by timestamp (converted to DATE)."""
    print("=" * 80)
    print("BigQuery - Timestamp Partitioning")
    print("=" * 80)

    generator = get_enhanced_ddl_generator('bigquery')

    options = DDLOptions(
        partitioning=PartitionConfig(
            column='event_ts',  # TIMESTAMP column
            partition_type=PartitionType.TIME
        ),
        clustering=ClusteringConfig(
            columns=['event_type', 'user_id']
        )
    )

    ddl = generator.generate(
        schema=SAMPLE_SCHEMA,
        table_name='events_by_timestamp',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)
    print("\n")


def example_snowflake_clustered():
    """Snowflake: Clustered table (auto micro-partitioning)."""
    print("=" * 80)
    print("Snowflake - Clustered Table")
    print("=" * 80)

    # Convert schema to Snowflake types
    snowflake_schema = [
        {'name': 'event_id', 'type': 'NUMBER(38,0)', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'NUMBER(38,0)', 'mode': 'REQUIRED'},
        {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'event_ts', 'type': 'TIMESTAMP_NTZ', 'mode': 'REQUIRED'},
        {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'revenue', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ]

    generator = get_enhanced_ddl_generator('snowflake')

    options = DDLOptions(
        clustering=ClusteringConfig(
            columns=['event_date', 'user_id']
        ),
        transient=False  # Set to True for temporary analytics tables
    )

    ddl = generator.generate(
        schema=snowflake_schema,
        table_name='events',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)
    print("\n")


def example_snowflake_transient():
    """Snowflake: Transient table for temporary analytics."""
    print("=" * 80)
    print("Snowflake - Transient Table")
    print("=" * 80)

    snowflake_schema = [
        {'name': 'event_id', 'type': 'NUMBER(38,0)', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'NUMBER(38,0)', 'mode': 'REQUIRED'},
        {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'metric_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    ]

    generator = get_enhanced_ddl_generator('snowflake')

    options = DDLOptions(
        transient=True,  # Don't need Time Travel for staging tables
        clustering=ClusteringConfig(
            columns=['event_date']
        )
    )

    ddl = generator.generate(
        schema=snowflake_schema,
        table_name='staging_events',
        dataset_name='staging',
        ddl_options=options
    )

    print(ddl)
    print("\n")


def example_redshift_distributed_sorted():
    """Redshift: Distributed by user_id, sorted by event_date."""
    print("=" * 80)
    print("Redshift - Distributed + Sorted Table")
    print("=" * 80)

    # Convert schema to Redshift types
    redshift_schema = [
        {'name': 'event_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'event_type', 'type': 'VARCHAR(50)', 'mode': 'NULLABLE'},
        {'name': 'event_ts', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'revenue', 'type': 'DOUBLE PRECISION', 'mode': 'NULLABLE'},
    ]

    generator = get_enhanced_ddl_generator('redshift')

    options = DDLOptions(
        distribution=DistributionConfig(
            style=DistributionStyle.KEY,
            key_column='user_id'
        ),
        sort_keys=SortKeyConfig(
            columns=['event_date', 'event_ts'],
            compound=True  # Use COMPOUND SORTKEY (default)
        )
    )

    ddl = generator.generate(
        schema=redshift_schema,
        table_name='events',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)
    print("\n")


def example_redshift_diststyle_all():
    """Redshift: Small dimension table with DISTSTYLE ALL."""
    print("=" * 80)
    print("Redshift - Dimension Table (DISTSTYLE ALL)")
    print("=" * 80)

    dimension_schema = [
        {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'username', 'type': 'VARCHAR(255)', 'mode': 'REQUIRED'},
        {'name': 'country', 'type': 'VARCHAR(50)', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    ]

    generator = get_enhanced_ddl_generator('redshift')

    options = DDLOptions(
        distribution=DistributionConfig(
            style=DistributionStyle.ALL  # Replicate to all nodes (small tables)
        ),
        sort_keys=SortKeyConfig(
            columns=['user_id'],
            compound=True
        )
    )

    ddl = generator.generate(
        schema=dimension_schema,
        table_name='dim_users',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)
    print("\n")


def example_postgresql_range_partitioned():
    """PostgreSQL: Range partitioned by event_date with index."""
    print("=" * 80)
    print("PostgreSQL - Range Partitioned Table")
    print("=" * 80)

    # Convert schema to PostgreSQL types
    postgres_schema = [
        {'name': 'event_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'event_type', 'type': 'TEXT', 'mode': 'NULLABLE'},
        {'name': 'event_ts', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'revenue', 'type': 'DOUBLE PRECISION', 'mode': 'NULLABLE'},
    ]

    generator = get_enhanced_ddl_generator('postgresql')

    options = DDLOptions(
        partitioning=PartitionConfig(
            column='event_date',
            partition_type=PartitionType.RANGE
        ),
        clustering=ClusteringConfig(
            columns=['event_date', 'user_id']  # Creates index for clustering
        )
    )

    ddl = generator.generate(
        schema=postgres_schema,
        table_name='events',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)
    print("\n")
    print("NOTE: You'll need to create child partitions separately:")
    print("""
CREATE TABLE analytics.events_2024_q1
PARTITION OF analytics.events
FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE analytics.events_2024_q2
PARTITION OF analytics.events
FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
    """)
    print("\n")


def example_postgresql_hash_partitioned():
    """PostgreSQL: Hash partitioned for even distribution."""
    print("=" * 80)
    print("PostgreSQL - Hash Partitioned Table")
    print("=" * 80)

    postgres_schema = [
        {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
        {'name': 'session_id', 'type': 'TEXT', 'mode': 'REQUIRED'},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'data', 'type': 'JSONB', 'mode': 'NULLABLE'},
    ]

    generator = get_enhanced_ddl_generator('postgresql')

    options = DDLOptions(
        partitioning=PartitionConfig(
            column='user_id',
            partition_type=PartitionType.HASH
        )
    )

    ddl = generator.generate(
        schema=postgres_schema,
        table_name='user_sessions',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)
    print("\n")
    print("NOTE: Create hash partitions like:")
    print("""
CREATE TABLE analytics.user_sessions_p0
PARTITION OF analytics.user_sessions
FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE analytics.user_sessions_p1
PARTITION OF analytics.user_sessions
FOR VALUES WITH (MODULUS 4, REMAINDER 1);
    """)
    print("\n")


def example_comparison_all_platforms():
    """Generate the same table DDL across all platforms for comparison."""
    print("=" * 80)
    print("CROSS-PLATFORM COMPARISON")
    print("Same logical table, optimized for each platform")
    print("=" * 80)
    print("\n")

    platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

    for platform in platforms:
        print(f"\n{'=' * 60}")
        print(f"{platform.upper()}")
        print('=' * 60)

        generator = get_enhanced_ddl_generator(platform)

        # Platform-specific schema
        if platform == 'bigquery':
            schema = SAMPLE_SCHEMA
            options = DDLOptions(
                partitioning=PartitionConfig(
                    column='event_date',
                    partition_type=PartitionType.TIME
                ),
                clustering=ClusteringConfig(columns=['user_id', 'event_type'])
            )
        elif platform == 'snowflake':
            schema = [
                {'name': 'event_id', 'type': 'NUMBER(38,0)', 'mode': 'REQUIRED'},
                {'name': 'user_id', 'type': 'NUMBER(38,0)', 'mode': 'REQUIRED'},
                {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'event_ts', 'type': 'TIMESTAMP_NTZ', 'mode': 'REQUIRED'},
                {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
            ]
            options = DDLOptions(
                clustering=ClusteringConfig(columns=['event_date', 'user_id'])
            )
        elif platform == 'redshift':
            schema = [
                {'name': 'event_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
                {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
                {'name': 'event_type', 'type': 'VARCHAR(50)', 'mode': 'NULLABLE'},
                {'name': 'event_ts', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
            ]
            options = DDLOptions(
                distribution=DistributionConfig(
                    style=DistributionStyle.KEY,
                    key_column='user_id'
                ),
                sort_keys=SortKeyConfig(columns=['event_date', 'event_ts'])
            )
        else:  # postgresql
            schema = [
                {'name': 'event_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
                {'name': 'user_id', 'type': 'BIGINT', 'mode': 'REQUIRED'},
                {'name': 'event_type', 'type': 'TEXT', 'mode': 'NULLABLE'},
                {'name': 'event_ts', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
            ]
            options = DDLOptions(
                partitioning=PartitionConfig(
                    column='event_date',
                    partition_type=PartitionType.RANGE
                ),
                clustering=ClusteringConfig(columns=['event_date', 'user_id'])
            )

        ddl = generator.generate(
            schema=schema,
            table_name='events',
            dataset_name='analytics',
            ddl_options=options
        )

        print(ddl)
        print("\n")


if __name__ == '__main__':
    # Run all examples
    example_bigquery_partitioned_clustered()
    example_bigquery_timestamp_partition()
    example_snowflake_clustered()
    example_snowflake_transient()
    example_redshift_distributed_sorted()
    example_redshift_diststyle_all()
    example_postgresql_range_partitioned()
    example_postgresql_hash_partitioned()

    # Cross-platform comparison
    example_comparison_all_platforms()
