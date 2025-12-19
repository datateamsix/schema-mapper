"""
Production Analytics Pipeline - Complete Use Case Example

This example demonstrates a real-world scenario:
- E-commerce company processing user event data
- Multi-platform deployment (BigQuery, Snowflake, Redshift)
- Optimized tables with clustering and partitioning
- Best practices for each platform

Scenario:
--------
You have raw event logs from your web application that need to be:
1. Cleaned and standardized
2. Loaded into multiple data warehouses for different teams
3. Optimized for query performance

Data: User events (page views, clicks, purchases)
Scale: 1M+ events per day
Requirements:
- Fast queries by user_id and event_type
- Time-based analysis (partitioning by date)
- Cost-effective storage (partition expiration)
"""

import pandas as pd
from datetime import datetime, timedelta
import random

from schema_mapper import SchemaMapper, prepare_for_load
from schema_mapper.generators_enhanced import get_enhanced_ddl_generator
from schema_mapper.ddl_mappings import (
    DDLOptions,
    ClusteringConfig,
    PartitionConfig,
    DistributionConfig,
    SortKeyConfig,
    PartitionType,
    DistributionStyle,
)


def generate_sample_events(num_events=1000):
    """Generate sample event data."""
    print(f"📊 Generating {num_events:,} sample events...")

    event_types = ['page_view', 'click', 'add_to_cart', 'purchase', 'logout']
    countries = ['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU']

    # Generate realistic timestamps (last 30 days)
    base_date = datetime.now() - timedelta(days=30)

    events = []
    for i in range(num_events):
        event_date = base_date + timedelta(
            days=random.randint(0, 29),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        events.append({
            'event_id': f'evt_{i:08d}',
            'user_id': random.randint(1000, 9999),
            'session_id': f'sess_{random.randint(1, 500):05d}',
            'event_type': random.choice(event_types),
            'event_timestamp': event_date.isoformat(),
            'event_date': event_date.date().isoformat(),
            'page_url': f'/page/{random.randint(1, 100)}',
            'country': random.choice(countries),
            'device_type': random.choice(['desktop', 'mobile', 'tablet']),
            'revenue': round(random.uniform(0, 500), 2) if random.random() > 0.8 else None,
            'is_conversion': 'yes' if random.random() > 0.9 else 'no',
        })

    df = pd.DataFrame(events)
    print(f"✅ Generated {len(df):,} events")
    return df


def prepare_data(df):
    """Clean and prepare data for all platforms."""
    print("\n" + "="*80)
    print("🧹 STEP 1: Data Preparation & Validation")
    print("="*80)

    # Prepare for BigQuery (will standardize columns and auto-cast types)
    df_clean, schema, issues = prepare_for_load(
        df,
        target_type='bigquery',
        standardize_columns=True,
        auto_cast=True,
        validate=True
    )

    # Check issues
    if issues['errors']:
        print("\n❌ Errors found:")
        for error in issues['errors']:
            print(f"  - {error}")
        raise Exception("Data validation failed")

    if issues['warnings']:
        print("\n⚠️  Warnings:")
        for warning in issues['warnings'][:5]:  # Show first 5
            print(f"  - {warning}")
        if len(issues['warnings']) > 5:
            print(f"  ... and {len(issues['warnings']) - 5} more")

    print(f"\n✅ Data cleaned and validated")
    print(f"   - Original columns: {len(df.columns)}")
    print(f"   - Cleaned columns: {len(df_clean.columns)}")
    print(f"   - Schema fields: {len(schema)}")
    print(f"\n📋 Detected types:")
    for col in df_clean.columns:
        print(f"   - {col}: {df_clean[col].dtype}")

    return df_clean, schema


def generate_bigquery_optimized_ddl(schema):
    """Generate optimized BigQuery DDL."""
    print("\n" + "="*80)
    print("🔵 BigQuery - Partitioned & Clustered Table")
    print("="*80)

    generator = get_enhanced_ddl_generator('bigquery')

    # Best practice for BigQuery:
    # - Partition by date for time-series data
    # - Cluster by high-cardinality columns used in WHERE/JOIN
    # - Set partition expiration for cost control
    options = DDLOptions(
        partitioning=PartitionConfig(
            column='event_date',
            partition_type=PartitionType.TIME,
            expiration_days=365,  # Auto-delete data older than 1 year
            require_partition_filter=True  # Force queries to filter by partition
        ),
        clustering=ClusteringConfig(
            columns=['user_id', 'event_type', 'country']
        )
    )

    ddl = generator.generate(
        schema=schema,
        table_name='events',
        dataset_name='analytics',
        project_id='my-project',
        ddl_options=options
    )

    print(ddl)

    print("\n💡 BigQuery Optimization Benefits:")
    print("   ✓ Partitioning by date: Reduce query costs by 90%+")
    print("   ✓ Clustering: Improve query performance by 2-10x")
    print("   ✓ Partition expiration: Automatic data lifecycle management")
    print("   ✓ Required partition filter: Prevent full table scans")

    return ddl


def generate_snowflake_optimized_ddl(schema):
    """Generate optimized Snowflake DDL."""
    print("\n" + "="*80)
    print("❄️  Snowflake - Clustered Table")
    print("="*80)

    # Convert BigQuery types to Snowflake types
    snowflake_schema = []
    for field in schema:
        sf_field = field.copy()
        # Type conversions
        if field['type'] == 'INT64' or field['type'] == 'INTEGER':
            sf_field['type'] = 'NUMBER(38,0)'
        elif field['type'] == 'FLOAT':
            sf_field['type'] = 'FLOAT'
        elif field['type'] == 'STRING':
            sf_field['type'] = 'VARCHAR(16777216)'
        elif field['type'] == 'TIMESTAMP':
            sf_field['type'] = 'TIMESTAMP_NTZ'
        elif field['type'] == 'DATE':
            sf_field['type'] = 'DATE'
        elif field['type'] == 'BOOLEAN':
            sf_field['type'] = 'BOOLEAN'
        snowflake_schema.append(sf_field)

    generator = get_enhanced_ddl_generator('snowflake')

    # Best practice for Snowflake:
    # - Cluster by columns used in WHERE/JOIN (auto micro-partitioning handles the rest)
    # - Order matters: most selective first
    options = DDLOptions(
        clustering=ClusteringConfig(
            columns=['event_date', 'user_id', 'event_type']
        )
    )

    ddl = generator.generate(
        schema=snowflake_schema,
        table_name='events',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)

    print("\n💡 Snowflake Optimization Benefits:")
    print("   ✓ Automatic micro-partitioning: No manual partition management")
    print("   ✓ Clustering: Optimize for common query patterns")
    print("   ✓ Order matters: event_date first for time-based queries")

    return ddl


def generate_redshift_optimized_ddl(schema):
    """Generate optimized Redshift DDL."""
    print("\n" + "="*80)
    print("🔴 Redshift - Distributed & Sorted Table")
    print("="*80)

    # Convert BigQuery types to Redshift types
    redshift_schema = []
    for field in schema:
        rs_field = field.copy()
        # Type conversions
        if field['type'] in ['INT64', 'INTEGER']:
            rs_field['type'] = 'BIGINT'
        elif field['type'] == 'FLOAT':
            rs_field['type'] = 'DOUBLE PRECISION'
        elif field['type'] == 'STRING':
            # Analyze field for appropriate VARCHAR size
            if 'id' in field['name'].lower():
                rs_field['type'] = 'VARCHAR(100)'
            elif 'url' in field['name'].lower():
                rs_field['type'] = 'VARCHAR(2048)'
            else:
                rs_field['type'] = 'VARCHAR(256)'
        elif field['type'] == 'TIMESTAMP':
            rs_field['type'] = 'TIMESTAMP'
        elif field['type'] == 'DATE':
            rs_field['type'] = 'DATE'
        elif field['type'] == 'BOOLEAN':
            rs_field['type'] = 'BOOLEAN'
        redshift_schema.append(rs_field)

    generator = get_enhanced_ddl_generator('redshift')

    # Best practice for Redshift:
    # - Distribute by join key (user_id for user-centric queries)
    # - Sort by filter columns (date range queries are common)
    options = DDLOptions(
        distribution=DistributionConfig(
            style=DistributionStyle.KEY,
            key_column='user_id'  # Even distribution for joins on user_id
        ),
        sort_keys=SortKeyConfig(
            columns=['event_date', 'event_timestamp'],
            compound=True  # COMPOUND SORTKEY for range queries
        )
    )

    ddl = generator.generate(
        schema=redshift_schema,
        table_name='events',
        dataset_name='analytics',
        ddl_options=options
    )

    print(ddl)

    print("\n💡 Redshift Optimization Benefits:")
    print("   ✓ DISTSTYLE KEY: Even data distribution for parallel processing")
    print("   ✓ DISTKEY user_id: Optimize joins on user dimension")
    print("   ✓ SORTKEY: Fast range scans on date columns")
    print("   ✓ COMPOUND: Best for queries filtering on multiple columns")

    return ddl


def generate_dimension_table_example():
    """Example: Dimension table with different optimization strategy."""
    print("\n" + "="*80)
    print("📊 BONUS: Dimension Table Optimization")
    print("="*80)

    # Small dimension table
    users_df = pd.DataFrame({
        'user_id': [1000, 1001, 1002],
        'username': ['alice', 'bob', 'charlie'],
        'country': ['US', 'UK', 'CA'],
        'account_type': ['premium', 'free', 'premium'],
        'created_at': ['2024-01-01', '2024-01-15', '2024-02-01']
    })

    mapper = SchemaMapper('redshift')
    schema, _ = mapper.generate_schema(users_df)

    # Convert to Redshift types
    for field in schema:
        if field['type'] == 'INTEGER':
            field['type'] = 'BIGINT'
        elif field['type'] == 'STRING':
            field['type'] = 'VARCHAR(255)'
        elif field['type'] == 'TIMESTAMP':
            field['type'] = 'TIMESTAMP'

    generator = get_enhanced_ddl_generator('redshift')

    # Best practice for small dimension tables:
    # - DISTSTYLE ALL: Replicate to all nodes (faster joins)
    options = DDLOptions(
        distribution=DistributionConfig(
            style=DistributionStyle.ALL  # Replicate small tables
        ),
        sort_keys=SortKeyConfig(
            columns=['user_id'],
            compound=True
        )
    )

    ddl = generator.generate(
        schema=schema,
        table_name='dim_users',
        dataset_name='analytics',
        ddl_options=options
    )

    print("🔴 Redshift - Small Dimension Table")
    print("-" * 60)
    print(ddl)
    print("\n💡 Why DISTSTYLE ALL for dimension tables?")
    print("   ✓ Small tables (<1GB) replicated to all nodes")
    print("   ✓ No network traffic during joins")
    print("   ✓ Faster query execution")


def save_artifacts(df_clean, bigquery_ddl, snowflake_ddl, redshift_ddl):
    """Save all generated artifacts."""
    print("\n" + "="*80)
    print("💾 Saving Artifacts")
    print("="*80)

    # Save cleaned data
    df_clean.to_csv('events_clean.csv', index=False)
    print("✓ events_clean.csv")

    # Save DDL files
    with open('events_bigquery.sql', 'w') as f:
        f.write(bigquery_ddl)
    print("✓ events_bigquery.sql")

    with open('events_snowflake.sql', 'w') as f:
        f.write(snowflake_ddl)
    print("✓ events_snowflake.sql")

    with open('events_redshift.sql', 'w') as f:
        f.write(redshift_ddl)
    print("✓ events_redshift.sql")

    # Save BigQuery schema JSON
    mapper = SchemaMapper('bigquery')
    schema_json = mapper.generate_bigquery_schema_json(df_clean)
    with open('events_schema.json', 'w') as f:
        f.write(schema_json)
    print("✓ events_schema.json")

    print("\n✅ All artifacts saved!")


def main():
    """Run the complete production pipeline."""
    print("="*80)
    print("🚀 Production Analytics Pipeline - Complete Use Case")
    print("="*80)
    print("\nScenario: E-commerce event tracking across multiple platforms")
    print("Goal: Generate optimized DDL for BigQuery, Snowflake, and Redshift\n")

    # Generate sample data
    df = generate_sample_events(num_events=1000)

    # Step 1: Prepare and validate data
    df_clean, schema = prepare_data(df)

    # Step 2: Generate platform-specific optimized DDL
    bigquery_ddl = generate_bigquery_optimized_ddl(schema)
    snowflake_ddl = generate_snowflake_optimized_ddl(schema)
    redshift_ddl = generate_redshift_optimized_ddl(schema)

    # Bonus: Dimension table example
    generate_dimension_table_example()

    # Step 3: Save all artifacts
    save_artifacts(df_clean, bigquery_ddl, snowflake_ddl, redshift_ddl)

    # Final summary
    print("\n" + "="*80)
    print("🎯 Summary")
    print("="*80)
    print("""
This pipeline demonstrates:

1. ✅ Data Preparation
   - Standardized column names
   - Auto-detected and cast types
   - Validated data quality

2. ✅ Platform-Optimized DDL
   - BigQuery: Partitioned by date, clustered by user_id/event_type
   - Snowflake: Clustered by date/user_id/event_type
   - Redshift: Distributed by user_id, sorted by date

3. ✅ Best Practices Applied
   - BigQuery: Partition expiration for cost control
   - Snowflake: Clustering for query performance
   - Redshift: Distribution for parallel processing

4. ✅ Production-Ready Artifacts
   - Cleaned CSV data
   - Platform-specific DDL files
   - BigQuery schema JSON

Next Steps:
-----------
1. Review generated DDL files
2. Execute DDL in respective platforms
3. Load events_clean.csv into tables
4. Run sample queries to verify optimization

Query Examples:
---------------
-- BigQuery (partition pruning + clustering)
SELECT user_id, COUNT(*) as event_count
FROM `my-project.analytics.events`
WHERE event_date >= '2024-01-01'
  AND user_id = 1234
  AND event_type = 'purchase'
GROUP BY user_id;

-- Cost: ~90% less due to partition pruning
-- Speed: ~5x faster due to clustering

Happy querying! 🚀
    """)


if __name__ == '__main__':
    main()
