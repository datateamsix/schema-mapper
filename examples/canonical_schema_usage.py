"""
Canonical Schema and Renderer Pattern - Complete Example

This example demonstrates the new architecture:
1. Canonical Schema → Single source of truth
2. Renderers → Platform-specific outputs
3. Multiple formats → DDL, JSON, CLI commands

One schema → Many execution surfaces
"""

import pandas as pd
from datetime import datetime, timedelta
import random

from schema_mapper.canonical import (
    infer_canonical_schema,
    canonical_schema_to_dict,
)
from schema_mapper.renderers import RendererFactory


def create_sample_data():
    """Create sample e-commerce events data."""
    print("📊 Creating sample data...")

    # Generate 100 events
    events = []
    base_date = datetime.now() - timedelta(days=7)

    for i in range(100):
        event_date = base_date + timedelta(days=random.randint(0, 6))

        events.append({
            'event_id': i + 1,
            'user_id': random.randint(1000, 1050),
            'event_type': random.choice(['page_view', 'click', 'purchase']),
            'event_timestamp': event_date.isoformat(),
            'event_date': event_date.date().isoformat(),
            'revenue': round(random.uniform(10, 500), 2) if random.random() > 0.7 else None,
            'country': random.choice(['US', 'UK', 'CA']),
        })

    df = pd.DataFrame(events)
    print(f"✅ Created {len(df)} events\n")
    return df


def demonstrate_canonical_schema(df):
    """Step 1: Infer canonical schema from DataFrame."""
    print("="*80)
    print("STEP 1: CANONICAL SCHEMA (Single Source of Truth)")
    print("="*80)

    # Infer schema with optimization hints
    canonical = infer_canonical_schema(
        df,
        table_name='events',
        dataset_name='analytics',
        project_id='my-project',
        partition_columns=['event_date'],
        cluster_columns=['user_id', 'event_type'],
        standardize_columns=True,
        auto_cast=True
    )

    print("\n📋 Canonical Schema:")
    print(canonical)

    print("\n🔍 Validation:")
    errors = canonical.validate()
    if errors:
        print(f"  ❌ Errors: {errors}")
    else:
        print("  ✅ Schema is valid")

    return canonical


def demonstrate_bigquery_renderer(canonical):
    """Step 2a: BigQuery - JSON + DDL + CLI."""
    print("\n" + "="*80)
    print("STEP 2a: BIGQUERY RENDERER (JSON Schema + DDL)")
    print("="*80)

    renderer = RendererFactory.get_renderer('bigquery', canonical)

    print(f"\n🔵 Platform: {renderer.platform_name()}")
    print(f"   JSON Schema Support: {renderer.supports_json_schema()}")

    # 1. JSON Schema (BigQuery-specific)
    print("\n📄 JSON Schema (for bq load):")
    print("-" * 60)
    json_schema = renderer.to_schema_json()
    print(json_schema[:500] + "..." if len(json_schema) > 500 else json_schema)

    # 2. DDL
    print("\n📝 DDL (with partitioning & clustering):")
    print("-" * 60)
    ddl = renderer.to_ddl()
    print(ddl)

    # 3. CLI Create Command
    print("\n💻 CLI Create Command:")
    print("-" * 60)
    create_cmd = renderer.to_cli_create()
    print(create_cmd)

    # 4. CLI Load Command
    print("\n📥 CLI Load Command:")
    print("-" * 60)
    load_cmd = renderer.to_cli_load('events_clean.csv')
    print(load_cmd)

    return {
        'json_schema': json_schema,
        'ddl': ddl,
        'create_cmd': create_cmd,
        'load_cmd': load_cmd
    }


def demonstrate_snowflake_renderer(canonical):
    """Step 2b: Snowflake - DDL only."""
    print("\n" + "="*80)
    print("STEP 2b: SNOWFLAKE RENDERER (DDL Only)")
    print("="*80)

    renderer = RendererFactory.get_renderer('snowflake', canonical)

    print(f"\n❄️  Platform: {renderer.platform_name()}")
    print(f"   JSON Schema Support: {renderer.supports_json_schema()}")

    # 1. DDL
    print("\n📝 DDL (with clustering):")
    print("-" * 60)
    ddl = renderer.to_ddl()
    print(ddl)

    # 2. CLI Create Command
    print("\n💻 CLI Create Command:")
    print("-" * 60)
    create_cmd = renderer.to_cli_create()
    print(create_cmd)

    # 3. CLI Load Command
    print("\n📥 CLI Load Command:")
    print("-" * 60)
    load_cmd = renderer.to_cli_load('events_clean.csv')
    print(load_cmd)

    return {
        'ddl': ddl,
        'create_cmd': create_cmd,
        'load_cmd': load_cmd
    }


def demonstrate_redshift_renderer(canonical):
    """Step 2c: Redshift - DDL with distribution/sort keys."""
    print("\n" + "="*80)
    print("STEP 2c: REDSHIFT RENDERER (Distribution + Sort Keys)")
    print("="*80)

    # Update canonical schema to use distribution instead of clustering
    # (Redshift doesn't support clustering)
    canonical.optimization.distribution_column = 'user_id'
    canonical.optimization.cluster_columns = []  # Clear cluster columns
    canonical.optimization.sort_columns = ['event_date', 'event_timestamp']

    renderer = RendererFactory.get_renderer('redshift', canonical)

    print(f"\n🔴 Platform: {renderer.platform_name()}")
    print(f"   JSON Schema Support: {renderer.supports_json_schema()}")

    # 1. DDL
    print("\n📝 DDL (with DISTSTYLE and SORTKEY):")
    print("-" * 60)
    ddl = renderer.to_ddl()
    print(ddl)

    # 2. CLI Create Command
    print("\n💻 CLI Create Command:")
    print("-" * 60)
    create_cmd = renderer.to_cli_create()
    print(create_cmd[:500] + "..." if len(create_cmd) > 500 else create_cmd)

    # 3. CLI Load Command
    print("\n📥 CLI Load Command (from S3):")
    print("-" * 60)
    load_cmd = renderer.to_cli_load('s3://my-bucket/events.csv')
    print(load_cmd[:500] + "..." if len(load_cmd) > 500 else load_cmd)

    return {
        'ddl': ddl,
        'create_cmd': create_cmd,
        'load_cmd': load_cmd
    }


def demonstrate_postgresql_renderer(canonical):
    """Step 2d: PostgreSQL - Partitioning + Indexes."""
    print("\n" + "="*80)
    print("STEP 2d: POSTGRESQL RENDERER (Partitioning + Indexes)")
    print("="*80)

    # Reset optimization for PostgreSQL
    canonical.optimization.distribution_column = None
    canonical.optimization.sort_columns = []
    canonical.optimization.cluster_columns = ['user_id', 'event_type']
    canonical.optimization.partition_columns = ['event_date']

    renderer = RendererFactory.get_renderer('postgresql', canonical)

    print(f"\n🐘 Platform: {renderer.platform_name()}")
    print(f"   JSON Schema Support: {renderer.supports_json_schema()}")

    # 1. DDL
    print("\n📝 DDL (with PARTITION BY and INDEX):")
    print("-" * 60)
    ddl = renderer.to_ddl()
    print(ddl)

    # 2. CLI Create Command
    print("\n💻 CLI Create Command:")
    print("-" * 60)
    create_cmd = renderer.to_cli_create()
    print(create_cmd[:500] + "..." if len(create_cmd) > 500 else create_cmd)

    # 3. CLI Load Command
    print("\n📥 CLI Load Command:")
    print("-" * 60)
    load_cmd = renderer.to_cli_load('events_clean.csv')
    print(load_cmd[:500] + "..." if len(load_cmd) > 500 else load_cmd)

    return {
        'ddl': ddl,
        'create_cmd': create_cmd,
        'load_cmd': load_cmd
    }


def demonstrate_multi_platform(canonical):
    """Step 3: Generate for all platforms at once."""
    print("\n" + "="*80)
    print("STEP 3: MULTI-PLATFORM GENERATION")
    print("="*80)

    platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

    print("\n🌍 Generating DDL for all platforms...\n")

    for platform in platforms:
        try:
            # Adjust optimization hints per platform
            if platform == 'redshift':
                canonical.optimization.distribution_column = 'user_id'
                canonical.optimization.cluster_columns = []
                canonical.optimization.sort_columns = ['event_date']
            else:
                canonical.optimization.distribution_column = None
                canonical.optimization.cluster_columns = ['user_id', 'event_type']
                canonical.optimization.sort_columns = []

            renderer = RendererFactory.get_renderer(platform, canonical)

            print(f"{'='*60}")
            print(f"{platform.upper()}")
            print('='*60)

            # Just show DDL for comparison
            ddl = renderer.to_ddl()
            print(ddl)
            print()

        except ValueError as e:
            print(f"⚠️  {platform}: {e}\n")


def demonstrate_serialization(canonical):
    """Step 4: Serialize canonical schema."""
    print("\n" + "="*80)
    print("STEP 4: SCHEMA SERIALIZATION")
    print("="*80)

    # Convert to dict (for JSON export)
    schema_dict = canonical_schema_to_dict(canonical)

    print("\n📦 Canonical Schema as Dictionary (for version control, schema registry):")
    print("-" * 60)

    import json
    print(json.dumps(schema_dict, indent=2)[:800] + "...")

    print("\n💡 Use Cases:")
    print("  - Save to JSON file for version control")
    print("  - Submit to schema registry")
    print("  - Documentation generation")
    print("  - Cross-team schema sharing")


def main():
    """Run complete demonstration."""
    print("="*80)
    print("🚀 CANONICAL SCHEMA + RENDERER PATTERN DEMONSTRATION")
    print("="*80)
    print()
    print("Architecture:")
    print("  1. Canonical Schema (Python) → Single source of truth")
    print("  2. Platform Renderers → Convert to platform-specific formats")
    print("  3. Multiple outputs → DDL, JSON, CLI commands")
    print()
    print("Key Insight:")
    print("  ✅ JSON Schema: Only where natively supported (BigQuery)")
    print("  ✅ DDL: Universal control plane (all platforms)")
    print("  ✅ CLI: Platform-specific execution commands")
    print()

    # Create sample data
    df = create_sample_data()

    # Step 1: Canonical schema
    canonical = demonstrate_canonical_schema(df)

    # Step 2: Platform-specific renderers
    bq_artifacts = demonstrate_bigquery_renderer(canonical)
    sf_artifacts = demonstrate_snowflake_renderer(canonical)
    rs_artifacts = demonstrate_redshift_renderer(canonical)
    pg_artifacts = demonstrate_postgresql_renderer(canonical)

    # Step 3: Multi-platform
    demonstrate_multi_platform(canonical)

    # Step 4: Serialization
    demonstrate_serialization(canonical)

    # Summary
    print("\n" + "="*80)
    print("✅ SUMMARY")
    print("="*80)
    print("""
This demonstration showed:

1. ✅ One Canonical Schema
   - Inferred from DataFrame
   - Platform-agnostic
   - Single source of truth

2. ✅ Platform-Specific Renderers
   - BigQuery: JSON + DDL + CLI
   - Snowflake: DDL + CLI
   - Redshift: DDL + CLI (with COPY from S3)
   - PostgreSQL: DDL + CLI (with partitioning)

3. ✅ Multiple Output Formats
   - DDL: CREATE TABLE statements
   - JSON: Schema files (BigQuery only)
   - CLI: Ready-to-run commands

4. ✅ Clean Architecture
   - No forced abstractions
   - Platform reality respected
   - Easy to extend

Next Steps:
-----------
1. Save canonical schema to JSON for version control
2. Generate DDL files for each platform
3. Execute CLI commands to create tables
4. Load data using platform-specific methods

The renderer pattern makes it easy to support new platforms
without changing the core schema representation!
    """)


if __name__ == '__main__':
    main()
