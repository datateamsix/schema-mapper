"""
Example 2: Multi-Cloud Migration - BigQuery to Snowflake

This example demonstrates schema-mapper's core value proposition:
1. Introspect schema from source database (BigQuery)
2. Convert to canonical format (platform-agnostic)
3. Deploy to target database (Snowflake)
4. Verify migration

This is README Use Case #1: "Migrate from BigQuery to Snowflake in minutes"

Time: ~10 minutes
Prerequisites: BigQuery and Snowflake credentials in config/connections.yaml
"""

import pandas as pd
from pathlib import Path
from typing import Optional

from schema_mapper.canonical import infer_canonical_schema, CanonicalSchema
from schema_mapper.connections import ConnectionFactory, ConnectionConfig
from schema_mapper.renderers.factory import RendererFactory


def introspect_source_table(
    source_platform: str,
    config: ConnectionConfig,
    table_name: str,
    schema_name: Optional[str] = None
) -> CanonicalSchema:
    """
    Introspect table from source database.

    Args:
        source_platform: Source platform (e.g., 'bigquery')
        config: Connection configuration
        table_name: Table to introspect
        schema_name: Schema/dataset name

    Returns:
        CanonicalSchema representing the source table
    """
    print(f"\n📊 Step 1: Introspecting {table_name} from {source_platform}...")

    with ConnectionFactory.get_connection(source_platform, config) as source_conn:
        print(f"   ✓ Connected to {source_conn.platform_name()}")

        # Verify table exists
        if not source_conn.table_exists(table_name, schema_name=schema_name):
            raise ValueError(f"Table {table_name} not found in {schema_name}")

        # Get canonical schema from source
        canonical_schema = source_conn.get_target_schema(
            table_name,
            schema_name=schema_name
        )

        print(f"   ✓ Introspected schema: {len(canonical_schema.columns)} columns")
        print(f"\n   Schema details:")
        print(f"      Table: {canonical_schema.table_name}")
        print(f"      Dataset/Schema: {canonical_schema.dataset_name}")
        if canonical_schema.database_name:
            print(f"      Database/Project: {canonical_schema.database_name}")

        print(f"\n   Column types (sample):")
        for col in canonical_schema.columns[:5]:
            nullable = "NULL" if col.nullable else "NOT NULL"
            print(f"      - {col.name}: {col.logical_type.name} ({nullable})")

        if len(canonical_schema.columns) > 5:
            print(f"      ... and {len(canonical_schema.columns) - 5} more columns")

        # Show optimization hints if any
        if canonical_schema.optimization_hints:
            hints = canonical_schema.optimization_hints
            print(f"\n   Optimization hints:")
            if hints.partition_columns:
                print(f"      Partitioning: {hints.partition_columns}")
            if hints.cluster_columns:
                print(f"      Clustering: {hints.cluster_columns}")

    return canonical_schema


def deploy_to_target(
    target_platform: str,
    config: ConnectionConfig,
    schema: CanonicalSchema,
    create_table: bool = True
) -> bool:
    """
    Deploy schema to target database.

    Args:
        target_platform: Target platform (e.g., 'snowflake')
        config: Connection configuration
        schema: Canonical schema to deploy
        create_table: If True, create the table

    Returns:
        True if deployment successful
    """
    print(f"\n🚀 Step 2: Deploying to {target_platform}...")

    # First, generate and preview the DDL
    print(f"\n   Generating {target_platform} DDL...")
    renderer = RendererFactory.get_renderer(target_platform, schema)
    ddl = renderer.to_ddl()

    print(f"   ✓ Generated DDL ({len(ddl)} characters)")
    print(f"\n   DDL Preview (first 400 chars):")
    print("   " + "-" * 76)
    for line in ddl[:400].split('\n'):
        print(f"   {line}")
    print("   " + "-" * 76)
    if len(ddl) > 400:
        print(f"   ... and {len(ddl) - 400} more characters")

    if not create_table:
        print(f"\n   ℹ️  Skipping table creation (preview mode)")
        return True

    # Connect and create table
    with ConnectionFactory.get_connection(target_platform, config) as target_conn:
        print(f"\n   ✓ Connected to {target_conn.platform_name()}")

        # Check if table already exists
        dataset = schema.dataset_name or 'PUBLIC'
        table_exists = target_conn.table_exists(
            schema.table_name,
            schema_name=dataset
        )

        if table_exists:
            print(f"   ⚠️  Table {dataset}.{schema.table_name} already exists")
            print(f"   ℹ️  Skipping creation - set create_table=False to regenerate DDL only")
            return True

        # Create table
        print(f"   Creating table {dataset}.{schema.table_name}...")
        success = target_conn.create_table_from_schema(
            schema,
            if_not_exists=True
        )

        if success:
            print(f"   ✓ Table created successfully")

            # Verify by introspecting
            print(f"\n   📋 Verifying deployment...")
            deployed_schema = target_conn.get_target_schema(
                schema.table_name,
                schema_name=dataset
            )

            print(f"   ✓ Verified: {len(deployed_schema.columns)} columns")

            # Compare schemas
            print(f"\n   Schema comparison (source → target):")
            for i, (src_col, tgt_col) in enumerate(zip(schema.columns[:5], deployed_schema.columns[:5])):
                print(f"      {src_col.name}:")
                print(f"         Canonical: {src_col.logical_type.name}")
                print(f"         {target_platform.title()}: {tgt_col.logical_type.name}")

            return True
        else:
            print(f"   ✗ Table creation failed")
            return False


def demonstrate_cross_platform_compatibility(schema: CanonicalSchema):
    """
    Demonstrate that canonical schema works across all platforms.

    Args:
        schema: Canonical schema
    """
    print(f"\n🌐 Step 3: Cross-Platform Compatibility Demo...")
    print(f"\n   The same canonical schema can be deployed to ANY platform:")

    platforms = ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']

    for platform in platforms:
        try:
            renderer = RendererFactory.get_renderer(platform, schema)
            ddl = renderer.to_ddl()
            print(f"   ✓ {platform.upper():12s}: {len(ddl):,} chars DDL generated")
        except Exception as e:
            print(f"   ✗ {platform.upper():12s}: {e}")

    print(f"\n   💡 One schema, five platforms - that's the power of canonical format!")


def main():
    """Run multi-cloud migration example."""

    print("=" * 80)
    print("Example 2: Multi-Cloud Migration (BigQuery → Snowflake)")
    print("=" * 80)
    print("\nThis example demonstrates:")
    print("  1. Schema introspection from source (BigQuery)")
    print("  2. Canonical format conversion (platform-agnostic)")
    print("  3. Schema deployment to target (Snowflake)")
    print("  4. Cross-platform compatibility")
    print("=" * 80)

    # Configuration
    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    if not config_path.exists():
        print(f"\n⚠️  Configuration file not found: {config_path}")
        print("\nDemonstrating with sample data instead...")

        # Create sample data to demonstrate the concept
        print("\n" + "=" * 80)
        print("Demonstration Mode (No Database Connection)")
        print("=" * 80)

        data_path = Path(__file__).parent.parent / 'sample_data' / 'problematic_clothing_retailer_data.csv'

        if data_path.exists():
            df = pd.read_csv(data_path, nrows=20)
        else:
            df = pd.DataFrame({
                'order_id': ['ORD-001', 'ORD-002'],
                'customer_email': ['alice@example.com', 'bob@example.com'],
                'order_date': pd.to_datetime(['2025-01-15', '2025-01-16']),
                'total_amount': [299.99, 599.50],
            })

        print(f"\n📊 Created sample DataFrame: {len(df)} rows, {len(df.columns)} columns")

        # Infer canonical schema
        schema = infer_canonical_schema(
            df,
            table_name='retail_orders',
            dataset_name='analytics'
        )

        print(f"\n✓ Inferred canonical schema: {len(schema.columns)} columns")

        # Show how it would work
        demonstrate_cross_platform_compatibility(schema)

        print("\n" + "=" * 80)
        print("To run with real database connections:")
        print("=" * 80)
        print("1. Create config/connections.yaml with credentials")
        print("2. Ensure table exists in source (BigQuery)")
        print("3. Run this example again")
        print("\nSee examples/README.md for configuration template")

        return

    # Load configuration
    config = ConnectionConfig(str(config_path))

    # Check if both platforms are configured
    if not config.has_target('bigquery'):
        print("\n⚠️  BigQuery not configured in connections.yaml")
        print("   Add BigQuery credentials to run this example")
        return

    if not config.has_target('snowflake'):
        print("\n⚠️  Snowflake not configured in connections.yaml")
        print("   Add Snowflake credentials to run this example")
        return

    # Define source and target
    source_platform = 'bigquery'
    target_platform = 'snowflake'
    source_table = 'retail_orders'  # Modify to match your source table
    source_dataset = 'analytics'     # Modify to match your dataset

    try:
        # Step 1: Introspect from source
        canonical_schema = introspect_source_table(
            source_platform=source_platform,
            config=config,
            table_name=source_table,
            schema_name=source_dataset
        )

        # Step 2: Deploy to target
        success = deploy_to_target(
            target_platform=target_platform,
            config=config,
            schema=canonical_schema,
            create_table=True
        )

        if success:
            # Step 3: Demonstrate cross-platform compatibility
            demonstrate_cross_platform_compatibility(canonical_schema)

            # Summary
            print("\n" + "=" * 80)
            print("✅ Migration Complete!")
            print("=" * 80)

            print(f"\n📊 Summary:")
            print(f"   Source: {source_platform.upper()} - {source_dataset}.{source_table}")
            print(f"   Target: {target_platform.upper()} - {canonical_schema.dataset_name}.{canonical_schema.table_name}")
            print(f"   Columns: {len(canonical_schema.columns)}")

            print(f"\n💡 Next Steps:")
            print(f"   1. Migrate data (use platform-specific tools):")
            print(f"      - BigQuery → GCS → Snowflake")
            print(f"      - Or use data transfer services")
            print(f"   2. Verify data: SELECT COUNT(*) FROM {canonical_schema.dataset_name}.{canonical_schema.table_name}")
            print(f"   3. Test queries on target platform")
            print(f"   4. Consider incremental loads (see example 04)")

            print(f"\n🎯 Key Takeaway:")
            print(f"   Schema-mapper eliminated platform-specific DDL writing!")
            print(f"   Same canonical format works for ALL 5 supported platforms.")

    except ValueError as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Common issues:")
        print("   - Table doesn't exist in source")
        print("   - Incorrect dataset/schema name")
        print("   - Missing permissions")
        print("\n   Create the source table first:")
        print("   Run example 01_basic_usage.py to create a table in BigQuery")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
