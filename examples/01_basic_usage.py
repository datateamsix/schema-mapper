"""
Example 1: Basic Usage - DataFrame to Database

This example demonstrates the simplest workflow with schema-mapper:
1. Load data from CSV
2. Infer canonical schema
3. Connect to database
4. Create table and verify

Time: ~5 minutes
Prerequisites: BigQuery or PostgreSQL credentials in config/connections.yaml
"""

import pandas as pd
from pathlib import Path

from schema_mapper.canonical import infer_canonical_schema
from schema_mapper.connections import ConnectionFactory, ConnectionConfig


def main():
    """Run basic usage example."""

    print("=" * 80)
    print("Example 1: Basic Usage - DataFrame to Database")
    print("=" * 80)

    # ========================================================================
    # Step 1: Load Sample Data
    # ========================================================================

    print("\n📂 Step 1: Loading sample data...")

    # Load from CSV
    data_path = Path(__file__).parent.parent / 'sample_data' / 'problematic_clothing_retailer_data.csv'

    if not data_path.exists():
        # Fallback: Create simple sample data
        print("   Sample CSV not found, creating sample DataFrame...")
        df = pd.DataFrame({
            'order_id': ['ORD-001', 'ORD-002', 'ORD-003'],
            'customer_email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
            'order_date': pd.to_datetime(['2025-01-15', '2025-01-16', '2025-01-17']),
            'total_amount': [299.99, 599.50, 149.75],
            'order_status': ['Shipped', 'Processing', 'Delivered']
        })
    else:
        print(f"   Loading from: {data_path}")
        df = pd.read_csv(data_path, nrows=20)  # Load first 20 rows

    print(f"   ✓ Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"\n   Sample columns: {list(df.columns[:5])}")

    # ========================================================================
    # Step 2: Infer Canonical Schema
    # ========================================================================

    print("\n🔍 Step 2: Inferring canonical schema...")

    schema = infer_canonical_schema(
        df,
        table_name='retail_orders',
        dataset_name='analytics',  # BigQuery dataset / Snowflake schema
        project_id='demo_project'   # Optional: for platforms that need it
    )

    print(f"   ✓ Inferred schema with {len(schema.columns)} columns")
    print(f"\n   Schema preview:")
    for col in schema.columns[:5]:
        nullable_str = "NULL" if col.nullable else "NOT NULL"
        print(f"      - {col.name}: {col.logical_type.name} ({nullable_str})")

    if len(schema.columns) > 5:
        print(f"      ... and {len(schema.columns) - 5} more columns")

    # ========================================================================
    # Step 3: Connect to Database
    # ========================================================================

    print("\n🔌 Step 3: Connecting to database...")

    # Load connection configuration
    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    if not config_path.exists():
        print(f"   ⚠️  Configuration file not found: {config_path}")
        print("   \n   To run this example with real connections:")
        print("   1. Create config/connections.yaml")
        print("   2. Add your database credentials")
        print("   3. See examples/README.md for configuration template")
        print("\n   📝 Example would create table: analytics.retail_orders")
        print(f"   📝 With {len(schema.columns)} columns from canonical schema")
        return

    # Load config and get connection
    config = ConnectionConfig(str(config_path))
    target = config.get_default_target()

    print(f"   Using target platform: {target}")

    try:
        with ConnectionFactory.get_connection(target, config) as conn:
            print(f"   ✓ Connected to {conn.platform_name()}")

            # Test connection
            if conn.test_connection():
                print("   ✓ Connection healthy")

            # ================================================================
            # Step 4: Create Table from Schema
            # ================================================================

            print("\n📝 Step 4: Creating table from schema...")

            # Check if table already exists
            dataset = schema.dataset_name or 'analytics'
            table_exists = conn.table_exists(
                schema.table_name,
                schema_name=dataset
            )

            if table_exists:
                print(f"   ⚠️  Table {dataset}.{schema.table_name} already exists")
                print("   ℹ️  Skipping table creation")

                # Introspect existing table
                print(f"\n   📋 Introspecting existing schema...")
                existing_schema = conn.get_target_schema(
                    schema.table_name,
                    schema_name=dataset
                )
                print(f"   ✓ Table has {len(existing_schema.columns)} columns")

            else:
                # Create table using canonical schema
                print(f"   Creating table: {dataset}.{schema.table_name}")

                success = conn.create_table_from_schema(
                    schema,
                    if_not_exists=True
                )

                if success:
                    print("   ✓ Table created successfully")

                    # Verify by introspecting
                    print("\n   📋 Verifying table creation...")
                    created_schema = conn.get_target_schema(
                        schema.table_name,
                        schema_name=dataset
                    )
                    print(f"   ✓ Verified: table has {len(created_schema.columns)} columns")

                    # Show column comparison
                    print(f"\n   Column type mapping (sample):")
                    for i, col in enumerate(created_schema.columns[:3]):
                        original_col = schema.columns[i]
                        print(f"      {col.name}:")
                        print(f"         Logical: {original_col.logical_type.name}")
                        print(f"         Platform: {col.logical_type.name}")

            # ================================================================
            # Step 5: List Tables (Verification)
            # ================================================================

            print(f"\n📊 Step 5: Listing tables in {dataset}...")

            tables = conn.list_tables(schema_name=dataset)
            print(f"   Found {len(tables)} tables:")
            for table in tables[:10]:  # Show first 10
                marker = "✓" if table == schema.table_name else " "
                print(f"   {marker} {table}")

            if len(tables) > 10:
                print(f"   ... and {len(tables) - 10} more")

            # ================================================================
            # Summary
            # ================================================================

            print("\n" + "=" * 80)
            print("✅ Example Complete!")
            print("=" * 80)

            print(f"\n📊 Summary:")
            print(f"   - Platform: {conn.platform_name()}")
            print(f"   - Table: {dataset}.{schema.table_name}")
            print(f"   - Columns: {len(schema.columns)}")
            print(f"   - Status: {'Exists' if table_exists else 'Created'}")

            print(f"\n💡 Next Steps:")
            print(f"   1. Load data: Use conn.execute_query() or platform-specific tools")
            print(f"   2. Query table: SELECT * FROM {dataset}.{schema.table_name} LIMIT 10")
            print(f"   3. See example 03 for complete ETL pipeline with data loading")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Troubleshooting:")
        print("   - Verify credentials in config/connections.yaml")
        print("   - Check .env file has required environment variables")
        print("   - Ensure you have permissions to create tables")
        raise


if __name__ == '__main__':
    main()
