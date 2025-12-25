"""
Example 5: SCD Type 2 Dimension Tracking

Demonstrates Slowly Changing Dimension Type 2 pattern:
1. Track historical changes in dimension tables
2. Maintain current and historical records
3. Use effective dates and current flags
4. Generate SCD2 merge logic

This is README Use Case #4: "SCD Type 2 tracking for dimension tables"

Time: ~15 minutes
Prerequisites: Database credentials in config/connections.yaml
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from schema_mapper.canonical import infer_canonical_schema
from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator
from schema_mapper.connections import ConnectionFactory, ConnectionConfig


def create_dimension_snapshots() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create initial dimension and changed records."""

    # Initial dimension state (2025-01-01)
    initial = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'customer_name': ['Alice Corp', 'Bob Inc', 'Charlie LLC'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'tier': ['Gold', 'Silver', 'Bronze'],
        'valid_from': pd.to_datetime(['2025-01-01', '2025-01-01', '2025-01-01']),
        'valid_to': pd.to_datetime(['9999-12-31', '9999-12-31', '9999-12-31']),
        'is_current': [True, True, True]
    })

    # Changed records (2025-01-15)
    # - Customer 1: Address changed (track history)
    # - Customer 2: Tier upgraded (track history)
    # - Customer 4: New customer
    changes = pd.DataFrame({
        'customer_id': [1, 2, 4],
        'customer_name': ['Alice Corp', 'Bob Inc', 'David Enterprises'],
        'address': ['999 New St', '456 Oak Ave', '111 First Ave'],  # Address changed for customer 1
        'tier': ['Gold', 'Gold', 'Silver'],  # Tier changed for customer 2
        'valid_from': pd.to_datetime(['2025-01-15', '2025-01-15', '2025-01-15']),
        'valid_to': pd.to_datetime(['9999-12-31', '9999-12-31', '9999-12-31']),
        'is_current': [True, True, True]
    })

    return initial, changes


def main():
    """Run SCD Type 2 example."""

    print("=" * 80)
    print("Example 5: SCD Type 2 Dimension Tracking")
    print("=" * 80)

    # ========================================================================
    # Step 1: Understand SCD Type 2
    # ========================================================================

    print("\n📖 SCD Type 2 Concept:")
    print("   • Tracks historical changes in dimension tables")
    print("   • Each change creates a new row with effective dates")
    print("   • Old rows get end date, new rows get start date")
    print("   • 'is_current' flag marks active record")

    # ========================================================================
    # Step 2: Create Sample Data
    # ========================================================================

    print("\n📊 Step 1: Creating dimension snapshots...")

    initial, changes = create_dimension_snapshots()

    print(f"   Initial dimension: {len(initial)} customers")
    print(f"   Changes: {len(changes)} records")
    print(f"      - {len(set(changes['customer_id']) & set(initial['customer_id']))} modified")
    print(f"      - {len(set(changes['customer_id']) - set(initial['customer_id']))} new")

    # Show what changed
    print(f"\n   Change details:")
    for idx, row in changes.iterrows():
        cust_id = row['customer_id']
        if cust_id in initial['customer_id'].values:
            old = initial[initial['customer_id'] == cust_id].iloc[0]
            if old['address'] != row['address']:
                print(f"      Customer {cust_id}: Address '{old['address']}' → '{row['address']}'")
            if old['tier'] != row['tier']:
                print(f"      Customer {cust_id}: Tier '{old['tier']}' → '{row['tier']}'")
        else:
            print(f"      Customer {cust_id}: New customer")

    # ========================================================================
    # Step 3: Define SCD2 Configuration
    # ========================================================================

    print("\n🔧 Step 2: Defining SCD2 configuration...")

    schema = infer_canonical_schema(
        initial,
        table_name='dim_customers',
        dataset_name='analytics'
    )

    config = IncrementalConfig(
        load_pattern=LoadPattern.SCD2,
        primary_keys=['customer_id'],
        scd2_columns=['customer_name', 'address', 'tier']  # Track changes to these
    )

    print(f"   ✓ Schema: {len(schema.columns)} columns")
    print(f"   ✓ Primary key: {config.primary_keys}")
    print(f"   ✓ SCD2 columns: {config.scd2_columns}")
    print(f"   ✓ Load pattern: {config.load_pattern.name}")

    # ========================================================================
    # Step 4: Generate SCD2 DDL
    # ========================================================================

    print("\n📝 Step 3: Generating SCD2 DDL...")

    platforms = ['bigquery', 'snowflake', 'postgresql']

    for platform in platforms:
        print(f"\n   {platform.upper()}:")
        try:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='dim_customers',
                config=config
            )

            print(f"      ✓ Generated: {len(ddl)} characters")

            # Show BigQuery preview
            if platform == 'bigquery':
                print(f"\n      Logic Preview:")
                print("      " + "-" * 72)
                print("      1. Identify changed records (compare SCD2 columns)")
                print("      2. Expire old records (set valid_to = CURRENT_DATE)")
                print("      3. Insert new records (set valid_from = CURRENT_DATE)")
                print("      4. Update is_current flags")
                print("      " + "-" * 72)

        except Exception as e:
            print(f"      ✗ Error: {e}")

    # ========================================================================
    # Step 5: Demonstrate SCD2 Result
    # ========================================================================

    print("\n🎯 Step 4: Demonstrating SCD2 behavior...")

    print(f"\n   After applying SCD2 logic, dimension table would have:")

    # Calculate expected state
    # Customer 1: 2 rows (old address expired, new address current)
    # Customer 2: 2 rows (old tier expired, new tier current)
    # Customer 3: 1 row (unchanged)
    # Customer 4: 1 row (new)

    expected_rows = 6  # 2 + 2 + 1 + 1

    print(f"      • {expected_rows} total rows (including history)")
    print(f"      • Customer 1: 2 rows")
    print(f"         - Row 1: Address='123 Main St', valid_to='2025-01-14', is_current=False")
    print(f"         - Row 2: Address='999 New St', valid_from='2025-01-15', is_current=True")
    print(f"      • Customer 2: 2 rows")
    print(f"         - Row 1: Tier='Silver', valid_to='2025-01-14', is_current=False")
    print(f"         - Row 2: Tier='Gold', valid_from='2025-01-15', is_current=True")
    print(f"      • Customer 3: 1 row (unchanged)")
    print(f"      • Customer 4: 1 row (new)")

    # ========================================================================
    # Step 6: Execute (if connection available)
    # ========================================================================

    print("\n🚀 Step 5: Execution...")

    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    if not config_path.exists():
        print(f"   ⚠️  Config not found - demo mode only")
        print(f"\n   💡 To run with real database:")
        print(f"      1. Create config/connections.yaml")
        print(f"      2. SCD2 DDL will be executed")
        print(f"      3. Historical records preserved")
    else:
        conn_config = ConnectionConfig(str(config_path))
        target = conn_config.get_default_target()

        try:
            with ConnectionFactory.get_connection(target, conn_config) as conn:
                print(f"   ✓ Connected: {conn.platform_name()}")

                # Create dimension table
                conn.create_table_from_schema(schema, if_not_exists=True)
                print(f"   ✓ Dimension table ready: {schema.table_name}")

                # Generate SCD2 logic
                generator = get_incremental_generator(conn.platform_name())
                scd2_ddl = generator.generate_incremental_ddl(
                    schema=schema,
                    table_name='dim_customers',
                    config=config
                )

                print(f"   ✓ SCD2 DDL ready for execution")

        except Exception as e:
            print(f"   ✗ Error: {e}")

    # ========================================================================
    # Summary
    # ========================================================================

    print("\n" + "=" * 80)
    print("✅ SCD Type 2 Demo Complete")
    print("=" * 80)

    print(f"\n📊 SCD2 Summary:")
    print(f"   Dimension: dim_customers")
    print(f"   Business keys: {config.primary_keys}")
    print(f"   Tracked columns: {len(config.scd2_columns)} attributes")
    print(f"   Expected rows: {expected_rows} (with history)")

    print(f"\n💡 Benefits:")
    print(f"   ✓ Full historical record of changes")
    print(f"   ✓ Point-in-time queries (use valid_from/valid_to)")
    print(f"   ✓ Current state always available (is_current=True)")
    print(f"   ✓ Audit trail for compliance")

    print(f"\n📚 Use Cases:")
    print(f"   • Customer dimensions (address, tier changes)")
    print(f"   • Product dimensions (price, category changes)")
    print(f"   • Employee dimensions (department, title changes)")
    print(f"   • Any dimension requiring history tracking")

    print(f"\n🎯 Query Examples:")
    print(f"   # Current snapshot")
    print(f"   SELECT * FROM dim_customers WHERE is_current = TRUE")
    print(f"\n   # Historical snapshot (2025-01-10)")
    print(f"   SELECT * FROM dim_customers")
    print(f"   WHERE '2025-01-10' BETWEEN valid_from AND valid_to")


if __name__ == '__main__':
    main()
