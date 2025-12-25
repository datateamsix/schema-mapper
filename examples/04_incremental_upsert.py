"""
Example 4: Incremental UPSERT Loads

Demonstrates incremental data loading with UPSERT (merge) pattern:
1. Define schema with primary keys
2. Generate UPSERT DDL for target platform
3. Execute incremental load
4. Track changes

This is README Use Case #3: "Incremental UPSERT loads with change tracking"

Time: ~10 minutes
Prerequisites: Database credentials in config/connections.yaml
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

from schema_mapper.canonical import infer_canonical_schema
from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator
from schema_mapper.connections import ConnectionFactory, ConnectionConfig


def create_sample_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create initial and updated datasets."""

    # Initial load
    initial_df = pd.DataFrame({
        'user_id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'status': ['active', 'active', 'active'],
        'updated_at': pd.to_datetime(['2025-01-01', '2025-01-01', '2025-01-01'])
    })

    # Updates: user 2 changed email, user 3 changed status, user 4 is new
    updated_df = pd.DataFrame({
        'user_id': [2, 3, 4],
        'username': ['bob', 'charlie', 'david'],
        'email': ['bob.new@example.com', 'charlie@example.com', 'david@example.com'],
        'status': ['active', 'inactive', 'active'],
        'updated_at': pd.to_datetime(['2025-01-15', '2025-01-15', '2025-01-15'])
    })

    return initial_df, updated_df


def main():
    """Run incremental UPSERT example."""

    print("=" * 80)
    print("Example 4: Incremental UPSERT Loads")
    print("=" * 80)

    # ========================================================================
    # Step 1: Prepare Initial Data
    # ========================================================================

    print("\n📊 Step 1: Preparing datasets...")

    initial_df, updated_df = create_sample_data()

    print(f"   Initial dataset: {len(initial_df)} rows")
    print(f"   Update dataset: {len(updated_df)} rows")
    print(f"      - {len(set(updated_df['user_id']) & set(initial_df['user_id']))} existing users (UPDATES)")
    print(f"      - {len(set(updated_df['user_id']) - set(initial_df['user_id']))} new users (INSERTS)")

    # ========================================================================
    # Step 2: Define Schema and Incremental Config
    # ========================================================================

    print("\n🔧 Step 2: Defining schema and incremental configuration...")

    # Infer schema from initial data
    schema = infer_canonical_schema(
        initial_df,
        table_name='users',
        dataset_name='analytics'
    )

    # Configure incremental load
    config = IncrementalConfig(
        load_pattern=LoadPattern.UPSERT,
        primary_keys=['user_id'],
        update_columns=['username', 'email', 'status', 'updated_at']
    )

    print(f"   ✓ Schema: {len(schema.columns)} columns")
    print(f"   ✓ Primary keys: {config.primary_keys}")
    print(f"   ✓ Load pattern: {config.load_pattern.name}")

    # ========================================================================
    # Step 3: Generate UPSERT DDL for Each Platform
    # ========================================================================

    print("\n📝 Step 3: Generating UPSERT DDL for each platform...")

    platforms = ['bigquery', 'snowflake', 'postgresql']

    for platform in platforms:
        print(f"\n   {platform.upper()}:")
        try:
            generator = get_incremental_generator(platform)
            ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='users',
                config=config
            )

            print(f"      ✓ Generated: {len(ddl)} characters")

            # Show DDL preview
            if platform == 'bigquery':
                print(f"\n      Preview (first 300 chars):")
                print("      " + "-" * 72)
                for line in ddl[:300].split('\n')[:6]:
                    print(f"      {line}")
                print("      " + "-" * 72)

        except Exception as e:
            print(f"      ✗ Error: {e}")

    # ========================================================================
    # Step 4: Execute UPSERT (with database connection)
    # ========================================================================

    print("\n🚀 Step 4: Executing UPSERT...")

    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    if not config_path.exists():
        print(f"   ⚠️  Config not found - running in demo mode")
        print(f"\n   Demo: MERGE statement would UPSERT {len(updated_df)} rows:")
        print(f"      - UPDATE {len(set(updated_df['user_id']) & set(initial_df['user_id']))} existing")
        print(f"      - INSERT {len(set(updated_df['user_id']) - set(initial_df['user_id']))} new")
        print(f"\n   Final state: {len(set(initial_df['user_id']) | set(updated_df['user_id']))} users")
        return

    conn_config = ConnectionConfig(str(config_path))
    target = conn_config.get_default_target()

    print(f"   Target platform: {target}")

    try:
        with ConnectionFactory.get_connection(target, conn_config) as conn:
            print(f"   ✓ Connected: {conn.platform_name()}")

            # Create table if not exists
            conn.create_table_from_schema(schema, if_not_exists=True)
            print(f"   ✓ Table ready: {schema.table_name}")

            # Generate UPSERT DDL
            generator = get_incremental_generator(conn.platform_name())
            upsert_ddl = generator.generate_incremental_ddl(
                schema=schema,
                table_name='users',
                config=config
            )

            print(f"   ✓ Generated UPSERT DDL: {len(upsert_ddl)} chars")

            # Note: Actual UPSERT execution varies by platform:
            # - BigQuery: Use MERGE statement with staging table
            # - Snowflake: MERGE INTO with CTE
            # - PostgreSQL: INSERT ... ON CONFLICT

            print(f"\n   💡 To execute UPSERT:")
            print(f"      1. Load data to staging table")
            print(f"      2. Execute generated MERGE/UPSERT DDL")
            print(f"      3. Verify changes")

            # ================================================================
            # Summary
            # ================================================================

            print("\n" + "=" * 80)
            print("✅ Incremental UPSERT Demo Complete")
            print("=" * 80)

            print(f"\n📊 Summary:")
            print(f"   Platform: {conn.platform_name()}")
            print(f"   Table: {schema.table_name}")
            print(f"   Load Pattern: {config.load_pattern.name}")
            print(f"   Primary Keys: {', '.join(config.primary_keys)}")
            print(f"   Update Columns: {len(config.update_columns or [])} columns")

            print(f"\n💡 Benefits of UPSERT:")
            print(f"   ✓ No full table reload - only changed rows")
            print(f"   ✓ Automatic deduplication via primary keys")
            print(f"   ✓ Tracks updates with updated_at timestamp")
            print(f"   ✓ Handles inserts and updates in one operation")

            print(f"\n📚 Next Steps:")
            print(f"   1. Schedule incremental loads (daily/hourly)")
            print(f"   2. Monitor change volume over time")
            print(f"   3. Add CDC tracking columns (see example 05)")
            print(f"   4. Integrate with orchestration (see example 06)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == '__main__':
    main()
