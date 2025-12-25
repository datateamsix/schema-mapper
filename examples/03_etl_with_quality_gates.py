"""
Example 3: ETL Pipeline with Quality Gates

This example demonstrates a complete production ETL workflow:
1. Extract from messy CSV
2. Profile data quality
3. Transform with quality checks
4. Quality gate validation
5. Load to database

This is README Use Case #2: "ETL pipelines with built-in quality gates"

Time: ~15 minutes
Prerequisites: Database credentials in config/connections.yaml
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Tuple

from schema_mapper.canonical import infer_canonical_schema, CanonicalSchema
from schema_mapper.connections import ConnectionFactory, ConnectionConfig


def extract_data(file_path: Path, max_rows: int = None) -> pd.DataFrame:
    """Extract data from CSV file."""
    print("\n📂 Stage 1: EXTRACT")
    print("=" * 80)

    if not file_path.exists():
        print(f"   Creating sample data...")
        df = pd.DataFrame({
            'order_id': ['ORD-001', 'ORD-002', 'ORD-003', 'ORD-002'],  # Duplicate
            'customer': ['Alice', 'Bob', 'Charlie', 'Bob'],
            'amount': [100.50, 200.75, None, 200.75],  # Missing value
            'date': ['2025-01-15', '2025-01-16', '2025-01-17', '2025-01-16']
        })
    else:
        df = pd.read_csv(file_path, nrows=max_rows)

    print(f"   ✓ Loaded: {len(df)} rows, {len(df.columns)} columns")
    return df


def profile_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Profile data quality metrics."""
    print("\n🔍 Stage 2: PROFILE DATA QUALITY")
    print("=" * 80)

    total_cells = df.shape[0] * df.shape[1]
    missing = df.isnull().sum().sum()
    duplicates = df.duplicated().sum()

    metrics = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_cells': missing,
        'missing_pct': round((missing / total_cells * 100) if total_cells > 0 else 0, 2),
        'duplicate_rows': duplicates,
        'duplicate_pct': round((duplicates / len(df) * 100) if len(df) > 0 else 0, 2)
    }

    print(f"   Rows: {metrics['total_rows']:,} | Columns: {metrics['total_columns']}")
    print(f"   Missing: {metrics['missing_cells']} ({metrics['missing_pct']}%)")
    print(f"   Duplicates: {metrics['duplicate_rows']} ({metrics['duplicate_pct']}%)")

    quality_score = 100 - (metrics['missing_pct'] + metrics['duplicate_pct'])
    print(f"\n   📊 Quality Score: {quality_score:.1f}/100")

    return metrics


def transform_data(df: pd.DataFrame, metrics: Dict) -> Tuple[pd.DataFrame, CanonicalSchema]:
    """Transform and clean data."""
    print("\n🔄 Stage 3: TRANSFORM")
    print("=" * 80)

    original_rows = len(df)

    # Remove duplicates
    if metrics['duplicate_rows'] > 0:
        print(f"   Removing {metrics['duplicate_rows']} duplicates...")
        df = df.drop_duplicates()

    # Clean column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Infer schema
    schema = infer_canonical_schema(
        df,
        table_name='orders_clean',
        dataset_name='analytics'
    )

    print(f"   ✓ Cleaned: {len(df)} rows (removed {original_rows - len(df)})")
    print(f"   ✓ Schema: {len(schema.columns)} columns")

    return df, schema


def quality_gate(df: pd.DataFrame, metrics: Dict, thresholds: Dict) -> bool:
    """Validate against quality thresholds."""
    print("\n🚦 Stage 4: QUALITY GATE")
    print("=" * 80)

    checks = []

    # Check missing data
    if metrics['missing_pct'] <= thresholds['max_missing_pct']:
        checks.append(f"✓ Missing: {metrics['missing_pct']}% <= {thresholds['max_missing_pct']}%")
    else:
        checks.append(f"✗ Missing: {metrics['missing_pct']}% > {thresholds['max_missing_pct']}%")
        return False

    # Check duplicates
    if metrics['duplicate_pct'] <= thresholds['max_duplicate_pct']:
        checks.append(f"✓ Duplicates: {metrics['duplicate_pct']}% <= {thresholds['max_duplicate_pct']}%")
    else:
        checks.append(f"✗ Duplicates: {metrics['duplicate_pct']}% > {thresholds['max_duplicate_pct']}%")
        return False

    # Check minimum rows
    if len(df) >= thresholds['min_rows']:
        checks.append(f"✓ Row count: {len(df):,} >= {thresholds['min_rows']}")
    else:
        checks.append(f"✗ Row count: {len(df):,} < {thresholds['min_rows']}")
        return False

    for check in checks:
        print(f"   {check}")

    print(f"\n   ✅ GATE PASSED - Proceeding to load")
    return True


def load_to_database(df: pd.DataFrame, schema: CanonicalSchema, config_path: Path, dry_run: bool = False):
    """Load data to database."""
    print("\n💾 Stage 5: LOAD")
    print("=" * 80)

    if dry_run or not config_path.exists():
        print(f"   DRY RUN - Would load {len(df)} rows to {schema.dataset_name}.{schema.table_name}")
        return

    config = ConnectionConfig(str(config_path))
    target = config.get_default_target()

    with ConnectionFactory.get_connection(target, config) as conn:
        print(f"   ✓ Connected: {conn.platform_name()}")

        # Create table
        conn.create_table_from_schema(schema, if_not_exists=True)
        print(f"   ✓ Table ready: {schema.dataset_name}.{schema.table_name}")

        # Note: Actual data loading depends on platform
        # BigQuery: client.load_table_from_dataframe(df, table_id)
        # Snowflake: session.write_pandas(df, table_name)
        # PostgreSQL: df.to_sql(table_name, conn)

        print(f"   ✓ Load complete ({len(df)} rows available)")


def main():
    """Run ETL pipeline with quality gates."""

    print("=" * 80)
    print("Example 3: ETL Pipeline with Quality Gates")
    print("=" * 80)

    # Configuration
    data_path = Path(__file__).parent.parent / 'sample_data' / 'problematic_clothing_retailer_data.csv'
    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    # Quality thresholds
    thresholds = {
        'max_missing_pct': 20.0,
        'max_duplicate_pct': 5.0,
        'min_rows': 1
    }

    try:
        # Execute pipeline
        df = extract_data(data_path, max_rows=100)
        metrics = profile_quality(df)
        df_clean, schema = transform_data(df, metrics)

        if quality_gate(df_clean, metrics, thresholds):
            load_to_database(df_clean, schema, config_path, dry_run=not config_path.exists())

            print("\n" + "=" * 80)
            print("✅ ETL PIPELINE COMPLETE")
            print("=" * 80)
            print(f"\nProcessed: {metrics['total_rows']} → {len(df_clean)} rows")
            print(f"Quality: {100 - metrics['missing_pct'] - metrics['duplicate_pct']:.1f}/100")
            print(f"Destination: {schema.dataset_name}.{schema.table_name}")
        else:
            print("\n❌ QUALITY GATE FAILED - Pipeline halted")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise


if __name__ == '__main__':
    main()
