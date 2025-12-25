"""
Example 6: Prefect Orchestration - Production ETL Pipeline

This example demonstrates integrating schema-mapper with Prefect for:
- Orchestrated ETL workflows
- Tagged pipeline stages
- Error handling and retries
- Monitoring and observability
- Data quality gates

Time: ~20 minutes
Prerequisites:
  - pip install prefect
  - Database credentials in config/connections.yaml
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, Dict, Any

# Prefect imports
try:
    from prefect import flow, task, get_run_logger
    from prefect.tasks import task_input_hash
    from prefect.artifacts import create_markdown_artifact
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False
    print("⚠️  Prefect not installed. Install with: pip install prefect")
    print("   This example will run in non-orchestrated mode.\n")

# Schema-mapper imports
from schema_mapper.canonical import infer_canonical_schema, CanonicalSchema
from schema_mapper.connections import ConnectionFactory, ConnectionConfig


# ============================================================================
# Prefect Tasks - Each stage of the ETL pipeline
# ============================================================================

if PREFECT_AVAILABLE:
    @task(
        name="Extract Data",
        description="Extract data from source CSV file",
        tags=["extract", "data-ingestion"],
        retries=2,
        retry_delay_seconds=10
    )
    def extract_data(file_path: str, max_rows: int = None) -> pd.DataFrame:
        """
        Extract data from CSV file.

        Args:
            file_path: Path to CSV file
            max_rows: Maximum rows to load (for testing)

        Returns:
            DataFrame with raw data
        """
        logger = get_run_logger()
        logger.info(f"Extracting data from: {file_path}")

        data_path = Path(file_path)

        if not data_path.exists():
            logger.warning(f"File not found: {file_path}, creating sample data")
            df = pd.DataFrame({
                'order_id': ['ORD-001', 'ORD-002', 'ORD-003'],
                'customer_email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
                'order_date': pd.to_datetime(['2025-01-15', '2025-01-16', '2025-01-17']),
                'total_amount': [299.99, 599.50, 149.75],
                'order_status': ['Shipped', 'Processing', 'Delivered']
            })
        else:
            df = pd.read_csv(data_path, nrows=max_rows)

        logger.info(f"✓ Extracted {len(df)} rows, {len(df.columns)} columns")

        # Create extraction artifact
        create_markdown_artifact(
            key="extraction-summary",
            markdown=f"""# Data Extraction Summary

**File**: `{file_path}`
**Rows**: {len(df):,}
**Columns**: {len(df.columns)}
**Timestamp**: {datetime.now().isoformat()}

## Sample Columns
{', '.join(list(df.columns[:10]))}
""",
            description="Summary of data extraction stage"
        )

        return df


    @task(
        name="Profile Data Quality",
        description="Profile data quality metrics",
        tags=["quality", "profiling"],
        cache_key_fn=task_input_hash,
        cache_expiration=timedelta(hours=1)
    )
    def profile_data(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Profile data quality metrics.

        Args:
            df: Input DataFrame

        Returns:
            Quality metrics dictionary
        """
        logger = get_run_logger()
        logger.info("Profiling data quality...")

        # Calculate quality metrics
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        missing_pct = (missing_cells / total_cells * 100) if total_cells > 0 else 0

        # Duplicate check
        duplicate_count = df.duplicated().sum()
        duplicate_pct = (duplicate_count / len(df) * 100) if len(df) > 0 else 0

        # Column completeness
        completeness_by_column = {}
        for col in df.columns:
            non_null = df[col].notna().sum()
            completeness = (non_null / len(df) * 100) if len(df) > 0 else 0
            completeness_by_column[col] = completeness

        # Find low quality columns (< 80% complete)
        low_quality_cols = [
            col for col, comp in completeness_by_column.items()
            if comp < 80
        ]

        metrics = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_cells': missing_cells,
            'missing_percentage': round(missing_pct, 2),
            'duplicate_rows': duplicate_count,
            'duplicate_percentage': round(duplicate_pct, 2),
            'low_quality_columns': low_quality_cols,
            'completeness_by_column': completeness_by_column
        }

        logger.info(f"✓ Quality Score: {100 - missing_pct:.1f}%")
        logger.info(f"  Missing: {missing_pct:.1f}% | Duplicates: {duplicate_pct:.1f}%")

        if low_quality_cols:
            logger.warning(f"  Low quality columns ({len(low_quality_cols)}): {low_quality_cols[:5]}")

        # Create quality artifact
        create_markdown_artifact(
            key="quality-report",
            markdown=f"""# Data Quality Report

## Overall Metrics
- **Total Rows**: {metrics['total_rows']:,}
- **Total Columns**: {metrics['total_columns']}
- **Missing Cells**: {metrics['missing_cells']:,} ({metrics['missing_percentage']}%)
- **Duplicate Rows**: {metrics['duplicate_rows']} ({metrics['duplicate_percentage']}%)

## Quality Issues
{f"**Low Quality Columns ({len(low_quality_cols)})**: {', '.join(low_quality_cols[:10])}" if low_quality_cols else "✓ No columns with <80% completeness"}

## Recommendation
{'⚠️ Data quality issues detected - review before proceeding' if low_quality_cols or missing_pct > 10 else '✓ Data quality acceptable'}
""",
            description="Data quality profiling results"
        )

        return metrics


    @task(
        name="Transform Data",
        description="Clean and transform data",
        tags=["transform", "data-cleaning"]
    )
    def transform_data(
        df: pd.DataFrame,
        quality_metrics: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, CanonicalSchema]:
        """
        Transform and clean data.

        Args:
            df: Input DataFrame
            quality_metrics: Quality metrics from profiling

        Returns:
            Tuple of (transformed DataFrame, canonical schema)
        """
        logger = get_run_logger()
        logger.info("Transforming data...")

        original_rows = len(df)

        # Remove duplicates if found
        if quality_metrics['duplicate_rows'] > 0:
            logger.info(f"  Removing {quality_metrics['duplicate_rows']} duplicates...")
            df = df.drop_duplicates()

        # Standardize column names (simple version)
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

        # Infer canonical schema
        logger.info("  Inferring canonical schema...")
        schema = infer_canonical_schema(
            df,
            table_name='retail_orders',
            dataset_name='analytics'
        )

        rows_removed = original_rows - len(df)
        logger.info(f"✓ Transformation complete")
        logger.info(f"  Rows removed: {rows_removed}")
        logger.info(f"  Final rows: {len(df)}")
        logger.info(f"  Schema columns: {len(schema.columns)}")

        # Create transformation artifact
        create_markdown_artifact(
            key="transformation-summary",
            markdown=f"""# Data Transformation Summary

## Changes Applied
- **Duplicates Removed**: {rows_removed}
- **Column Names Standardized**: {len(df.columns)}
- **Canonical Schema Inferred**: {len(schema.columns)} columns

## Final Dataset
- **Rows**: {len(df):,}
- **Columns**: {len(df.columns)}

## Schema Preview
{chr(10).join([f'- `{col.name}`: {col.logical_type.name}' for col in schema.columns[:10]])}
{f'... and {len(schema.columns) - 10} more' if len(schema.columns) > 10 else ''}
""",
            description="Data transformation results"
        )

        return df, schema


    @task(
        name="Quality Gate",
        description="Validate data quality before load",
        tags=["quality-gate", "validation"]
    )
    def quality_gate(
        df: pd.DataFrame,
        quality_metrics: Dict[str, Any],
        max_missing_pct: float = 20.0,
        max_duplicate_pct: float = 5.0
    ) -> bool:
        """
        Quality gate to validate data before loading.

        Args:
            df: Transformed DataFrame
            quality_metrics: Quality metrics
            max_missing_pct: Maximum allowed missing percentage
            max_duplicate_pct: Maximum allowed duplicate percentage

        Returns:
            True if quality gate passes

        Raises:
            ValueError: If quality gate fails
        """
        logger = get_run_logger()
        logger.info("Running quality gate checks...")

        checks_passed = []
        checks_failed = []

        # Check 1: Missing data threshold
        missing_pct = quality_metrics['missing_percentage']
        if missing_pct <= max_missing_pct:
            checks_passed.append(f"✓ Missing data: {missing_pct:.1f}% <= {max_missing_pct}%")
        else:
            checks_failed.append(f"✗ Missing data: {missing_pct:.1f}% > {max_missing_pct}%")

        # Check 2: Duplicate threshold
        duplicate_pct = quality_metrics['duplicate_percentage']
        if duplicate_pct <= max_duplicate_pct:
            checks_passed.append(f"✓ Duplicates: {duplicate_pct:.1f}% <= {max_duplicate_pct}%")
        else:
            checks_failed.append(f"✗ Duplicates: {duplicate_pct:.1f}% > {max_duplicate_pct}%")

        # Check 3: Minimum row count
        min_rows = 1
        if len(df) >= min_rows:
            checks_passed.append(f"✓ Row count: {len(df):,} >= {min_rows}")
        else:
            checks_failed.append(f"✗ Row count: {len(df):,} < {min_rows}")

        # Log results
        for check in checks_passed:
            logger.info(f"  {check}")

        for check in checks_failed:
            logger.error(f"  {check}")

        # Create quality gate artifact
        status = "✅ PASSED" if not checks_failed else "❌ FAILED"
        create_markdown_artifact(
            key="quality-gate-result",
            markdown=f"""# Quality Gate Results

**Status**: {status}

## Checks Passed ({len(checks_passed)})
{chr(10).join([f'- {check}' for check in checks_passed])}

## Checks Failed ({len(checks_failed)})
{chr(10).join([f'- {check}' for check in checks_failed]) if checks_failed else '_None_'}

## Thresholds
- **Max Missing**: {max_missing_pct}%
- **Max Duplicates**: {max_duplicate_pct}%
- **Min Rows**: {min_rows:,}
""",
            description="Quality gate validation results"
        )

        if checks_failed:
            error_msg = f"Quality gate failed: {len(checks_failed)} checks failed"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info("✓ Quality gate passed - proceeding to load")
        return True


    @task(
        name="Load to Database",
        description="Load data to target database",
        tags=["load", "database"],
        retries=3,
        retry_delay_seconds=30
    )
    def load_to_database(
        df: pd.DataFrame,
        schema: CanonicalSchema,
        config_path: str,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Load data to database.

        Args:
            df: Transformed DataFrame
            schema: Canonical schema
            config_path: Path to connections.yaml
            dry_run: If True, skip actual database operations

        Returns:
            Load results dictionary
        """
        logger = get_run_logger()
        logger.info(f"Loading data to database (dry_run={dry_run})...")

        if dry_run:
            logger.info("  DRY RUN MODE - skipping actual database operations")
            return {
                'status': 'dry_run',
                'rows_loaded': len(df),
                'table': f"{schema.dataset_name}.{schema.table_name}"
            }

        config_file = Path(config_path)
        if not config_file.exists():
            logger.warning(f"Config file not found: {config_path}")
            return {'status': 'skipped', 'reason': 'config_not_found'}

        # Load config and connect
        config = ConnectionConfig(str(config_file))
        target = config.get_default_target()

        logger.info(f"  Target platform: {target}")

        with ConnectionFactory.get_connection(target, config) as conn:
            logger.info(f"  ✓ Connected to {conn.platform_name()}")

            # Create table from schema
            dataset = schema.dataset_name or 'analytics'

            success = conn.create_table_from_schema(
                schema,
                if_not_exists=True
            )

            if success:
                logger.info(f"  ✓ Table {dataset}.{schema.table_name} ready")
            else:
                logger.info(f"  ℹ️  Table {dataset}.{schema.table_name} already exists")

            # In a real scenario, you would load the data here using:
            # - BigQuery: client.load_table_from_dataframe()
            # - Snowflake: write_pandas()
            # - PostgreSQL: to_sql() or COPY
            # For this example, we just verify the table exists

            # Verify table
            if conn.table_exists(schema.table_name, schema_name=dataset):
                logger.info(f"  ✓ Verified table exists")
            else:
                raise RuntimeError(f"Table {dataset}.{schema.table_name} not found after creation")

            result = {
                'status': 'success',
                'platform': conn.platform_name(),
                'table': f"{dataset}.{schema.table_name}",
                'rows_available': len(df),
                'columns': len(schema.columns)
            }

            logger.info("✓ Load complete")

            # Create load artifact
            create_markdown_artifact(
                key="load-summary",
                markdown=f"""# Data Load Summary

**Status**: ✅ Success
**Platform**: {conn.platform_name()}
**Table**: `{dataset}.{schema.table_name}`
**Rows Available**: {len(df):,}
**Columns**: {len(schema.columns)}
**Timestamp**: {datetime.now().isoformat()}

## Next Steps
1. Verify data: `SELECT COUNT(*) FROM {dataset}.{schema.table_name}`
2. Sample query: `SELECT * FROM {dataset}.{schema.table_name} LIMIT 10`
3. Monitor data quality in production
""",
                description="Data load results"
            )

            return result


    # ========================================================================
    # Prefect Flow - Orchestrate the ETL Pipeline
    # ========================================================================

    @flow(
        name="Retail Orders ETL Pipeline",
        description="Complete ETL pipeline with quality gates using schema-mapper",
        version="1.0.0"
    )
    def retail_etl_pipeline(
        source_file: str,
        config_path: str,
        max_rows: int = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Complete ETL pipeline with Prefect orchestration.

        Args:
            source_file: Path to source CSV file
            config_path: Path to connections.yaml
            max_rows: Maximum rows to process (for testing)
            dry_run: If True, skip database operations

        Returns:
            Pipeline execution results
        """
        logger = get_run_logger()
        logger.info("=" * 80)
        logger.info("Starting Retail Orders ETL Pipeline")
        logger.info("=" * 80)

        pipeline_start = datetime.now()

        # Stage 1: Extract
        df = extract_data(source_file, max_rows)

        # Stage 2: Profile
        quality_metrics = profile_data(df)

        # Stage 3: Transform
        df_clean, schema = transform_data(df, quality_metrics)

        # Stage 4: Quality Gate
        gate_passed = quality_gate(
            df_clean,
            quality_metrics,
            max_missing_pct=20.0,
            max_duplicate_pct=5.0
        )

        # Stage 5: Load
        load_result = load_to_database(
            df_clean,
            schema,
            config_path,
            dry_run=dry_run
        )

        # Pipeline summary
        pipeline_duration = (datetime.now() - pipeline_start).total_seconds()

        summary = {
            'status': 'success',
            'pipeline_duration_seconds': pipeline_duration,
            'stages_completed': 5,
            'quality_gate_passed': gate_passed,
            'load_result': load_result,
            'final_row_count': len(df_clean)
        }

        logger.info("=" * 80)
        logger.info(f"✅ Pipeline Complete - Duration: {pipeline_duration:.1f}s")
        logger.info("=" * 80)

        return summary


# ============================================================================
# Non-Prefect Fallback
# ============================================================================

def run_without_prefect():
    """Run ETL without Prefect orchestration."""
    print("Running in non-orchestrated mode (Prefect not available)")
    print("Install Prefect to enable orchestration: pip install prefect\n")

    # Simple execution
    data_path = Path(__file__).parent.parent / 'sample_data' / 'problematic_clothing_retailer_data.csv'
    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    print("This example requires Prefect for full functionality.")
    print("Please install Prefect and run again.")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Run the Prefect-orchestrated ETL pipeline."""

    if not PREFECT_AVAILABLE:
        run_without_prefect()
        return

    # Configuration
    data_path = Path(__file__).parent.parent / 'sample_data' / 'problematic_clothing_retailer_data.csv'
    config_path = Path(__file__).parent.parent / 'config' / 'connections.yaml'

    print("\n" + "=" * 80)
    print("Example 6: Prefect Orchestration - Production ETL Pipeline")
    print("=" * 80)
    print("\nThis example demonstrates:")
    print("  ✓ Orchestrated ETL with Prefect")
    print("  ✓ Tagged pipeline stages (extract, transform, load)")
    print("  ✓ Data quality profiling and gates")
    print("  ✓ Retry logic and error handling")
    print("  ✓ Monitoring with Prefect artifacts")
    print("\n" + "=" * 80)

    # Run the flow
    result = retail_etl_pipeline(
        source_file=str(data_path),
        config_path=str(config_path),
        max_rows=50,  # Process first 50 rows for demo
        dry_run=not config_path.exists()  # Dry run if no config
    )

    print("\n" + "=" * 80)
    print("Pipeline Execution Summary")
    print("=" * 80)
    print(f"Status: {result['status']}")
    print(f"Duration: {result['pipeline_duration_seconds']:.1f}s")
    print(f"Stages: {result['stages_completed']}")
    print(f"Final Rows: {result['final_row_count']:,}")
    print(f"\n💡 View in Prefect UI: prefect server start")


if __name__ == '__main__':
    main()
