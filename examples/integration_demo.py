"""
Demo: Integrated ETL Workflow with SchemaMapper, Profiler, and PreProcessor

This example demonstrates the complete integrated workflow using SchemaMapper
with profiling and preprocessing capabilities for a production-ready ETL pipeline.
"""

import pandas as pd
import numpy as np
from schema_mapper import SchemaMapper, prepare_for_load, Profiler, PreProcessor
import warnings
warnings.filterwarnings('ignore')


def create_messy_customer_data():
    """Create a realistic messy customer dataset."""
    np.random.seed(42)
    n = 500

    data = {
        '  Customer_ID  ': range(1, n + 1),
        'First Name': [f'  Customer{i}  ' if i % 10 != 0 else None for i in range(n)],
        'LAST-NAME': [f'Last{i}' for i in range(n)],
        'Email Address': [f'customer{i}@example.com' if i % 15 != 0 else 'invalid-email' for i in range(n)],
        'Phone_Number': [f'+1-555-{i:04d}' if i % 20 != 0 else None for i in range(n)],
        'Age  ': np.random.randint(18, 80, n),
        'Annual Income': np.random.randint(20000, 200000, n),
        'Credit Score': np.random.randint(300, 850, n),
        '  Join Date ': pd.date_range('2020-01-01', periods=n, freq='D'),
        'Account_Status': np.random.choice(['Active', 'Inactive', 'Suspended', None], n, p=[0.7, 0.15, 0.1, 0.05]),
        'Total_Purchases': np.random.randint(0, 100, n),
        'Lifetime Value': np.random.uniform(0, 50000, n)
    }

    df = pd.DataFrame(data)

    # Introduce some duplicates
    df = pd.concat([df, df.sample(10)], ignore_index=True)

    # Introduce some outliers in income
    df.loc[np.random.choice(df.index, 5), 'Annual Income'] = np.random.randint(500000, 1000000, 5)

    return df


# ============================================================================
# DEMO 1: Basic Integration - prepare_for_load()
# ============================================================================

def demo_basic_integration():
    """Demonstrate basic integrated workflow with prepare_for_load."""
    print("=" * 80)
    print("DEMO 1: Basic Integration with prepare_for_load()")
    print("=" * 80)

    df = create_messy_customer_data()
    print(f"\nCreated messy dataset: {len(df)} rows, {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)[:5]}... (showing first 5)")

    print("\nCalling prepare_for_load() with default settings...")
    df_prepared, schema, issues = prepare_for_load(df, target_type='bigquery')

    print(f"\nResults:")
    print(f"  Prepared DataFrame: {len(df_prepared)} rows, {len(df_prepared.columns)} columns")
    print(f"  Schema fields: {len(schema)}")
    print(f"  Validation errors: {len(issues['errors'])}")
    print(f"  Validation warnings: {len(issues['warnings'])}")

    if issues['warnings']:
        print(f"\n  Sample warnings:")
        for warning in issues['warnings'][:3]:
            print(f"    - {warning}")

    print(f"\n  Sample schema fields:")
    for field in schema[:3]:
        print(f"    - {field['name']}: {field['type']} ({field.get('mode', 'NULLABLE')})")

    print("\n  ✓ Basic integration successful!")


# ============================================================================
# DEMO 2: With Profiling - Understanding Data Quality
# ============================================================================

def demo_with_profiling():
    """Demonstrate workflow with profiling enabled."""
    print("\n" + "=" * 80)
    print("DEMO 2: Integrated Workflow with Profiling")
    print("=" * 80)

    df = create_messy_customer_data()
    print(f"\nDataset: {len(df)} rows, {len(df.columns)} columns")

    print("\nCalling prepare_for_load() with profiling enabled...")
    df_prepared, schema, issues, report = prepare_for_load(
        df,
        target_type='bigquery',
        profile=True
    )

    print(f"\nProfiling Report Summary:")
    print(f"  Overall Quality Score: {report['quality']['overall_score']:.2f}/100")
    print(f"  Interpretation: {report['quality']['interpretation']}")
    print(f"\n  Component Scores:")
    print(f"    - Completeness: {report['quality']['completeness_score']:.2f}")
    print(f"    - Validity: {report['quality']['validity_score']:.2f}")
    print(f"    - Consistency: {report['quality']['consistency_score']:.2f}")

    print(f"\n  Data Quality Metrics:")
    print(f"    - Total Missing Values: {report['missing_values']['total_missing']:,}")
    print(f"    - Missing Percentage: {report['missing_values']['total_missing_percentage']:.2f}%")
    print(f"    - Duplicate Rows: {report['duplicates']['count']}")

    if report['missing_values']['high_missing_columns']:
        print(f"\n  Columns with >50% missing:")
        for col in report['missing_values']['high_missing_columns']:
            print(f"    - {col}")

    print(f"\n  ✓ Profiling provided valuable insights before loading!")


# ============================================================================
# DEMO 3: With Preprocessing - Cleaning Messy Data
# ============================================================================

def demo_with_preprocessing():
    """Demonstrate workflow with preprocessing pipeline."""
    print("\n" + "=" * 80)
    print("DEMO 3: Integrated Workflow with Preprocessing")
    print("=" * 80)

    df = create_messy_customer_data()
    print(f"\nOriginal dataset: {len(df)} rows, {len(df.columns)} columns")
    print(f"Sample original column names: {list(df.columns)[:4]}")

    print("\nDefining preprocessing pipeline:")
    pipeline = [
        'fix_whitespace',
        'standardize_column_names',
        'remove_duplicates',
        'handle_missing'
    ]
    print(f"  Pipeline steps: {pipeline}")

    print("\nCalling prepare_for_load() with preprocessing...")
    df_prepared, schema, issues = prepare_for_load(
        df,
        target_type='bigquery',
        preprocess_pipeline=pipeline
    )

    print(f"\nResults:")
    print(f"  Original rows: {len(df)}")
    print(f"  After preprocessing: {len(df_prepared)} rows")
    print(f"  Rows removed (duplicates): {len(df) - len(df_prepared)}")

    print(f"\n  Column name transformations (sample):")
    print(f"    '  Customer_ID  ' → {schema[0]['name']}")
    print(f"    'LAST-NAME' → {schema[2]['name']}")
    print(f"    'Email Address' → {schema[3]['name']}")

    print(f"\n  ✓ Data cleaned and standardized for BigQuery!")


# ============================================================================
# DEMO 4: Complete Workflow - Profiling + Preprocessing
# ============================================================================

def demo_complete_workflow():
    """Demonstrate complete workflow with both profiling and preprocessing."""
    print("\n" + "=" * 80)
    print("DEMO 4: Complete ETL Workflow (Profiling + Preprocessing)")
    print("=" * 80)

    df = create_messy_customer_data()
    print(f"\nStarting with messy dataset: {len(df)} rows, {len(df.columns)} columns")

    print("\nStep 1: Profile + Preprocess + Validate + Generate Schema")
    pipeline = [
        'fix_whitespace',
        'standardize_column_names',
        'remove_duplicates',
        'handle_missing'
    ]

    df_prepared, schema, issues, report = prepare_for_load(
        df,
        target_type='bigquery',
        profile=True,
        preprocess_pipeline=pipeline
    )

    print(f"\nProfiling Results (Original Data):")
    print(f"  Quality Score: {report['quality']['overall_score']:.2f}/100")
    print(f"  Duplicate Rows: {report['duplicates']['count']}")
    print(f"  Missing Values: {report['missing_values']['total_missing']:,}")

    print(f"\nPreprocessing Results:")
    print(f"  Rows removed: {len(df) - len(df_prepared)}")
    print(f"  Columns standardized: {len(schema)}")

    print(f"\nValidation Results:")
    print(f"  Errors: {len(issues['errors'])}")
    print(f"  Warnings: {len(issues['warnings'])}")

    print(f"\nFinal Schema ({len(schema)} fields):")
    for i, field in enumerate(schema[:5], 1):
        print(f"  {i}. {field['name']}: {field['type']} ({field.get('mode', 'NULLABLE')})")
    if len(schema) > 5:
        print(f"  ... and {len(schema) - 5} more fields")

    print(f"\n  ✓ Complete ETL workflow executed successfully!")
    print(f"  ✓ Data is ready for BigQuery load!")


# ============================================================================
# DEMO 5: Advanced - Step-by-Step with SchemaMapper
# ============================================================================

def demo_advanced_step_by_step():
    """Demonstrate advanced step-by-step workflow using SchemaMapper directly."""
    print("\n" + "=" * 80)
    print("DEMO 5: Advanced Step-by-Step Workflow with SchemaMapper")
    print("=" * 80)

    df = create_messy_customer_data()
    print(f"\nDataset: {len(df)} rows, {len(df.columns)} columns")

    # Step 1: Initialize SchemaMapper
    print("\nStep 1: Initialize SchemaMapper for Snowflake")
    mapper = SchemaMapper(target_type='snowflake')
    print(f"  ✓ Initialized for {mapper.target_type}")

    # Step 2: Profile the data
    print("\nStep 2: Profile original data")
    profile_report = mapper.profile_data(df, detailed=True, show_progress=False)
    original_quality = profile_report['quality']['overall_score']
    print(f"  Original Quality Score: {original_quality:.2f}/100")
    print(f"  Duplicate Count: {profile_report['duplicates']['count']}")
    print(f"  Missing Values: {profile_report['missing_values']['total_missing']:,}")

    # Step 3: Preprocess the data
    print("\nStep 3: Preprocess data")
    df_clean = mapper.preprocess_data(df, pipeline=[
        'fix_whitespace',
        'standardize_column_names',
        'remove_duplicates',
        'handle_missing'
    ])
    print(f"  Rows before: {len(df)}")
    print(f"  Rows after: {len(df_clean)}")
    print(f"  Duplicates removed: {len(df) - len(df_clean)}")

    # Step 4: Validate cleaned data
    print("\nStep 4: Validate cleaned data")
    validation = mapper.validate_dataframe(df_clean)
    print(f"  Errors: {len(validation['errors'])}")
    print(f"  Warnings: {len(validation['warnings'])}")

    # Step 5: Generate schema
    print("\nStep 5: Generate Snowflake schema")
    schema, column_mapping = mapper.generate_schema(
        df_clean,
        standardize_columns=True,
        include_descriptions=False
    )
    print(f"  Schema fields: {len(schema)}")
    print(f"  Column mappings: {len(column_mapping)}")

    # Step 6: Generate DDL
    print("\nStep 6: Generate CREATE TABLE DDL")
    ddl = mapper.generate_ddl(
        df_clean,
        table_name='customers',
        dataset_name='analytics'
    )
    print(f"  DDL generated: {len(ddl)} characters")
    print(f"\n  Sample DDL (first 300 chars):")
    print(f"  {ddl[:300]}...")

    print(f"\n  ✓ Advanced workflow complete!")
    print(f"  ✓ Ready to execute DDL and load data into Snowflake!")


# ============================================================================
# DEMO 6: Profiler Visualizations Integration
# ============================================================================

def demo_profiler_visualizations():
    """Demonstrate using profiler visualizations after profiling."""
    print("\n" + "=" * 80)
    print("DEMO 6: Profiler Visualizations Integration")
    print("=" * 80)

    df = create_messy_customer_data()
    print(f"\nDataset: {len(df)} rows, {len(df.columns)} columns")

    # Profile the data
    print("\nProfiling data with SchemaMapper...")
    mapper = SchemaMapper('bigquery')
    report = mapper.profile_data(df, show_progress=False)

    print(f"  Quality Score: {report['quality']['overall_score']:.2f}/100")

    # Access the profiler for visualizations
    print("\nAccessing profiler for visualizations...")
    profiler = mapper.profiler

    try:
        # Attempt to create visualizations
        print("  Creating visualizations...")

        # These will only work if matplotlib/seaborn are installed
        fig1 = profiler.plot_distributions()
        if fig1:
            print("    ✓ Distribution plots created")
            # fig1.savefig('distributions.png')

        fig2 = profiler.plot_correlations()
        if fig2:
            print("    ✓ Correlation heatmap created")
            # fig2.savefig('correlations.png')

        fig3 = profiler.plot_missing_values()
        if fig3:
            print("    ✓ Missing values plot created")
            # fig3.savefig('missing_values.png')

        print("\n  ✓ Visualizations available for further analysis!")

    except ImportError:
        print("  ℹ Visualizations require matplotlib and seaborn")
        print("    Install with: pip install matplotlib seaborn")


# ============================================================================
# DEMO 7: Multi-Platform Schema Generation
# ============================================================================

def demo_multi_platform():
    """Demonstrate generating schemas for multiple platforms."""
    print("\n" + "=" * 80)
    print("DEMO 7: Multi-Platform Schema Generation")
    print("=" * 80)

    df = create_messy_customer_data()
    print(f"\nDataset: {len(df)} rows, {len(df.columns)} columns")

    # Preprocess once
    print("\nPreprocessing data...")
    preprocessor = PreProcessor(df)
    df_clean = (preprocessor
               .fix_whitespace()
               .standardize_column_names()
               .remove_duplicates()
               .apply())
    print(f"  ✓ Data preprocessed: {len(df_clean)} rows")

    # Generate schemas for multiple platforms
    platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

    print(f"\nGenerating schemas for {len(platforms)} platforms...")
    schemas = {}

    for platform in platforms:
        mapper = SchemaMapper(target_type=platform)
        schema, _ = mapper.generate_schema(df_clean, standardize_columns=False)
        schemas[platform] = schema
        print(f"  ✓ {platform.upper()}: {len(schema)} fields")

    # Show type differences
    print("\nData type mapping differences (first 3 columns):")
    for i in range(min(3, len(schemas['bigquery']))):
        print(f"\n  Column: {schemas['bigquery'][i]['name']}")
        for platform in platforms:
            print(f"    {platform:12s}: {schemas[platform][i]['type']}")

    print(f"\n  ✓ Multi-platform schemas generated!")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Run all demonstrations."""
    print("\n" + "=" * 80)
    print("Schema Mapper - Integrated ETL Workflow Demonstrations")
    print("=" * 80)
    print("\nThese demos showcase the integrated capabilities of SchemaMapper,")
    print("Profiler, and PreProcessor for production-ready ETL pipelines.")

    try:
        demo_basic_integration()
        demo_with_profiling()
        demo_with_preprocessing()
        demo_complete_workflow()
        demo_advanced_step_by_step()
        demo_profiler_visualizations()
        demo_multi_platform()

        print("\n" + "=" * 80)
        print("✓ ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("=" * 80)

        print("\nKey Takeaways:")
        print("  1. prepare_for_load() provides one-stop ETL preparation")
        print("  2. Profiling helps understand data quality before loading")
        print("  3. Preprocessing pipelines clean and standardize data")
        print("  4. SchemaMapper integrates seamlessly with all components")
        print("  5. Multi-platform support with consistent API")
        print("  6. Visualizations available through profiler integration")

        print("\nNext Steps:")
        print("  - Use prepare_for_load() in your ETL pipelines")
        print("  - Profile data to identify quality issues early")
        print("  - Customize preprocessing pipelines for your needs")
        print("  - Generate schemas for any supported platform")

    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
