"""
Basic usage example for schema-mapper.

This example demonstrates the most common use case: preparing
data for loading into BigQuery.
"""

import pandas as pd
from schema_mapper import prepare_for_load, SchemaMapper


def main():
    # Create sample data
    df = pd.DataFrame({
        'User ID': [1, 2, 3, 4, 5],
        'First Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'Email Address': ['alice@example.com', 'bob@example.com', 
                         'charlie@example.com', 'david@example.com', 'eve@example.com'],
        'Created At': ['2024-01-01', '2024-01-02', '2024-01-03', 
                      '2024-01-04', '2024-01-05'],
        'Is Active': ['yes', 'no', 'yes', 'yes', 'no'],
        'Account Balance': ['1000.50', '2500.75', '100.00', '5000.25', '750.00']
    })
    
    print("Original DataFrame:")
    print(df)
    print(f"\nOriginal dtypes:\n{df.dtypes}")
    
    # Prepare data for BigQuery
    print("\n" + "="*60)
    print("Preparing data for BigQuery...")
    print("="*60)
    
    df_clean, schema, issues = prepare_for_load(
        df,
        target_type='bigquery',
        standardize_columns=True,
        auto_cast=True,
        validate=True
    )
    
    # Check for issues
    if issues['errors']:
        print("\n❌ Errors found:")
        for error in issues['errors']:
            print(f"  - {error}")
    
    if issues['warnings']:
        print("\n⚠️  Warnings:")
        for warning in issues['warnings']:
            print(f"  - {warning}")
    
    if not issues['errors']:
        print("\n✅ Data prepared successfully!")
        print(f"\nCleaned DataFrame:")
        print(df_clean)
        print(f"\nNew dtypes:\n{df_clean.dtypes}")
        
        print(f"\n📋 Generated Schema ({len(schema)} fields):")
        for field in schema:
            print(f"  - {field['name']}: {field['type']} ({field['mode']})")
        
        # Generate BigQuery schema JSON
        mapper = SchemaMapper('bigquery')
        schema_json = mapper.generate_bigquery_schema_json(df)
        
        print("\n💾 Saving files...")
        df_clean.to_csv('data_clean.csv', index=False)
        with open('schema.json', 'w') as f:
            f.write(schema_json)
        
        print("  ✓ data_clean.csv")
        print("  ✓ schema.json")
        
        print("\n📝 Next steps:")
        print("  1. Upload data_clean.csv to your system")
        print("  2. Use schema.json with bq CLI:")
        print("     bq mk --table --schema schema.json project:dataset.table")


if __name__ == '__main__':
    main()
