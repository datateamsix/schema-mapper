"""
Multi-platform example for schema-mapper.

This example shows how to generate schemas and DDL for all
supported platforms from the same DataFrame.
"""

import pandas as pd
from schema_mapper import SchemaMapper


def main():
    # Create sample data
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'created_at': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'is_active': [True, False, True],
        'balance': [1000.50, 2500.75, 100.00]
    })
    
    print("Sample DataFrame:")
    print(df)
    
    platforms = ['bigquery', 'snowflake', 'redshift', 'sqlserver', 'postgresql']
    
    for platform in platforms:
        print(f"\n\n{'='*70}")
        print(f"Platform: {platform.upper()}")
        print('='*70)
        
        mapper = SchemaMapper(platform)
        
        # Generate schema
        schema, mapping = mapper.generate_schema(df, standardize_columns=False)
        
        print(f"\n📋 Schema ({len(schema)} fields):")
        for field in schema:
            mode_str = f" [{field['mode']}]" if field.get('mode') else ""
            print(f"  {field['name']}: {field['type']}{mode_str}")
        
        # Generate DDL
        ddl = mapper.generate_ddl(
            df,
            table_name='users',
            dataset_name='analytics',
            project_id='my-project' if platform == 'bigquery' else None
        )
        
        print(f"\n📝 DDL (first 300 chars):")
        print(ddl[:300] + "...")
        
        # Save DDL to file
        filename = f'create_users_{platform}.sql'
        with open(filename, 'w') as f:
            f.write(ddl)
        print(f"\n💾 Saved to: {filename}")
    
    print("\n\n" + "="*70)
    print("✅ All platforms processed successfully!")
    print("="*70)
    
    print("\n📁 Generated files:")
    for platform in platforms:
        print(f"  - create_users_{platform}.sql")


if __name__ == '__main__':
    main()
