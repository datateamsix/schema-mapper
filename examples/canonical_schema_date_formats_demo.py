"""
Demonstration of canonical schema with date format integration.

This example shows how to:
1. Define date formats in a canonical schema (single source of truth)
2. Automatically apply those formats during preprocessing
3. Validate data against the schema's format specifications

This makes the canonical schema the true single source of truth for all
data specifications, including date formats.
"""

import pandas as pd
import logging
from datetime import datetime

from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType
from schema_mapper.preprocessor import PreProcessor
from schema_mapper.validators import DataFrameValidator
from schema_mapper import prepare_for_load, SchemaMapper

# Configure logging to see what's happening
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


# ============================================================================
# EXAMPLE 1: Define Canonical Schema with Date Formats
# ============================================================================

print("=" * 80)
print("EXAMPLE 1: Defining Canonical Schema with Date Formats")
print("=" * 80)

# Create a canonical schema that specifies the exact date formats expected
events_schema = CanonicalSchema(
    table_name='events',
    dataset_name='analytics',
    columns=[
        # Event metadata with specific date format
        ColumnDefinition(
            'event_id',
            LogicalType.BIGINT,
            nullable=False,
            description='Unique event identifier'
        ),
        ColumnDefinition(
            'event_date',
            LogicalType.DATE,
            date_format='%d/%m/%Y',  # European format DD/MM/YYYY
            description='Date when event occurred'
        ),
        ColumnDefinition(
            'created_at',
            LogicalType.TIMESTAMP,
            date_format='%d/%m/%Y %H:%M',  # European format with time
            timezone='UTC',
            description='Timestamp when record was created'
        ),
        # Other fields
        ColumnDefinition('event_type', LogicalType.STRING),
        ColumnDefinition('user_id', LogicalType.INTEGER),
        ColumnDefinition('amount', LogicalType.DECIMAL, precision=10, scale=2)
    ],
    description='Analytics events table with European date format'
)

print("\n[CANONICAL SCHEMA] Schema Defined:")
print(f"   Table: {events_schema.table_name}")
print(f"   Columns: {len(events_schema.columns)}")

for col in events_schema.columns:
    format_info = f" (format: {col.date_format})" if col.date_format else ""
    tz_info = f" [TZ: {col.timezone}]" if col.timezone else ""
    print(f"   - {col.name}: {col.logical_type.value}{format_info}{tz_info}")

# Validate schema
schema_errors = events_schema.validate()
if schema_errors:
    print(f"\n[ERROR] Schema validation errors: {schema_errors}")
else:
    print("\n[SUCCESS] Schema is valid!")


# ============================================================================
# EXAMPLE 2: Create Sample Data (with dates in various formats)
# ============================================================================

print("\n" + "=" * 80)
print("EXAMPLE 2: Sample Data with Mixed Date Formats")
print("=" * 80)

# Simulate messy data with dates in different formats
messy_data = pd.DataFrame({
    'event_id': [1001, 1002, 1003, 1004, 1005],
    'event_date': [
        '15/01/2024',  # DD/MM/YYYY - matches schema
        '20-02-2024',  # DD-MM-YYYY - close but different separator
        '2024-03-10',  # YYYY-MM-DD - ISO format
        '05/04/2024',  # DD/MM/YYYY - matches schema
        '12/05/2024'   # DD/MM/YYYY - matches schema
    ],
    'created_at': [
        '15/01/2024 10:30',  # Matches schema
        '20/02/2024 14:45',  # Matches schema
        '10/03/2024 08:15',  # Matches schema
        '05/04/2024 16:20',  # Matches schema
        '12/05/2024 09:00'   # Matches schema
    ],
    'event_type': ['signup', 'purchase', 'login', 'purchase', 'logout'],
    'user_id': [101, 102, 103, 104, 105],
    'amount': [0.00, 49.99, 0.00, 29.99, 0.00]
})

print("\n[DATA] Original messy data (mixed date formats):")
print(messy_data.head())
print(f"\nData types:\n{messy_data.dtypes}")


# ============================================================================
# EXAMPLE 3: Apply Schema Formats Using PreProcessor
# ============================================================================

print("\n" + "=" * 80)
print("EXAMPLE 3: Applying Date Formats from Canonical Schema")
print("=" * 80)

# Create preprocessor with canonical schema
preprocessor = PreProcessor(messy_data, canonical_schema=events_schema)

# Apply the schema's date formats automatically
print("\n[PROCESSING] Applying schema formats...")
df_formatted = preprocessor.apply_schema_formats().apply()

print("\n[SUCCESS] Data after applying schema formats:")
print(df_formatted.head())
print(f"\nData types:\n{df_formatted.dtypes}")


# ============================================================================
# EXAMPLE 4: Validate Data Against Schema
# ============================================================================

print("\n" + "=" * 80)
print("EXAMPLE 4: Validating Data Against Canonical Schema")
print("=" * 80)

# Create data that matches the schema format
good_data = pd.DataFrame({
    'event_id': [2001, 2002, 2003],
    'event_date': ['15/01/2024', '20/02/2024', '10/03/2024'],
    'created_at': ['15/01/2024 10:30', '20/02/2024 14:45', '10/03/2024 08:15'],
    'event_type': ['login', 'purchase', 'logout'],
    'user_id': [201, 202, 203],
    'amount': [0.00, 99.99, 0.00]
})

# Validate with schema
validator = DataFrameValidator()
validation_result = validator.validate_with_schema(good_data, events_schema)

print("\n[SCHEMA] Validation result for data matching schema format:")
print(f"   Errors: {len(validation_result.errors)}")
print(f"   Warnings: {len(validation_result.warnings)}")

if validation_result.has_errors():
    print("\n[ERROR] Validation errors:")
    for error in validation_result.errors:
        print(f"   - {error}")
else:
    print("\n[SUCCESS] No validation errors - data matches schema format!")

if validation_result.has_warnings():
    print("\n[WARNING]  Warnings:")
    for warning in validation_result.warnings:
        print(f"   - {warning}")


# Now test with data that doesn't match format
print("\n" + "-" * 80)

bad_data = pd.DataFrame({
    'event_id': [3001, 3002, 3003],
    'event_date': ['2024-01-15', '2024-02-20', '2024-03-10'],  # Wrong format (YYYY-MM-DD)
    'created_at': ['2024-01-15 10:30:00', '2024-02-20 14:45:00', '2024-03-10 08:15:00'],  # Wrong format
    'event_type': ['login', 'purchase', 'logout'],
    'user_id': [301, 302, 303],
    'amount': [0.00, 79.99, 0.00]
})

validation_result = validator.validate_with_schema(bad_data, events_schema)

print("\n[SCHEMA] Validation result for data NOT matching schema format:")
print(f"   Errors: {len(validation_result.errors)}")
print(f"   Warnings: {len(validation_result.warnings)}")

if validation_result.has_errors():
    print("\n[ERROR] Validation errors (as expected):")
    for error in validation_result.errors:
        print(f"   - {error}")


# ============================================================================
# EXAMPLE 5: End-to-End Workflow with prepare_for_load()
# ============================================================================

print("\n" + "=" * 80)
print("EXAMPLE 5: Complete Workflow with prepare_for_load()")
print("=" * 80)

# Create sample data with some quality issues
workflow_data = pd.DataFrame({
    'event_id': [4001, 4002, 4003, 4003, 4004],  # Duplicate 4003
    'event_date': ['15/01/2024', '20/02/2024', '10/03/2024', '10/03/2024', '05/04/2024'],
    'created_at': ['15/01/2024 10:30', '20/02/2024 14:45', '10/03/2024 08:15', '10/03/2024 08:15', '05/04/2024 16:20'],
    'event_type': ['  login  ', 'purchase', 'logout', 'logout', 'signup'],  # Whitespace
    'user_id': [401, 402, 403, 403, 404],
    'amount': [0.00, 99.99, 0.00, 0.00, 0.00]
})

print("\n[DATA] Original data (with duplicates and whitespace):")
print(workflow_data)

# Use prepare_for_load() with canonical schema
print("\n[PROCESSING] Running prepare_for_load() with canonical schema and preprocessing...")

df_prepared, schema, issues = prepare_for_load(
    workflow_data,
    target_type='bigquery',
    canonical_schema=events_schema,  # Our date format schema
    preprocess_pipeline=[
        'fix_whitespace',
        'remove_duplicates'
    ],
    validate=True
)

print("\n[SUCCESS] Prepared data:")
print(df_prepared)

print(f"\n[SCHEMA] Generated schema: {len(schema)} columns")
for field in schema[:3]:  # Show first 3 fields
    print(f"   - {field}")

print(f"\n[DATA] Validation issues:")
print(f"   Errors: {len(issues['errors'])}")
print(f"   Warnings: {len(issues['warnings'])}")

if issues['errors']:
    for error in issues['errors']:
        print(f"   [ERROR] {error}")

if issues['warnings']:
    for warning in issues['warnings'][:5]:  # Show first 5 warnings
        print(f"   [WARNING]  {warning}")


# ============================================================================
# EXAMPLE 6: Using SchemaMapper with Canonical Schema
# ============================================================================

print("\n" + "=" * 80)
print("EXAMPLE 6: Using SchemaMapper with Canonical Schema")
print("=" * 80)

mapper_data = pd.DataFrame({
    'event_id': [5001, 5002, 5003],
    'event_date': ['15/01/2024', '20/02/2024', '10/03/2024'],
    'created_at': ['15/01/2024 10:30', '20/02/2024 14:45', '10/03/2024 08:15'],
    'event_type': ['login', 'purchase', 'logout'],
    'user_id': [501, 502, 503],
    'amount': [0.00, 149.99, 0.00]
})

# Create mapper
mapper = SchemaMapper('bigquery')

# Preprocess with canonical schema
print("\n[PROCESSING] Preprocessing with canonical schema...")
df_clean = mapper.preprocess_data(
    mapper_data,
    canonical_schema=events_schema
)

print("\n[SUCCESS] Cleaned data:")
print(df_clean)


# ============================================================================
# EXAMPLE 7: Different Date Format Schema (US Format)
# ============================================================================

print("\n" + "=" * 80)
print("EXAMPLE 7: Alternative Schema with US Date Format")
print("=" * 80)

# Create schema with US date format
us_events_schema = CanonicalSchema(
    table_name='events_us',
    columns=[
        ColumnDefinition('event_id', LogicalType.BIGINT),
        ColumnDefinition('event_date', LogicalType.DATE, date_format='%m/%d/%Y'),  # US format MM/DD/YYYY
        ColumnDefinition('created_at', LogicalType.TIMESTAMP, date_format='%m/%d/%Y %H:%M:%S'),
        ColumnDefinition('event_type', LogicalType.STRING),
        ColumnDefinition('user_id', LogicalType.INTEGER)
    ],
    description='Events table with US date format'
)

print("\n[SCHEMA] US Format Schema:")
for col in us_events_schema.columns:
    if col.date_format:
        print(f"   - {col.name}: {col.date_format}")

# Sample US formatted data
us_data = pd.DataFrame({
    'event_id': [6001, 6002],
    'event_date': ['01/15/2024', '02/20/2024'],  # MM/DD/YYYY
    'created_at': ['01/15/2024 10:30:00', '02/20/2024 14:45:00'],
    'event_type': ['login', 'purchase'],
    'user_id': [601, 602]
})

print("\n[DATA] US formatted data:")
print(us_data)

# Process with US schema
preprocessor_us = PreProcessor(us_data, canonical_schema=us_events_schema)
df_us_formatted = preprocessor_us.apply_schema_formats().apply()

print("\n[SUCCESS] After applying US date format schema:")
print(df_us_formatted)


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("SUMMARY: Benefits of Canonical Schema Date Format Integration")
print("=" * 80)

print("""
[INFO] Key Benefits:

1. [SCHEMA] Single Source of Truth
   - Define date formats once in the canonical schema
   - No need to specify formats in multiple places

2. [PROCESSING] Automatic Format Application
   - Preprocessing automatically applies schema formats
   - No manual format specification required

3. [SUCCESS] Built-in Validation
   - Validators check data against schema specifications
   - Catch format mismatches before loading

4. [GLOBAL] Flexible Format Support
   - Support any date format (US, EU, ISO8601, custom)
   - Easy to change format by updating schema

5. [WORKFLOW] Seamless Integration
   - Works with prepare_for_load()
   - Works with SchemaMapper
   - Works with PreProcessor directly

Example Workflow:
1. Define canonical schema with date_format specifications
2. Call prepare_for_load(df, canonical_schema=schema)
3. Date formats are automatically applied during preprocessing
4. Data is validated against schema specifications
5. Ready to load with confidence!
""")

print("=" * 80)
print("Demo complete! [INFO]")
print("=" * 80)
