<img src="https://raw.githubusercontent.com/datateamsix/schema-mapper/main/images/sm-logo.png" alt="Schema Mapper Logo" width="200"/>

# schema-mapper

[![PyPI version](https://badge.fury.io/py/schema-mapper.svg)](https://badge.fury.io/py/schema-mapper)
[![Python Support](https://img.shields.io/pypi/pyversions/schema-mapper.svg)](https://pypi.org/project/schema-mapper/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Universal database schema mapper for BigQuery, Snowflake, Redshift, SQL Server, and PostgreSQL**

Automatically generate schemas, DDL statements, and prepare your data for loading into any major database platform. Perfect for data engineers working across multiple cloud providers.

## Features

- **5 Platform Support**: BigQuery, Snowflake, Redshift, SQL Server, PostgreSQL
- **Automatic Type Detection**: Intelligently converts strings to dates, numbers, booleans
- **Column Standardization**: Cleans messy column names for database compatibility
- **Data Validation**: Pre-load validation to catch errors early
- **Schema Generation**: JSON/DDL formats ready for CLI or API usage
- **NULL Handling**: Automatically determines REQUIRED vs NULLABLE
- **DDL Generation**: Platform-specific CREATE TABLE statements
- **Table Optimization**: Clustering, partitioning, and distribution strategies
- **Incremental Loads**: 9 production-ready load patterns (UPSERT, SCD2, CDC, etc.) (NEW!)
- **Data Profiling**: Comprehensive quality assessment, anomaly detection, pattern recognition (NEW!)
- **Data Preprocessing**: Intelligent cleaning, transformation, and validation pipelines (NEW!)
- **Canonical Schema Date Formats**: Define date formats once, apply everywhere (NEW!)
- **High Performance**: Efficiently handles datasets from 1K to 1M+ rows

## Architecture

<div align="center">
  <img src="https://raw.githubusercontent.com/datateamsix/schema-mapper/main/images/schema-mapper-architecture.png" alt="Schema Mapper Architecture" width="800"/>
</div>

The schema-mapper uses a **canonical schema** approach: infer once, render to any platform. This ensures consistent type mapping and optimization strategies across all supported databases.

<div align="center">
  <img src="https://raw.githubusercontent.com/datateamsix/schema-mapper/main/images/canonical-schema.png" alt="Canonical Schema Flow" width="700"/>
</div>

## Installation

```bash
# Basic installation
pip install schema-mapper

# With platform-specific dependencies
pip install schema-mapper[bigquery]
pip install schema-mapper[snowflake]
pip install schema-mapper[redshift]
pip install schema-mapper[sqlserver]
pip install schema-mapper[postgresql]

# Install all platform dependencies
pip install schema-mapper[all]
```

## Quick Start

```python
from schema_mapper import prepare_for_load
import pandas as pd

# Load your messy data
df = pd.read_csv('messy_data.csv')

# Prepare for ANY platform in one line!
df_clean, schema, issues = prepare_for_load(
    df,
    target_type='bigquery',  # or 'snowflake', 'redshift', 'sqlserver', 'postgresql'
)

# Check for issues
if not issues['errors']:
    print(f"SUCCESS: {len(schema)} columns prepared and ready to load!")
else:
    print("ERROR: Fix these errors:", issues['errors'])
```

## Data Quality & Preprocessing (NEW!)

### Profile Your Data

```python
from schema_mapper import SchemaMapper
import pandas as pd

df = pd.read_csv('messy_data.csv')
mapper = SchemaMapper('bigquery')

# Generate comprehensive data quality report
report = mapper.profile_data(df, detailed=True)

print(f"Overall Quality Score: {report['quality']['overall_score']}/100")
print(f"Completeness: {report['quality']['completeness_score']:.1f}%")
print(f"Missing Values: {report['missing_values']['total_missing_percentage']:.1f}%")
print(f"Duplicates: {report['duplicates']['count']} rows")

# Detect anomalies
if report['anomalies']:
    print("\nAnomalies detected:")
    for col, info in report['anomalies'].items():
        print(f"  {col}: {info['count']} outliers ({info['percentage']:.1f}%)")

# Detect patterns (emails, phone numbers, URLs, etc.)
if report['patterns']:
    for col, patterns in report['patterns'].items():
        print(f"\nPatterns in {col}:")
        for pattern, pct in patterns.items():
            print(f"  {pattern}: {pct:.1f}%")
```

### Clean and Transform Data

```python
from schema_mapper import SchemaMapper
import pandas as pd

df = pd.read_csv('messy_data.csv')
mapper = SchemaMapper('bigquery')

# Apply preprocessing pipeline
df_clean = mapper.preprocess_data(
    df,
    pipeline=[
        'fix_whitespace',           # Remove leading/trailing whitespace
        'standardize_column_names', # Convert to snake_case
        'remove_duplicates',        # Remove duplicate rows
        'handle_missing',           # Handle missing values intelligently
        'validate_emails',          # Validate email formats
        'standardize_dates'         # Standardize date formats
    ]
)

# Or use PreProcessor directly for fine-grained control
from schema_mapper.preprocessor import PreProcessor

preprocessor = PreProcessor(df)
df_clean = (preprocessor
    .fix_whitespace()
    .standardize_column_names()
    .validate_emails(columns=['email'])
    .standardize_dates(columns=['created_at'], target_format='%Y-%m-%d')
    .remove_duplicates()
    .handle_missing(strategy='auto')
    .apply())

# Check transformation log
print("Transformations applied:")
for transform in preprocessor.transformation_log:
    print(f"  - {transform}")
```

### Canonical Schema with Date Formats (Single Source of Truth)

```python
from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType
from schema_mapper import prepare_for_load
import pandas as pd

# Define your schema with date formats ONCE
schema = CanonicalSchema(
    table_name='events',
    columns=[
        ColumnDefinition('event_id', LogicalType.BIGINT, nullable=False),
        ColumnDefinition(
            'event_date',
            LogicalType.DATE,
            date_format='%d/%m/%Y'  # European format - defined once!
        ),
        ColumnDefinition(
            'created_at',
            LogicalType.TIMESTAMP,
            date_format='%d/%m/%Y %H:%M:%S',
            timezone='UTC'
        ),
        ColumnDefinition('user_id', LogicalType.INTEGER),
        ColumnDefinition('amount', LogicalType.DECIMAL, precision=10, scale=2)
    ]
)

# Prepare data - date formats applied automatically!
df_prepared, db_schema, issues = prepare_for_load(
    df,
    'bigquery',
    canonical_schema=schema  # Date formats applied and validated!
)

# Benefits:
# ✓ Define date format ONCE in canonical schema
# ✓ Automatically applied during preprocessing
# ✓ Automatically validated against data
# ✓ No manual date formatting code needed
```

### Complete ETL with Profiling and Preprocessing

```python
from schema_mapper import prepare_for_load
import pandas as pd

df = pd.read_csv('messy_data.csv')

# Profile, clean, validate, and generate schema in one call
df_clean, schema, issues, report = prepare_for_load(
    df,
    'bigquery',
    profile=True,  # Generate quality report
    preprocess_pipeline=[
        'fix_whitespace',
        'standardize_column_names',
        'remove_duplicates',
        'handle_missing'
    ],
    validate=True
)

print(f"Data Quality Score: {report['quality']['overall_score']}/100")
print(f"Cleaned {len(df) - len(df_clean)} rows")
print(f"Generated schema with {len(schema)} columns")

if not issues['errors']:
    print("✓ Ready to load!")
    # df_clean.to_gbq('dataset.table', project_id='my-project')
```

## Incremental Loads (Production-Ready)

Generate optimized DDL for incremental data loading patterns across all platforms. Perfect for production ETL/ELT pipelines.

### Supported Load Patterns

- **UPSERT (MERGE)** - Insert new, update existing records
- **SCD Type 2** - Full history tracking with versioning
- **CDC (Change Data Capture)** - Process insert/update/delete streams
- **Incremental Timestamp** - Load only recent records
- **Append Only** - Add new records without updates
- **Delete-Insert** - Transactional upsert alternative
- **Full Refresh** - Complete table reload
- **SCD Type 1** - Current state only, no history
- **Snapshot** - Point-in-time snapshots with metadata

### Quick Example

```python
from schema_mapper import SchemaMapper, IncrementalConfig, LoadPattern
import pandas as pd

df = pd.read_csv('users.csv')
mapper = SchemaMapper('bigquery')

# Configure UPSERT pattern
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id']
)

# Generate MERGE DDL
ddl = mapper.generate_incremental_ddl(
    df=df,
    table_name='users',
    config=config,
    dataset_name='analytics',
    project_id='my-project'
)

print(ddl)
# Outputs platform-specific MERGE statement
```

### SCD Type 2 Example

```python
# Track full history with SCD Type 2
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['customer_id'],
    effective_date_column='effective_date',
    end_date_column='end_date',
    is_current_column='is_current'
)

ddl = mapper.generate_incremental_ddl(df, 'customers', config)
```

### Platform Support

| Pattern | BigQuery | Snowflake | Redshift | SQL Server | PostgreSQL |
|---------|----------|-----------|----------|------------|------------|
| UPSERT (MERGE) | ✓ Native | ✓ Native | ✓ Via DELETE+INSERT | ✓ Native | ✓ Native |
| SCD Type 2 | ✓ | ✓ | ✓ | ✓ | ✓ |
| CDC | ✓ | ✓ | ✓ | ✓ | ✓ |
| Incremental Timestamp | ✓ | ✓ | ✓ | ✓ | ✓ |
| All Patterns | ✓ | ✓ | ✓ | ✓ | ✓ |

**For complete documentation, patterns, and examples**: See [docs/INCREMENTAL_LOADS.md](docs/INCREMENTAL_LOADS.md)

## Usage Examples

**Complete Example Scripts:**
- [basic_usage.py](schema-mapper-pkg/examples/basic_usage.py) - Simple schema generation workflow
- [multi_platform.py](schema-mapper-pkg/examples/multi_platform.py) - Generate for all platforms at once
- [production_analytics_pipeline.py](schema-mapper-pkg/examples/production_analytics_pipeline.py) - **Production use case with clustering & partitioning**
- [ddl_with_clustering_examples.py](schema-mapper-pkg/examples/ddl_with_clustering_examples.py) - All platform optimization examples
- [canonical_schema_usage.py](schema-mapper-pkg/examples/canonical_schema_usage.py) - **New renderer architecture** (canonical schema → multiple outputs)
- [profiler_demo.py](schema-mapper-pkg/examples/profiler_demo.py) - **Data profiling and quality assessment** (NEW!)
- [preprocessor_demo.py](schema-mapper-pkg/examples/preprocessor_demo.py) - **Data cleaning and transformation** (NEW!)
- [canonical_schema_date_formats_demo.py](schema-mapper-pkg/examples/canonical_schema_date_formats_demo.py) - **Canonical schema with date formats** (NEW!)

### Generate Schema

```python
from schema_mapper import SchemaMapper
import pandas as pd

df = pd.read_csv('data.csv')
mapper = SchemaMapper('bigquery')

# Generate schema
schema, column_mapping = mapper.generate_schema(df)

# See column transformations
print(column_mapping)
# {'User ID': 'user_id', 'First Name': 'first_name', ...}
```

### Generate DDL

```python
from schema_mapper import SchemaMapper
import pandas as pd

df = pd.read_csv('data.csv')

# BigQuery
mapper = SchemaMapper('bigquery')
ddl = mapper.generate_ddl(df, 'customers', 'analytics', 'my-project')

# Snowflake
mapper = SchemaMapper('snowflake')
ddl = mapper.generate_ddl(df, 'customers', 'analytics')

# PostgreSQL
mapper = SchemaMapper('postgresql')
ddl = mapper.generate_ddl(df, 'customers', 'public')

print(ddl)
```

### Generate Optimized DDL with Clustering & Partitioning

```python
from schema_mapper.generators import get_ddl_generator
from schema_mapper import SchemaMapper
import pandas as pd

df = pd.read_csv('events.csv')

# Generate schema first
mapper = SchemaMapper('bigquery')
schema, _ = mapper.generate_schema(df)

# BigQuery: Partitioned by date, clustered by user_id
generator = get_ddl_generator('bigquery')
ddl = generator.generate(
    schema=schema,
    table_name='events',
    dataset_name='analytics',
    project_id='my-project',
    partition_by='event_date',
    partition_type='time',
    partition_expiration_days=365,
    cluster_by=['user_id', 'event_type']
)

print(ddl)
# Output:
# CREATE TABLE `my-project.analytics.events` (
#   event_id INT64,
#   user_id INT64,
#   event_date DATE
# )
# PARTITION BY event_date
# CLUSTER BY user_id, event_type
# OPTIONS(
#   partition_expiration_days=365
# );
```

### Generate BigQuery Schema JSON

```python
from schema_mapper import SchemaMapper
import pandas as pd

df = pd.read_csv('data.csv')
mapper = SchemaMapper('bigquery')

# Generate schema JSON for bq CLI
schema_json = mapper.generate_bigquery_schema_json(df)

# Save to file
with open('schema.json', 'w') as f:
    f.write(schema_json)

# Use with bq CLI
# bq mk --table --schema schema.json project:dataset.table
```

### Complete ETL Workflow

```python
from schema_mapper import prepare_for_load, SchemaMapper
import pandas as pd

# 1. Load data
df = pd.read_csv('customer_data.csv')

# 2. Prepare and validate
df_clean, schema, issues = prepare_for_load(
    df,
    target_type='bigquery',
    standardize_columns=True,
    auto_cast=True,
    validate=True
)

# 3. Check issues
if issues['errors']:
    print("ERRORS:")
    for error in issues['errors']:
        print(f"  - {error}")
    exit(1)

if issues['warnings']:
    print("WARNINGS:")
    for warning in issues['warnings']:
        print(f"  - {warning}")

# 4. Generate artifacts
mapper = SchemaMapper('bigquery')

# Save cleaned data
df_clean.to_csv('customers_clean.csv', index=False)

# Save schema
schema_json = mapper.generate_bigquery_schema_json(df)
with open('customers_schema.json', 'w') as f:
    f.write(schema_json)

# Save DDL
ddl = mapper.generate_ddl(df, 'customers', 'analytics', 'my-project')
with open('create_customers.sql', 'w') as f:
    f.write(ddl)

print("SUCCESS: Ready for loading!")
```

## Table Optimization Features

### Platform Capabilities

| Feature | BigQuery | Snowflake | Redshift | SQL Server | PostgreSQL |
|---------|----------|-----------|----------|------------|------------|
| **Partitioning** | ✓ DATE/TIMESTAMP/RANGE | ~ Auto Micro | ✗ | ✗ | ✓ RANGE/LIST/HASH |
| **Clustering** | ✓ Up to 4 cols | ✓ Up to 4 cols | ✗ | ✓ Clustered Index | ✓ Via Indexes |
| **Distribution** | ✗ | ✗ | ✓ KEY/ALL/EVEN/AUTO | ✗ | ✗ |
| **Sort Keys** | ✗ | ✗ | ✓ Compound/Interleaved | ✗ | ✗ |
| **Columnstore** | ✗ | ✗ | ✗ | ✓ Analytics | ✗ |
| **CREATE OR REPLACE** | ✗ | ✓ Native | ~ via DROP | ~ via DROP | ~ via DROP |

### Quick Examples

```python
from schema_mapper.generators import get_ddl_generator

# BigQuery: Partitioned + Clustered
generator = get_ddl_generator('bigquery')
ddl = generator.generate(
    schema=schema,
    table_name='events',
    dataset_name='analytics',
    partition_by='event_date',
    partition_type='time',
    cluster_by=['user_id', 'event_type']
)

# Redshift: Distributed + Sorted
generator = get_ddl_generator('redshift')
ddl = generator.generate(
    schema=schema,
    table_name='events',
    dataset_name='analytics',
    distribution_style='key',
    distribution_key='user_id',
    sort_keys=['event_date', 'event_ts']
)

# Snowflake: Clustered + Transient
generator = get_ddl_generator('snowflake')
ddl = generator.generate(
    schema=schema,
    table_name='staging_events',
    dataset_name='staging',
    cluster_by=['event_date', 'user_id'],
    transient=True,  # For staging tables
    create_or_replace=True  # Native support
)

# SQL Server: Clustered Index + Columnstore
generator = get_ddl_generator('sqlserver')
ddl = generator.generate(
    schema=schema,
    table_name='events',
    dataset_name='analytics',
    clustered_index=['event_id'],
    columnstore=True  # For analytics workloads
)

# PostgreSQL: Range Partitioned + Clustered
generator = get_ddl_generator('postgresql')
ddl = generator.generate(
    schema=schema,
    table_name='events',
    dataset_name='public',
    partition_by='event_date',
    partition_type='range',
    cluster_by=['event_date', 'user_id']  # Creates index
)
```

See [examples/production_analytics_pipeline.py](schema-mapper-pkg/examples/production_analytics_pipeline.py) for complete use cases.

### Unified Generator Architecture

The DDL generator architecture has been consolidated for simplicity and maintainability. The enhanced features (clustering, partitioning, distribution) are now integrated into the base generators.

**What Changed:**
- `generators.py` - Unified DDL generation with all features
- `generators_enhanced.py` - **DEPRECATED** (consolidated into generators.py)
- All examples and documentation updated to use the unified API

**Migration:**
```python
# Old API (still works but deprecated)
from schema_mapper.generators_enhanced import BigQueryEnhancedDDLGenerator

# New API (recommended)
from schema_mapper.generators import get_ddl_generator
generator = get_ddl_generator('bigquery')
```

### New Renderer Architecture (Canonical Schema)

```python
from schema_mapper.canonical import infer_canonical_schema
from schema_mapper.renderers import RendererFactory

# Step 1: Create canonical schema (platform-agnostic)
canonical = infer_canonical_schema(
    df,
    table_name='events',
    dataset_name='analytics',
    partition_columns=['event_date'],
    cluster_columns=['user_id', 'event_type']
)

# Step 2: Get platform-specific renderer
renderer = RendererFactory.get_renderer('bigquery', canonical)

# Step 3: Generate all artifacts
ddl = renderer.to_ddl()                          # CREATE TABLE statement
create_cmd = renderer.to_cli_create()            # CLI command to create
load_cmd = renderer.to_cli_load('data.csv')     # CLI command to load

# BigQuery also supports JSON schema
if renderer.supports_json_schema():
    json_schema = renderer.to_schema_json()      # JSON for bq load

# Step 4: Multi-platform generation
for platform in ['bigquery', 'snowflake', 'redshift', 'postgresql']:
    renderer = RendererFactory.get_renderer(platform, canonical)
    print(f"{platform} DDL:", renderer.to_ddl())
```

**Benefits:**
- **One Schema, Many Outputs** - Canonical schema → DDL, JSON, CLI commands
- **Platform Reality** - JSON only where natively supported (BigQuery)
- **Clean Architecture** - Renderer pattern, easy to extend
- **Type Safety** - Logical types converted to physical types per platform

See [examples/canonical_schema_usage.py](schema-mapper-pkg/examples/canonical_schema_usage.py) and [ARCHITECTURE.md](schema-mapper-pkg/ARCHITECTURE.md) for details.

## Type Mapping

| Pandas Type | BigQuery | Snowflake | Redshift | SQL Server | PostgreSQL |
|-------------|----------|-----------|----------|------------|------------|
| int64 | INTEGER | NUMBER(38,0) | BIGINT | BIGINT | BIGINT |
| float64 | FLOAT | FLOAT | DOUBLE PRECISION | FLOAT | DOUBLE PRECISION |
| object | STRING | VARCHAR(16MB) | VARCHAR(64KB) | NVARCHAR(MAX) | TEXT |
| datetime64[ns] | TIMESTAMP | TIMESTAMP_NTZ | TIMESTAMP | DATETIME2 | TIMESTAMP |
| bool | BOOLEAN | BOOLEAN | BOOLEAN | BIT | BOOLEAN |

## API Reference

### `SchemaMapper`

Main class for schema generation.

```python
mapper = SchemaMapper(target_type='bigquery')
```

**Methods:**
- `generate_schema(df, ...)` - Generate schema from DataFrame
- `generate_ddl(df, table_name, ...)` - Generate CREATE TABLE DDL
- `generate_incremental_ddl(df, table_name, config, ...)` - **NEW!** Generate incremental load DDL (MERGE, SCD2, CDC, etc.)
- `prepare_dataframe(df, ...)` - Clean and prepare DataFrame
- `validate_dataframe(df, ...)` - Validate DataFrame quality
- `generate_bigquery_schema_json(df, ...)` - Generate BigQuery JSON schema
- `profile_data(df, detailed=True, show_progress=True)` - **NEW!** Generate data quality report
- `preprocess_data(df, pipeline=None, canonical_schema=None)` - **NEW!** Clean and transform data

### `Profiler` (NEW!)

Comprehensive data profiling and quality assessment.

```python
from schema_mapper.profiler import Profiler

profiler = Profiler(df, name='my_dataset')
report = profiler.generate_report(output_format='dict')
```

**Methods:**
- `profile_dataset()` - Profile entire dataset
- `profile_column(col_name)` - Profile specific column
- `assess_quality()` - Calculate quality scores
- `detect_anomalies(method='iqr')` - Detect outliers
- `detect_patterns()` - Detect emails, phones, URLs, etc.
- `analyze_missing_values()` - Analyze missing data
- `find_correlations()` - Find correlated columns
- `analyze_distributions()` - Analyze data distributions
- `generate_report(output_format='dict')` - Generate full report

### `PreProcessor` (NEW!)

Intelligent data cleaning and transformation.

```python
from schema_mapper.preprocessor import PreProcessor

preprocessor = PreProcessor(df, canonical_schema=schema)
df_clean = preprocessor.fix_whitespace().standardize_column_names().apply()
```

**Methods:**
- `fix_whitespace(columns=None, strategy='trim')` - Remove whitespace
- `standardize_column_names()` - Convert to snake_case
- `standardize_dates(columns=None, target_format='ISO8601')` - Standardize dates
- `validate_emails(columns)` - Validate email addresses
- `validate_phone_numbers(columns)` - Validate phone numbers
- `remove_duplicates(subset=None, keep='first')` - Remove duplicates
- `handle_missing(strategy='auto', columns=None)` - Handle missing values
- `one_hot_encode(columns, drop_original=True)` - One-hot encode categoricals
- `apply_schema_formats()` - **NEW!** Apply formats from canonical schema
- `create_pipeline(operations)` - Create preprocessing pipeline
- `apply()` - Apply all transformations

### `IncrementalConfig` (NEW!)

Configure incremental load patterns.

```python
from schema_mapper import IncrementalConfig, LoadPattern, MergeStrategy

config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id'],
    merge_strategy=MergeStrategy.UPDATE_ALL
)
```

**Parameters:**
- `load_pattern` - LoadPattern enum (UPSERT, SCD_TYPE2, CDC, etc.)
- `primary_keys` - List of primary key columns
- `merge_strategy` - MergeStrategy enum (UPDATE_ALL, UPDATE_SELECTIVE, etc.)
- `update_columns` - Columns to update (for UPDATE_SELECTIVE)
- `incremental_column` - Column for timestamp-based incremental loads
- `effective_date_column` - Effective date for SCD Type 2
- `end_date_column` - End date for SCD Type 2
- `is_current_column` - Current flag for SCD Type 2
- `operation_column` - Operation type column for CDC (I/U/D)
- `lookback_window` - Time window for incremental loads

### `LoadPattern` (NEW!)

Available load patterns:
- `FULL_REFRESH` - Complete table reload
- `APPEND_ONLY` - Insert new records only
- `UPSERT` - Insert new, update existing (MERGE)
- `DELETE_INSERT` - Delete then insert (transactional)
- `INCREMENTAL_TIMESTAMP` - Load only recent records
- `INCREMENTAL_APPEND` - Append only new records (by PK)
- `SCD_TYPE1` - Current state, no history
- `SCD_TYPE2` - Full history tracking
- `CDC` - Change data capture (I/U/D operations)
- `SNAPSHOT` - Point-in-time snapshots

### `ColumnDefinition` (Enhanced)

Define columns with format specifications.

```python
from schema_mapper.canonical import ColumnDefinition, LogicalType

col = ColumnDefinition(
    'event_date',
    LogicalType.DATE,
    date_format='%d/%m/%Y',  # NEW!
    timezone='UTC'           # NEW!
)
```

**New Fields:**
- `date_format` - Python strptime format for temporal types
- `timezone` - Timezone specification for timestamps

### `prepare_for_load()`

High-level convenience function for complete ETL preparation.

```python
df_clean, schema, issues = prepare_for_load(
    df,
    target_type='bigquery',
    standardize_columns=True,
    auto_cast=True,
    validate=True,
    profile=False,              # NEW! Generate quality report
    preprocess_pipeline=None,   # NEW! Apply preprocessing
    canonical_schema=None       # NEW! Use canonical schema for date formats
)
```

**New Parameters:**
- `profile` - Set to `True` to generate data quality report (returns 4 values instead of 3)
- `preprocess_pipeline` - List of preprocessing operations to apply (e.g., `['fix_whitespace', 'remove_duplicates']`)
- `canonical_schema` - CanonicalSchema with date format specifications (formats applied automatically)

### `create_schema()`

Quick schema generation.

```python
schema = create_schema(df, target_type='bigquery')
schema, mapping = create_schema(df, target_type='bigquery', return_mapping=True)
```

## Command-Line Interface

```bash
# Generate BigQuery schema
schema-mapper input.csv --platform bigquery --output schema.json

# Generate DDL
schema-mapper input.csv --platform snowflake --ddl --table-name customers

# Prepare and clean data
schema-mapper input.csv --platform redshift --prepare --output clean.csv

# Validate data
schema-mapper input.csv --validate

# Generate for all platforms
schema-mapper input.csv --platform all --ddl --table-name users
```

## Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=schema_mapper --cov-report=html
```

## Platform Selection Guide

| Platform | Best For |
|----------|----------|
| **BigQuery** | GCP ecosystem, serverless, real-time analytics |
| **Snowflake** | Multi-cloud, data sharing, semi-structured data |
| **Redshift** | AWS ecosystem, cost-effective large-scale |
| **SQL Server** | Azure/Windows, enterprise Microsoft stack |
| **PostgreSQL** | Open-source, maximum flexibility, any cloud |

## Type Detection Examples

```python
from schema_mapper.utils import detect_and_cast_types
import pandas as pd

# Input: All strings
df = pd.DataFrame({
    'id': ['1', '2', '3'],
    'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
    'active': ['yes', 'no', 'yes'],
    'price': ['19.99', '29.99', '39.99']
})

# Automatically detect and convert
df_typed = detect_and_cast_types(df)

print(df_typed.dtypes)
# id: int64
# date: datetime64[ns]
# active: bool
# price: float64
```

## Column Standardization

| Original | Standardized |
|----------|-------------|
| `User ID#` | `user_id` |
| `First Name (Legal)` | `first_name_legal` |
| `Email@Address` | `email_address` |
| `Account Balance ($)` | `account_balance` |
| `% Complete` | `complete` |
| `123InvalidStart` | `_123invalidstart` |

## Data Quality Features

### Profiler Capabilities

The `Profiler` class provides comprehensive data analysis:

**Quality Assessment:**
- Overall quality score (0-100)
- Completeness, uniqueness, validity, consistency scores
- Automated quality interpretation

**Anomaly Detection:**
- IQR (Interquartile Range) method
- Z-score method
- Isolation Forest method
- Per-column outlier detection

**Pattern Recognition:**
- Email addresses
- Phone numbers (US & international)
- URLs
- IP addresses
- Credit card numbers
- Social Security Numbers

**Statistical Analysis:**
- Missing value analysis
- Cardinality analysis
- Distribution analysis (skewness, kurtosis)
- Correlation detection
- Duplicate detection

**Visualization Support:**
- Distribution plots
- Correlation heatmaps
- Missing value visualizations
- Outlier visualizations

### PreProcessor Capabilities

The `PreProcessor` class provides intelligent data cleaning:

**Text Cleaning:**
- Whitespace removal (trim, normalize)
- Case standardization
- Column name standardization (snake_case)
- Special character handling

**Data Standardization:**
- Date format standardization (auto-detection)
- Email validation and standardization
- Phone number validation and standardization
- URL normalization

**Quality Improvement:**
- Duplicate removal
- Missing value handling (mean, median, mode, forward/backward fill, KNN imputation)
- Outlier detection and handling
- Type casting and conversion

**Data Transformation:**
- One-hot encoding
- Label encoding
- Binning and discretization
- Custom transformations

**Pipeline Features:**
- Method chaining for fluent API
- Transformation logging for reproducibility
- Schema-aware processing
- Custom pipeline creation

## Production Status

**Version**: 1.0.0
**Status**: Production-Ready
**Test Coverage**: 84-89% (incremental module), expanding coverage across core modules

### Recent Improvements (Dec 2024)

**Code Quality Enhancements:**
- Consolidated generator architecture for better maintainability
- Enhanced incremental load support with comprehensive testing
- Improved platform-specific renderers (BigQuery, Snowflake, Redshift, PostgreSQL)
- Added 9 production-ready incremental load patterns

**Platform Support:**
- ✅ BigQuery - Full support with comprehensive tests
- ✅ Snowflake - Full support with comprehensive tests
- ✅ Redshift - Full support with comprehensive tests
- ✅ SQL Server - Full support with comprehensive tests
- ✅ PostgreSQL - Full support, incremental tests in progress

**Known Improvements in Progress:**
- Expanding test coverage to 80%+ across all core modules (see [CODE_REVIEW_2025-12-23.md](CODE_REVIEW_2025-12-23.md))
- Standardizing logging practices
- Adding complete type hints to public APIs

The project is actively maintained and suitable for production use. See the [Code Review Report](CODE_REVIEW_2025-12-23.md) for detailed quality analysis.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built for data engineers working across:
- Google Cloud Platform (BigQuery)
- Snowflake (Multi-Cloud)
- Amazon Web Services (Redshift)
- Microsoft Azure (SQL Server)
- PostgreSQL (Open Source)

## Related Projects

- [pandas](https://pandas.pydata.org/) - Data analysis library
- [pandas-gbq](https://pandas-gbq.readthedocs.io/) - BigQuery connector
- [snowflake-connector-python](https://docs.snowflake.com/en/user-guide/python-connector.html) - Snowflake connector

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/datateamsix/schema-mapper).

---

**Made for universal cloud data engineering!**
