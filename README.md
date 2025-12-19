<div align="center">
  <img src="images/sm-logo.png" alt="Schema Mapper Logo" width="200"/>
</div>

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
- **Table Optimization**: Clustering, partitioning, and distribution strategies (NEW!)
- **High Performance**: Efficiently handles datasets from 1K to 1M+ rows

## Architecture

<div align="center">
  <img src="images/schema-mapper-architecture.png" alt="Schema Mapper Architecture" width="800"/>
</div>

The schema-mapper uses a **canonical schema** approach: infer once, render to any platform. This ensures consistent type mapping and optimization strategies across all supported databases.

<div align="center">
  <img src="images/canonical-schema.png" alt="Canonical Schema Flow" width="700"/>
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

## Usage Examples

**Complete Example Scripts:**
- [basic_usage.py](schema-mapper-pkg/examples/basic_usage.py) - Simple schema generation workflow
- [multi_platform.py](schema-mapper-pkg/examples/multi_platform.py) - Generate for all platforms at once
- [production_analytics_pipeline.py](schema-mapper-pkg/examples/production_analytics_pipeline.py) - **Production use case with clustering & partitioning**
- [ddl_with_clustering_examples.py](schema-mapper-pkg/examples/ddl_with_clustering_examples.py) - All platform optimization examples
- [canonical_schema_usage.py](schema-mapper-pkg/examples/canonical_schema_usage.py) - **New renderer architecture** (canonical schema → multiple outputs)

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

### Generate Optimized DDL with Clustering & Partitioning (NEW!)

```python
from schema_mapper.generators_enhanced import get_enhanced_ddl_generator
from schema_mapper.ddl_mappings import (
    DDLOptions, ClusteringConfig, PartitionConfig, PartitionType
)
import pandas as pd

df = pd.read_csv('events.csv')

# BigQuery: Partitioned by date, clustered by user_id
generator = get_enhanced_ddl_generator('bigquery')
options = DDLOptions(
    partitioning=PartitionConfig(
        column='event_date',
        partition_type=PartitionType.TIME,
        expiration_days=365
    ),
    clustering=ClusteringConfig(columns=['user_id', 'event_type'])
)

# Generate schema first
mapper = SchemaMapper('bigquery')
schema, _ = mapper.generate_schema(df)

# Generate optimized DDL
ddl = generator.generate(
    schema=schema,
    table_name='events',
    dataset_name='analytics',
    project_id='my-project',
    ddl_options=options
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

## Table Optimization Features (NEW!)

### Platform Capabilities

| Feature | BigQuery | Snowflake | Redshift | SQL Server | PostgreSQL |
|---------|----------|-----------|----------|------------|------------|
| **Partitioning** | ✓ DATE/TIMESTAMP/RANGE | ~ Auto Micro | ✗ | ✓ Function+Scheme | ✓ RANGE/LIST/HASH |
| **Clustering** | ✓ Up to 4 cols | ✓ Up to 4 cols | ✗ | ✓ Clustered Index | ~ Via Indexes |
| **Distribution** | ✗ | ✗ | ✓ KEY/ALL/EVEN/AUTO | ✗ | ✗ |
| **Sort Keys** | ✗ | ✗ | ✓ Compound/Interleaved | ✗ | ✗ |

### Quick Examples

```python
from schema_mapper.ddl_mappings import *
from schema_mapper.generators_enhanced import get_enhanced_ddl_generator

# BigQuery: Partitioned + Clustered
options = DDLOptions(
    partitioning=PartitionConfig(column='event_date', partition_type=PartitionType.TIME),
    clustering=ClusteringConfig(columns=['user_id', 'event_type'])
)

# Redshift: Distributed + Sorted
options = DDLOptions(
    distribution=DistributionConfig(style=DistributionStyle.KEY, key_column='user_id'),
    sort_keys=SortKeyConfig(columns=['event_date', 'event_ts'])
)

# Snowflake: Clustered
options = DDLOptions(
    clustering=ClusteringConfig(columns=['event_date', 'user_id']),
    transient=True  # For staging tables
)

# PostgreSQL: Range Partitioned
options = DDLOptions(
    partitioning=PartitionConfig(column='event_date', partition_type=PartitionType.RANGE),
    clustering=ClusteringConfig(columns=['event_date', 'user_id'])  # Creates index
)
```

See [examples/production_analytics_pipeline.py](schema-mapper-pkg/examples/production_analytics_pipeline.py) for a complete use case.

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
- `prepare_dataframe(df, ...)` - Clean and prepare DataFrame
- `validate_dataframe(df, ...)` - Validate DataFrame quality
- `generate_bigquery_schema_json(df, ...)` - Generate BigQuery JSON schema

### `prepare_for_load()`

High-level convenience function for complete ETL preparation.

```python
df_clean, schema, issues = prepare_for_load(
    df,
    target_type='bigquery',
    standardize_columns=True,
    auto_cast=True,
    validate=True
)
```

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
