<img src="https://raw.githubusercontent.com/datateamsix/schema-mapper/main/images/sm-logo.png" alt="Schema Mapper Logo" width="200"/>

# schema-mapper

[![PyPI version](https://badge.fury.io/py/schema-mapper.svg)](https://badge.fury.io/py/schema-mapper)
[![Python Support](https://img.shields.io/pypi/pyversions/schema-mapper.svg)](https://pypi.org/project/schema-mapper/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Production-grade schema management and data pipeline orchestration for modern data platforms**

Stop wrestling with platform-specific DDL, manual schema management, and fragile data pipelines. Schema-mapper provides a **unified, canonical approach** to working with BigQuery, Snowflake, Redshift, SQL Server, and PostgreSQL—from schema inference to database execution.

---

## 🎯 The Problem

Modern data teams face a frustrating reality:

```python
# ❌ The Old Way: Platform-specific chaos
if platform == 'bigquery':
    client = bigquery.Client()
    # Write BigQuery-specific DDL
    # Handle BigQuery partitioning syntax
    # Deal with BigQuery type quirks
elif platform == 'snowflake':
    conn = snowflake.connect(...)
    # Rewrite everything for Snowflake
    # Different clustering syntax
    # Different type mappings
# ... repeat for each platform

# Result: 5x the code, 5x the bugs, 5x the maintenance
```

**Pain points:**
- 🔧 **Fragmented tooling** - Different APIs for each database
- 📝 **Manual schema management** - Hand-writing DDL for every platform
- 🐛 **Type mapping hell** - BIGINT vs NUMBER vs INT64 confusion
- 🔄 **Duplicate logic** - Rewriting MERGE statements per platform
- ⚠️ **No validation** - Catching errors only after failed loads
- 🌐 **Multi-cloud complexity** - Can't easily move between platforms

---

## ✨ The Solution

```python
# ✅ The schema-mapper Way: Write once, run everywhere
from schema_mapper import prepare_for_load
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

# 1. Prepare data for ANY platform (automatic cleaning, validation, type detection)
df_clean, schema, issues = prepare_for_load(df, target_type='bigquery')

# 2. Connect to ANY database with unified API
config = ConnectionConfig('connections.yaml')  # Single config for all platforms
with ConnectionFactory.get_connection('bigquery', config) as conn:
    # Create table from canonical schema
    conn.create_table_from_schema(schema, if_not_exists=True)

# 3. Switch platforms? Just change one parameter!
# Same code works for Snowflake, Redshift, PostgreSQL, SQL Server
```

**One codebase, five platforms, zero headaches.**

---

## 🚀 Key Features

### 🔌 **Unified Connection Layer** (NEW!)
- **Single API** for all 5 database platforms
- **Connection pooling** with thread-safe management
- **Automatic retry logic** with exponential backoff
- **Configuration-driven** with YAML + .env support
- **Transaction support** across platforms
- **Introspection** - Read existing schemas from any database

### 🎨 **Canonical Schema Architecture**
- **Platform-agnostic** schema representation
- **Bidirectional mapping** - Database → CanonicalSchema → Database
- **Single source of truth** for cross-platform migrations
- **Type safety** with logical type system
- **Metadata preservation** (partitioning, clustering, etc.)

### 📊 **Intelligent Schema Generation**
- **Automatic type detection** - Convert strings to dates, numbers, booleans
- **Column standardization** - `User ID#` → `user_id`
- **NULL handling** - Automatic REQUIRED vs NULLABLE detection
- **Multi-platform DDL** - Generate CREATE TABLE for any target
- **Optimization support** - Partitioning, clustering, distribution keys

### 🔄 **Production-Ready Incremental Loads**
- **9 load patterns**: UPSERT, SCD Type 2, CDC, Snapshot, Append-Only, etc.
- **Platform-optimized SQL** - Native MERGE, optimized DELETE+INSERT
- **Primary key detection** - Automatic composite key suggestions
- **Change tracking** - Full history with SCD Type 2
- **Transactional safety** - Atomic operations where supported

### 🔍 **Data Quality & Profiling**
- **Quality scoring** - Overall health assessment (0-100)
- **Anomaly detection** - IQR, Z-score, Isolation Forest methods
- **Pattern recognition** - Emails, phones, URLs, credit cards
- **Missing value analysis** - Completeness and imputation strategies
- **Statistical profiling** - Distributions, correlations, cardinality

### 🧹 **Intelligent Data Preprocessing**
- **Schema-aware cleaning** - Apply date formats from canonical schema
- **Validation pipelines** - Email, phone, URL validation
- **Missing data handling** - Mean, median, KNN imputation
- **Duplicate removal** - Smart deduplication strategies
- **Transformation logging** - Full audit trail

### 📚 **Metadata & Data Dictionary Framework** (NEW!)
- **Schema = Structure + Meaning** - Metadata as first-class citizen
- **YAML-driven schemas** - Version control for schemas + metadata
- **Data dictionary exports** - Markdown, CSV, JSON formats
- **PII governance** - Built-in PII flags for compliance
- **Metadata validation** - Enforce required fields (description, owner, etc.)
- **Documentation generation** - Never write docs twice
- **Bidirectional metadata** - Read from and write to databases

---

## 📦 Installation

```bash
# Basic installation
pip install schema-mapper

# With specific platform support
pip install schema-mapper[bigquery]
pip install schema-mapper[snowflake]
pip install schema-mapper[redshift]
pip install schema-mapper[postgresql]
pip install schema-mapper[sqlserver]

# Install everything
pip install schema-mapper[all]
```

---

## ⚡ Quick Start

### Basic Workflow: DataFrame → Schema → Database

```python
from schema_mapper import prepare_for_load
from schema_mapper.connections import ConnectionFactory, ConnectionConfig
import pandas as pd

# 1. Load messy data
df = pd.read_csv('messy_data.csv')

# 2. Prepare for target platform (cleaning, validation, type detection)
df_clean, schema, issues = prepare_for_load(
    df,
    target_type='bigquery',  # or snowflake, redshift, postgresql, sqlserver
    standardize_columns=True,
    auto_cast=True,
    validate=True
)

# 3. Check for issues
if issues['errors']:
    print("Errors found:", issues['errors'])
    exit(1)

# 4. Connect and create table (unified API across all platforms)
config = ConnectionConfig('connections.yaml')
with ConnectionFactory.get_connection('bigquery', config) as conn:
    # Test connection
    conn.test_connection()

    # Create table from canonical schema
    conn.create_table_from_schema(schema, if_not_exists=True)

    # Or execute raw DDL
    # ddl = renderer.to_ddl()
    # conn.execute_ddl(ddl)

print(f"✓ Successfully loaded {len(df_clean)} rows to BigQuery!")
```

### Cross-Platform Migration: Snowflake → BigQuery

```python
from schema_mapper.connections import ConnectionFactory, ConnectionConfig
from schema_mapper.renderers import RendererFactory

config = ConnectionConfig('connections.yaml')

# 1. Introspect schema from Snowflake
with ConnectionFactory.get_connection('snowflake', config) as sf_conn:
    canonical_schema = sf_conn.get_target_schema(
        table='customers',
        schema_name='public',
        database='analytics'
    )

# 2. Render for BigQuery (automatic type conversion)
renderer = RendererFactory.get_renderer('bigquery', canonical_schema)
bq_ddl = renderer.to_ddl()

# 3. Create in BigQuery
with ConnectionFactory.get_connection('bigquery', config) as bq_conn:
    bq_conn.execute_ddl(bq_ddl)

print("✓ Migrated Snowflake → BigQuery!")
```

---

## 🔌 Unified Connection System (NEW!)

The connection system provides **one API for five databases**, eliminating platform-specific code.

### Configuration (connections.yaml)

```yaml
target: bigquery  # Default connection

connections:
  bigquery:
    project: ${GCP_PROJECT_ID}
    credentials_path: ${BQ_CREDENTIALS_PATH}
    location: US

  snowflake:
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC

  postgresql:
    host: ${PG_HOST}
    port: 5432
    database: analytics
    user: ${PG_USER}
    password: ${PG_PASSWORD}

  redshift:
    host: ${REDSHIFT_HOST}
    port: 5439
    database: analytics
    user: ${REDSHIFT_USER}
    password: ${REDSHIFT_PASSWORD}

  sqlserver:
    server: ${MSSQL_SERVER}
    database: analytics
    user: ${MSSQL_USER}
    password: ${MSSQL_PASSWORD}
    driver: '{ODBC Driver 17 for SQL Server}'

# Optional: Connection pooling
pooling:
  enabled: true
  default:
    min_size: 2
    max_size: 10
```

### Environment Variables (.env)

```bash
# BigQuery
GCP_PROJECT_ID=my-project
BQ_CREDENTIALS_PATH=/path/to/service-account.json

# Snowflake
SNOWFLAKE_ACCOUNT=abc123
SNOWFLAKE_USER=svc_etl
SNOWFLAKE_PASSWORD=********

# PostgreSQL
PG_HOST=localhost
PG_USER=etl_user
PG_PASSWORD=********

# Redshift
REDSHIFT_HOST=my-cluster.redshift.amazonaws.com
REDSHIFT_USER=etl_user
REDSHIFT_PASSWORD=********

# SQL Server
MSSQL_SERVER=my-server.database.windows.net
MSSQL_USER=etl_user
MSSQL_PASSWORD=********
```

### Connection API

All platforms implement the same interface:

```python
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

config = ConnectionConfig('connections.yaml')

# Works identically for all platforms
with ConnectionFactory.get_connection('bigquery', config) as conn:
    # Connection lifecycle
    conn.test_connection()  # Health check

    # Introspection
    exists = conn.table_exists('users', schema_name='public')
    schema = conn.get_target_schema('users', schema_name='public')
    tables = conn.list_tables(schema_name='public')

    # Execution
    conn.execute_ddl("CREATE TABLE ...")
    results = conn.execute_query("SELECT COUNT(*) FROM users")
    conn.create_table_from_schema(canonical_schema)

    # Transactions
    with conn.transaction():
        conn.execute_ddl("INSERT INTO ...")
        conn.execute_ddl("UPDATE ...")
        # Auto-commit on success, rollback on error
```

### Connection Features

| Feature | BigQuery | Snowflake | PostgreSQL | Redshift | SQL Server |
|---------|----------|-----------|------------|----------|------------|
| **Connection Pooling** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Auto Retry** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Transactions** | ○ Auto-commit | ✓ Full | ✓ Full | ✓ Full | ✓ Full |
| **Savepoints** | ✗ | ✓ | ✓ | ✓ | ✓ |
| **Introspection** | ✓ API | ✓ INFORMATION_SCHEMA | ✓ pg_catalog | ✓ INFORMATION_SCHEMA | ✓ INFORMATION_SCHEMA |
| **Context Manager** | ✓ | ✓ | ✓ | ✓ | ✓ |

---

## 🎨 Canonical Schema Architecture

The **canonical schema** is schema-mapper's secret sauce—a platform-agnostic representation that ensures consistency.

### Flow

```
┌─────────────────────────────────────────────────────────┐
│  INPUT SOURCES          CANONICAL SCHEMA                │
├─────────────────────────────────────────────────────────┤
│  DataFrame ──┐                                          │
│  CSV ────────┼──→ infer ──→ CanonicalSchema ──┐        │
│  JSON ───────┘                                 │        │
│  Database ──→ introspect ──→ CanonicalSchema ─┤        │
│                                                │        │
│                                                ▼        │
│                                         Renderer/       │
│                                         Generator       │
│                                                │        │
│                                                ▼        │
│                                           DDL, JSON,    │
│                                           CLI Commands  │
└─────────────────────────────────────────────────────────┘
```

### Creating Canonical Schemas

```python
from schema_mapper.canonical import infer_canonical_schema, CanonicalSchema, ColumnDefinition, LogicalType
import pandas as pd

# Option 1: Infer from DataFrame
df = pd.read_csv('data.csv')
schema = infer_canonical_schema(
    df,
    table_name='customers',
    dataset_name='analytics',
    partition_columns=['created_date'],
    cluster_columns=['customer_id', 'region']
)

# Option 2: Define manually
schema = CanonicalSchema(
    table_name='customers',
    dataset_name='analytics',
    columns=[
        ColumnDefinition(
            name='customer_id',
            logical_type=LogicalType.BIGINT,
            nullable=False
        ),
        ColumnDefinition(
            name='email',
            logical_type=LogicalType.STRING,
            nullable=False
        ),
        ColumnDefinition(
            name='created_at',
            logical_type=LogicalType.TIMESTAMP,
            nullable=False,
            date_format='%Y-%m-%d %H:%M:%S',  # Applied during preprocessing
            timezone='UTC'
        ),
        ColumnDefinition(
            name='balance',
            logical_type=LogicalType.DECIMAL,
            nullable=True,
            precision=10,
            scale=2
        )
    ],
    partition_columns=['created_date'],
    cluster_columns=['customer_id', 'region']
)

# Option 3: Introspect from existing database
with ConnectionFactory.get_connection('snowflake', config) as conn:
    schema = conn.get_target_schema('customers', schema_name='public')
```

### Rendering to Platforms

```python
from schema_mapper.renderers import RendererFactory

# One schema, many outputs
for platform in ['bigquery', 'snowflake', 'postgresql', 'redshift']:
    renderer = RendererFactory.get_renderer(platform, schema)

    print(f"\n{platform.upper()} DDL:")
    print(renderer.to_ddl())

    # Platform-specific artifacts
    if platform == 'bigquery' and renderer.supports_json_schema():
        print(renderer.to_schema_json())  # BigQuery JSON schema
```

### Logical Type System

| Logical Type | BigQuery | Snowflake | PostgreSQL | Redshift | SQL Server |
|--------------|----------|-----------|------------|----------|------------|
| `BIGINT` | INT64 | NUMBER(38,0) | BIGINT | BIGINT | BIGINT |
| `INTEGER` | INT64 | NUMBER(38,0) | INTEGER | INTEGER | INT |
| `DECIMAL` | NUMERIC | NUMBER(p,s) | NUMERIC(p,s) | DECIMAL(p,s) | DECIMAL(p,s) |
| `FLOAT` | FLOAT64 | FLOAT | DOUBLE PRECISION | DOUBLE PRECISION | FLOAT |
| `STRING` | STRING | VARCHAR(16MB) | TEXT | VARCHAR(65535) | NVARCHAR(MAX) |
| `TEXT` | STRING | VARCHAR(16MB) | TEXT | VARCHAR(65535) | NVARCHAR(MAX) |
| `BOOLEAN` | BOOL | BOOLEAN | BOOLEAN | BOOLEAN | BIT |
| `DATE` | DATE | DATE | DATE | DATE | DATE |
| `TIMESTAMP` | TIMESTAMP | TIMESTAMP_NTZ | TIMESTAMP | TIMESTAMP | DATETIME2 |
| `TIMESTAMPTZ` | TIMESTAMP | TIMESTAMP_TZ | TIMESTAMPTZ | TIMESTAMPTZ | DATETIMEOFFSET |
| `JSON` | JSON | VARIANT | JSONB | VARCHAR | NVARCHAR(MAX) |

---

## 🔄 Incremental Loads (Production-Grade)

Generate optimized DDL for **9 incremental load patterns** across all platforms.

### Supported Patterns

| Pattern | Use Case | BigQuery | Snowflake | Redshift | PostgreSQL | SQL Server |
|---------|----------|----------|-----------|----------|------------|------------|
| **UPSERT (MERGE)** | Insert new, update existing | ✓ Native | ✓ Native | ✓ DELETE+INSERT | ✓ Native | ✓ Native |
| **SCD Type 2** | Full history tracking | ✓ | ✓ | ✓ | ✓ | ✓ |
| **CDC** | Change data capture (I/U/D) | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Incremental Timestamp** | Load recent records | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Append Only** | Insert only | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Delete-Insert** | Transactional replacement | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Full Refresh** | Complete reload | ✓ | ✓ | ✓ | ✓ | ✓ |
| **SCD Type 1** | Current state only | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Snapshot** | Point-in-time captures | ✓ | ✓ | ✓ | ✓ | ✓ |

### UPSERT Example

```python
from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator

# Configure UPSERT pattern
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id']
)

# Generate platform-specific MERGE statement
generator = get_incremental_generator('bigquery')
ddl = generator.generate_incremental_ddl(
    schema=canonical_schema,
    table_name='users',
    config=config
)

# Execute via connection
with ConnectionFactory.get_connection('bigquery', conn_config) as conn:
    conn.execute_ddl(ddl)
```

### SCD Type 2 Example

```python
# Track full history with slowly changing dimensions
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['customer_id'],
    scd2_columns=['name', 'address', 'phone'],  # Tracked attributes
    effective_date_column='valid_from',
    end_date_column='valid_to',
    is_current_column='is_current'
)

generator = get_incremental_generator('snowflake')
ddl = generator.generate_incremental_ddl(schema, 'dim_customers', config)
```

### CDC (Change Data Capture) Example

```python
# Process insert/update/delete streams
config = IncrementalConfig(
    load_pattern=LoadPattern.CDC,
    primary_keys=['order_id'],
    operation_column='_cdc_operation'  # I, U, D
)

generator = get_incremental_generator('postgresql')
ddl = generator.generate_incremental_ddl(schema, 'orders', config)
```

**Complete incremental load documentation**: [docs/INCREMENTAL_LOADS.md](docs/INCREMENTAL_LOADS.md)

---

## 🔍 Data Quality & Profiling

### Comprehensive Data Profiling

```python
from schema_mapper import SchemaMapper
import pandas as pd

df = pd.read_csv('customer_data.csv')
mapper = SchemaMapper('bigquery')

# Generate full quality report
report = mapper.profile_data(df, detailed=True)

print(f"Overall Quality Score: {report['quality']['overall_score']}/100")
print(f"Completeness: {report['quality']['completeness_score']:.1f}%")
print(f"Missing Values: {report['missing_values']['total_missing_percentage']:.1f}%")
print(f"Duplicates: {report['duplicates']['count']} rows")

# Anomaly detection
if report['anomalies']:
    print("\nAnomalies detected:")
    for col, info in report['anomalies'].items():
        print(f"  {col}: {info['count']} outliers ({info['percentage']:.1f}%)")

# Pattern recognition (emails, phones, URLs, credit cards, etc.)
if report['patterns']:
    for col, patterns in report['patterns'].items():
        print(f"\nPatterns in {col}:")
        for pattern, pct in patterns.items():
            print(f"  {pattern}: {pct:.1f}%")
```

### Intelligent Data Cleaning

```python
from schema_mapper.preprocessor import PreProcessor

preprocessor = PreProcessor(df, canonical_schema=schema)

# Fluent API with method chaining
df_clean = (preprocessor
    .fix_whitespace()                    # Remove leading/trailing whitespace
    .standardize_column_names()          # Convert to snake_case
    .validate_emails(columns=['email'])  # Validate email formats
    .standardize_dates(                  # Standardize date formats
        columns=['created_at'],
        target_format='%Y-%m-%d'
    )
    .remove_duplicates()                 # Smart deduplication
    .handle_missing(strategy='auto')     # Intelligent imputation
    .apply())

# Check transformation log
print("Transformations applied:")
for transform in preprocessor.transformation_log:
    print(f"  - {transform}")
```

### Schema-Aware Preprocessing (NEW!)

```python
# Define date formats ONCE in canonical schema
schema = CanonicalSchema(
    table_name='events',
    columns=[
        ColumnDefinition(
            'event_date',
            LogicalType.DATE,
            date_format='%d/%m/%Y'  # European format
        ),
        ColumnDefinition(
            'created_at',
            LogicalType.TIMESTAMP,
            date_format='%d/%m/%Y %H:%M:%S',
            timezone='UTC'
        )
    ]
)

# Formats applied automatically during preprocessing!
df_clean, db_schema, issues = prepare_for_load(
    df,
    'bigquery',
    canonical_schema=schema  # ✨ Magic happens here
)
```

---

## 📊 Table Optimization

### Platform-Specific Optimizations

| Feature | BigQuery | Snowflake | Redshift | PostgreSQL | SQL Server |
|---------|----------|-----------|----------|------------|------------|
| **Partitioning** | ✓ DATE/TIMESTAMP/RANGE | ~ Auto Micro | ✗ | ✓ RANGE/LIST/HASH | ✗ |
| **Clustering** | ✓ Up to 4 cols | ✓ Up to 4 cols | ✗ | ✓ Via Indexes | ✓ Clustered Index |
| **Distribution** | ✗ | ✗ | ✓ KEY/ALL/EVEN/AUTO | ✗ | ✗ |
| **Sort Keys** | ✗ | ✗ | ✓ Compound/Interleaved | ✗ | ✗ |
| **Columnstore** | ✗ | ✗ | ✗ | ✗ | ✓ Analytics |

### Examples

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
    partition_expiration_days=365,
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
    create_or_replace=True
)
```

---

## 🎯 Use Cases

### 1. Multi-Cloud Data Migration

**Scenario**: Migrating from AWS (Redshift) to GCP (BigQuery)

```python
from schema_mapper.connections import ConnectionFactory, ConnectionConfig
from schema_mapper.renderers import RendererFactory

config = ConnectionConfig('connections.yaml')

# Introspect Redshift tables
with ConnectionFactory.get_connection('redshift', config) as rs_conn:
    tables = rs_conn.list_tables(schema_name='public')

    for table in tables:
        # Get schema from Redshift
        schema = rs_conn.get_target_schema(table, schema_name='public')

        # Render for BigQuery
        renderer = RendererFactory.get_renderer('bigquery', schema)
        bq_ddl = renderer.to_ddl()

        # Create in BigQuery
        with ConnectionFactory.get_connection('bigquery', config) as bq_conn:
            bq_conn.execute_ddl(bq_ddl)

        print(f"✓ Migrated {table}")
```

### 2. ETL Pipeline with Quality Checks

**Scenario**: Production ETL with profiling, cleaning, validation

```python
from schema_mapper import prepare_for_load
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

# Extract
df = pd.read_csv('daily_transactions.csv')

# Transform + Profile
df_clean, schema, issues, report = prepare_for_load(
    df,
    'snowflake',
    profile=True,
    preprocess_pipeline=[
        'fix_whitespace',
        'standardize_column_names',
        'remove_duplicates',
        'handle_missing'
    ],
    validate=True
)

# Quality gate
if report['quality']['overall_score'] < 80:
    print(f"❌ Quality score too low: {report['quality']['overall_score']}/100")
    exit(1)

if issues['errors']:
    print("❌ Validation errors:", issues['errors'])
    exit(1)

# Load
config = ConnectionConfig('connections.yaml')
with ConnectionFactory.get_connection('snowflake', config) as conn:
    conn.create_table_from_schema(schema, if_not_exists=True)
    # Load df_clean to Snowflake...

print(f"✓ Loaded {len(df_clean)} rows with quality score {report['quality']['overall_score']}/100")
```

### 3. Incremental UPSERT Pipeline

**Scenario**: Daily UPSERT of customer data

```python
from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

# New/updated customer records
df = pd.read_csv('customers_delta.csv')

# Generate MERGE DDL
schema = infer_canonical_schema(df, table_name='customers')
config_inc = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['customer_id'],
    update_columns=['email', 'phone', 'address', 'updated_at']
)

generator = get_incremental_generator('bigquery')
merge_ddl = generator.generate_incremental_ddl(schema, 'customers', config_inc)

# Execute MERGE
conn_config = ConnectionConfig('connections.yaml')
with ConnectionFactory.get_connection('bigquery', conn_config) as conn:
    conn.execute_ddl(merge_ddl)

print(f"✓ UPSERT complete: {len(df)} customers processed")
```

### 4. SCD Type 2 Dimension Tracking

**Scenario**: Maintain full history of customer changes

```python
# Track customer dimension changes with history
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['customer_id'],
    scd2_columns=['name', 'address', 'phone', 'email'],
    effective_date_column='valid_from',
    end_date_column='valid_to',
    is_current_column='is_current'
)

generator = get_incremental_generator('snowflake')
scd2_ddl = generator.generate_incremental_ddl(schema, 'dim_customers', config)

with ConnectionFactory.get_connection('snowflake', conn_config) as conn:
    conn.execute_ddl(scd2_ddl)

# Result: Full customer history with versioning
# customer_id | name  | address | valid_from | valid_to   | is_current
# ------------|-------|---------|------------|------------|------------
# 1           | Alice | NYC     | 2024-01-01 | 2024-06-01 | false
# 1           | Alice | LA      | 2024-06-01 | NULL       | true
```

---

## 📚 API Reference

### Core Classes

#### `ConnectionFactory`
```python
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

config = ConnectionConfig('connections.yaml')
conn = ConnectionFactory.get_connection('bigquery', config)
pool = ConnectionFactory.create_pool('bigquery', config, min_size=2, max_size=10)
```

#### `SchemaMapper`
```python
from schema_mapper import SchemaMapper

mapper = SchemaMapper('bigquery')
schema, mapping = mapper.generate_schema(df)
ddl = mapper.generate_ddl(df, 'table_name')
ddl_inc = mapper.generate_incremental_ddl(df, 'table_name', config)
report = mapper.profile_data(df, detailed=True)
df_clean = mapper.preprocess_data(df, pipeline=['fix_whitespace', 'remove_duplicates'])
```

#### `Profiler`
```python
from schema_mapper.profiler import Profiler

profiler = Profiler(df, name='my_dataset')
report = profiler.generate_report(output_format='dict')
quality = profiler.assess_quality()
anomalies = profiler.detect_anomalies(method='iqr')
patterns = profiler.detect_patterns()
```

#### `PreProcessor`
```python
from schema_mapper.preprocessor import PreProcessor

preprocessor = PreProcessor(df, canonical_schema=schema)
df_clean = (preprocessor
    .fix_whitespace()
    .standardize_column_names()
    .standardize_dates(columns=['created_at'])
    .handle_missing(strategy='auto')
    .apply())
```

### Connection Methods

All platforms implement:

```python
# Lifecycle
conn.connect()
conn.disconnect()
conn.test_connection() -> bool

# Introspection
conn.table_exists(table, schema, database) -> bool
conn.get_target_schema(table, schema, database) -> CanonicalSchema
conn.list_tables(schema, database) -> List[str]

# Execution
conn.execute_ddl(ddl) -> None
conn.execute_query(query) -> List[Dict]
conn.create_table_from_schema(schema, if_not_exists) -> None

# Transactions
conn.begin_transaction()
conn.commit()
conn.rollback()
conn.transaction(isolation_level) -> ContextManager
conn.savepoint(name)
conn.rollback_to_savepoint(name)

# Context Manager
with conn:
    # Auto-connect/disconnect
    pass
```

### Incremental Load Patterns

```python
from schema_mapper import LoadPattern, IncrementalConfig

LoadPattern.UPSERT              # MERGE: Insert new, update existing
LoadPattern.SCD_TYPE2           # Full history with versioning
LoadPattern.CDC                 # Change data capture (I/U/D)
LoadPattern.INCREMENTAL_TIMESTAMP  # Load recent records only
LoadPattern.APPEND_ONLY         # Insert only, no updates
LoadPattern.DELETE_INSERT       # Transactional replacement
LoadPattern.FULL_REFRESH        # Complete reload
LoadPattern.SCD_TYPE1           # Current state only
LoadPattern.SNAPSHOT            # Point-in-time captures
```

---

## 🔧 Configuration

### YAML Configuration Structure

```yaml
# connections.yaml
target: bigquery  # Default platform

connections:
  bigquery:
    project: ${GCP_PROJECT_ID}
    credentials_path: ${BQ_CREDENTIALS_PATH}
    location: US

  snowflake:
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC
    role: TRANSFORMER

  postgresql:
    host: ${PG_HOST}
    port: 5432
    database: analytics
    user: ${PG_USER}
    password: ${PG_PASSWORD}

  redshift:
    host: ${REDSHIFT_HOST}
    port: 5439
    database: analytics
    user: ${REDSHIFT_USER}
    password: ${REDSHIFT_PASSWORD}

  sqlserver:
    server: ${MSSQL_SERVER}
    database: analytics
    user: ${MSSQL_USER}
    password: ${MSSQL_PASSWORD}
    driver: '{ODBC Driver 17 for SQL Server}'

pooling:
  enabled: true
  default:
    min_size: 2
    max_size: 10
  overrides:
    bigquery:
      max_size: 5  # Platform-specific override
```

### Environment Variables

Create `.env` file:

```bash
# BigQuery
GCP_PROJECT_ID=my-gcp-project
BQ_CREDENTIALS_PATH=/path/to/service-account.json

# Snowflake
SNOWFLAKE_ACCOUNT=abc123
SNOWFLAKE_USER=svc_etl
SNOWFLAKE_PASSWORD=********

# PostgreSQL
PG_HOST=localhost
PG_USER=etl_user
PG_PASSWORD=********

# Redshift
REDSHIFT_HOST=my-cluster.redshift.amazonaws.com
REDSHIFT_USER=etl_user
REDSHIFT_PASSWORD=********

# SQL Server
MSSQL_SERVER=my-server.database.windows.net
MSSQL_USER=etl_user
MSSQL_PASSWORD=********
```

---

## 🧪 Testing

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run unit tests
pytest tests/connections/ -v

# Run with coverage
pytest tests/connections/ --cov=schema_mapper.connections --cov-report=html

# Run integration tests (requires database credentials)
RUN_INTEGRATION_TESTS=1 pytest tests/integration/ -v
```

**Test Coverage:**
- Configuration system: 78% (30 tests)
- Retry logic: 95% (26 tests)
- Integration tests: 65+ tests covering renderers, generators, workflows

---

## 📖 Examples

Explore complete, production-ready examples in [`examples/`](examples/):

### Core Use Cases
- [`01_basic_usage.py`](examples/01_basic_usage.py) - Simple DataFrame to database workflow (5 min)
- [`02_multi_cloud_migration.py`](examples/02_multi_cloud_migration.py) - **Multi-cloud migration** (BigQuery → Snowflake) (10 min)
- [`03_etl_with_quality_gates.py`](examples/03_etl_with_quality_gates.py) - **ETL pipeline with quality gates** (15 min)
- [`04_incremental_upsert.py`](examples/04_incremental_upsert.py) - **Incremental UPSERT loads** (10 min)
- [`05_scd_type2_tracking.py`](examples/05_scd_type2_tracking.py) - **SCD Type 2 dimension tracking** (15 min)

### Production Integration
- [`06_prefect_orchestration.py`](examples/06_prefect_orchestration.py) - 🌟 **Prefect orchestration** with tagged stages, quality gates, and artifacts (20 min)
- [`07_connection_pooling.py`](examples/07_connection_pooling.py) - Connection pooling for high-concurrency workloads (10 min)
- [`08_metadata_data_dictionary.py`](examples/08_metadata_data_dictionary.py) - **Metadata & Data Dictionary Framework** - YAML schemas, PII governance, auto-generated docs (20 min)

**📚 See [`examples/README.md`](examples/README.md)** for setup instructions, configuration templates, and learning path.

**🔧 Quick Setup:**
```bash
# 1. Install with platform dependencies
pip install schema-mapper[bigquery,snowflake,postgresql]

# 2. Create configuration (see examples/README.md)
cp config/connections.yaml.example config/connections.yaml
# Edit connections.yaml with your credentials

# 3. Run examples
python examples/01_basic_usage.py
python examples/06_prefect_orchestration.py  # Prefect integration
```

---

## 🏢 Production Status

**Version**: 1.0.0
**Status**: Production-Ready
**Test Coverage**: 78-95% on core modules

### Platform Support

| Platform | Schema Gen | DDL Gen | Incremental | Connections | Status |
|----------|------------|---------|-------------|-------------|--------|
| **BigQuery** | ✓ | ✓ | ✓ | ✓ | Production |
| **Snowflake** | ✓ | ✓ | ✓ | ✓ | Production |
| **Redshift** | ✓ | ✓ | ✓ | ✓ | Production |
| **PostgreSQL** | ✓ | ✓ | ✓ | ✓ | Production |
| **SQL Server** | ✓ | ✓ | ✓ | ✓ | Production |

### Recent Enhancements (Dec 2024)

- ✅ **Unified Connection System** - One API for all 5 platforms
- ✅ **Connection Pooling** - Thread-safe pool management
- ✅ **Retry Logic** - Exponential backoff with platform-specific error detection
- ✅ **Schema Introspection** - Read schemas from existing databases
- ✅ **Transaction Support** - Full ACID support where available
- ✅ **Comprehensive Testing** - 56 core connection tests, 65+ integration tests

---

## 🤝 Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Built for data engineers working across:
- Google Cloud Platform (BigQuery)
- Snowflake (Multi-Cloud)
- Amazon Web Services (Redshift)
- Microsoft Azure (SQL Server)
- PostgreSQL (Open Source)

---

## 🔗 Resources

**Documentation:**
- [Incremental Loads Guide](docs/INCREMENTAL_LOADS.md)
- [Architecture Overview](ARCHITECTURE.md)
- [Code Review Report](CODE_REVIEW_2025-12-23.md)

**Related Projects:**
- [pandas](https://pandas.pydata.org/) - Data analysis library
- [SQLAlchemy](https://www.sqlalchemy.org/) - SQL toolkit
- [Great Expectations](https://greatexpectations.io/) - Data validation

**Support:**
- [GitHub Issues](https://github.com/datateamsix/schema-mapper/issues)
- [GitHub Discussions](https://github.com/datateamsix/schema-mapper/discussions)

---

**Made with ❤️ for universal cloud data engineering**
