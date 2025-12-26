<img src="https://raw.githubusercontent.com/datateamsix/schema-mapper/main/images/sm-logo.png" alt="Schema Mapper Logo" width="200"/>

# schema-mapper

[![PyPI version](https://badge.fury.io/py/schema-mapper.svg)](https://badge.fury.io/py/schema-mapper)
[![Python Support](https://img.shields.io/pypi/pyversions/schema-mapper.svg)](https://pypi.org/project/schema-mapper/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Production-grade ETL toolkit with ML feature engineering, intelligent data profiling, and unified database connections for modern data teams.**

Work seamlessly across BigQuery, Snowflake, Redshift, SQL Server, and PostgreSQL—with built-in ML preprocessing, automated feature analysis, and zero platform-specific rewrites.

---

## Overview

Schema-mapper eliminates the complexity of working across multiple database platforms by providing:

- **Unified Connection Layer** - Single API for all five database platforms
- **ML Feature Engineering** - Automated target correlation analysis and categorical encoding
- **Intelligent Data Profiling** - Statistical analysis, quality scoring, and anomaly detection
- **Canonical Schema System** - Platform-agnostic schema representation
- **Production-Ready ETL** - 9 incremental load patterns with platform-optimized SQL
- **Data Quality Framework** - Comprehensive validation and preprocessing pipelines

---

## The Problem

Modern data teams waste time managing platform-specific code:

```python
# The Old Way: Platform-specific chaos
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
- Fragmented tooling - Different APIs for each database
- Manual schema management - Hand-writing DDL for every platform
- Type mapping confusion - BIGINT vs NUMBER vs INT64
- Duplicate logic - Rewriting MERGE statements per platform
- No validation - Catching errors only after failed loads
- Multi-cloud complexity - Can't easily move between platforms
- Manual ML prep - Repetitive feature engineering workflows

---

## The Solution

```python
# The schema-mapper Way: Write once, run everywhere
from schema_mapper import prepare_for_load
from schema_mapper.connections import ConnectionFactory, ConnectionConfig
from schema_mapper.profiler import Profiler

# 1. Analyze and profile data with ML feature importance
df = pd.read_csv('customer_churn.csv')
profiler = Profiler(df)
feature_importance = profiler.analyze_target_correlation('churn', top_n=10)

# 2. Prepare data for ANY platform (automatic cleaning, validation, ML encoding)
df_clean, schema, issues = prepare_for_load(df, target_type='bigquery')

# 3. Connect to ANY database with unified API
config = ConnectionConfig('connections.yaml')  # Single config for all platforms
with ConnectionFactory.get_connection('bigquery', config) as conn:
    conn.create_table_from_schema(schema, if_not_exists=True)

# 4. Switch platforms? Just change one parameter!
# Same code works for Snowflake, Redshift, PostgreSQL, SQL Server
```

**One codebase, five platforms, zero headaches.**

---

## Key Features

### NEW in v1.4.0: Machine Learning Feature Engineering

**Automate ML preprocessing and feature analysis for faster model development.**

- **Target Correlation Analysis** - Automatically identify important features for classification/regression
- **Smart Categorical Encoding** - Intelligent one-hot encoding with frequency filtering
- **Feature Importance Visualization** - Color-coded bar charts for model explainability
- **Multi-target Support** - Binary classification, multi-class, and regression workflows

```python
from schema_mapper.profiler import Profiler
from schema_mapper.preprocessor import PreProcessor

# Analyze feature importance
profiler = Profiler(df, name='churn_analysis')
importance = profiler.analyze_target_correlation(
    target_column='churn',  # Handles categorical targets automatically
    method='pearson',
    top_n=15
)

# Visualize
fig = profiler.plot_target_correlation('churn', top_n=15)
fig.savefig('feature_importance.png')

# Auto-encode categoricals for ML
preprocessor = PreProcessor(df)
preprocessor.auto_encode_categorical(
    exclude_columns=['churn'],
    max_categories=10,
    drop_first=True
)
```

### NEW in v1.3.0: DataFrame-First API & Enhanced Discovery

**All queries now return pandas DataFrames, plus powerful new introspection methods.**

- **DataFrame Query Results** - All `execute_query()` calls return pandas DataFrames
- **Database Discovery** - `get_tables()`, `get_schemas()`, `get_database_tree()`
- **Metadata Inspection** - Explore warehouse structure programmatically
- **Multi-Platform Inventory** - Same API across all five platforms

```python
# Query returns DataFrame automatically
df = conn.execute_query("SELECT * FROM analytics.users LIMIT 100")
df.to_csv('users.csv')  # Export directly

# Get detailed table metadata as DataFrame
tables = conn.get_tables(schema_name='analytics')
large_tables = tables[tables['size_mb'] > 1000]

# Get complete warehouse structure
tree = conn.get_database_tree(format='dict')
```

### Unified Connection Layer

**Single API for all five database platforms with production-grade features.**

- **Connection Pooling** - Thread-safe management with configurable pool sizes
- **Automatic Retry Logic** - Exponential backoff with platform-specific error detection
- **Configuration-Driven** - YAML + .env support with environment variable substitution
- **Transaction Support** - Full ACID support where available (Snowflake, PostgreSQL, Redshift, SQL Server)
- **Schema Introspection** - Read schemas from existing databases
- **Context Managers** - Automatic connection lifecycle management

### Canonical Schema Architecture

**Platform-agnostic schema representation for cross-platform consistency.**

- **Bidirectional Mapping** - Database → CanonicalSchema → Database
- **Type Safety** - Logical type system with automatic conversions
- **Metadata Preservation** - Partitioning, clustering, optimization hints
- **Single Source of Truth** - One schema definition, multiple platform outputs

### Intelligent Schema Generation

**Automatic type detection and column standardization.**

- **Type Detection** - Automatic conversion of strings to dates, numbers, booleans
- **Column Standardization** - `User ID#` → `user_id`
- **NULL Handling** - Automatic REQUIRED vs NULLABLE detection
- **Multi-Platform DDL** - Generate CREATE TABLE for any target
- **Optimization Support** - Platform-specific partitioning, clustering, distribution

### Production-Ready Incremental Loads

**9 load patterns with platform-optimized SQL.**

- **UPSERT (MERGE)** - Insert new, update existing records
- **SCD Type 2** - Full history tracking with versioning
- **CDC** - Change data capture (Insert/Update/Delete operations)
- **Incremental Timestamp** - Load recent records based on timestamp
- **Append-Only** - Insert-only workflows
- **Delete-Insert** - Transactional replacement
- **Full Refresh** - Complete table reload
- **SCD Type 1** - Current state only (no history)
- **Snapshot** - Point-in-time captures

### Data Quality & Profiling

**Comprehensive data analysis and quality assessment.**

- **Quality Scoring** - Overall health assessment (0-100 scale)
- **Statistical Profiling** - Distributions, correlations, cardinality analysis
- **Anomaly Detection** - IQR, Z-score, Isolation Forest methods
- **Pattern Recognition** - Emails, phones, URLs, credit cards, dates
- **Missing Value Analysis** - Completeness scoring and imputation strategies
- **Feature Correlation** - Identify multicollinearity and feature relationships

### Intelligent Data Preprocessing

**Schema-aware cleaning and transformation pipelines.**

- **Validation Pipelines** - Email, phone, URL validation
- **Missing Data Handling** - Mean, median, KNN imputation
- **Duplicate Removal** - Smart deduplication strategies
- **Transformation Logging** - Full audit trail of all changes
- **Date Standardization** - Apply formats from canonical schema
- **Categorical Encoding** - Intelligent one-hot encoding for ML

### Metadata & Data Dictionary Framework

**Schema metadata as a first-class citizen.**

- **YAML-Driven Schemas** - Version control for schemas + metadata
- **Data Dictionary Exports** - Markdown, CSV, JSON formats
- **PII Governance** - Built-in PII flags for compliance
- **Metadata Validation** - Enforce required fields (description, owner, tags)
- **Documentation Generation** - Auto-generate data catalogs
- **Bidirectional Metadata** - Read from and write to databases

---

## Installation

```bash
# Basic installation
pip install schema-mapper

# With specific platform support
pip install schema-mapper[bigquery]
pip install schema-mapper[snowflake]
pip install schema-mapper[redshift]
pip install schema-mapper[postgresql]
pip install schema-mapper[sqlserver]

# With ML features (TensorFlow, scikit-learn)
pip install schema-mapper[ml]

# Install everything (all platforms + ML)
pip install schema-mapper[all]
```

---

## Quick Start

### Basic Workflow: DataFrame to Database

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
    conn.test_connection()
    conn.create_table_from_schema(schema, if_not_exists=True)

print(f"Successfully loaded {len(df_clean)} rows to BigQuery!")
```

### ML Feature Engineering Workflow

```python
from schema_mapper.profiler import Profiler
from schema_mapper.preprocessor import PreProcessor

# Load customer churn data
df = pd.read_csv('customer_churn.csv')

# 1. Analyze feature importance for churn prediction
profiler = Profiler(df, name='churn_analysis')
feature_importance = profiler.analyze_target_correlation(
    target_column='churn',
    method='pearson',
    top_n=15
)

print("Top features correlated with churn:")
print(feature_importance)

# 2. Visualize feature importance
fig = profiler.plot_target_correlation('churn', top_n=15, figsize=(10, 8))
fig.savefig('churn_feature_importance.png', dpi=300, bbox_inches='tight')

# 3. Auto-encode categorical features for ML
preprocessor = PreProcessor(df)
preprocessor.auto_encode_categorical(
    exclude_columns=['churn', 'customer_id'],
    max_categories=10,
    drop_first=True  # Avoid multicollinearity
)

# 4. ML-ready dataset
X = preprocessor.df.drop(['churn', 'customer_id'], axis=1)
y = preprocessor.df['churn'].map({'No': 0, 'Yes': 1})

# 5. Train your model
from sklearn.ensemble import RandomForestClassifier
clf = RandomForestClassifier()
clf.fit(X, y)
```

### Cross-Platform Migration

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

print("Migrated Snowflake → BigQuery!")
```

---

## Unified Connection System

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
    conn.test_connection()

    # Introspection
    exists = conn.table_exists('users', schema_name='public')
    schema = conn.get_target_schema('users', schema_name='public')
    tables = conn.list_tables(schema_name='public')

    # Execution (returns DataFrames)
    df = conn.execute_query("SELECT COUNT(*) FROM users")
    conn.execute_ddl("CREATE TABLE ...")
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
| **Connection Pooling** | Yes | Yes | Yes | Yes | Yes |
| **Auto Retry** | Yes | Yes | Yes | Yes | Yes |
| **Transactions** | Auto-commit | Full | Full | Full | Full |
| **Savepoints** | No | Yes | Yes | Yes | Yes |
| **Context Manager** | Yes | Yes | Yes | Yes | Yes |
| **DataFrame Queries** | Yes | Yes | Yes | Yes | Yes |
| **get_tables()** | Yes | Yes | Yes | Yes | Yes |
| **get_schemas()** | Yes (datasets) | Yes | Yes | Yes | Yes |
| **get_database_tree()** | Yes (project) | Yes | Yes | Yes | Yes |

---

## Canonical Schema Architecture

The canonical schema is schema-mapper's core abstraction—a platform-agnostic representation that ensures consistency across databases.

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
            date_format='%Y-%m-%d %H:%M:%S',
            timezone='UTC'
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
```

### Logical Type System

| Logical Type | BigQuery | Snowflake | PostgreSQL | Redshift | SQL Server |
|--------------|----------|-----------|------------|----------|------------|
| `BIGINT` | INT64 | NUMBER(38,0) | BIGINT | BIGINT | BIGINT |
| `INTEGER` | INT64 | NUMBER(38,0) | INTEGER | INTEGER | INT |
| `DECIMAL` | NUMERIC | NUMBER(p,s) | NUMERIC(p,s) | DECIMAL(p,s) | DECIMAL(p,s) |
| `FLOAT` | FLOAT64 | FLOAT | DOUBLE PRECISION | DOUBLE PRECISION | FLOAT |
| `STRING` | STRING | VARCHAR(16MB) | TEXT | VARCHAR(65535) | NVARCHAR(MAX) |
| `BOOLEAN` | BOOL | BOOLEAN | BOOLEAN | BOOLEAN | BIT |
| `DATE` | DATE | DATE | DATE | DATE | DATE |
| `TIMESTAMP` | TIMESTAMP | TIMESTAMP_NTZ | TIMESTAMP | TIMESTAMP | DATETIME2 |
| `TIMESTAMPTZ` | TIMESTAMP | TIMESTAMP_TZ | TIMESTAMPTZ | TIMESTAMPTZ | DATETIMEOFFSET |
| `JSON` | JSON | VARIANT | JSONB | VARCHAR | NVARCHAR(MAX) |

---

## Incremental Loads

Generate optimized DDL for 9 incremental load patterns across all platforms.

### Supported Patterns

| Pattern | Use Case | BigQuery | Snowflake | Redshift | PostgreSQL | SQL Server |
|---------|----------|----------|-----------|----------|------------|------------|
| **UPSERT (MERGE)** | Insert new, update existing | Native | Native | DELETE+INSERT | Native | Native |
| **SCD Type 2** | Full history tracking | Yes | Yes | Yes | Yes | Yes |
| **CDC** | Change data capture (I/U/D) | Yes | Yes | Yes | Yes | Yes |
| **Incremental Timestamp** | Load recent records | Yes | Yes | Yes | Yes | Yes |
| **Append Only** | Insert only | Yes | Yes | Yes | Yes | Yes |
| **Delete-Insert** | Transactional replacement | Yes | Yes | Yes | Yes | Yes |
| **Full Refresh** | Complete reload | Yes | Yes | Yes | Yes | Yes |
| **SCD Type 1** | Current state only | Yes | Yes | Yes | Yes | Yes |
| **Snapshot** | Point-in-time captures | Yes | Yes | Yes | Yes | Yes |

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
    scd2_columns=['name', 'address', 'phone'],
    effective_date_column='valid_from',
    end_date_column='valid_to',
    is_current_column='is_current'
)

generator = get_incremental_generator('snowflake')
ddl = generator.generate_incremental_ddl(schema, 'dim_customers', config)
```

---

## Use Cases

### 1. Multi-Cloud Data Migration

Migrate from AWS (Redshift) to GCP (BigQuery) with zero manual DDL writing.

```python
from schema_mapper.connections import ConnectionFactory, ConnectionConfig
from schema_mapper.renderers import RendererFactory

config = ConnectionConfig('connections.yaml')

# Introspect Redshift tables
with ConnectionFactory.get_connection('redshift', config) as rs_conn:
    tables = rs_conn.list_tables(schema_name='public')

    for table in tables:
        schema = rs_conn.get_target_schema(table, schema_name='public')
        renderer = RendererFactory.get_renderer('bigquery', schema)
        bq_ddl = renderer.to_ddl()

        with ConnectionFactory.get_connection('bigquery', config) as bq_conn:
            bq_conn.execute_ddl(bq_ddl)

        print(f"Migrated {table}")
```

### 2. ETL Pipeline with Quality Checks

Production ETL with profiling, cleaning, and validation gates.

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
    preprocess_pipeline=['fix_whitespace', 'standardize_column_names', 'remove_duplicates'],
    validate=True
)

# Quality gate
if report['quality']['overall_score'] < 80:
    print(f"Quality score too low: {report['quality']['overall_score']}/100")
    exit(1)

# Load
config = ConnectionConfig('connections.yaml')
with ConnectionFactory.get_connection('snowflake', config) as conn:
    conn.create_table_from_schema(schema, if_not_exists=True)

print(f"Loaded {len(df_clean)} rows with quality score {report['quality']['overall_score']}/100")
```

### 3. ML Feature Engineering for Churn Prediction

Automated feature analysis and preprocessing for machine learning models.

```python
from schema_mapper.profiler import Profiler
from schema_mapper.preprocessor import PreProcessor
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

# Load data
df = pd.read_csv('customer_churn.csv')

# 1. Analyze feature importance
profiler = Profiler(df, name='churn')
importance = profiler.analyze_target_correlation('churn', top_n=10)
print("Top 10 features:", importance['feature'].tolist())

# 2. Visualize
fig = profiler.plot_target_correlation('churn', top_n=15)
fig.savefig('feature_importance.png')

# 3. Auto-encode categoricals
preprocessor = PreProcessor(df)
preprocessor.auto_encode_categorical(
    exclude_columns=['churn', 'customer_id'],
    max_categories=15,
    drop_first=True
)

# 4. Prepare for ML
X = preprocessor.df.drop(['churn', 'customer_id'], axis=1)
y = preprocessor.df['churn'].map({'No': 0, 'Yes': 1})

# 5. Train model
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
clf = RandomForestClassifier(n_estimators=100)
clf.fit(X_train, y_train)
accuracy = clf.score(X_test, y_test)

print(f"Model accuracy: {accuracy:.2%}")
```

### 4. Incremental UPSERT Pipeline

Daily UPSERT of customer data with automatic merge statement generation.

```python
from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator

# New/updated customer records
df = pd.read_csv('customers_delta.csv')

# Generate MERGE DDL
schema = infer_canonical_schema(df, table_name='customers')
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['customer_id'],
    update_columns=['email', 'phone', 'address', 'updated_at']
)

generator = get_incremental_generator('bigquery')
merge_ddl = generator.generate_incremental_ddl(schema, 'customers', config)

# Execute MERGE
with ConnectionFactory.get_connection('bigquery', conn_config) as conn:
    conn.execute_ddl(merge_ddl)

print(f"UPSERT complete: {len(df)} customers processed")
```

---

## Examples

Explore complete, production-ready examples in [`examples/`](examples/):

### Core Use Cases
- [`01_basic_usage.py`](examples/01_basic_usage.py) - Simple DataFrame to database workflow
- [`02_multi_cloud_migration.py`](examples/02_multi_cloud_migration.py) - Multi-cloud migration (BigQuery to Snowflake)
- [`03_etl_with_quality_gates.py`](examples/03_etl_with_quality_gates.py) - ETL pipeline with quality gates
- [`04_incremental_upsert.py`](examples/04_incremental_upsert.py) - Incremental UPSERT loads
- [`05_scd_type2_tracking.py`](examples/05_scd_type2_tracking.py) - SCD Type 2 dimension tracking

### Production Integration
- [`06_prefect_orchestration.py`](examples/06_prefect_orchestration.py) - Prefect orchestration with tagged stages
- [`07_connection_pooling.py`](examples/07_connection_pooling.py) - Connection pooling for high-concurrency workloads
- [`08_metadata_data_dictionary.py`](examples/08_metadata_data_dictionary.py) - Metadata & data dictionary framework

### Data Science & ML
- [`09_data_profiling_analysis.py`](examples/09_data_profiling_analysis.py) - Statistical profiling and data quality analysis
- [`10_ml_feature_engineering.py`](examples/10_ml_feature_engineering.py) - ML feature importance and preprocessing

See [`examples/README.md`](examples/README.md) for setup instructions and configuration templates.

---

## Production Status

**Version**: 1.4.0
**Status**: Production-Ready
**Test Coverage**: 78-95% on core modules

### Platform Support

| Platform | Schema Gen | DDL Gen | Incremental | Connections | ML Features | Status |
|----------|------------|---------|-------------|-------------|-------------|--------|
| **BigQuery** | Yes | Yes | Yes | Yes | Yes | Production |
| **Snowflake** | Yes | Yes | Yes | Yes | Yes | Production |
| **Redshift** | Yes | Yes | Yes | Yes | Yes | Production |
| **PostgreSQL** | Yes | Yes | Yes | Yes | Yes | Production |
| **SQL Server** | Yes | Yes | Yes | Yes | Yes | Production |

### Recent Releases

**v1.4.0 (December 2024)** - Machine Learning Feature Engineering
- Target correlation analysis for classification and regression
- Automated categorical encoding with intelligent filtering
- Feature importance visualization
- ML preprocessing pipeline integration

**v1.3.0 (December 2024)** - DataFrame-First API & Enhanced Discovery
- All queries return pandas DataFrames
- Enhanced database introspection (get_tables, get_schemas, get_database_tree)
- Improved metadata inspection across platforms

**v1.2.0 (December 2024)** - Production-Grade Connections
- Unified connection system for all 5 platforms
- Connection pooling with thread-safe management
- Automatic retry logic with exponential backoff
- Full transaction support and schema introspection

---

## Testing

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=schema_mapper --cov-report=html

# Run integration tests (requires database credentials)
RUN_INTEGRATION_TESTS=1 pytest tests/integration/ -v
```

**Test Coverage:**
- ML features: 24 tests (20 passed, 4 skipped for optional dependencies)
- Connection system: 56 core tests (78% coverage)
- Integration tests: 65+ tests covering renderers, generators, workflows

---

## Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Built for data engineers and data scientists working across:
- Google Cloud Platform (BigQuery)
- Snowflake (Multi-Cloud)
- Amazon Web Services (Redshift)
- Microsoft Azure (SQL Server)
- PostgreSQL (Open Source)

---

## Resources

**Documentation:**
- [Incremental Loads Guide](docs/INCREMENTAL_LOADS.md)
- [Architecture Overview](ARCHITECTURE.md)
- [API Reference](docs/api-reference.md)

**Related Projects:**
- [pandas](https://pandas.pydata.org/) - Data analysis library
- [scikit-learn](https://scikit-learn.org/) - Machine learning framework
- [TensorFlow](https://www.tensorflow.org/) - Deep learning platform
- [Great Expectations](https://greatexpectations.io/) - Data validation

**Support:**
- [GitHub Issues](https://github.com/datateamsix/schema-mapper/issues)
- [GitHub Discussions](https://github.com/datateamsix/schema-mapper/discussions)

---

**Made for universal cloud data engineering and machine learning**
