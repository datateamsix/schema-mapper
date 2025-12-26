# 🚀 schema-mapper v1.3.0: Enhanced Database Discovery & DataFrame-First API

**Query Once. Analyze Anywhere. DataFrames Everywhere.**

We're excited to announce v1.3.0—a major enhancement to schema-mapper's connection layer! This release transforms how you discover, query, and explore data across BigQuery, Snowflake, Redshift, SQL Server, and PostgreSQL with a **DataFrame-first API** and powerful introspection methods.

---

## 🎯 What's New

### 1. 📊 DataFrame Query Results (Breaking Change → Better UX)
**All queries return pandas DataFrames. No more cursors or iterators.**

Before (v1.2.0):
```python
# BigQuery returned RowIterator
result = conn.execute_query("SELECT * FROM analytics.users")
for row in result:  # Had to iterate
    print(row)

# Snowflake returned cursor
result = conn.execute_query("SELECT * FROM PUBLIC.users")
for row in result:  # Had to iterate
    print(row)
```

After (v1.3.0):
```python
# ALL platforms return DataFrame
df = conn.execute_query("SELECT * FROM analytics.users")
print(df.head())  # Immediate DataFrame access
df.to_csv('users.csv')  # Export directly
df[df['age'] > 25]  # Filter easily
```

**What Changed:**
- BigQuery: Converts `RowIterator` → DataFrame automatically
- SQL platforms: Converts cursors → DataFrame with column names
- Consistent API across all 5 platforms
- Enhanced error handling with `ExecutionError` exceptions

### 2. 🔍 Enhanced Database Discovery
**Programmatically explore your entire data warehouse.**

#### New Methods:

**`get_tables()` - Detailed table inventory:**
```python
# Get all tables with metadata
tables = conn.get_tables(schema_name='analytics')
print(tables)
#   table_name table_type  num_rows  size_mb                 created
# 0      users      TABLE    150000    245.5  2024-01-01 10:00:00
# 1     events      TABLE   5000000   8920.3  2024-01-05 11:00:00
# 2  user_view       VIEW         0      0.0  2024-01-10 12:00:00

# Find large tables
large_tables = tables[tables['size_mb'] > 1000]

# Export inventory
tables.to_csv('table_inventory.csv')
```

**`get_schemas()` / `get_datasets()` - List all schemas:**
```python
# BigQuery
datasets = conn.get_datasets()
#   dataset_id  location         description
# 0  analytics        US  Analytics data
# 1  marketing        US  Marketing data

# Snowflake/Others
schemas = conn.get_schemas(database_name='MYDB')
#   schema_name schema_owner
# 0      PUBLIC     SYSADMIN
# 1   ANALYTICS      ANALYST
```

**`get_database_tree()` / `get_project_tree()` - Full hierarchy:**
```python
# Get as dictionary (JSON-ready)
tree = conn.get_project_tree(format='dict')
# {
#   'project_id': 'my-project',
#   'dataset_count': 2,
#   'datasets': [
#     {
#       'dataset_id': 'analytics',
#       'table_count': 15,
#       'tables': ['users', 'events', ...]
#     }
#   ]
# }

# Or get as flattened DataFrame
tree_df = conn.get_project_tree(format='dataframe')
#   project_id dataset_id  table_count              tables
# 0 my-project  analytics           15  users, events, ...
# 1 my-project  marketing            8  campaigns, ...

# Export warehouse structure
import json
with open('warehouse_structure.json', 'w') as f:
    json.dump(tree, f, indent=2)
```

### 3. 📚 Warehouse Structure Comparison Guide
**New comprehensive documentation for multi-platform teams.**

Added `docs/WAREHOUSE_STRUCTURE_COMPARISON.md`:
- **Quick Reference Table**: Side-by-side hierarchy comparison
  - BigQuery: Project → Dataset → Tables
  - Snowflake: Account → Database → Schema → Tables
  - Redshift: Cluster → Database → Schema → Tables
  - PostgreSQL: Server → Database → Schema → Tables
  - SQL Server: Server → Database → Schema → Tables

- **Terminology Mapping**: Cross-platform translation guide
  - "Dataset" (BigQuery) ≈ "Schema" (others)
  - Default schemas: PUBLIC (Snowflake), public (PG/Redshift), dbo (SQL Server)

- **Migration Considerations**: Platform-specific guidance
- **Query Syntax Reference**: How to reference tables on each platform
- **Complete API Coverage**: All available methods across platforms

---

## 💡 Why This Release Matters

### For Data Discovery Workflows

**Problem**: Exploring databases across platforms was inconsistent and required learning each platform's SDK:
```python
# BigQuery - had to learn google-cloud-bigquery
from google.cloud import bigquery
client = bigquery.Client()
for dataset in client.list_datasets():
    for table in client.list_tables(dataset):
        # Complex iteration...

# Snowflake - had to learn snowflake-connector
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
# Different cursor API...
```

**Solution**: Unified DataFrame-first API:
```python
# SAME CODE works for ALL platforms
tables = conn.get_tables(schema_name='...')
tree = conn.get_database_tree(format='dataframe')

# Export inventory across ALL your warehouses
for platform in ['bigquery', 'snowflake', 'redshift']:
    conn = ConnectionFactory.get_connection(platform, config)
    conn.connect()
    inventory = conn.get_database_tree(format='dataframe')
    inventory.to_csv(f'{platform}_inventory.csv')
```

### For Migration Planning

```python
# Discover source structure
source_conn = ConnectionFactory.get_connection('bigquery', config)
source_tree = source_conn.get_project_tree(format='dict')

# Plan target migration
for dataset in source_tree['datasets']:
    print(f"Migrating {dataset['dataset_id']}: {dataset['table_count']} tables")
    for table_name in dataset['tables']:
        # Get source schema and migrate
        schema = source_conn.get_target_schema(table_name,
                                                schema_name=dataset['dataset_id'])

        # Deploy to Snowflake with same code
        target_conn = ConnectionFactory.get_connection('snowflake', config)
        target_conn.create_table_from_schema(schema)
```

---

## 📊 What's Included in v1.3.0

**New Features:**
- DataFrame return type for `execute_query()` on all platforms
- `get_tables()` method with detailed metadata
- `get_schemas()` / `get_datasets()` methods
- `get_database_tree()` / `get_project_tree()` methods with dict/DataFrame outputs
- Enhanced BigQuery documentation (dataset.table format requirement)
- Comprehensive warehouse structure comparison guide

**Platform Support:**
- ✅ Google BigQuery
- ✅ Snowflake
- ✅ Amazon Redshift
- ✅ Microsoft SQL Server
- ✅ PostgreSQL

**Documentation:**
- WAREHOUSE_STRUCTURE_COMPARISON.md
- Updated API examples
- Migration guides

---

## 🚀 Getting Started

### Installation

```bash
pip install schema-mapper==1.3.0

# Or with specific platforms
pip install schema-mapper[bigquery,snowflake]

# Or install everything
pip install schema-mapper[all]
```

### Quick Examples

#### 1. Query and Get DataFrames
```python
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

config = ConnectionConfig('config/connections.yaml')
with ConnectionFactory.get_connection('bigquery', config) as conn:
    # Query returns DataFrame immediately
    df = conn.execute_query("SELECT * FROM analytics.users LIMIT 100")

    # Use DataFrame methods
    print(df.describe())
    df.to_csv('users.csv')
    df[df['age'] > 25].to_parquet('users_filtered.parquet')
```

#### 2. Discover Database Structure
```python
# List all tables with metadata
tables = conn.get_tables(schema_name='analytics')
print(f"Found {len(tables)} tables")

# Find large tables
large = tables[tables['size_mb'] > 1000]

# Get full warehouse structure
tree = conn.get_database_tree(format='dataframe')
tree.to_csv('warehouse_structure.csv')
```

#### 3. Cross-Platform Inventory
```python
platforms = ['bigquery', 'snowflake', 'redshift']
for platform in platforms:
    conn = ConnectionFactory.get_connection(platform, config)
    conn.connect()

    # Same API for all platforms
    tree = conn.get_database_tree(format='dict')
    print(f"{platform}: {tree['schema_count']} schemas")

    conn.disconnect()
```

---

## 📚 Resources

- **Documentation**: [README.md](https://github.com/datateamsix/schema-mapper#readme)
- **Warehouse Comparison**: [docs/WAREHOUSE_STRUCTURE_COMPARISON.md](https://github.com/datateamsix/schema-mapper/blob/main/docs/WAREHOUSE_STRUCTURE_COMPARISON.md)
- **Examples**: [examples/](https://github.com/datateamsix/schema-mapper/tree/main/examples)
- **PyPI**: [pypi.org/project/schema-mapper](https://pypi.org/project/schema-mapper/)
- **Issues**: [Report bugs or request features](https://github.com/datateamsix/schema-mapper/issues)

---

## ⬆️ Upgrading from v1.2.0

### Breaking Change: `execute_query()` Return Type

**Before (v1.2.0):**
```python
result = conn.execute_query("SELECT * FROM table")
for row in result:  # Iterator/cursor
    print(row)
```

**After (v1.3.0):**
```python
df = conn.execute_query("SELECT * FROM table")
print(df)  # DataFrame
```

**Migration:**
If you were iterating over results, simply use DataFrame iteration:
```python
# Old iteration still works
for _, row in df.iterrows():
    print(row)

# But DataFrames give you more power
df.apply(lambda row: process(row), axis=1)
```

### All Other Features: Fully Backward Compatible

All existing v1.2.0 code continues to work. New introspection methods are opt-in additions.

---

## 🙏 Thank You

Thank you to everyone using schema-mapper for their multi-platform data workflows. This release makes database discovery and querying more consistent, powerful, and Pythonic across all five supported platforms.

**Built for lean data teams. Not lean capabilities.**

---

**Full Changelog**: https://github.com/datateamsix/schema-mapper/compare/v1.2.0...v1.3.0
