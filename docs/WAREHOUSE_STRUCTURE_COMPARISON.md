# Data Warehouse Structure Comparison

This document provides a comprehensive comparison of database hierarchy and terminology across all supported platforms in schema-mapper.

## Quick Reference: Hierarchy Levels

| Platform | Level 1 | Level 2 | Level 3 | Level 4 | Objects |
|----------|---------|---------|---------|---------|---------|
| **BigQuery** | Project | Dataset | - | - | Tables, Views, External Tables, Materialized Views |
| **Snowflake** | Account | Database | Schema | - | Tables, Views, Materialized Views, External Tables |
| **Redshift** | Cluster | Database | Schema | - | Tables, Views, Materialized Views |
| **PostgreSQL** | Server | Database | Schema | - | Tables, Views, Materialized Views |
| **SQL Server** | Server | Database | Schema | - | Tables, Views, Indexed Views |

## Detailed Platform Structures

### Google BigQuery

```
Project (billing & organization container)
└── Dataset (logical grouping, location-specific)
    └── Tables & Views
        ├── TABLE (standard table)
        ├── VIEW (virtual table)
        ├── EXTERNAL (federated table)
        └── MATERIALIZED_VIEW (precomputed view)
```

**Key Characteristics:**
- **Project**: Top-level billing and organization unit (GCP project)
- **Dataset**: Similar to a "schema" or "database" in other systems
  - Location-specific (US, EU, etc.)
  - Access control boundary
  - Cannot span locations
- **No traditional "schema" layer** - datasets serve this purpose
- Tables must be referenced as `project.dataset.table` or `dataset.table`

**schema-mapper Methods:**
- `get_datasets()` - List all datasets in project
- `get_tables(schema_name='dataset_name')` - List tables in dataset
- `get_project_tree()` - Full project → datasets → tables hierarchy

---

### Snowflake

```
Account (organizational container)
└── Database (logical grouping)
    └── Schema (namespace)
        └── Tables & Views
            ├── TABLE (permanent or transient)
            ├── VIEW (virtual table)
            ├── MATERIALIZED_VIEW (precomputed view)
            ├── EXTERNAL TABLE (data in cloud storage)
            └── TEMPORARY TABLE (session-scoped)
```

**Key Characteristics:**
- **Account**: Organization-wide container (e.g., `myorg.snowflakecomputing.com`)
- **Database**: Logical grouping of schemas
  - Can be shared across accounts
  - Contains schemas
- **Schema**: Namespace within a database
  - Similar to PostgreSQL schemas
  - Contains tables, views, and other objects
- Tables referenced as `database.schema.table`

**schema-mapper Methods:**
- `get_schemas(database_name='mydb')` - List all schemas in database
- `get_tables(schema_name='PUBLIC', database_name='mydb')` - List tables in schema
- `get_database_tree()` - Full database → schemas → tables hierarchy

---

### Amazon Redshift

```
Cluster (compute & storage)
└── Database (logical grouping)
    └── Schema (namespace)
        └── Tables & Views
            ├── TABLE (standard table)
            ├── VIEW (virtual table)
            └── MATERIALIZED_VIEW (precomputed view)
```

**Key Characteristics:**
- **Cluster**: Physical compute and storage unit
  - Single database per cluster connection
- **Database**: Container for schemas
  - Based on PostgreSQL
- **Schema**: Namespace within a database
  - Default schema is `public`
  - PostgreSQL-compatible
- Tables referenced as `schema.table` (database implicit in connection)

**schema-mapper Methods:**
- `get_schemas()` - List all schemas in current database
- `get_tables(schema_name='public')` - List tables in schema
- `get_database_tree()` - Full database → schemas → tables hierarchy

---

### PostgreSQL

```
Server (instance)
└── Database (logical grouping)
    └── Schema (namespace)
        └── Tables & Views
            ├── TABLE (heap, partitioned)
            ├── VIEW (virtual table)
            ├── MATERIALIZED_VIEW (precomputed view)
            ├── FOREIGN TABLE (FDW)
            └── TEMPORARY TABLE (session-scoped)
```

**Key Characteristics:**
- **Server**: PostgreSQL instance
  - Can host multiple databases
- **Database**: Logical grouping of schemas
  - Databases are isolated (no cross-database queries)
- **Schema**: Namespace within a database
  - Default schema is `public`
  - Search path determines implicit schema resolution
- Tables referenced as `schema.table` (database implicit in connection)

**schema-mapper Methods:**
- `get_schemas()` - List all schemas in current database
- `get_tables(schema_name='public')` - List tables in schema
- `get_database_tree()` - Full database → schemas → tables hierarchy

---

### Microsoft SQL Server

```
Server (instance)
└── Database (logical grouping)
    └── Schema (namespace)
        └── Tables & Views
            ├── USER_TABLE (standard table)
            ├── VIEW (virtual table)
            ├── INDEXED_VIEW (materialized view equivalent)
            └── TEMPORARY TABLE (#temp, ##temp)
```

**Key Characteristics:**
- **Server**: SQL Server instance
  - Can host multiple databases
- **Database**: Logical grouping of schemas
  - Can perform cross-database queries
- **Schema**: Namespace within a database
  - Default schema is `dbo`
  - Four-part naming: `server.database.schema.table`
- Tables referenced as `database.schema.table` or `schema.table`

**schema-mapper Methods:**
- `get_schemas(database_name='mydb')` - List all schemas in database
- `get_tables(schema_name='dbo', database_name='mydb')` - List tables in schema
- `get_database_tree()` - Full database → schemas → tables hierarchy

---

## Terminology Mapping

When migrating between platforms, use this mapping to translate concepts:

| Concept | BigQuery | Snowflake | Redshift | PostgreSQL | SQL Server |
|---------|----------|-----------|----------|------------|------------|
| **Top Container** | Project | Account | Cluster | Server | Server |
| **Database-like** | Dataset | Database | Database | Database | Database |
| **Schema-like** | Dataset | Schema | Schema | Schema | Schema |
| **Namespace** | Dataset | Schema | Schema | Schema | Schema |
| **Default Schema** | - | PUBLIC | public | public | dbo |
| **Table Reference** | `project.dataset.table` | `database.schema.table` | `schema.table` | `schema.table` | `database.schema.table` |

## Common Use Cases

### Listing All Databases/Datasets

```python
# BigQuery
datasets = bq_conn.get_datasets()

# Snowflake
schemas = sf_conn.get_schemas(database_name='MYDB')

# Redshift
schemas = rs_conn.get_schemas()

# PostgreSQL
schemas = pg_conn.get_schemas()

# SQL Server
schemas = ss_conn.get_schemas(database_name='MyDB')
```

### Getting Full Hierarchy

```python
# BigQuery
tree = bq_conn.get_project_tree(format='dict')
# Returns: {'project_id': '...', 'datasets': [...]}

# Snowflake
tree = sf_conn.get_database_tree(format='dict')
# Returns: {'database': '...', 'schemas': [...]}

# Other platforms (similar pattern)
tree = conn.get_database_tree(format='dict')
```

### Listing Tables with Metadata

```python
# All platforms (consistent API)
tables = conn.get_tables(schema_name='...')
# Returns pandas DataFrame with: table_name, table_type, size_mb, etc.
```

## Migration Considerations

### BigQuery ↔ Snowflake

- **BigQuery Dataset** ≈ **Snowflake Database + Schema**
- BigQuery datasets are location-specific; Snowflake databases are not
- Both support external tables and materialized views
- Query syntax: `dataset.table` (BQ) vs `database.schema.table` (SF)

### PostgreSQL ↔ Redshift

- Nearly identical schema structure (Redshift is PostgreSQL-based)
- Both use `schema.table` references
- Both have `public` as default schema
- Redshift adds columnar storage and distribution styles

### SQL Server ↔ PostgreSQL

- Both support multi-schema databases
- Different default schemas: `dbo` (SQL Server) vs `public` (PostgreSQL)
- SQL Server supports cross-database queries; PostgreSQL requires FDW
- Table references: `database.schema.table` (SQL Server) vs `schema.table` (PostgreSQL)

---

## Summary Table: schema-mapper API Coverage

| Method | BigQuery | Snowflake | Redshift | PostgreSQL | SQL Server |
|--------|----------|-----------|----------|------------|------------|
| **list_tables()** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **get_tables()** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **get_schemas() / get_datasets()** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **get_database_tree() / get_project_tree()** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **get_target_schema()** | ✓ | ✓ | ✓ | ✓ | ✓ |
| **execute_query()** → DataFrame | ✓ | ✓ | ✓ | ✓ | ✓ |

All methods return **pandas DataFrames** for consistent data manipulation across platforms.
