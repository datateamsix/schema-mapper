# 🚀 schema-mapper v1.2.0: Production-Grade ETL for Lean Data Teams

**One API. Five Databases. Zero Rewrites.**

We're excited to announce v1.2.0—the biggest release yet! Schema-mapper has evolved from a schema management tool into a **complete production-grade ETL toolkit** for lean data teams working across BigQuery, Snowflake, Redshift, SQL Server, and PostgreSQL.

---

## 🎯 What's New

### 1. 🔌 Unified Connection Layer
**Stop learning five different SDKs. Use one API.**

- Single unified API for all 5 database platforms
- Connection pooling with thread-safe management
- Auto-retry with exponential backoff
- YAML + .env configuration (no hardcoded credentials)

```python
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

config = ConnectionConfig('config/connections.yaml')
with ConnectionFactory.get_connection('bigquery', config) as conn:
    conn.create_table_from_schema(schema)
    conn.load_dataframe(df, table_name, dataset_name)

# Switch platforms? Just change 'bigquery' to 'snowflake'
# Same code works everywhere!
```

### 2. 📖 Metadata & Data Dictionary Framework
**Schema = Structure + Meaning. Built-in governance.**

- YAML-driven schemas with version control
- Auto-generate data dictionaries (Markdown, CSV, JSON)
- PII governance flags for compliance
- Metadata validation—enforce required fields

```python
from schema_mapper import save_schema_to_yaml, load_schema_from_yaml

# Save schema + metadata to YAML (version control this!)
save_schema_to_yaml(schema, 'schemas/events.yaml')

# Validate metadata completeness (CI/CD ready)
errors = schema.validate_metadata(
    required_table_fields=['description', 'owner'],
    required_column_fields=['description', 'pii']
)

# Export data dictionary
markdown = schema.export_data_dictionary('markdown')
```

### 3. 🔄 Incremental Loads
**Production-ready UPSERT, SCD Type 2, and CDC patterns.**

- 9 incremental load patterns across all platforms
- Platform-optimized MERGE statements
- Automatic primary key detection
- Change tracking with full history

```python
from schema_mapper.incremental import IncrementalDDLGeneratorFactory

generator = IncrementalDDLGeneratorFactory.create('bigquery')
ddl = generator.generate(schema, pattern='upsert',
                        merge_keys=['user_id'])
```

### 4. 🏊 Connection Pooling
**High-concurrency workloads with efficient resource management.**

- Thread-safe connection pools
- Health checks and automatic validation
- Real-time pool statistics
- Configurable pool sizing and timeouts

```python
from schema_mapper.connections.utils.pooling import ConnectionPool

pool = ConnectionPool(platform='bigquery', config=config,
                     max_size=10, min_size=2)
with pool.get_connection() as conn:
    # Use connection with automatic health checking
    conn.execute_query(sql)
```

---

## 💡 Why This Release Matters

### For Lean Data Teams

If you're a small data team supporting multiple database platforms, you know the pain:
- Rewriting the same schema 5 different ways
- Learning platform-specific SDKs and DDL syntax
- Debugging type mismatches across platforms
- Maintaining separate codebases for each database

**v1.2.0 eliminates the "rewrite tax."**

### Write Once. Deploy Anywhere. Zero Rewrites.

```python
# Define your schema once (in Python or YAML)
schema = infer_canonical_schema(df, table_name='events')

# Deploy to ANY platform with the same code
for platform in ['bigquery', 'snowflake', 'redshift', 'postgresql', 'sqlserver']:
    with ConnectionFactory.get_connection(platform, config) as conn:
        conn.create_table_from_schema(schema)
        conn.load_dataframe(df, schema.table_name, schema.dataset_name)

# 5 databases, 1 codebase, 0 rewrites
```

---

## 📊 What's Included in v1.2.0

**New Features:**
- Unified connection factory for all 5 platforms
- YAML schema loader/saver
- Data dictionary export (Markdown, CSV, JSON)
- Metadata validation framework
- 9 incremental load patterns (UPSERT, SCD2, CDC, Snapshot, Append, etc.)
- Connection pooling with health checks
- PII governance flags
- Enhanced transaction support with savepoints

**Platform Support:**
- ✅ Google BigQuery
- ✅ Snowflake
- ✅ Amazon Redshift
- ✅ Microsoft SQL Server
- ✅ PostgreSQL

**Documentation:**
- 8 production-ready examples
- Comprehensive API reference
- Migration guides
- Best practices for lean data teams

---

## 🚀 Getting Started

### Installation

```bash
pip install schema-mapper==1.2.0

# Or with specific platforms
pip install schema-mapper[bigquery,snowflake]

# Or install everything
pip install schema-mapper[all]
```

### Quick Start

Check out the [examples directory](https://github.com/datateamsix/schema-mapper/tree/main/examples) for complete working examples:

1. **Basic Usage** - DataFrame to database in 5 minutes
2. **Multi-Cloud Migration** - Move data between platforms
3. **ETL with Quality Gates** - Production pipelines with validation
4. **Incremental UPSERT** - Merge new/updated records
5. **SCD Type 2** - Track historical changes
6. **Prefect Integration** - Orchestrate ETL workflows
7. **Connection Pooling** - High-concurrency workloads
8. **Metadata Framework** - YAML schemas + data dictionaries

---

## 📚 Resources

- **Documentation**: [README.md](https://github.com/datateamsix/schema-mapper#readme)
- **Examples**: [examples/](https://github.com/datateamsix/schema-mapper/tree/main/examples)
- **PyPI**: [pypi.org/project/schema-mapper](https://pypi.org/project/schema-mapper/)
- **Issues**: [Report bugs or request features](https://github.com/datateamsix/schema-mapper/issues)

---

## 🙏 Thank You

Thank you to everyone who provided feedback and helped shape this release. We're building schema-mapper for lean data teams who need enterprise-grade capabilities without enterprise-grade complexity.

**Built for lean data teams. Not lean capabilities.**

---

## ⬆️ Upgrading from v1.0.0 or v1.1.0

All existing code continues to work—this release is 100% backward compatible. New features are opt-in.

To use the new unified connection layer:

```python
# Old way (still works)
from schema_mapper import prepare_for_load
schema, df_clean = prepare_for_load(df)

# New way (recommended)
from schema_mapper import infer_canonical_schema
from schema_mapper.connections import ConnectionFactory, ConnectionConfig

schema = infer_canonical_schema(df)
config = ConnectionConfig('config/connections.yaml')
with ConnectionFactory.get_connection('bigquery', config) as conn:
    conn.create_table_from_schema(schema)
```

---

**Full Changelog**: https://github.com/datateamsix/schema-mapper/compare/v1.0.0...v1.2.0
