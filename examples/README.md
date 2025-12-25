# Schema-Mapper Examples

Production-ready examples demonstrating schema-mapper's unified connection system and data pipeline capabilities.

## 📁 Example Files

### Core Use Cases (From README)

1. **[01_basic_usage.py](01_basic_usage.py)** - Simple DataFrame to database workflow
   - Load sample data
   - Infer canonical schema
   - Create table with connection
   - **Time: 5 minutes**

2. **[02_multi_cloud_migration.py](02_multi_cloud_migration.py)** - Multi-Cloud Migration
   - Introspect BigQuery table
   - Convert schema to canonical format
   - Deploy to Snowflake
   - **Time: 10 minutes** | **README Use Case #1**

3. **[03_etl_with_quality_gates.py](03_etl_with_quality_gates.py)** - ETL Pipeline with Quality Gates
   - Load messy CSV data
   - Profile and validate
   - Transform with quality checks
   - Load to database
   - **Time: 15 minutes** | **README Use Case #2**

4. **[04_incremental_upsert.py](04_incremental_upsert.py)** - Incremental UPSERT Loads
   - Generate UPSERT DDL
   - Merge new/updated records
   - Track changes
   - **Time: 10 minutes** | **README Use Case #3**

5. **[05_scd_type2_tracking.py](05_scd_type2_tracking.py)** - SCD Type 2 Dimension Tracking
   - Track historical changes
   - Maintain current/historical records
   - Generate SCD2 DDL
   - **Time: 15 minutes** | **README Use Case #4**

### Advanced Integration

6. **[06_prefect_orchestration.py](06_prefect_orchestration.py)** - Prefect Integration
   - Orchestrate ETL with Prefect
   - Tag pipeline stages
   - Error handling and retries
   - Monitoring and observability
   - **Time: 20 minutes**

7. **[07_connection_pooling.py](07_connection_pooling.py)** - Connection Pooling
   - Multi-threaded workloads
   - Pool management and statistics
   - **Time: 10 minutes**

8. **[08_metadata_data_dictionary.py](08_metadata_data_dictionary.py)** - Metadata & Data Dictionary Framework
   - Schema + Metadata as single source of truth
   - Enrich with descriptions, PII flags, tags
   - YAML-driven schema definitions
   - Export data dictionaries (Markdown, CSV, JSON)
   - Metadata validation and governance
   - **Time: 20 minutes** | **NEW FRAMEWORK**

## 🚀 Quick Start

### Prerequisites

```bash
# Install schema-mapper with platform dependencies
pip install schema-mapper[bigquery,snowflake,postgresql]

# For Prefect example
pip install prefect

# For connection pooling example (optional)
pip install pytest-benchmark
```

### Configuration

Create `config/connections.yaml`:

```yaml
target: bigquery

connections:
  bigquery:
    project: ${GCP_PROJECT}
    credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
    location: US

  snowflake:
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    account: ${SNOWFLAKE_ACCOUNT}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC

  postgresql:
    host: ${PG_HOST:-localhost}
    port: ${PG_PORT:-5432}
    database: ${PG_DATABASE}
    user: ${PG_USER}
    password: ${PG_PASSWORD}
```

Create `.env` file with credentials:

```bash
# GCP
GCP_PROJECT=my-project
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

# Snowflake
SNOWFLAKE_USER=admin
SNOWFLAKE_PASSWORD=secret123
SNOWFLAKE_ACCOUNT=abc123.us-east-1

# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=analytics
PG_USER=admin
PG_PASSWORD=secret123
```

### Running Examples

```bash
# Basic usage
python 01_basic_usage.py

# Multi-cloud migration
python 02_multi_cloud_migration.py

# ETL with quality gates
python 03_etl_with_quality_gates.py

# Incremental UPSERT
python 04_incremental_upsert.py

# SCD Type 2
python 05_scd_type2_tracking.py

# Prefect orchestration
python 06_prefect_orchestration.py

# Connection pooling
python 07_connection_pooling.py
```

## 📊 Sample Data

All examples use `../sample_data/problematic_clothing_retailer_data.csv`:
- **Realistic mess**: Multiple date formats, inconsistent naming, data quality issues
- **~100 rows** of retail order data
- **CDC columns** for SCD Type 2 examples
- Perfect for demonstrating schema-mapper's data quality and transformation capabilities

## 🎯 Learning Path

**Beginner**: Start with examples 1-2
- Understand basic workflow
- Learn canonical schema concept

**Intermediate**: Examples 3-5
- Production ETL patterns
- Incremental loads
- Change data capture

**Advanced**: Examples 6-7
- Orchestration integration
- Performance optimization
- Production deployment

## 🔧 Troubleshooting

**Connection errors**:
- Verify `.env` file exists and has correct credentials
- Check `connections.yaml` paths are correct
- Ensure platform dependencies installed

**Import errors**:
```bash
pip install schema-mapper[all]  # Install all platform dependencies
```

**Permission errors**:
- Verify service account has necessary permissions
- Check firewall allows database connections

## 📚 Additional Resources

- [Main Documentation](../README.md)
- [API Reference](../docs/api-reference.md)
- [Connection Configuration](../docs/connections.md)
- [Incremental Loads Guide](../docs/incremental-loads.md)

## 💡 Tips

1. **Start simple**: Run example 1 first to verify setup
2. **Use mock mode**: Most examples have a `DRY_RUN` flag to test without actual connections
3. **Check logs**: Enable debug logging with `SCHEMA_MAPPER_LOG_LEVEL=DEBUG`
4. **Iterate quickly**: Use sample data to test transformations before production
