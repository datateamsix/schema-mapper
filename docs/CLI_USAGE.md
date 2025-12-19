# CLI Usage Guide - Renderer Architecture

## Overview

The `schema-mapper` CLI now uses the **renderer architecture** for clean, powerful schema generation across all platforms.

**Flow:** CSV → Canonical Schema → Platform Renderer → DDL/JSON/CLI

---

## Quick Examples

### Basic DDL Generation

```bash
# BigQuery (default)
schema-mapper events.csv --table-name events

# Snowflake
schema-mapper events.csv --platform snowflake --table-name events

# All platforms at once
schema-mapper events.csv --platform all --table-name events
```

### With Optimization

```bash
# BigQuery with partitioning and clustering
schema-mapper events.csv \
  --platform bigquery \
  --table-name events \
  --dataset-name analytics \
  --partition-by event_date \
  --cluster-by "user_id,event_type" \
  --partition-expiration-days 365

# Snowflake with clustering
schema-mapper events.csv \
  --platform snowflake \
  --table-name events \
  --cluster-by "event_date,user_id" \
  --transient

# Redshift with distribution and sort keys
schema-mapper events.csv \
  --platform redshift \
  --table-name events \
  --distribution-key user_id \
  --sort-keys "event_date,event_ts"
```

---

## All Options

### Input/Output

| Option | Description | Example |
|--------|-------------|---------|
| `input` | Input CSV file (required) | `events.csv` |
| `--output`, `-o` | Output file path | `-o events.sql` |
| `--platform` | Target platform | `--platform bigquery` |

**Platforms:** `bigquery`, `snowflake`, `redshift`, `postgresql`, `all`

### Table Identity

| Option | Description | Example |
|--------|-------------|---------|
| `--table-name` | Table name | `--table-name events` |
| `--dataset-name` | Dataset/schema name | `--dataset-name analytics` |
| `--project-id` | Project ID (BigQuery) | `--project-id my-project` |

### Output Formats

| Option | Description | Platforms |
|--------|-------------|-----------|
| `--ddl` | Generate DDL (default) | All |
| `--json-schema` | Generate JSON schema | BigQuery only |
| `--cli-create` | CLI command to create table | All |
| `--cli-load FILE` | CLI command to load data | All |
| `--canonical` | Output canonical schema JSON | All |

### Clustering

| Option | Description | Platforms |
|--------|-------------|-----------|
| `--cluster-by` | Comma-separated columns | BigQuery, Snowflake |

```bash
--cluster-by "user_id,event_type,country"
```

**Limits:**
- BigQuery: Max 4 columns
- Snowflake: Max 4 columns

### Partitioning

| Option | Description | Platforms |
|--------|-------------|-----------|
| `--partition-by` | Partition column | BigQuery, PostgreSQL |
| `--partition-type` | Type: time, range, list, hash | BigQuery, PostgreSQL |
| `--partition-expiration-days` | Auto-delete after N days | BigQuery |
| `--require-partition-filter` | Force partition filter | BigQuery |

```bash
--partition-by event_date \
--partition-expiration-days 365 \
--require-partition-filter
```

### Distribution (Redshift)

| Option | Description | Values |
|--------|-------------|--------|
| `--distribution-key` | Column for KEY distribution | Column name |
| `--distribution-style` | Distribution style | `auto`, `key`, `all`, `even` |

```bash
--distribution-style key --distribution-key user_id
```

### Sort Keys (Redshift)

| Option | Description | Default |
|--------|-------------|---------|
| `--sort-keys` | Comma-separated columns | - |
| `--sort-interleaved` | Use INTERLEAVED vs COMPOUND | COMPOUND |

```bash
--sort-keys "event_date,user_id,event_ts"
```

### Table Options

| Option | Description | Platforms |
|--------|-------------|-----------|
| `--transient` | Create transient table | Snowflake |

### Data Processing

| Option | Description | Default |
|--------|-------------|---------|
| `--no-standardize` | Don't standardize column names | Standardize |
| `--no-auto-cast` | Don't auto-cast types | Auto-cast |
| `--validate` | Validate data quality | No validation |
| `--prepare` | Output cleaned CSV | Output DDL |

---

## Complete Examples

### 1. BigQuery Production Table

```bash
schema-mapper user_events.csv \
  --platform bigquery \
  --table-name user_events \
  --dataset-name production \
  --project-id my-gcp-project \
  --partition-by event_date \
  --cluster-by "user_id,event_type,country" \
  --partition-expiration-days 730 \
  --require-partition-filter \
  --output user_events.sql
```

**Output:** `user_events.sql`
```sql
CREATE TABLE `my-gcp-project.production.user_events` (
  event_id INT64 NOT NULL,
  user_id INT64 NOT NULL,
  event_type STRING,
  event_date DATE NOT NULL,
  country STRING
)
PARTITION BY event_date
CLUSTER BY user_id, event_type, country
OPTIONS(
  partition_expiration_days=730,
  require_partition_filter=true
);
```

### 2. Snowflake Staging Table

```bash
schema-mapper staging_data.csv \
  --platform snowflake \
  --table-name staging_events \
  --dataset-name staging \
  --cluster-by "load_date,user_id" \
  --transient \
  --output staging_events.sql
```

**Output:** `staging_events.sql`
```sql
CREATE TRANSIENT TABLE staging.staging_events (
  event_id NUMBER(38,0),
  user_id NUMBER(38,0),
  load_date DATE
)
CLUSTER BY (load_date, user_id);
```

### 3. Redshift Analytics Table

```bash
schema-mapper analytics_facts.csv \
  --platform redshift \
  --table-name fact_events \
  --dataset-name analytics \
  --distribution-style key \
  --distribution-key user_id \
  --sort-keys "event_date,event_timestamp" \
  --output fact_events.sql
```

**Output:** `fact_events.sql`
```sql
CREATE TABLE analytics.fact_events (
  "event_id" BIGINT NOT NULL,
  "user_id" BIGINT NOT NULL,
  "event_date" DATE NOT NULL
)
DISTSTYLE KEY
DISTKEY (user_id)
SORTKEY (event_date, event_timestamp);
```

### 4. Multi-Platform Generation

```bash
schema-mapper events.csv \
  --platform all \
  --table-name events \
  --dataset-name analytics \
  --cluster-by "user_id,event_type" \
  --partition-by event_date \
  --output events.sql
```

**Output:**
- `events_bigquery.sql` - With PARTITION BY and CLUSTER BY
- `events_snowflake.sql` - With CLUSTER BY
- `events_redshift.sql` - With SORTKEY (adapts cluster-by to sort-keys)
- `events_postgresql.sql` - With PARTITION BY and INDEX

### 5. Generate CLI Commands

```bash
# Get command to create table
schema-mapper events.csv \
  --platform bigquery \
  --cli-create \
  --table-name events \
  --dataset-name analytics \
  --project-id my-project \
  --partition-by event_date \
  --cluster-by "user_id,event_type"
```

**Output:**
```bash
bq query --use_legacy_sql=false "CREATE TABLE `my-project.analytics.events` (...) PARTITION BY event_date CLUSTER BY user_id, event_type"
```

```bash
# Get command to load data
schema-mapper events.csv \
  --platform bigquery \
  --cli-load events_clean.csv \
  --table-name events \
  --dataset-name analytics \
  --project-id my-project
```

**Output:**
```bash
# Save schema to temporary file
cat > /tmp/bq_schema.json << 'EOF'
[{"name": "event_id", "type": "INT64", ...}]
EOF

# Load data into BigQuery
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --schema=/tmp/bq_schema.json \
  my-project:analytics.events \
  events_clean.csv
```

### 6. Version Control Schema

```bash
# Save canonical schema for version control
schema-mapper events.csv \
  --canonical \
  --table-name events \
  --cluster-by "user_id,event_type" \
  --partition-by event_date \
  --output schemas/events_v1.json
```

**Output:** `schemas/events_v1.json`
```json
{
  "table_name": "events",
  "columns": [
    {
      "name": "event_id",
      "logical_type": "bigint",
      "nullable": false
    },
    ...
  ],
  "optimization": {
    "partition_columns": ["event_date"],
    "cluster_columns": ["user_id", "event_type"]
  }
}
```

Then commit to git:
```bash
git add schemas/events_v1.json
git commit -m "Add events table schema v1"
```

### 7. Data Validation

```bash
schema-mapper dirty_data.csv \
  --validate \
  --table-name events
```

**Output:**
```
📊 Loaded 1,000 rows, 10 columns

⚠️  Validation Warnings:
  • Column 'email' has 5 null values
  • Column 'revenue' has 150 null values

❌ Validation Errors:
  • Column 'id' has duplicate values
  • Column 'date' has invalid date formats in 3 rows
```

### 8. Prepare Clean Data

```bash
schema-mapper raw_data.csv \
  --prepare \
  --platform bigquery \
  --output clean_data.csv
```

**Output:** `clean_data.csv` with:
- Standardized column names
- Auto-cast types (strings → dates, numbers, booleans)
- Platform-compatible formatting

---

## Common Workflows

### Workflow 1: Development to Production

```bash
# 1. Validate raw data
schema-mapper raw_events.csv --validate

# 2. Clean data
schema-mapper raw_events.csv --prepare --output events_clean.csv

# 3. Generate DDL with optimization
schema-mapper events_clean.csv \
  --platform bigquery \
  --table-name events \
  --dataset-name production \
  --partition-by event_date \
  --cluster-by "user_id,event_type" \
  --output create_events.sql

# 4. Execute DDL
bq query --use_legacy_sql=false < create_events.sql

# 5. Load data
bq load --source_format=CSV production.events events_clean.csv
```

### Workflow 2: Multi-Cloud Deployment

```bash
# Generate DDL for all platforms
schema-mapper events.csv \
  --platform all \
  --table-name events \
  --cluster-by "user_id,event_type" \
  --partition-by event_date \
  --output ddl/events.sql

# Results:
# ddl/events_bigquery.sql
# ddl/events_snowflake.sql
# ddl/events_redshift.sql
# ddl/events_postgresql.sql

# Deploy to each platform
bq query < ddl/events_bigquery.sql
snowsql -f ddl/events_snowflake.sql
psql -f ddl/events_redshift.sql
psql -f ddl/events_postgresql.sql
```

### Workflow 3: Schema Evolution

```bash
# Save current schema
schema-mapper events_v1.csv \
  --canonical \
  --table-name events \
  --output schemas/events_v1.json

# Later, with new columns...
schema-mapper events_v2.csv \
  --canonical \
  --table-name events \
  --output schemas/events_v2.json

# Compare schemas
diff schemas/events_v1.json schemas/events_v2.json

# Generate migration DDL (future feature)
```

---

## Tips & Best Practices

### 1. Always Validate First
```bash
schema-mapper data.csv --validate
```

### 2. Use --canonical for Version Control
```bash
schema-mapper data.csv --canonical -o schema.json
git add schema.json
```

### 3. Test with --platform all
```bash
# Ensure schema works on all platforms
schema-mapper data.csv --platform all --ddl
```

### 4. Save CLI Commands for Documentation
```bash
schema-mapper data.csv --cli-create > docs/create_table.sh
schema-mapper data.csv --cli-load data.csv > docs/load_data.sh
```

### 5. Use Platform-Specific Optimizations

**BigQuery:** Partition + Cluster
```bash
--partition-by date --cluster-by "user_id,type"
```

**Snowflake:** Just Cluster (auto micro-partitioning)
```bash
--cluster-by "date,user_id"
```

**Redshift:** Distribution + Sort
```bash
--distribution-key user_id --sort-keys "date,timestamp"
```

---

## Error Messages

### "does not support partitioning"
```bash
# ❌ Snowflake doesn't support user-defined partitioning
schema-mapper data.csv --platform snowflake --partition-by date

# ✅ Use clustering instead
schema-mapper data.csv --platform snowflake --cluster-by "date,user_id"
```

### "supports max 4 clustering columns"
```bash
# ❌ Too many columns
schema-mapper data.csv --cluster-by "a,b,c,d,e"

# ✅ Pick top 4 most selective
schema-mapper data.csv --cluster-by "user_id,date,type,country"
```

### "does not support JSON schemas"
```bash
# ❌ Only BigQuery supports JSON schemas
schema-mapper data.csv --platform snowflake --json-schema

# ✅ Use DDL instead
schema-mapper data.csv --platform snowflake --ddl
```

---

## Next Steps

1. **Try examples** - Run the commands above
2. **Read architecture** - [ARCHITECTURE.md](ARCHITECTURE.md)
3. **Quick start** - [QUICKSTART_RENDERER.md](QUICKSTART_RENDERER.md)
4. **Full guide** - [README.md](README.md)

**Happy schema mapping!** 🚀
