## 🚀 Quick Start - Renderer Architecture

### 30-Second Overview

**Old Way:**
```python
mapper = SchemaMapper('bigquery')
ddl = mapper.generate_ddl(df, 'table')  # Just DDL
```

**New Way:**
```python
canonical = infer_canonical_schema(df, table_name='table')
renderer = RendererFactory.get_renderer('bigquery', canonical)

ddl = renderer.to_ddl()                    # DDL
json_schema = renderer.to_schema_json()    # JSON (BigQuery)
create_cmd = renderer.to_cli_create()      # CLI command
load_cmd = renderer.to_cli_load('data.csv') # Load command
```

**Why Better:** One schema → Multiple outputs (DDL, JSON, CLI)

---

### Example 1: BigQuery with Partitioning

```python
import pandas as pd
from schema_mapper.canonical import infer_canonical_schema
from schema_mapper.renderers import RendererFactory

# Your data
df = pd.read_csv('events.csv')

# Infer canonical schema with optimization
canonical = infer_canonical_schema(
    df,
    table_name='events',
    dataset_name='analytics',
    project_id='my-project',
    partition_columns=['event_date'],        # Partition by date
    cluster_columns=['user_id', 'event_type'], # Cluster by these
    partition_expiration_days=365            # Auto-delete old partitions
)

# Get BigQuery renderer
renderer = RendererFactory.get_renderer('bigquery', canonical)

# Generate DDL
print(renderer.to_ddl())
# Output:
# CREATE TABLE `my-project.analytics.events` (
#   event_id INT64,
#   user_id INT64,
#   event_date DATE
# )
# PARTITION BY event_date
# CLUSTER BY user_id, event_type
# OPTIONS(partition_expiration_days=365);

# Generate JSON schema (BigQuery-specific)
with open('schema.json', 'w') as f:
    f.write(renderer.to_schema_json())

# Get CLI command to create table
print(renderer.to_cli_create())

# Get CLI command to load data
print(renderer.to_cli_load('events_clean.csv'))
```

---

### Example 2: Snowflake with Clustering

```python
# Same canonical schema, different platform
renderer = RendererFactory.get_renderer('snowflake', canonical)

print(renderer.to_ddl())
# Output:
# CREATE TABLE analytics.events (
#   event_id NUMBER(38,0),
#   user_id NUMBER(38,0),
#   event_date DATE
# )
# CLUSTER BY (event_date, user_id);

# Snowflake uses DDL, not JSON (correctly handled)
print(renderer.supports_json_schema())  # False

# CLI commands for Snowflake
print(renderer.to_cli_create())
print(renderer.to_cli_load('events_clean.csv'))
```

---

### Example 3: Redshift with Distribution

```python
# For Redshift, use distribution instead of clustering
canonical.optimization.distribution_column = 'user_id'
canonical.optimization.sort_columns = ['event_date']

renderer = RendererFactory.get_renderer('redshift', canonical)

print(renderer.to_ddl())
# Output:
# CREATE TABLE analytics.events (
#   "event_id" BIGINT,
#   "user_id" BIGINT,
#   "event_date" DATE
# )
# DISTSTYLE KEY
# DISTKEY (user_id)
# SORTKEY (event_date);
```

---

### Example 4: All Platforms at Once

```python
# Generate DDL for all platforms
platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

for platform in platforms:
    # Adjust optimization per platform
    if platform == 'redshift':
        canonical.optimization.distribution_column = 'user_id'
        canonical.optimization.cluster_columns = []
    else:
        canonical.optimization.distribution_column = None
        canonical.optimization.cluster_columns = ['user_id', 'event_type']

    renderer = RendererFactory.get_renderer(platform, canonical)

    # Save DDL
    with open(f'{platform}_ddl.sql', 'w') as f:
        f.write(renderer.to_ddl())

    # Save CLI commands
    with open(f'{platform}_commands.sh', 'w') as f:
        f.write(renderer.to_cli_create())
        f.write('\n\n')
        f.write(renderer.to_cli_load('data.csv'))

print("Generated DDL and CLI commands for all platforms!")
```

---

### Example 5: Save Canonical Schema for Version Control

```python
from schema_mapper.canonical import canonical_schema_to_dict
import json

# Convert to dict for JSON serialization
schema_dict = canonical_schema_to_dict(canonical)

# Save to file
with open('schemas/events_v1.json', 'w') as f:
    json.dump(schema_dict, f, indent=2)

# Now in git:
# git add schemas/events_v1.json
# git commit -m "Add events table schema v1"
```

---

### Key Concepts

#### 1. Canonical Schema = Source of Truth

```python
canonical = CanonicalSchema(
    table_name='events',
    columns=[...],              # Platform-agnostic types
    optimization=OptimizationHints(...)  # Logical hints
)
```

- **Logical types** (INTEGER, STRING, TIMESTAMP)
- **Platform-agnostic**
- **Reusable** across all platforms

#### 2. Renderers = Platform Adapters

```python
renderer = RendererFactory.get_renderer('bigquery', canonical)
```

- Converts logical types → physical types
- Validates against platform capabilities
- Generates platform-specific DDL/JSON/CLI

#### 3. JSON Schema ≠ DDL

| Platform | JSON Schema | DDL | Why |
|----------|-------------|-----|-----|
| BigQuery | ✅ Yes | ✅ Yes | Native JSON support |
| Snowflake | ❌ No | ✅ Yes | DDL-driven |
| Redshift | ❌ No | ✅ Yes | DDL-driven |
| PostgreSQL | ❌ No | ✅ Yes | DDL-driven |

Don't try to force JSON where it doesn't belong!

---

### Common Patterns

#### Pattern: Transient Staging Table (Snowflake)

```python
canonical = infer_canonical_schema(
    df,
    table_name='staging_events',
    transient=True  # No Time Travel (cheaper)
)

renderer = RendererFactory.get_renderer('snowflake', canonical)
print(renderer.to_ddl())
# CREATE TRANSIENT TABLE ...
```

#### Pattern: Small Dimension Table (Redshift)

```python
# For small lookup tables, replicate to all nodes
canonical.optimization.distribution_column = None  # No DISTKEY
canonical.optimization.sort_columns = ['id']

# Note: Renderer will use DISTSTYLE ALL for small tables
```

#### Pattern: Partition Expiration (BigQuery)

```python
canonical = infer_canonical_schema(
    df,
    table_name='logs',
    partition_columns=['log_date'],
    partition_expiration_days=30,  # Delete after 30 days
    require_partition_filter=True  # Force partition filter in queries
)
```

---

### Troubleshooting

#### Error: "Schema not compatible with snowflake: snowflake does not support partitioning"

```python
# ❌ Wrong: Trying to partition Snowflake
canonical.optimization.partition_columns = ['date']

# ✅ Right: Use clustering instead
canonical.optimization.partition_columns = []
canonical.optimization.cluster_columns = ['date', 'user_id']
```

#### Error: "BigQuery supports max 4 clustering columns"

```python
# ❌ Wrong: Too many columns
canonical.optimization.cluster_columns = ['a', 'b', 'c', 'd', 'e']

# ✅ Right: Pick top 4 most selective
canonical.optimization.cluster_columns = ['user_id', 'event_type']
```

#### Error: "Redshift does not support clustering"

```python
# ❌ Wrong: Clustering not supported
canonical.optimization.cluster_columns = ['user_id']

# ✅ Right: Use sort keys instead
canonical.optimization.sort_columns = ['user_id', 'event_date']
canonical.optimization.distribution_column = 'user_id'
```

---

### Next Steps

1. **Run the example** - [canonical_schema_usage.py](examples/canonical_schema_usage.py)
2. **Read architecture** - [ARCHITECTURE.md](ARCHITECTURE.md)
3. **Review platforms** - [DDL_REVIEW.md](DDL_REVIEW.md)
4. **Check implementation** - [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)

---

### Complete Workflow

```python
# 1. Load data
df = pd.read_csv('events.csv')

# 2. Infer canonical schema
canonical = infer_canonical_schema(
    df,
    table_name='events',
    dataset_name='analytics',
    partition_columns=['event_date'],
    cluster_columns=['user_id', 'event_type']
)

# 3. Validate
errors = canonical.validate()
if errors:
    print(f"Errors: {errors}")
    exit(1)

# 4. Get renderer
renderer = RendererFactory.get_renderer('bigquery', canonical)

# 5. Generate artifacts
ddl = renderer.to_ddl()
create_cmd = renderer.to_cli_create()
load_cmd = renderer.to_cli_load('events_clean.csv')

# 6. Save files
with open('create_table.sql', 'w') as f:
    f.write(ddl)

with open('create_table.sh', 'w') as f:
    f.write(create_cmd)

with open('load_data.sh', 'w') as f:
    f.write(load_cmd)

# 7. Execute (optional)
# subprocess.run(['bash', 'create_table.sh'])
# subprocess.run(['bash', 'load_data.sh'])

print("✅ Done! Check create_table.sql and .sh files")
```

**Happy rendering!** 🚀
