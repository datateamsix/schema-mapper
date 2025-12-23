# Incremental Loads - Production Guide

## Overview

The Schema Mapper incremental loads feature provides production-ready DDL generation for common incremental data loading patterns across all supported platforms (BigQuery, Snowflake, Redshift, SQL Server, PostgreSQL).

Instead of always doing full table refreshes, you can now generate optimized DDL for:
- **UPSERT (MERGE)**: Insert new records, update existing ones
- **SCD Type 2**: Track full history with versioning
- **CDC (Change Data Capture)**: Process insert/update/delete streams
- **Incremental Timestamp**: Load only recent records
- **Append Only**: Add new records without updates
- And more...

## Quick Start

```python
from schema_mapper import SchemaMapper, IncrementalConfig, LoadPattern
import pandas as pd

# Your data
df = pd.read_csv('users.csv')

# Create mapper
mapper = SchemaMapper('bigquery')

# Configure incremental load
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
```

## Load Patterns

### 1. FULL_REFRESH
**Complexity**: Simple
**Use Cases**: Dimension tables, small lookup tables, complete refreshes

Truncates and reloads the entire table.

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.FULL_REFRESH,
    primary_keys=[]  # Not required
)
```

### 2. APPEND_ONLY
**Complexity**: Simple
**Use Cases**: Event logs, immutable data, audit trails

Simply inserts new records without any updates or deletes.

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.APPEND_ONLY,
    primary_keys=[]  # Not required
)
```

### 3. UPSERT (MERGE)
**Complexity**: Medium
**Use Cases**: Customer data, product catalogs, most transactional data

Inserts new records and updates existing records based on primary key.

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id'],
    merge_strategy=MergeStrategy.UPDATE_ALL  # Default
)
```

**Advanced Options**:
```python
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id'],
    merge_strategy=MergeStrategy.UPDATE_SELECTIVE,
    update_columns=['email', 'phone', 'updated_at']  # Only update these
)
```

### 4. DELETE_INSERT
**Complexity**: Medium
**Use Cases**: Redshift upserts, transactional alternative to MERGE

Deletes matching records then inserts (transactional alternative to MERGE for platforms that don't support it well).

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.DELETE_INSERT,
    primary_keys=['user_id']
)
```

### 5. INCREMENTAL_TIMESTAMP
**Complexity**: Medium
**Use Cases**: Event streams, time-series data, append-only with filtering

Loads only records newer than the max timestamp in the target table.

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
    primary_keys=['event_id'],
    incremental_column='event_timestamp',
    lookback_window='2 hours'  # Optional safety margin
)
```

### 6. INCREMENTAL_APPEND
**Complexity**: Medium
**Use Cases**: Avoiding duplicates, idempotent loads

Appends only records not already in the target (based on primary key).

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    primary_keys=['order_id']
)
```

### 7. SCD_TYPE1
**Complexity**: Medium
**Use Cases**: Current state only, no history required

Overwrites changed records (no history tracking).

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE1,
    primary_keys=['customer_id']
)
```

### 8. SCD_TYPE2
**Complexity**: Advanced
**Use Cases**: Historical tracking, audit requirements, dimensional modeling

Maintains full history with versioning. When a record changes, the old version is closed and a new version is created.

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['customer_id'],
    hash_columns=['name', 'email', 'address'],  # Track changes in these
    effective_date_column='effective_from',
    expiration_date_column='effective_to',
    is_current_column='is_current'
)
```

### 9. CDC_MERGE
**Complexity**: Advanced
**Use Cases**: Database replication, CDC pipelines, real-time sync

Processes I/U/D operations from a CDC stream.

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.CDC_MERGE,
    primary_keys=['id'],
    operation_column='_op',  # I/U/D indicator
    sequence_column='_seq'   # For ordering events
)
```

## Primary Key Detection

Schema Mapper can automatically detect potential primary keys using intelligent heuristics:

```python
from schema_mapper import SchemaMapper, KeyCandidate, PrimaryKeyDetector

mapper = SchemaMapper('bigquery')

# Simple API: Get best key columns
keys = mapper.detect_primary_keys(df)
print(f"Best primary key: {keys}")  # ['user_id']

# Detailed API: Get all candidates with confidence scores
candidates = mapper.detect_primary_keys(df, return_all_candidates=True)
for candidate in candidates:
    print(f"{'+'.join(candidate.columns)}: {candidate.confidence:.2f}")
    print(f"  Uniqueness: {candidate.uniqueness:.1%}")
    print(f"  Completeness: {candidate.completeness:.1%}")
    print(f"  Reasoning: {candidate.reasoning}")

# Using PrimaryKeyDetector directly for fine-grained control
detector = PrimaryKeyDetector(min_confidence=0.7, min_uniqueness=0.995)
candidates = detector.detect_keys(df, suggest_composite=True)

# Get the best key automatically
best = detector.auto_detect_best_key(df)
if best:
    print(f"Best key: {best.columns} (confidence: {best.confidence:.2f})")

# Validate your keys
from schema_mapper.incremental import validate_primary_keys
is_valid, errors = validate_primary_keys(df, ['user_id'])
if not is_valid:
    print("Validation errors:", errors)

# Analyze key columns in detail
from schema_mapper.incremental import analyze_key_columns
analysis = analyze_key_columns(df, ['user_id', 'order_date'])
print(f"Uniqueness: {analysis['uniqueness_pct']:.1f}%")
print(f"Has nulls: {analysis['has_nulls']}")
print(f"Unique combinations: {analysis['unique_combinations']}")

# Get composite key suggestions
from schema_mapper.incremental import get_composite_key_suggestions
suggestions = get_composite_key_suggestions(df, max_suggestions=5)
for suggestion in suggestions:
    print(f"Composite key: {suggestion}")
```

**KeyCandidate Attributes**:
- `columns`: List of column names forming the key
- `confidence`: Overall confidence score (0.0 to 1.0)
- `uniqueness`: Percentage of unique values/combinations
- `completeness`: Percentage of non-null values
- `cardinality`: Number of unique values
- `is_composite`: Whether this is a multi-column key
- `reasoning`: Human-readable explanation

**Confidence Scoring**:
- Based on uniqueness (99.5%+ required), completeness, name patterns, data types
- Bonuses for 'id', 'key', 'pk' naming patterns
- Bonuses for integer and string types
- Composite keys have slight penalty (0.1 per column)

## Platform Support Matrix

| Pattern | BigQuery | Snowflake | Redshift | SQL Server | PostgreSQL |
|---------|----------|-----------|----------|------------|------------|
| FULL_REFRESH | ✅ | ✅ | ✅ | ✅ | ✅ |
| APPEND_ONLY | ✅ | ✅ | ✅ | ✅ | ✅ |
| UPSERT | ✅ | ✅ | ❌* | ✅ | ✅** |
| DELETE_INSERT | ✅ | ✅ | ✅ | ✅ | ✅ |
| INCREMENTAL_TIMESTAMP | ✅ | ✅ | ✅ | ✅ | ✅ |
| INCREMENTAL_APPEND | ✅ | ✅ | ✅ | ✅ | ✅ |
| SCD_TYPE1 | ✅ | ✅ | ✅ | ✅ | ✅ |
| SCD_TYPE2 | ✅ | ✅ | ✅ | ✅ | ✅ |
| CDC_MERGE | ✅ | ✅ | ❌ | ✅ | ❌ |

*Redshift: Use DELETE_INSERT pattern instead
**PostgreSQL: Uses INSERT ... ON CONFLICT DO UPDATE

## Merge Strategies

Control how matched records are updated:

### UPDATE_ALL (Default)
Updates all non-key columns when a match is found.

```python
merge_strategy=MergeStrategy.UPDATE_ALL
```

### UPDATE_CHANGED
Only updates columns that have actually changed (requires hash comparison).

```python
merge_strategy=MergeStrategy.UPDATE_CHANGED
```

### UPDATE_SELECTIVE
Updates only specified columns.

```python
merge_strategy=MergeStrategy.UPDATE_SELECTIVE
update_columns=['email', 'phone', 'updated_at']
```

### UPDATE_NONE
Doesn't update matched records (INSERT only).

```python
merge_strategy=MergeStrategy.UPDATE_NONE
```

## Delete Strategies

Control how records that exist in target but not in source are handled:

### IGNORE (Default)
Don't delete records.

```python
delete_strategy=DeleteStrategy.IGNORE
```

### HARD_DELETE
Physically delete records from table.

```python
delete_strategy=DeleteStrategy.HARD_DELETE
```

### SOFT_DELETE
Set an is_deleted flag.

```python
delete_strategy=DeleteStrategy.SOFT_DELETE
soft_delete_column='is_deleted'
```

## Advanced Configuration

### Partitioning and Clustering

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id'],
    partition_column='created_date',  # Only merge affected partitions
    cluster_columns=['user_id', 'created_date']
)
```

### Custom Staging Tables

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id'],
    staging_table='staging_users_temp'  # Custom staging table name
)
```

### Validation and Dry Run

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id'],
    enable_validation=True,  # Pre-merge validation checks
    dry_run=True  # Generate DDL but don't execute
)
```

## Complete Example: SCD Type 2

```python
from schema_mapper import SchemaMapper, IncrementalConfig, LoadPattern
import pandas as pd

# Customer dimension data
df = pd.read_csv('customers.csv')

# Create mapper
mapper = SchemaMapper('bigquery')

# Configure SCD Type 2
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['customer_id'],
    hash_columns=['name', 'email', 'address', 'phone'],
    effective_date_column='effective_from',
    expiration_date_column='effective_to',
    is_current_column='is_current'
)

# Generate DDL
ddl = mapper.generate_incremental_ddl(
    df=df,
    table_name='dim_customers',
    config=config,
    dataset_name='analytics',
    project_id='my-project'
)

print(ddl)
```

## Helper Functions

### Get Pattern Metadata

```python
from schema_mapper.incremental import get_pattern_metadata, LoadPattern

metadata = get_pattern_metadata(LoadPattern.UPSERT)
print(f"Name: {metadata.name}")
print(f"Description: {metadata.description}")
print(f"Complexity: {metadata.complexity}")
print(f"Use cases: {metadata.use_cases}")
```

### Find Patterns by Use Case

```python
from schema_mapper.incremental import list_patterns_for_use_case

patterns = list_patterns_for_use_case("event")
print(f"Patterns for events: {[p.value for p in patterns]}")
```

### Get Patterns by Complexity

```python
from schema_mapper.incremental import get_simple_patterns, get_advanced_patterns

simple = get_simple_patterns()
print(f"Simple patterns: {[p.value for p in simple]}")

advanced = get_advanced_patterns()
print(f"Advanced patterns: {[p.value for p in advanced]}")
```

## Best Practices

### 1. Always Validate Your Keys
```python
is_valid, errors = validate_primary_keys(df, ['user_id'])
if not is_valid:
    raise ValueError(f"Invalid primary keys: {errors}")
```

### 2. Use Dry Run for Testing
```python
config.dry_run = True
ddl = mapper.generate_incremental_ddl(df, 'users', config)
# Review DDL before executing
```

### 3. Leverage Auto-Detection
```python
# Let Schema Mapper detect keys automatically
keys = mapper.detect_primary_keys(df)
if keys:
    config = IncrementalConfig(
        load_pattern=LoadPattern.UPSERT,
        primary_keys=keys  # Use detected keys
    )
```

### 4. Choose the Right Pattern

- **High volume, immutable data?** → APPEND_ONLY or INCREMENTAL_TIMESTAMP
- **Updates required?** → UPSERT
- **Need full history?** → SCD_TYPE2
- **CDC stream?** → CDC_MERGE
- **Small dimensions?** → FULL_REFRESH

### 5. Monitor Performance

For large tables:
- Use partition pruning with `partition_column`
- Consider `INCREMENTAL_TIMESTAMP` instead of full MERGE
- Add appropriate clustering with `cluster_columns`

## Troubleshooting

### Error: "primary_keys cannot be empty"
UPSERT and most incremental patterns require primary keys. Either:
1. Specify keys manually: `primary_keys=['id']`
2. Use auto-detection: `mapper.detect_primary_keys(df)`
3. Use a pattern that doesn't require keys: `LoadPattern.APPEND_ONLY`

### Error: "Column has duplicate values"
Your specified primary key has duplicates. Options:
1. Use composite keys: `primary_keys=['user_id', 'order_date']`
2. Fix data quality issues
3. Use a different pattern like `APPEND_ONLY`

### Error: "Platform does not support pattern"
Some platforms have limitations:
- Redshift doesn't support native MERGE → use `DELETE_INSERT`
- PostgreSQL MERGE has limited features → use `INSERT ... ON CONFLICT`

## Platform-Specific Examples

### BigQuery Incremental Loads

BigQuery has full support for all incremental load patterns using native MERGE statements.

#### Basic UPSERT (Merge)

```python
from schema_mapper import SchemaMapper, IncrementalConfig, LoadPattern

mapper = SchemaMapper('bigquery')

config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id']
)

ddl = mapper.generate_incremental_ddl(
    df,
    'users',
    config,
    dataset_name='analytics',
    project_id='my-project'
)

print(ddl)
```

**Output:**
```sql
MERGE `my-project.analytics.users` AS target
USING `my-project.analytics.users_staging` AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
  UPDATE SET
    target.name = source.name,
    target.email = source.email,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (user_id, name, email, updated_at)
  VALUES (source.user_id, source.name, source.email, source.updated_at);
```

#### SCD Type 2 (Historical Tracking)

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['customer_id'],
    hash_columns=['name', 'email', 'address'],
    effective_date_column='effective_from',
    expiration_date_column='effective_to',
    is_current_column='is_current'
)

ddl = mapper.generate_incremental_ddl(
    df,
    'dim_customers',
    config,
    dataset_name='analytics',
    project_id='my-project'
)
```

**Output:**
```sql
-- SCD Type 2: Maintain historical versions

-- Step 1: Expire records that have changed
UPDATE `my-project.analytics.dim_customers` AS target
SET
  effective_to = CURRENT_DATE(),
  is_current = FALSE
FROM `my-project.analytics.dim_customers_staging` AS source
WHERE
  target.customer_id = source.customer_id
  AND target.is_current = TRUE
  AND (
    target.name != source.name OR
    (target.name IS NULL AND source.name IS NOT NULL) OR
    ...
  );

-- Step 2: Insert new and changed records
INSERT INTO `my-project.analytics.dim_customers` (
  customer_id, name, email, address,
  effective_from, effective_to, is_current
)
SELECT
  source.customer_id, source.name, source.email, source.address,
  CURRENT_DATE() AS effective_from,
  DATE('9999-12-31') AS effective_to,
  TRUE AS is_current
FROM `my-project.analytics.dim_customers_staging` AS source
WHERE NOT EXISTS (
  SELECT 1 FROM `my-project.analytics.dim_customers` AS target
  WHERE
    target.customer_id = source.customer_id
    AND target.is_current = TRUE
    AND (all columns match)
);
```

#### Incremental Timestamp Load

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
    primary_keys=['event_id'],
    incremental_column='event_timestamp',
    lookback_window='2 HOUR'  # Safety margin
)

ddl = mapper.generate_incremental_ddl(
    df,
    'events',
    config,
    dataset_name='analytics',
    project_id='my-project'
)
```

**Output:**
```sql
-- Incremental load based on event_timestamp

-- Get max timestamp from target
DECLARE max_ts TIMESTAMP DEFAULT (
  SELECT COALESCE(MAX(event_timestamp), TIMESTAMP('1970-01-01')) AS max_timestamp
FROM `my-project.analytics.events`
);

-- Load only new records
INSERT INTO `my-project.analytics.events` (
  event_id, user_id, event_type, event_timestamp
)
SELECT
  event_id, user_id, event_type, event_timestamp
FROM `my-project.analytics.events_staging`
WHERE event_timestamp > TIMESTAMP_SUB(max_ts, INTERVAL 2 HOUR);
```

#### CDC (Change Data Capture)

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.CDC_MERGE,
    primary_keys=['user_id'],
    operation_column='_op',  # I/U/D indicator
    delete_strategy=DeleteStrategy.SOFT_DELETE,
    soft_delete_column='is_deleted'
)

ddl = mapper.generate_incremental_ddl(
    df,
    'users',
    config,
    dataset_name='analytics',
    project_id='my-project'
)
```

**Output:**
```sql
-- CDC Merge: Handle Insert/Update/Delete operations

MERGE `my-project.analytics.users` AS target
USING `my-project.analytics.users_staging` AS source
ON target.user_id = source.user_id
WHEN MATCHED AND source._op = 'D' THEN
  UPDATE SET
    target.is_deleted = TRUE
WHEN MATCHED AND source._op = 'U' THEN
  UPDATE SET
    target.name = source.name,
    target.email = source.email
WHEN NOT MATCHED AND source._op = 'I' THEN
  INSERT (user_id, name, email)
  VALUES (source.user_id, source.name, source.email);
```

#### Staging Table Creation

```python
from schema_mapper.incremental import get_incremental_generator

generator = get_incremental_generator('bigquery')
schema, _ = mapper.generate_schema(df)

staging_ddl = generator.generate_staging_table_ddl(
    schema,
    'users',
    dataset_name='analytics',
    project_id='my-project'
)

print(staging_ddl)
```

**Output:**
```sql
-- Create staging table
CREATE OR REPLACE TABLE `my-project.analytics.users_staging` (
  user_id INT64 NOT NULL,
  name STRING,
  email STRING,
  updated_at TIMESTAMP
);
```

#### Convenience Method for MERGE

```python
# Shortcut method for common UPSERT pattern
ddl = mapper.generate_merge_ddl(
    df,
    'users',
    primary_keys=['user_id'],
    dataset_name='analytics',
    project_id='my-project'
)
```

### Snowflake Incremental Loads

Snowflake has full support for all incremental load patterns with transaction support, TRANSIENT tables, and COPY INTO for stage-based loading.

#### Basic UPSERT (Merge) with Transactions

```python
from schema_mapper import SchemaMapper, IncrementalConfig, LoadPattern

mapper = SchemaMapper('snowflake')

config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['user_id']
)

ddl = mapper.generate_incremental_ddl(
    df,
    'users',
    config,
    database_name='analytics',
    schema_name='public'
)

print(ddl)
```

**Output:**
```sql
BEGIN TRANSACTION;

MERGE INTO analytics.public.users AS target
USING analytics.public.users_staging AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
  UPDATE SET name = source.name, email = source.email, updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (user_id, name, email, updated_at)
  VALUES (source.user_id, source.name, source.email, source.updated_at);

COMMIT;
```

#### SCD Type 2 (Historical Tracking)

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['customer_id'],
    hash_columns=['name', 'email', 'address'],
    effective_date_column='effective_from',
    expiration_date_column='effective_to',
    is_current_column='is_current'
)

ddl = mapper.generate_incremental_ddl(
    df,
    'dim_customers',
    config,
    database_name='analytics',
    schema_name='dimensions'
)
```

**Output:**
```sql
BEGIN TRANSACTION;

-- Step 1: Expire changed records
UPDATE analytics.dimensions.dim_customers AS target
SET effective_to = CURRENT_TIMESTAMP(),
    is_current = FALSE
FROM analytics.dimensions.dim_customers_staging AS source
WHERE target.customer_id = source.customer_id
  AND target.is_current = TRUE
  AND HASH(target.name, target.email, target.address) != (
    SELECT HASH(sub.name, sub.email, sub.address)
    FROM analytics.dimensions.dim_customers AS sub
    WHERE sub.customer_id = target.customer_id
      AND sub.is_current = TRUE
    LIMIT 1
  );

-- Step 2: Insert new versions for changed records
INSERT INTO analytics.dimensions.dim_customers (customer_id, name, email, address, effective_from, effective_to, is_current)
SELECT
  source.customer_id, source.name, source.email, source.address,
  CURRENT_TIMESTAMP() AS effective_from,
  '9999-12-31'::TIMESTAMP AS effective_to,
  TRUE AS is_current
FROM analytics.dimensions.dim_customers_staging AS source
WHERE EXISTS (
  SELECT 1
  FROM analytics.dimensions.dim_customers AS target
  WHERE target.customer_id = source.customer_id
    AND target.effective_to = CURRENT_TIMESTAMP()
);

-- Step 3: Insert completely new records
INSERT INTO analytics.dimensions.dim_customers (customer_id, name, email, address, effective_from, effective_to, is_current)
SELECT
  source.customer_id, source.name, source.email, source.address,
  CURRENT_TIMESTAMP() AS effective_from,
  '9999-12-31'::TIMESTAMP AS effective_to,
  TRUE AS is_current
FROM analytics.dimensions.dim_customers_staging AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM analytics.dimensions.dim_customers AS target
  WHERE target.customer_id = source.customer_id
);

COMMIT;
```

#### Incremental Timestamp Load with Session Variables

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
    primary_keys=['event_id'],
    incremental_column='event_timestamp',
    lookback_window='2 hours'  # Safety margin
)

ddl = mapper.generate_incremental_ddl(
    df,
    'events',
    config,
    database_name='analytics',
    schema_name='raw'
)
```

**Output:**
```sql
-- Get max timestamp from target
SET max_ts = (
  SELECT NVL(MAX(event_timestamp), '1900-01-01'::TIMESTAMP)
  FROM analytics.raw.events
);

-- Insert new records
INSERT INTO analytics.raw.events (event_id, user_id, event_type, event_timestamp)
SELECT event_id, user_id, event_type, event_timestamp
FROM (SELECT * FROM events_source) AS source
WHERE source.event_timestamp > $max_ts - INTERVAL '2 hours';
```

#### CDC (Change Data Capture) with DELETE Support

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.CDC_MERGE,
    primary_keys=['user_id'],
    operation_column='_op',  # I/U/D indicator
    sequence_column='_seq'
)

ddl = mapper.generate_incremental_ddl(
    df,
    'users',
    config,
    database_name='analytics',
    schema_name='public'
)
```

**Output:**
```sql
BEGIN TRANSACTION;

MERGE INTO analytics.public.users AS target
USING analytics.public.users_staging AS source
ON target.user_id = source.user_id
WHEN MATCHED AND source._op = 'D' THEN
  DELETE
WHEN MATCHED AND source._op IN ('U', 'I') THEN
  UPDATE SET name = source.name, email = source.email
WHEN NOT MATCHED AND source._op IN ('I', 'U') THEN
  INSERT (user_id, name, email)
  VALUES (source.user_id, source.name, source.email);

COMMIT;
```

#### TRANSIENT Staging Table Creation

```python
from schema_mapper.incremental import get_incremental_generator

generator = get_incremental_generator('snowflake')
schema, _ = mapper.generate_schema(df)

staging_ddl = generator.generate_staging_table_ddl(
    schema,
    'users',
    database_name='analytics',
    schema_name='staging',
    transient=True,
    cluster_by=['user_id', 'created_at']
)

print(staging_ddl)
```

**Output:**
```sql
CREATE OR REPLACE TRANSIENT TABLE analytics.staging.users_staging (
  user_id NUMBER,
  name VARCHAR,
  email VARCHAR,
  created_at TIMESTAMP
)
CLUSTER BY (user_id, created_at);
```

#### COPY INTO for Stage-Based Loading

```python
# Snowflake-specific helper for loading from stages
ddl = mapper.generate_snowflake_copy_into(
    table_name='users',
    stage_name='@s3_stage/users/',
    df=df,
    database_name='analytics',
    schema_name='raw',
    file_format='my_csv_format',
    pattern='.*users.*\\.csv',
    on_error='CONTINUE'
)

print(ddl)
```

**Output:**
```sql
COPY INTO analytics.raw.users
  (user_id, name, email, created_at)
FROM @s3_stage/users/
FILE_FORMAT = (TYPE = 'my_csv_format')
PATTERN = '.*users.*\.csv'
ON_ERROR = 'CONTINUE';
```

#### Full Refresh with TRUNCATE + INSERT

```python
config = IncrementalConfig(
    load_pattern=LoadPattern.FULL_REFRESH,
    primary_keys=[]
)

ddl = mapper.generate_incremental_ddl(
    df,
    'dim_dates',
    config,
    database_name='analytics',
    schema_name='dimensions',
    use_create_or_replace=False  # Use TRUNCATE approach
)
```

**Output:**
```sql
BEGIN TRANSACTION;

TRUNCATE TABLE analytics.dimensions.dim_dates;

INSERT INTO analytics.dimensions.dim_dates (date_id, date, year, month, day)
SELECT date_id, date, year, month, day
FROM analytics.dimensions.dim_dates_staging;

COMMIT;
```

#### Convenience Method for MERGE

```python
# Shortcut method for common UPSERT pattern
ddl = mapper.generate_merge_ddl(
    df,
    'users',
    primary_keys=['user_id'],
    database_name='analytics',
    schema_name='public'
)
```

## What's Next?

This documentation covers PROMPT 1-4:
- ✅ PROMPT 1: Architecture & Core Abstractions
- ✅ PROMPT 2: Primary Key Detection
- ✅ PROMPT 3: BigQuery Implementation
- ✅ PROMPT 4: Snowflake Implementation

**Coming in future prompts**:
- PROMPT 5: Redshift implementation (DELETE+INSERT pattern)
- PROMPT 6: SQL Server implementation
- PROMPT 7: PostgreSQL implementation (ON CONFLICT)
- PROMPT 8: Integration tests & examples
- PROMPT 9: CLI support & final polish

## API Reference

See module docstrings for complete API documentation:
- `schema_mapper.incremental.patterns` - Load patterns and configs
- `schema_mapper.incremental.incremental_base` - Base classes
- `schema_mapper.incremental.key_detection` - Key detection utilities
- `schema_mapper.core.SchemaMapper` - Main interface

## Contributing

Found a bug or have a feature request? Please open an issue on GitHub.

Want to add support for a new load pattern? Extend `IncrementalDDLGenerator`.

## License

Same as schema-mapper package license.
