# Schema Mapper Architecture - Canonical Schema to Execution

## Core Design Principle

**One Canonical Schema → Multiple Renderers → Platform-Specific Execution**

```
┌─────────────────────────────────┐
│   Canonical Schema (Python)     │  ← Single source of truth
│   - Table metadata              │
│   - Column definitions          │
│   - Optimization hints          │
│   - Logical types only          │
└─────────────────────────────────┘
                │
                ├─────────────┬─────────────┬─────────────┬─────────────┐
                ▼             ▼             ▼             ▼             ▼
         ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
         │ BigQuery │  │Snowflake │  │ Redshift │  │ Postgres │  │   MSSQL  │
         │ Renderer │  │ Renderer │  │ Renderer │  │ Renderer │  │ Renderer │
         └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘
              │             │             │             │             │
         ┌────┴────┐   ┌────┴────┐   ┌────┴────┐   ┌────┴────┐   ┌────┴────┐
         │ JSON    │   │ DDL     │   │ DDL     │   │ DDL     │   │ DDL     │
         │ DDL     │   │ CLI Cmd │   │ CLI Cmd │   │ CLI Cmd │   │ CLI Cmd │
         │ CLI Cmd │   └─────────┘   └─────────┘   └─────────┘   └─────────┘
         └─────────┘
```

## The Critical Distinction

### Load-Time Schema (BigQuery Only)
- Schema defined at **load time** via JSON
- DDL is optional (can infer from data)
- Native JSON schema support in CLI
- `bq load --schema schema.json`

### DDL-Time Schema (Everyone Else)
- Schema defined at **CREATE TABLE time** via SQL
- JSON is internal metadata only
- CLI executes SQL, not JSON
- `psql -f create_table.sql` or programmatic execution

**❌ DON'T:** Try to make JSON schemas universal
**✅ DO:** Emit JSON only where natively consumed (BigQuery)

## Canonical Schema Definition

```python
from dataclasses import dataclass
from typing import List, Optional
from enum import Enum

class LogicalType(Enum):
    """Platform-agnostic logical types."""
    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DECIMAL = "decimal"
    STRING = "string"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    JSON = "json"
    BINARY = "binary"

@dataclass
class ColumnDefinition:
    """Canonical column definition - engine agnostic."""
    name: str
    logical_type: LogicalType
    nullable: bool = True
    description: Optional[str] = None

    # Type parameters (platform-agnostic)
    max_length: Optional[int] = None  # For STRING/VARCHAR
    precision: Optional[int] = None   # For DECIMAL
    scale: Optional[int] = None       # For DECIMAL

    # Metadata
    original_name: Optional[str] = None  # Before standardization

@dataclass
class OptimizationHints:
    """Platform-agnostic optimization hints."""
    # Will be rendered differently per platform
    partition_columns: List[str] = None
    cluster_columns: List[str] = None
    sort_columns: List[str] = None
    distribution_column: Optional[str] = None

    # Time-based hints
    partition_expiration_days: Optional[int] = None
    require_partition_filter: bool = False

@dataclass
class CanonicalSchema:
    """
    The single source of truth for a table schema.

    This is platform-agnostic and contains only logical information.
    Physical rendering is delegated to platform-specific renderers.
    """
    # Identity
    table_name: str
    dataset_name: Optional[str] = None
    project_id: Optional[str] = None  # BigQuery-specific, but harmless

    # Schema
    columns: List[ColumnDefinition] = None

    # Optimization (logical hints, not physical implementation)
    optimization: Optional[OptimizationHints] = None

    # Metadata
    description: Optional[str] = None
    created_from: Optional[str] = None  # e.g., "CSV", "DataFrame", "Manual"

    def __post_init__(self):
        if self.columns is None:
            self.columns = []
        if self.optimization is None:
            self.optimization = OptimizationHints()
```

## Renderer Pattern

### Base Renderer Interface

```python
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

class SchemaRenderer(ABC):
    """Base class for platform-specific schema renderers."""

    def __init__(self, canonical_schema: CanonicalSchema):
        self.schema = canonical_schema
        self.validate()

    @abstractmethod
    def validate(self) -> List[str]:
        """
        Validate schema against platform capabilities.
        Returns list of error messages.
        """
        pass

    @abstractmethod
    def to_ddl(self) -> str:
        """Render CREATE TABLE DDL."""
        pass

    @abstractmethod
    def to_cli_create(self) -> str:
        """Render CLI command to create table."""
        pass

    @abstractmethod
    def to_cli_load(self, data_path: str) -> str:
        """Render CLI command to load data."""
        pass

    @abstractmethod
    def to_physical_types(self) -> Dict[str, str]:
        """
        Convert logical types to platform-specific types.
        Returns: {column_name: physical_type}
        """
        pass

    # Optional methods (override if platform supports)
    def to_schema_json(self) -> Optional[str]:
        """Render JSON schema (BigQuery only)."""
        return None

    def supports_json_schema(self) -> bool:
        """Does this platform natively support JSON schemas?"""
        return False
```

### Platform-Specific Renderers

#### BigQuery Renderer (Special Case)

```python
class BigQueryRenderer(SchemaRenderer):
    """
    BigQuery is unique: it supports both JSON schemas and DDL.

    Emit both because:
    - JSON schema: Used by bq load command
    - DDL: Used for CREATE TABLE with partitioning/clustering
    """

    def supports_json_schema(self) -> bool:
        return True

    def to_schema_json(self) -> str:
        """Native BigQuery JSON schema for bq load."""
        fields = []
        for col in self.schema.columns:
            field = {
                "name": col.name,
                "type": self._to_bigquery_type(col.logical_type),
                "mode": "NULLABLE" if col.nullable else "REQUIRED"
            }
            if col.description:
                field["description"] = col.description
            fields.append(field)
        return json.dumps(fields, indent=2)

    def to_ddl(self) -> str:
        """Full CREATE TABLE with partitioning/clustering."""
        # Use generators_enhanced.py
        from schema_mapper.generators_enhanced import get_enhanced_ddl_generator
        from schema_mapper.ddl_mappings import DDLOptions, ClusteringConfig, PartitionConfig

        generator = get_enhanced_ddl_generator('bigquery')

        # Convert canonical schema to BigQuery schema format
        bq_schema = self._to_bigquery_schema_format()

        # Build DDL options from optimization hints
        ddl_options = self._build_ddl_options()

        return generator.generate(
            schema=bq_schema,
            table_name=self.schema.table_name,
            dataset_name=self.schema.dataset_name,
            project_id=self.schema.project_id,
            ddl_options=ddl_options
        )

    def to_cli_create(self) -> str:
        """
        BigQuery has two options:
        1. Execute DDL via bq query
        2. Create empty table with schema
        """
        if self.schema.optimization.partition_columns or self.schema.optimization.cluster_columns:
            # Must use DDL for partitioning/clustering
            ddl = self.to_ddl()
            return f'bq query --use_legacy_sql=false "{ddl}"'
        else:
            # Can use simpler schema-based creation
            full_table = f"{self.schema.project_id}:{self.schema.dataset_name}.{self.schema.table_name}"
            return f"bq mk --table --schema <(echo '{self.to_schema_json()}') {full_table}"

    def to_cli_load(self, data_path: str) -> str:
        """Load CSV into BigQuery using JSON schema."""
        full_table = f"{self.schema.project_id}:{self.schema.dataset_name}.{self.schema.table_name}"
        schema_json = self.to_schema_json()

        return f"""# Save schema to file
echo '{schema_json}' > /tmp/schema.json

# Load data
bq load --source_format=CSV --skip_leading_rows=1 \\
  --schema=/tmp/schema.json \\
  {full_table} \\
  {data_path}"""
```

#### Snowflake Renderer (DDL-Only)

```python
class SnowflakeRenderer(SchemaRenderer):
    """
    Snowflake: DDL-driven, no native JSON schema support.

    Emit:
    - DDL with clustering
    - CLI commands execute SQL
    """

    def supports_json_schema(self) -> bool:
        return False

    def to_ddl(self) -> str:
        """Generate CREATE TABLE with clustering."""
        from schema_mapper.generators_enhanced import get_enhanced_ddl_generator
        from schema_mapper.ddl_mappings import DDLOptions, ClusteringConfig

        generator = get_enhanced_ddl_generator('snowflake')

        sf_schema = self._to_snowflake_schema_format()
        ddl_options = self._build_ddl_options()

        return generator.generate(
            schema=sf_schema,
            table_name=self.schema.table_name,
            dataset_name=self.schema.dataset_name,
            ddl_options=ddl_options
        )

    def to_cli_create(self) -> str:
        """Execute DDL via snowsql."""
        ddl = self.to_ddl()
        return f'''snowsql -q "{ddl}"'''

    def to_cli_load(self, data_path: str) -> str:
        """Load CSV via COPY INTO."""
        full_table = f"{self.schema.dataset_name}.{self.schema.table_name}"

        return f"""# Put file to stage
snowsql -q "PUT file://{data_path} @%{full_table};"

# Copy into table
snowsql -q "COPY INTO {full_table}
  FROM @%{full_table}
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
  ON_ERROR = ABORT_STATEMENT;"
```

#### Redshift Renderer (DDL-Only)

```python
class RedshiftRenderer(SchemaRenderer):
    """
    Redshift: DDL-driven with COPY command.

    Emit:
    - DDL with distribution/sort keys
    - COPY command for bulk load
    """

    def to_cli_load(self, data_path: str, s3_path: Optional[str] = None) -> str:
        """
        Redshift loads from S3, not local files.
        Either upload to S3 first or assume it's already there.
        """
        if not s3_path:
            s3_path = f"s3://your-bucket/data/{self.schema.table_name}/"

        full_table = f"{self.schema.dataset_name}.{self.schema.table_name}"

        return f"""# Upload to S3 (if needed)
aws s3 cp {data_path} {s3_path}

# COPY into Redshift
psql -h your-cluster.redshift.amazonaws.com -c "
COPY {full_table}
FROM '{s3_path}'
IAM_ROLE 'arn:aws:iam::account:role/RedshiftCopyRole'
CSV
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto';"
```

## Factory Pattern

```python
class RendererFactory:
    """Factory to get the right renderer for each platform."""

    _renderers = {
        'bigquery': BigQueryRenderer,
        'snowflake': SnowflakeRenderer,
        'redshift': RedshiftRenderer,
        'postgresql': PostgreSQLRenderer,
        'sqlserver': SQLServerRenderer,
    }

    @classmethod
    def get_renderer(cls, platform: str, schema: CanonicalSchema) -> SchemaRenderer:
        """Get platform-specific renderer."""
        if platform not in cls._renderers:
            raise ValueError(f"Unsupported platform: {platform}")

        return cls._renderers[platform](schema)

    @classmethod
    def supports_json_schema(cls, platform: str) -> bool:
        """Check if platform supports JSON schemas."""
        renderer_class = cls._renderers.get(platform)
        if not renderer_class:
            return False
        # Check class method
        return getattr(renderer_class, 'supports_json_schema', lambda: False)()
```

## Usage Examples

### End-to-End Flow

```python
# 1. Infer canonical schema from DataFrame
from schema_mapper.canonical import infer_canonical_schema

df = pd.read_csv('events.csv')
canonical = infer_canonical_schema(
    df,
    table_name='events',
    dataset_name='analytics',
    partition_columns=['event_date'],
    cluster_columns=['user_id', 'event_type']
)

# 2. Get platform renderer
from schema_mapper.renderers import RendererFactory

renderer = RendererFactory.get_renderer('bigquery', canonical)

# 3. Generate all artifacts
ddl = renderer.to_ddl()
create_cmd = renderer.to_cli_create()
load_cmd = renderer.to_cli_load('events_clean.csv')

if renderer.supports_json_schema():
    schema_json = renderer.to_schema_json()
    print("JSON Schema:")
    print(schema_json)

print("\nDDL:")
print(ddl)

print("\nCreate Command:")
print(create_cmd)

print("\nLoad Command:")
print(load_cmd)
```

### Multi-Platform Generation

```python
platforms = ['bigquery', 'snowflake', 'redshift']

for platform in platforms:
    print(f"\n{'='*60}")
    print(f"{platform.upper()}")
    print('='*60)

    renderer = RendererFactory.get_renderer(platform, canonical)

    # Validate
    errors = renderer.validate()
    if errors:
        print(f"⚠️  Validation errors: {errors}")
        continue

    # Generate DDL
    print("\nDDL:")
    print(renderer.to_ddl())

    # Generate CLI commands
    print("\nCreate:")
    print(renderer.to_cli_create())

    print("\nLoad:")
    print(renderer.to_cli_load('data.csv'))

    # JSON schema (if supported)
    if renderer.supports_json_schema():
        print("\nJSON Schema:")
        print(renderer.to_schema_json())
```

## Benefits of This Architecture

### 1. Clean Separation of Concerns
- **Canonical Schema**: What the table *is*
- **Renderer**: How to *create* it on a platform
- **CLI Commands**: How to *use* it

### 2. Platform Reality Respected
- BigQuery gets JSON + DDL (both native)
- Others get DDL only (as designed)
- No forced abstractions

### 3. Easy to Extend
```python
# Add new platform
class DatabricksRenderer(SchemaRenderer):
    def to_ddl(self):
        return "CREATE TABLE ... USING DELTA"
    ...

# Register it
RendererFactory._renderers['databricks'] = DatabricksRenderer
```

### 4. Testable
```python
def test_bigquery_renderer():
    schema = CanonicalSchema(
        table_name='test',
        columns=[ColumnDefinition('id', LogicalType.INTEGER)]
    )
    renderer = BigQueryRenderer(schema)
    ddl = renderer.to_ddl()
    assert 'CREATE TABLE' in ddl
    assert 'id INT64' in ddl
```

### 5. JSON is Optional Metadata
- Emit JSON where consumed (BigQuery)
- Store JSON for lineage/versioning
- Never confuse JSON with DDL

## Migration Path

### Phase 1: Canonical Schema ✅
- [x] Define `CanonicalSchema` dataclass
- [ ] Implement `infer_canonical_schema()` from DataFrame
- [ ] Add optimization hints

### Phase 2: Base Renderer
- [ ] Create `SchemaRenderer` ABC
- [ ] Implement `RendererFactory`
- [ ] Add validation framework

### Phase 3: Platform Renderers
- [ ] `BigQueryRenderer` (JSON + DDL)
- [ ] `SnowflakeRenderer` (DDL only)
- [ ] `RedshiftRenderer` (DDL + COPY)
- [ ] `PostgreSQLRenderer` (DDL + COPY)
- [ ] `SQLServerRenderer` (DDL + BULK INSERT)

### Phase 4: CLI Integration
- [ ] Update CLI to use renderers
- [ ] Add `--output-format` flag (ddl, json, cli-create, cli-load)
- [ ] Generate multiple artifacts

### Phase 5: Integration
- [ ] Replace existing generators with renderers
- [ ] Deprecate old `generate_ddl()` methods
- [ ] Update documentation

## Design Rules (Golden Rules)

1. **Canonical Schema is Platform-Agnostic**
   - Use logical types only
   - No SQL syntax
   - Optimization hints, not implementations

2. **Renderers are Platform-Specific**
   - Know platform capabilities
   - Emit native formats
   - Validate early

3. **JSON ≠ DDL**
   - JSON is load-time schema (BigQuery)
   - DDL is control plane (everyone)
   - Don't conflate them

4. **CLI Commands are Scripts, Not APIs**
   - Emit ready-to-run commands
   - Include comments for context
   - Assume reasonable defaults

5. **Fail Fast, Fail Clear**
   - Validate at renderer creation
   - Clear error messages
   - Don't silently drop features

---

**Status**: Architecture Approved, Ready for Implementation
**Priority**: High - Core abstraction
**Effort**: 3-5 days for full implementation
