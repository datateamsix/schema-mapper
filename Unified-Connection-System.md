# Unified Connection & Schema Abstraction

This document provides context and design guidance for a **multi-warehouse connection layer** that prioritizes developer flow, schema consistency, and safe extensibility. The goal is to let users stay "in the zone" while working across BigQuery, Snowflake, Redshift, Microsoft SQL Server, and PostgreSQL.

---

## Design Goals

1. **Single Mental Model** – One connection interface regardless of backend
2. **Fast Feedback** – Test connectivity and table existence immediately
3. **Canonical Schema First** – Normalize all schemas into a platform-agnostic format
4. **Minimal SQL Exposure** – Abstract vendor-specific metadata queries
5. **Config-Driven** – Credentials and targets defined outside application code

---

## Recommended Credential Strategy

Yes — **credentials should live outside code**.

### Preferred Options

1. **`.env` file** (local development)
2. **YAML config** (checked into repo *without secrets*)
3. **Secret managers** (production)

The system should support **both `.env` and YAML**, with YAML referencing environment variables.

---

## Canonical Schema (Core Abstraction)

All platforms normalize into this structure:

```json
{
  "columns": [
    {
      "name": "id",
      "type": "INT64",
      "nullable": false
    },
    {
      "name": "created_at",
      "type": "TIMESTAMP",
      "nullable": true
    }
  ]
}
```

This schema becomes the source of truth for:

* DDL generation
* Schema diffs
* Table creation / evolution
* DataFrame validation

---

## YAML Configuration Specification

### High-Level Structure

```yaml
target: snowflake

connections:
  snowflake:
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    account: ${SNOWFLAKE_ACCOUNT}
    warehouse: ANALYTICS_WH
    role: TRANSFORMER
    database: ANALYTICS
    schema: PUBLIC

  bigquery:
    project: my-gcp-project
    credentials_path: ${BQ_CREDENTIALS_PATH}

  postgres:
    host: localhost
    port: 5432
    database: analytics
    user: ${PG_USER}
    password: ${PG_PASSWORD}

  redshift:
    host: redshift-cluster.amazonaws.com
    port: 5439
    database: analytics
    user: ${REDSHIFT_USER}
    password: ${REDSHIFT_PASSWORD}

  mssql:
    server: myserver.database.windows.net
    database: analytics
    user: ${MSSQL_USER}
    password: ${MSSQL_PASSWORD}
```

---

## `.env` Example

```env
SNOWFLAKE_USER=svc_transformer
SNOWFLAKE_PASSWORD=********
SNOWFLAKE_ACCOUNT=abc123

PG_USER=etl_user
PG_PASSWORD=********

REDSHIFT_USER=etl_user
REDSHIFT_PASSWORD=********

MSSQL_USER=etl_user
MSSQL_PASSWORD=********

BQ_CREDENTIALS_PATH=/secrets/bq_service_account.json
```

---

## Connection Lifecycle

### Factory Pattern

```python
conn = get_connection(target="snowflake", config=yaml_config)
conn.connect()
conn.test_connection()
```

### Table Introspection

```python
if conn.table_exists("ANALYTICS", "PUBLIC", "events"):
    schema = conn.get_target_schema("ANALYTICS", "PUBLIC", "events")
```

Returns a **CanonicalSchema** object regardless of backend.

---

## Base Connection Contract

All connectors implement the same interface:

```python
connect()
test_connection() -> bool
table_exists(database, schema, table) -> bool
get_target_schema(database, schema, table) -> CanonicalSchema
```

This ensures predictable behavior across platforms.

---

## Why This Helps Users

* No vendor-specific SQL
* No branching logic in pipelines
* One config file, many targets
* Safe introspection before writes
* Easy schema reuse and modification

---

## Future Extensions (Non-Breaking)

* Schema diff & migration planning
* Column lineage metadata
* Permission validation
* DataFrame-to-table auto-sync
* CLI: `schema inspect`, `schema diff`, `schema apply`

---

## Summary

This design turns database connections into a **schema-aware control plane** rather than a low-level plumbing concern. By anchoring everything to a canonical schema and config-driven connections, users remain focused on transformation logic — not warehouse quirks.


I’ve created a clean, print-ready markdown doc in canvas that:

Explains the design philosophy (flow, abstraction, canonical schema)

Confirms .env + YAML is the ideal approach

Defines a clear YAML spec with env var interpolation

Shows the connection lifecycle from a user’s POV

Anchors everything around the canonical schema control plane

These libraries are installed when schema-mapper is installed. 

| Platform   | Primary Library              | Notes                  |
| ---------- | ---------------------------- | ---------------------- |
| Snowflake  | `snowflake-connector-python` | Official               |
| BigQuery   | `google-cloud-bigquery`      | HTTP-based             |
| Redshift   | `psycopg2`                   | Postgres protocol      |
| SQL Server | `pyodbc`                     | Enterprise standard    |
| PostgreSQL | `psycopg2`                   | Most common            |
| Multi-DB   | `sqlalchemy`                 | Best abstraction layer |


  Executive Summary

     Implement a first-class, production-grade unified database connection system for schema-mapper that seamlessly integrates with existing generators, DDL,     
     and renderers. This system will enable schema-mapper to not only generate DDL but also execute it, introspect existing databases, and provide a single       
     mental model for working across all 5 supported platforms.

     Scope: All 5 platforms (BigQuery, Snowflake, Redshift, PostgreSQL, SQL Server)
     Features: Connection management, transaction support, retry logic, connection pooling
     Timeline: 10-12 days for complete implementation

     Core Design Principles

     1. Single Mental Model - One BaseConnection interface across all platforms
     2. Canonical Schema First - Bidirectional flow: read from DB → CanonicalSchema, CanonicalSchema → write to DB
     3. Config-Driven - YAML + .env support with environment variable interpolation
     4. Seamless Integration - Works with renderers, generators, and incremental loads
     5. Production-Grade - Transaction support, retry logic, connection pooling, rich error handling
     6. Non-Breaking - Purely additive, fully backward compatible
     7. Extensible - Easy to add new platforms via factory pattern

     Architecture Overview

     ┌─────────────────────────────────────────────────────────────┐
     │                 SCHEMA-MAPPER WITH CONNECTIONS               │
     ├─────────────────────────────────────────────────────────────┤
     │                                                             │
     │  INPUT SOURCES          CANONICAL SCHEMA                    │
     │  ══════════════         ═══════════════                     │
     │                                                             │
     │  DataFrame ────┐              │                             │
     │  CSV ──────────┼──→ infer ────┤                             │
     │  JSON ─────────┘              │                             │
     │                               │                             │
     │  Database ──→ introspect ─────┤                             │
     │  (via Connection)             │                             │
     │                               ▼                             │
     │                      CanonicalSchema                        │
     │                               │                             │
     │               ┌───────────────┼──────────────┐              │
     │               │               │              │              │
     │               ▼               ▼              ▼              │
     │          Renderer       Generator      Validator            │
     │               │               │              │              │
     │               ▼               ▼              ▼              │
     │              DDL      Incremental DDL   Validation          │
     │               │               │                             │
     │               └───────┬───────┘                             │
     │                       │                                     │
     │                       ▼                                     │
     │                  Connection                                 │
     │                (with pooling, retry, transactions)          │
     │                       │                                     │
     │                       ├─ execute_ddl()                      │
     │                       ├─ test_connection()                  │
     │                       ├─ table_exists()                     │
     │                       └─ get_target_schema()                │
     │                                                             │
     └─────────────────────────────────────────────────────────────┘

     File Structure

     schema_mapper/
     ├── connections/                          # NEW MODULE
     │   ├── __init__.py                       # Factory exports
     │   ├── base.py                           # BaseConnection abstract class
     │   ├── factory.py                        # ConnectionFactory
     │   ├── config.py                         # YAML + .env configuration
     │   ├── exceptions.py                     # Custom exceptions
     │   ├── introspection.py                  # Introspection utilities
     │   │
     │   ├── platform_connectors/              # Platform implementations
     │   │   ├── __init__.py
     │   │   ├── bigquery.py                   # BigQueryConnection
     │   │   ├── snowflake.py                  # SnowflakeConnection
     │   │   ├── redshift.py                   # RedshiftConnection
     │   │   ├── postgresql.py                 # PostgreSQLConnection
     │   │   └── sqlserver.py                  # SQLServerConnection
     │   │
     │   └── utils/                            # Connection utilities
     │       ├── __init__.py
     │       ├── retry.py                      # Retry logic with backoff
     │       ├── pooling.py                    # Connection pool manager
     │       ├── validation.py                 # Credential validation
     │       └── type_mapping.py               # DB types → LogicalType
     │
     ├── config/                               # NEW: Configuration files
     │   ├── connections.yaml.example          # Example connection config
     │   └── .env.example                      # Example environment file
     │
     ├── canonical.py                          # EXISTING (no changes)
     ├── renderers/                            # EXISTING (no changes)
     ├── generators.py                         # EXISTING (no changes)
     └── incremental/                          # EXISTING (no changes)

     Key Components

     1. BaseConnection Abstract Class

     File: schema_mapper/connections/base.py

     Core Methods:
     - connect() - Establish connection with retry logic
     - disconnect() - Close connection gracefully
     - test_connection() - Health check (SELECT 1 equivalent)
     - table_exists(table, schema, database) - Check table existence
     - get_target_schema(table, schema, database) - Introspect → CanonicalSchema
     - list_tables(schema, database) - List tables
     - execute_ddl(ddl) - Execute CREATE/ALTER/DROP statements
     - execute_query(query) - Execute SELECT queries
     - begin_transaction(), commit(), rollback() - Transaction support
     - create_table_from_schema(canonical_schema) - Integration with renderers

     Features:
     - Connection state tracking (DISCONNECTED, CONNECTING, CONNECTED, ERROR)
     - Context manager support (with statement)
     - Rich logging with structured messages
     - Automatic retry on transient failures

     2. Configuration System

     File: schema_mapper/connections/config.py

     Features:
     - Load YAML configuration files
     - Environment variable interpolation (${VAR_NAME})
     - .env file support via python-dotenv
     - Validation of required credentials
     - Multi-environment support (dev, staging, prod)

     Example YAML:
     target: snowflake

     connections:
       snowflake:
         user: ${SNOWFLAKE_USER}
         password: ${SNOWFLAKE_PASSWORD}
         account: ${SNOWFLAKE_ACCOUNT}
         warehouse: ANALYTICS_WH
         database: ANALYTICS
         schema: PUBLIC

       bigquery:
         project: ${GCP_PROJECT_ID}
         credentials_path: ${BQ_CREDENTIALS_PATH}
         location: US

     3. Platform Connectors

     Each platform implements the BaseConnection interface:

     BigQueryConnection:
     - Uses google-cloud-bigquery client
     - Introspects via BigQuery schema API
     - Maps INT64 → BIGINT, STRING → STRING, etc.
     - No traditional transactions (auto-commit)
     - Extracts clustering/partitioning metadata

     SnowflakeConnection:
     - Uses snowflake-connector-python
     - Introspects via INFORMATION_SCHEMA queries
     - Full transaction support
     - Maps NUMBER(38,0) → BIGINT, VARIANT → JSON, etc.

     PostgreSQLConnection:
     - Uses psycopg2-binary
     - Introspects via pg_catalog
     - Full transaction support with isolation levels
     - Maps BIGINT → BIGINT, JSONB → JSON, etc.

     RedshiftConnection:
     - Uses redshift-connector (or psycopg2 fallback)
     - Similar to PostgreSQL with Redshift-specific optimizations
     - COPY command support for bulk loading

     SQLServerConnection:
     - Uses pyodbc
     - Introspects via INFORMATION_SCHEMA
     - Transaction support with snapshot isolation
     - Maps BIGINT → BIGINT, NVARCHAR(MAX) → TEXT, etc.

     4. Retry Logic

     File: schema_mapper/connections/utils/retry.py

     Features:
     - Exponential backoff with jitter
     - Configurable max retries (default: 3)
     - Platform-specific transient error detection
     - Rich logging of retry attempts

     Example:
     @retry_on_transient_error(max_retries=3, backoff_factor=2)
     def connect(self):
         # Connection logic
         pass

     5. Connection Pooling

     File: schema_mapper/connections/utils/pooling.py

     Features:
     - Thread-safe connection pool
     - Configurable pool size (min/max connections)
     - Connection health checks before reuse
     - Automatic cleanup of stale connections
     - Statistics (active, idle, total connections)

     Usage:
     pool = ConnectionPool(
         connection_factory=lambda: BigQueryConnection(config),
         min_size=2,
         max_size=10
     )

     with pool.get_connection() as conn:
         conn.execute_ddl(ddl)

     Integration Examples

     Example 1: Create Table from DataFrame

     import pandas as pd
     from schema_mapper import infer_canonical_schema
     from schema_mapper.connections import ConnectionFactory, ConnectionConfig

     # Infer schema from DataFrame
     df = pd.read_csv('events.csv')
     canonical = infer_canonical_schema(
         df,
         table_name='events',Y
         dataset_name='analytics',
         partition_columns=['event_date'],
         cluster_columns=['user_id']
     )

     # Connect and create table
     config = ConnectionConfig('config/connections.yaml')
     with ConnectionFactory.get_connection('bigquery', config) as conn:
         # Integration with renderers happens automatically
         conn.create_table_from_schema(canonical, if_not_exists=True)
         print("Table created!")

     Example 2: Execute Incremental Load DDL

     from schema_mapper.incremental import IncrementalConfig, LoadPattern, get_incremental_generator
     from schema_mapper.connections import ConnectionFactory, ConnectionConfig

     # Generate incremental DDL (existing functionality)
     generator = get_incremental_generator('bigquery')
     config_inc = IncrementalConfig(
         load_pattern=LoadPattern.UPSERT,
         primary_keys=['user_id']
     )
     ddl = generator.generate_incremental_ddl(
         schema=schema,
         table_name='users',
         config=config_inc
     )

     # Execute via connection (new functionality)
     config = ConnectionConfig('config/connections.yaml')
     with ConnectionFactory.get_connection('bigquery', config) as conn:
         conn.execute_ddl(ddl)
         print("UPSERT executed!")

     Example 3: Cross-Platform Schema Migration

     # Introspect schema from Snowflake
     config = ConnectionConfig('config/connections.yaml')
     with ConnectionFactory.get_connection('snowflake', config) as sf_conn:
         canonical = sf_conn.get_target_schema('customers', schema_name='public')

     # Create in BigQuery (automatic type conversion via renderers)
     with ConnectionFactory.get_connection('bigquery', config) as bq_conn:
         bq_conn.create_table_from_schema(canonical)
         print("Migrated Snowflake → BigQuery!")

     Example 4: Database Introspection

     with ConnectionFactory.get_connection('postgresql', config) as conn:
         # Check table existence
         if conn.table_exists('users', schema_name='public'):
             # Get canonical schema
             schema = conn.get_target_schema('users', schema_name='public')

             # Use with different renderer
             from schema_mapper.renderers import RendererFactory
             renderer = RendererFactory.get_renderer('snowflake', schema)
             print(renderer.to_ddl())  # PostgreSQL → Snowflake DDL!

     Implementation Phases

     Phase 1: Foundation (Days 1-2)

     Goal: Core infrastructure

     Tasks:
     - Create connections/ module structure
     - Implement base.py - BaseConnection abstract class (~500 lines)
     - Implement config.py - YAML + .env configuration (~200 lines)
     - Implement factory.py - ConnectionFactory (~100 lines)
     - Implement exceptions.py - Custom exceptions (~100 lines)
     - Implement utils/retry.py - Retry logic with exponential backoff (~150 lines)
     - Implement utils/validation.py - Credential validation (~100 lines)
     - Create config/connections.yaml.example - Example config (~80 lines)
     - Create config/.env.example - Example env file (~30 lines)
     - Add dependencies to pyproject.toml: pyyaml>=6.0.0, python-dotenv>=1.0.0
     - Write unit tests for config system and retry logic (~200 lines)

     Deliverables:
     - Core abstractions ready
     - Configuration system working
     - Tests passing

     Phase 2: BigQuery Connector (Day 3)

     Goal: First platform implementation as reference

     Tasks:
     - Implement platform_connectors/bigquery.py (~500 lines)
       - Connection lifecycle with retry
       - Table introspection → CanonicalSchema
       - Type mapping (BigQuery → LogicalType)
       - DDL execution
       - Integration with BigQueryRenderer
     - Implement utils/type_mapping.py - Type conversion utilities (~150 lines)
     - Write comprehensive unit tests (~300 lines)
     - Write integration tests (optional, requires credentials)

     Deliverables:
     - BigQuery connector fully functional
     - Type mapping established
     - Tests passing

     Phase 3: Snowflake Connector (Day 4)

     Goal: Second platform, establish INFORMATION_SCHEMA pattern

     Tasks:
     - Implement platform_connectors/snowflake.py (~400 lines)
       - INFORMATION_SCHEMA introspection
       - Transaction support (begin/commit/rollback)
       - Type mapping (NUMBER, VARIANT, etc.)
     - Implement introspection.py - SQL-based introspection helpers (~200 lines)
     - Write unit tests (~250 lines)

     Deliverables:
     - Snowflake connector functional
     - Transaction support validated
     - Reusable introspection utilities

     Phase 4: PostgreSQL, Redshift, SQL Server (Days 5-7)

     Goal: Remaining platforms

     Tasks:
     - Implement platform_connectors/postgresql.py (~400 lines)
       - pg_catalog introspection
       - Full transaction support
       - Type mapping
     - Implement platform_connectors/redshift.py (~400 lines)
       - Similar to PostgreSQL
       - Redshift-specific optimizations
       - COPY command support
     - Implement platform_connectors/sqlserver.py (~400 lines)
       - INFORMATION_SCHEMA introspection
       - Transaction support
       - ODBC driver configuration
     - Write unit tests for each platform (~250 lines each)

     Deliverables:
     - All 5 platforms operational
     - Tests passing for all connectors

     Phase 5: Connection Pooling (Day 8)

     Goal: Production-grade pooling

     Tasks:
     - Implement utils/pooling.py - ConnectionPool class (~300 lines)
       - Thread-safe pool management
       - Connection health checks
       - Statistics and monitoring
       - Configurable min/max sizes
     - Add pool support to ConnectionFactory
     - Write unit tests with threading (~200 lines)

     Deliverables:
     - Connection pooling functional
     - Thread-safety validated
     - Performance benchmarks

     Phase 6: Enhanced Transaction Support (Day 9)

     Goal: Advanced transaction features

     Tasks:
     - Add transaction context managers to BaseConnection
     - Implement isolation level support (where applicable)
     - Add savepoint support for PostgreSQL/SQL Server
     - Add transaction statistics and logging
     - Write transaction tests (~150 lines)

     Deliverables:
     - Robust transaction support
     - Isolation level configuration
     - Comprehensive error handling

     Phase 7: Integration & Testing (Day 10)

     Goal: Ensure seamless integration

     Tasks:
     - Integration tests with renderers
     - Integration tests with generators
     - Integration tests with incremental loads
     - End-to-end workflow tests
     - Performance testing
     - Error scenario testing

     Deliverables:
     - All integration points validated
     - Edge cases covered
     - Performance metrics documented

     Phase 8: Documentation (Days 11-12)

     Goal: Comprehensive documentation

     Tasks:
     - API documentation for all public methods
     - User guide with examples
     - Migration guide for existing users
     - Configuration reference
     - Troubleshooting guide
     - Platform-specific notes
     - Update README.md with connection examples
     - Create docs/CONNECTIONS.md - detailed guide

     Deliverables:
     - Complete documentation
     - Ready for production use

     Critical Files to Create/Modify

     New Files to Create

     1. schema_mapper/connections/base.py (~500 lines)
       - BaseConnection abstract class
       - Connection lifecycle management
       - Core contract for all platforms
     2. schema_mapper/connections/config.py (~200 lines)
       - ConnectionConfig class
       - YAML parsing with env var interpolation
       - .env file loading
     3. schema_mapper/connections/factory.py (~100 lines)
       - ConnectionFactory with lazy loading
       - Platform selection logic
     4. schema_mapper/connections/exceptions.py (~100 lines)
       - ConnectionError, ConfigurationError, TableNotFoundError, etc.
     5. schema_mapper/connections/introspection.py (~200 lines)
       - SQL-based introspection utilities
       - INFORMATION_SCHEMA query builders
     6. schema_mapper/connections/utils/retry.py (~150 lines)
       - Retry decorator with exponential backoff
       - Platform-specific transient error detection
     7. schema_mapper/connections/utils/pooling.py (~300 lines)
       - ConnectionPool class
       - Thread-safe pool management
     8. schema_mapper/connections/utils/validation.py (~100 lines)
       - Credential validation
       - Config validation helpers
     9. schema_mapper/connections/utils/type_mapping.py (~150 lines)
       - Platform type → LogicalType mappings
       - Shared conversion utilities
     10. schema_mapper/connections/platform_connectors/bigquery.py (~500 lines)
       - BigQueryConnection implementation
     11. schema_mapper/connections/platform_connectors/snowflake.py (~400 lines)
       - SnowflakeConnection implementation
     12. schema_mapper/connections/platform_connectors/postgresql.py (~400 lines)
       - PostgreSQLConnection implementation
     13. schema_mapper/connections/platform_connectors/redshift.py (~400 lines)
       - RedshiftConnection implementation
     14. schema_mapper/connections/platform_connectors/sqlserver.py (~400 lines)
       - SQLServerConnection implementation
     15. config/connections.yaml.example (~80 lines)
       - Example configuration with all platforms
     16. config/.env.example (~30 lines)
       - Example environment variables
     17. tests/test_connections.py (~400 lines)
       - Core connection tests
     18. tests/connections/test_config.py (~200 lines)
       - Configuration system tests
     19. tests/connections/test_bigquery.py (~300 lines)
       - BigQuery connector tests
     20. tests/connections/test_snowflake.py (~250 lines)
       - Snowflake connector tests
     21. tests/connections/test_postgresql.py (~250 lines)
       - PostgreSQL connector tests
     22. tests/connections/test_redshift.py (~250 lines)
       - Redshift connector tests
     23. tests/connections/test_sqlserver.py (~250 lines)
       - SQL Server connector tests
     24. tests/connections/test_pooling.py (~200 lines)
       - Connection pool tests
     25. tests/integration/test_connection_integration.py (~300 lines)
       - Integration tests with renderers/generators
     26. docs/CONNECTIONS.md (~500 lines)
       - Comprehensive connection guide

     Files to Modify

     1. schema_mapper/connections/init.py
       - Export ConnectionFactory, ConnectionConfig, BaseConnection
     2. pyproject.toml
       - Add dependencies: pyyaml>=6.0.0, python-dotenv>=1.0.0
     3. README.md
       - Add connection system documentation
       - Add usage examples
       - Update features list
     4. setup.py
       - Add new dependencies to install_requires

     Testing Strategy

     Unit Tests

     - Mock connections without real databases
     - Test configuration parsing
     - Test type mappings
     - Test retry logic
     - Test connection pooling
     - Test error handling

     Integration Tests (Optional)

     - Real database connections (require credentials)
     - Mark with @pytest.mark.integration
     - Skip by default unless RUN_INTEGRATION_TESTS=1
     - Test introspection accuracy
     - Test DDL execution

     End-to-End Tests

     - DataFrame → Schema → Connection → Execute
     - Connection → Introspect → CanonicalSchema → Renderer
     - Multi-platform workflows

     Dependencies

     Existing (Already in pyproject.toml)

     - google-cloud-bigquery>=3.0.0 (bigquery extra)
     - snowflake-connector-python>=3.0.0 (snowflake extra)
     - redshift-connector>=2.0.0 (redshift extra)
     - psycopg2-binary>=2.9.0 (postgresql extra)
     - pyodbc>=4.0.0 (sqlserver extra)

     New (Add to pyproject.toml)

     - pyyaml>=6.0.0 (core dependency)
     - python-dotenv>=1.0.0 (core dependency)

     Backward Compatibility

     100% Backward Compatible - The connection system is purely additive:
     - No changes to existing canonical.py
     - No changes to existing renderers/
     - No changes to existing generators.py
     - No changes to existing incremental/
     - Users can continue using schema-mapper exactly as before
     - Connection features are opt-in

     Success Criteria

     1. ✅ All 5 platforms connect successfully
     2. ✅ Table introspection returns accurate CanonicalSchema
     3. ✅ DDL execution works via connections
     4. ✅ Seamless integration with renderers and generators
     5. ✅ Transaction support on platforms that support it
     6. ✅ Retry logic handles transient failures
     7. ✅ Connection pooling improves performance
     8. ✅ Comprehensive tests (unit + integration)
     9. ✅ Complete documentation
     10. ✅ Zero breaking changes to existing code

     Timeline Summary

     - Days 1-2: Foundation (base, config, factory, retry)
     - Day 3: BigQuery connector
     - Day 4: Snowflake connector
     - Days 5-7: PostgreSQL, Redshift, SQL Server connectors
     - Day 8: Connection pooling
     - Day 9: Enhanced transactions
     - Day 10: Integration testing
     - Days 11-12: Documentation

     Total: 10-12 days for production-ready implementation

     Next Steps After Approval

     1. Create module structure
     2. Implement base classes and configuration
     3. Start with BigQuery as reference implementation
     4. Expand to other platforms
     5. Add pooling and advanced features
     6. Comprehensive testing
     7. Documentation
     8. Code review and refinement