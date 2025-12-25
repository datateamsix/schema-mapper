# Metadata & Data Dictionary Framework

## Purpose

This document defines **what** we are building and **how** it should be implemented so metadata and data dictionaries are **first-class, non-optional artifacts** in the canonical schema framework.

The goal is to ensure:

* Metadata is captured once
* Travels with schema across warehouses
* Is introspectable, writable, and exportable
* Never becomes stale or tribal knowledge

This is designed for **engineers and analysts**, not just platform maintainers.

---

## Core Principles

1. **Schema = Structure + Meaning**
2. **Metadata must be bidirectional** (read + write)
3. **Warehouse-native support where possible**
4. **Canonical normalization always wins**
5. **Documentation should be generated, not written twice**

---

## Canonical Metadata Contract

### Table-Level Metadata (Required)

```yaml
table:
  name: events
  description: User interaction events captured from web and mobile clients
  owner: analytics
  domain: product_analytics
  tags:
    - events
    - core
    - user_behavior
```

### Column-Level Metadata (Required)

```yaml
columns:
  - name: event_id
    type: STRING
    nullable: false
    description: Unique identifier for the event
    source: client_sdk
    pii: false

  - name: created_at
    type: TIMESTAMP
    nullable: false
    description: UTC timestamp when the event occurred
    source: server
    pii: false
```

### Required Metadata Fields

| Level  | Field       | Reason                |
| ------ | ----------- | --------------------- |
| Table  | description | Analyst understanding |
| Table  | owner       | Accountability        |
| Column | description | Data dictionary       |
| Column | pii         | Governance hooks      |

---

## YAML-Driven Data Dictionary Specification

This YAML is the **single source of truth** for schema + metadata.

```yaml
target: bigquery

table:
  name: events
  description: User interaction events captured from web and mobile clients
  owner: analytics
  tags: [events, core]

columns:
  - name: event_id
    type: STRING
    nullable: false
    description: Unique identifier for the event
    source: client_sdk
    pii: false

  - name: created_at
    type: TIMESTAMP
    nullable: false
    description: UTC timestamp when the event occurred
    source: server
    pii: false
```

### Notes

* YAML may be authored manually or generated
* Supports env var substitution for CI workflows
* Can be versioned alongside code

---

## Implementation Architecture

### 1. CanonicalSchema Object

```python
class CanonicalSchema:
    table: dict
    columns: list[dict]
```

Responsibilities:

* Store structure + metadata
* Validate required fields
* Export data dictionaries

---

### 2. Metadata Introspection (Read Path)

Each warehouse connector must:

* Pull native metadata if available
* Normalize into canonical fields

| Warehouse | Native Field        |
| --------- | ------------------- |
| BigQuery  | field.description   |
| Snowflake | COMMENT             |
| Postgres  | col_description     |
| Redshift  | COMMENT             |
| MSSQL     | Extended Properties |

---

### 3. Metadata Propagation (Write Path)

DDL generators must:

* Apply column comments
* Apply table comments where supported

Example (BigQuery):

```sql
OPTIONS(description="UTC timestamp when the event occurred")
```

Example (Snowflake):

```sql
COMMENT ON COLUMN events.created_at IS 'UTC timestamp when the event occurred';
```

---

## Data Dictionary Generation

The CanonicalSchema must support exports:

```python
schema.export(format="markdown")
schema.export(format="csv")
schema.export(format="json")
```

Example Output:

| Column   | Type   | Nullable | Description                     | PII |
| -------- | ------ | -------- | ------------------------------- | --- |
| event_id | STRING | No       | Unique identifier for the event | No  |

---

## Validation & Enforcement

### Metadata Validation

```python
schema.validate_metadata(
  required_table_fields=["description", "owner"],
  required_column_fields=["description", "pii"]
)
```

CI should fail if required metadata is missing.

---

## Developer Responsibilities

### Connector Implementers

* Implement metadata read normalization
* Implement metadata write propagation

### Schema / DDL Implementers

* Treat metadata as mandatory inputs
* Ensure metadata is not dropped

### CLI / UX

* Surface missing metadata early
* Make metadata visible, not hidden

---

## Why This Matters

This framework ensures:

* Analysts get documentation on day one
* Metadata never drifts
* Governance hooks exist without heavy tooling
* Schema evolution is safe and auditable

This is not just convenience — it is **institutional memory encoded in code**.

---

## Non-Goals (For Now)

* Full data catalog replacement
* Column-level lineage graphs
* BI semantic modeling

These can be layered later.

---

## Summary

By formalizing a metadata contract and YAML-driven data dictionary spec, we turn schema management into a **documentation engine**. The canonical schema becomes the backbone for structure, meaning, governance, and trust — without adding friction for developers or analysts.
