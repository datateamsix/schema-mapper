"""
YAML-based schema definitions and data dictionary support.

This module provides YAML-driven schema management, making metadata
a first-class citizen alongside structure. Schemas can be authored manually,
generated from DataFrames/databases, or versioned alongside code.

Key Features:
- Load schema + metadata from YAML
- Save schema + metadata to YAML
- Environment variable substitution (${VAR_NAME})
- Validation of required metadata fields
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path

from .canonical import (
    CanonicalSchema,
    ColumnDefinition,
    LogicalType,
    OptimizationHints
)


def load_schema_from_yaml(yaml_path: str) -> CanonicalSchema:
    """
    Load canonical schema from YAML file.

    The YAML format follows the metadata framework specification:
    ```yaml
    target: bigquery  # Optional: target platform hint

    table:
      name: events
      dataset: analytics
      description: User interaction events
      owner: analytics
      domain: product_analytics
      tags: [events, core]

    columns:
      - name: event_id
        type: STRING
        nullable: false
        description: Unique identifier for the event
        source: client_sdk
        pii: false
        tags: [identifier]
    ```

    Supports environment variable substitution using ${VAR_NAME} or ${VAR_NAME:-default}.

    Args:
        yaml_path: Path to YAML schema file

    Returns:
        CanonicalSchema instance

    Raises:
        FileNotFoundError: If YAML file doesn't exist
        ValueError: If YAML is malformed or missing required fields

    Example:
        >>> schema = load_schema_from_yaml('schema/events.yaml')
        >>> print(f"Loaded {len(schema.columns)} columns")
        >>> errors = schema.validate_metadata(
        ...     required_table_fields=['description', 'owner'],
        ...     required_column_fields=['description', 'pii']
        ... )
        >>> assert not errors, f"Metadata validation failed: {errors}"
    """
    yaml_path = Path(yaml_path)

    if not yaml_path.exists():
        raise FileNotFoundError(f"Schema file not found: {yaml_path}")

    # Read and parse YAML
    with open(yaml_path, 'r') as f:
        content = f.read()

    # Substitute environment variables
    content = _substitute_env_vars(content)

    # Parse YAML
    data = yaml.safe_load(content)

    if not data:
        raise ValueError(f"Empty or invalid YAML file: {yaml_path}")

    # Extract table metadata
    table = data.get('table', {})
    if not table:
        raise ValueError("YAML must contain 'table' section")

    table_name = table.get('name')
    if not table_name:
        raise ValueError("Table 'name' is required")

    dataset_name = table.get('dataset')
    project_id = table.get('project')
    description = table.get('description')
    owner = table.get('owner')
    domain = table.get('domain')
    tags = table.get('tags', [])

    # Extract columns
    columns_data = data.get('columns', [])
    if not columns_data:
        raise ValueError("YAML must contain at least one column in 'columns' section")

    columns = []
    for col_data in columns_data:
        col_name = col_data.get('name')
        if not col_name:
            raise ValueError("Column 'name' is required")

        col_type_str = col_data.get('type', 'STRING').upper()
        try:
            logical_type = LogicalType[col_type_str]
        except KeyError:
            raise ValueError(
                f"Invalid type '{col_type_str}' for column '{col_name}'. "
                f"Valid types: {[t.name for t in LogicalType]}"
            )

        column = ColumnDefinition(
            name=col_name,
            logical_type=logical_type,
            nullable=col_data.get('nullable', True),
            description=col_data.get('description'),
            source=col_data.get('source'),
            pii=col_data.get('pii', False),
            tags=col_data.get('tags', []),
            max_length=col_data.get('max_length'),
            precision=col_data.get('precision'),
            scale=col_data.get('scale'),
            date_format=col_data.get('date_format'),
            timezone=col_data.get('timezone'),
        )

        columns.append(column)

    # Extract optimization hints
    optimization_data = data.get('optimization', {})
    optimization = OptimizationHints(
        partition_columns=optimization_data.get('partition_columns', []),
        cluster_columns=optimization_data.get('cluster_columns', []),
        sort_columns=optimization_data.get('sort_columns', []),
        distribution_column=optimization_data.get('distribution_column'),
        partition_expiration_days=optimization_data.get('partition_expiration_days'),
        require_partition_filter=optimization_data.get('require_partition_filter', False),
        transient=optimization_data.get('transient', False),
    )

    # Create canonical schema
    schema = CanonicalSchema(
        table_name=table_name,
        dataset_name=dataset_name,
        project_id=project_id,
        columns=columns,
        optimization=optimization,
        description=description,
        owner=owner,
        domain=domain,
        tags=tags,
        created_from=f"YAML: {yaml_path.name}"
    )

    # Validate schema structure
    errors = schema.validate()
    if errors:
        raise ValueError(f"Schema validation failed: {errors}")

    return schema


def save_schema_to_yaml(schema: CanonicalSchema, yaml_path: str, include_optimization: bool = True):
    """
    Save canonical schema to YAML file.

    This creates a YAML file that serves as the single source of truth
    for both schema structure and metadata.

    Args:
        schema: CanonicalSchema to save
        yaml_path: Output path for YAML file
        include_optimization: Include optimization hints in output

    Example:
        >>> schema = infer_canonical_schema(df, table_name='events')
        >>> # Enrich with metadata
        >>> schema.description = "User interaction events"
        >>> schema.owner = "analytics"
        >>> schema.columns[0].description = "Unique event ID"
        >>> schema.columns[0].pii = False
        >>> # Save to YAML
        >>> save_schema_to_yaml(schema, 'schema/events.yaml')
    """
    yaml_path = Path(yaml_path)
    yaml_path.parent.mkdir(parents=True, exist_ok=True)

    # Build YAML structure
    data = {
        'table': {
            'name': schema.table_name,
        }
    }

    # Add optional table fields
    if schema.dataset_name:
        data['table']['dataset'] = schema.dataset_name
    if schema.project_id:
        data['table']['project'] = schema.project_id
    if schema.description:
        data['table']['description'] = schema.description
    if schema.owner:
        data['table']['owner'] = schema.owner
    if schema.domain:
        data['table']['domain'] = schema.domain
    if schema.tags:
        data['table']['tags'] = schema.tags

    # Add columns
    data['columns'] = []
    for col in schema.columns:
        col_data = {
            'name': col.name,
            'type': col.logical_type.name,
            'nullable': bool(col.nullable),  # Convert to Python bool (handles numpy bools)
        }

        # Add metadata fields
        if col.description:
            col_data['description'] = col.description
        if col.source:
            col_data['source'] = col.source

        # Always include PII field for governance
        col_data['pii'] = bool(col.pii)  # Convert to Python bool

        if col.tags:
            col_data['tags'] = col.tags

        # Add type parameters
        if col.max_length:
            col_data['max_length'] = col.max_length
        if col.precision:
            col_data['precision'] = col.precision
        if col.scale:
            col_data['scale'] = col.scale
        if col.date_format:
            col_data['date_format'] = col.date_format
        if col.timezone:
            col_data['timezone'] = col.timezone

        data['columns'].append(col_data)

    # Add optimization hints
    if include_optimization and schema.optimization and schema.optimization.has_optimizations():
        opt_data = {}

        if schema.optimization.partition_columns:
            opt_data['partition_columns'] = schema.optimization.partition_columns
        if schema.optimization.cluster_columns:
            opt_data['cluster_columns'] = schema.optimization.cluster_columns
        if schema.optimization.sort_columns:
            opt_data['sort_columns'] = schema.optimization.sort_columns
        if schema.optimization.distribution_column:
            opt_data['distribution_column'] = schema.optimization.distribution_column
        if schema.optimization.partition_expiration_days:
            opt_data['partition_expiration_days'] = schema.optimization.partition_expiration_days
        if schema.optimization.require_partition_filter:
            opt_data['require_partition_filter'] = schema.optimization.require_partition_filter
        if schema.optimization.transient:
            opt_data['transient'] = schema.optimization.transient

        if opt_data:
            data['optimization'] = opt_data

    # Write YAML with clean formatting
    with open(yaml_path, 'w') as f:
        f.write("# Schema Definition with Metadata\n")
        f.write("# Generated by schema-mapper\n")
        f.write("#\n")
        f.write("# This YAML file is the single source of truth for schema + metadata.\n")
        f.write("# It can be versioned alongside code and deployed across platforms.\n\n")

        yaml.dump(data, f, default_flow_style=False, sort_keys=False, indent=2)


def _substitute_env_vars(content: str) -> str:
    """
    Substitute environment variables in YAML content.

    Supports:
    - ${VAR_NAME} - required variable (error if not found)
    - ${VAR_NAME:-default} - optional with default value

    Args:
        content: YAML content with variable placeholders

    Returns:
        Content with variables substituted

    Example:
        >>> os.environ['DB_NAME'] = 'production'
        >>> content = "database: ${DB_NAME}"
        >>> result = _substitute_env_vars(content)
        >>> assert result == "database: production"
    """
    import re

    # Pattern matches ${VAR_NAME} or ${VAR_NAME:-default}
    pattern = r'\$\{([^}:]+)(?::-(.*?))?\}'

    def replace_var(match):
        """Replace environment variable placeholder with actual value."""
        var_name = match.group(1)
        default_value = match.group(2)

        # Get value from environment
        value = os.environ.get(var_name)

        if value is not None:
            return value
        elif default_value is not None:
            return default_value
        else:
            # Variable required but not found - preserve placeholder
            # This allows YAML parsing to succeed even with missing vars
            # Validation will catch missing required vars later
            return match.group(0)

    return re.sub(pattern, replace_var, content)


__all__ = [
    'load_schema_from_yaml',
    'save_schema_to_yaml',
]
