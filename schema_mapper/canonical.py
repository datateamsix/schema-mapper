"""
Canonical schema definitions - the single source of truth.

This module defines platform-agnostic schema representations that can be
rendered to any target platform via the renderer pattern.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum
import pandas as pd


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
    """
    Canonical column definition - engine agnostic.

    Represents a single column with logical types and metadata,
    independent of any specific database platform.
    """
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
    pandas_dtype: Optional[str] = None   # Original pandas dtype

    def __str__(self):
        """Human-readable representation."""
        nullable_str = "NULL" if self.nullable else "NOT NULL"
        type_str = self.logical_type.value.upper()

        if self.max_length:
            type_str += f"({self.max_length})"
        elif self.precision and self.scale:
            type_str += f"({self.precision},{self.scale})"

        return f"{self.name}: {type_str} {nullable_str}"


@dataclass
class OptimizationHints:
    """
    Platform-agnostic optimization hints.

    These are logical hints that will be rendered differently on each platform:
    - BigQuery: PARTITION BY, CLUSTER BY
    - Snowflake: CLUSTER BY
    - Redshift: DISTKEY, SORTKEY
    - PostgreSQL: PARTITION BY, CREATE INDEX
    """
    # Columns for optimization
    partition_columns: List[str] = field(default_factory=list)
    cluster_columns: List[str] = field(default_factory=list)
    sort_columns: List[str] = field(default_factory=list)
    distribution_column: Optional[str] = None

    # Time-based hints (BigQuery)
    partition_expiration_days: Optional[int] = None
    require_partition_filter: bool = False

    # Table properties
    transient: bool = False  # Snowflake staging tables

    def has_optimizations(self) -> bool:
        """Check if any optimizations are specified."""
        return bool(
            self.partition_columns
            or self.cluster_columns
            or self.sort_columns
            or self.distribution_column
        )


@dataclass
class CanonicalSchema:
    """
    The single source of truth for a table schema.

    This is platform-agnostic and contains only logical information.
    Physical rendering is delegated to platform-specific renderers.

    Example:
        schema = CanonicalSchema(
            table_name='events',
            dataset_name='analytics',
            columns=[
                ColumnDefinition('event_id', LogicalType.BIGINT, nullable=False),
                ColumnDefinition('event_date', LogicalType.DATE),
            ],
            optimization=OptimizationHints(
                partition_columns=['event_date'],
                cluster_columns=['user_id', 'event_type']
            )
        )
    """
    # Identity
    table_name: str
    dataset_name: Optional[str] = None
    project_id: Optional[str] = None  # BigQuery-specific, but harmless for others

    # Schema
    columns: List[ColumnDefinition] = field(default_factory=list)

    # Optimization (logical hints, not physical implementation)
    optimization: Optional[OptimizationHints] = None

    # Metadata
    description: Optional[str] = None
    created_from: Optional[str] = None  # e.g., "CSV", "DataFrame", "Manual"

    def __post_init__(self):
        """Initialize defaults."""
        if self.optimization is None:
            self.optimization = OptimizationHints()

    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]

    def validate(self) -> List[str]:
        """
        Validate schema consistency.

        Returns:
            List of error messages (empty if valid)
        """
        errors = []

        if not self.table_name:
            errors.append("Table name is required")

        if not self.columns:
            errors.append("At least one column is required")

        # Check for duplicate column names
        col_names = [col.name for col in self.columns]
        if len(col_names) != len(set(col_names)):
            duplicates = [name for name in col_names if col_names.count(name) > 1]
            errors.append(f"Duplicate column names: {set(duplicates)}")

        # Validate optimization hints reference real columns
        if self.optimization:
            all_cols = set(col_names)

            for col in self.optimization.partition_columns:
                if col not in all_cols:
                    errors.append(f"Partition column '{col}' not found in schema")

            for col in self.optimization.cluster_columns:
                if col not in all_cols:
                    errors.append(f"Cluster column '{col}' not found in schema")

            for col in self.optimization.sort_columns:
                if col not in all_cols:
                    errors.append(f"Sort column '{col}' not found in schema")

            if self.optimization.distribution_column:
                if self.optimization.distribution_column not in all_cols:
                    errors.append(
                        f"Distribution column '{self.optimization.distribution_column}' "
                        f"not found in schema"
                    )

        return errors

    def __str__(self):
        """Human-readable representation."""
        lines = [f"Table: {self.dataset_name}.{self.table_name}" if self.dataset_name else f"Table: {self.table_name}"]
        lines.append(f"Columns: {len(self.columns)}")
        for col in self.columns:
            lines.append(f"  - {col}")

        if self.optimization and self.optimization.has_optimizations():
            lines.append("Optimization:")
            if self.optimization.partition_columns:
                lines.append(f"  - Partition: {', '.join(self.optimization.partition_columns)}")
            if self.optimization.cluster_columns:
                lines.append(f"  - Cluster: {', '.join(self.optimization.cluster_columns)}")
            if self.optimization.sort_columns:
                lines.append(f"  - Sort: {', '.join(self.optimization.sort_columns)}")

        return "\n".join(lines)


def _infer_logical_type(dtype: str) -> LogicalType:
    """Infer logical type from pandas dtype."""
    dtype_lower = str(dtype).lower()

    # Integer types
    if 'int' in dtype_lower:
        return LogicalType.BIGINT

    # Float types
    if 'float' in dtype_lower:
        return LogicalType.FLOAT

    # Boolean
    if 'bool' in dtype_lower:
        return LogicalType.BOOLEAN

    # Datetime
    if 'datetime64' in dtype_lower:
        if 'utc' in dtype_lower or 'tz' in dtype_lower:
            return LogicalType.TIMESTAMPTZ
        return LogicalType.TIMESTAMP

    # Date
    if 'date' in dtype_lower and 'datetime' not in dtype_lower:
        return LogicalType.DATE

    # Timedelta (treat as string for now)
    if 'timedelta' in dtype_lower:
        return LogicalType.STRING

    # Default to STRING for object/string types
    return LogicalType.STRING


def infer_canonical_schema(
    df: pd.DataFrame,
    table_name: str,
    dataset_name: Optional[str] = None,
    project_id: Optional[str] = None,
    partition_columns: Optional[List[str]] = None,
    cluster_columns: Optional[List[str]] = None,
    sort_columns: Optional[List[str]] = None,
    distribution_column: Optional[str] = None,
    partition_expiration_days: Optional[int] = None,
    require_partition_filter: bool = False,
    transient: bool = False,
    standardize_columns: bool = True,
    auto_cast: bool = True,
) -> CanonicalSchema:
    """
    Infer canonical schema from pandas DataFrame.

    Args:
        df: Input DataFrame
        table_name: Table name
        dataset_name: Dataset/schema name
        project_id: Project ID (BigQuery)
        partition_columns: Columns to partition by
        cluster_columns: Columns to cluster by
        sort_columns: Columns to sort by
        distribution_column: Column for distribution (Redshift)
        partition_expiration_days: Auto-delete partitions after N days (BigQuery)
        require_partition_filter: Require partition filter in queries (BigQuery)
        transient: Create transient table (Snowflake)
        standardize_columns: Standardize column names
        auto_cast: Automatically cast types

    Returns:
        CanonicalSchema instance
    """
    from .utils import standardize_column_name, detect_and_cast_types

    # Optionally standardize column names
    if standardize_columns:
        df = df.copy()
        original_names = dict(zip(df.columns, df.columns))
        df.columns = [standardize_column_name(col) for col in df.columns]
        # Create reverse mapping
        name_mapping = dict(zip(df.columns, original_names.values()))
    else:
        name_mapping = {}

    # Optionally auto-cast types
    if auto_cast:
        df = detect_and_cast_types(df)

    # Build column definitions
    columns = []
    for col_name in df.columns:
        col_data = df[col_name]

        # Infer logical type
        logical_type = _infer_logical_type(col_data.dtype)

        # Determine nullability
        nullable = col_data.isna().any()

        # Create column definition
        col_def = ColumnDefinition(
            name=col_name,
            logical_type=logical_type,
            nullable=nullable,
            original_name=name_mapping.get(col_name, col_name),
            pandas_dtype=str(col_data.dtype)
        )

        # Add max_length for string columns (estimate from data)
        if logical_type in (LogicalType.STRING, LogicalType.TEXT):
            if not col_data.isna().all():
                max_len = col_data.astype(str).str.len().max()
                # Round up to reasonable values
                if max_len <= 255:
                    col_def.max_length = 255
                elif max_len <= 1000:
                    col_def.max_length = 1000
                elif max_len <= 5000:
                    col_def.max_length = 5000
                else:
                    col_def.max_length = None  # Use platform default

        columns.append(col_def)

    # Build optimization hints
    optimization = OptimizationHints(
        partition_columns=partition_columns or [],
        cluster_columns=cluster_columns or [],
        sort_columns=sort_columns or [],
        distribution_column=distribution_column,
        partition_expiration_days=partition_expiration_days,
        require_partition_filter=require_partition_filter,
        transient=transient,
    )

    # Create canonical schema
    schema = CanonicalSchema(
        table_name=table_name,
        dataset_name=dataset_name,
        project_id=project_id,
        columns=columns,
        optimization=optimization,
        description=None,
        created_from="DataFrame"
    )

    return schema


def canonical_schema_to_dict(schema: CanonicalSchema) -> Dict:
    """
    Convert canonical schema to dictionary for serialization.

    Useful for:
    - JSON export
    - Version control
    - Schema registry
    """
    return {
        "table_name": schema.table_name,
        "dataset_name": schema.dataset_name,
        "project_id": schema.project_id,
        "columns": [
            {
                "name": col.name,
                "logical_type": col.logical_type.value,
                "nullable": col.nullable,
                "description": col.description,
                "max_length": col.max_length,
                "precision": col.precision,
                "scale": col.scale,
                "original_name": col.original_name,
                "pandas_dtype": col.pandas_dtype,
            }
            for col in schema.columns
        ],
        "optimization": {
            "partition_columns": schema.optimization.partition_columns,
            "cluster_columns": schema.optimization.cluster_columns,
            "sort_columns": schema.optimization.sort_columns,
            "distribution_column": schema.optimization.distribution_column,
            "partition_expiration_days": schema.optimization.partition_expiration_days,
            "require_partition_filter": schema.optimization.require_partition_filter,
            "transient": schema.optimization.transient,
        } if schema.optimization else None,
        "description": schema.description,
        "created_from": schema.created_from,
    }


__all__ = [
    'LogicalType',
    'ColumnDefinition',
    'OptimizationHints',
    'CanonicalSchema',
    'infer_canonical_schema',
    'canonical_schema_to_dict',
]
