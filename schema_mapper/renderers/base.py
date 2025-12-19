"""
Base renderer interface for platform-specific schema rendering.

All platform renderers inherit from SchemaRenderer and implement
platform-specific logic for DDL, JSON schemas, and CLI commands.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from ..canonical import CanonicalSchema, LogicalType


class SchemaRenderer(ABC):
    """
    Abstract base class for platform-specific schema renderers.

    Each platform (BigQuery, Snowflake, Redshift, etc.) implements this
    interface to convert canonical schemas to platform-specific formats.

    Responsibilities:
    - Validate schema against platform capabilities
    - Convert logical types to physical types
    - Generate DDL statements
    - Generate CLI commands
    - Optionally generate JSON schemas (BigQuery only)
    """

    def __init__(self, canonical_schema: CanonicalSchema):
        """
        Initialize renderer with canonical schema.

        Args:
            canonical_schema: Platform-agnostic schema

        Raises:
            ValueError: If schema validation fails
        """
        self.schema = canonical_schema

        # Validate schema consistency
        schema_errors = self.schema.validate()
        if schema_errors:
            raise ValueError(f"Invalid canonical schema: {', '.join(schema_errors)}")

        # Validate against platform capabilities
        platform_errors = self.validate()
        if platform_errors:
            raise ValueError(
                f"Schema not compatible with {self.platform_name()}: "
                f"{', '.join(platform_errors)}"
            )

    @abstractmethod
    def platform_name(self) -> str:
        """Return platform name (e.g., 'bigquery', 'snowflake')."""
        pass

    @abstractmethod
    def validate(self) -> List[str]:
        """
        Validate schema against platform capabilities.

        Check that optimization hints are supported, column types are valid,
        and all features are compatible with the platform.

        Returns:
            List of error messages (empty if valid)
        """
        pass

    @abstractmethod
    def to_ddl(self) -> str:
        """
        Render CREATE TABLE DDL statement.

        Returns:
            SQL DDL string
        """
        pass

    @abstractmethod
    def to_cli_create(self) -> str:
        """
        Render CLI command to create table.

        For platforms that support it, this may be different from executing DDL.
        For example, BigQuery can create tables from JSON schemas.

        Returns:
            Shell command string
        """
        pass

    @abstractmethod
    def to_cli_load(self, data_path: str, **kwargs) -> str:
        """
        Render CLI command to load data from file.

        Args:
            data_path: Path to data file (CSV, JSON, etc.)
            **kwargs: Platform-specific options

        Returns:
            Shell command string (may be multi-line)
        """
        pass

    @abstractmethod
    def to_physical_types(self) -> Dict[str, str]:
        """
        Convert logical types to platform-specific physical types.

        Returns:
            Dictionary mapping column names to physical type strings
            Example: {"user_id": "BIGINT", "name": "VARCHAR(255)"}
        """
        pass

    # Optional methods (override if platform supports)

    def to_schema_json(self) -> Optional[str]:
        """
        Render JSON schema (BigQuery only).

        Returns:
            JSON string if supported, None otherwise
        """
        return None

    def supports_json_schema(self) -> bool:
        """
        Does this platform natively support JSON schemas?

        Returns:
            True for BigQuery, False for others
        """
        return False

    # Helper methods for subclasses

    def _get_full_table_name(self, use_backticks: bool = False, separator: str = ".") -> str:
        """
        Get fully qualified table name.

        Args:
            use_backticks: Wrap in backticks (BigQuery style)
            separator: Separator between parts (. or ::)

        Returns:
            Qualified table name
        """
        parts = []

        if self.schema.project_id:
            parts.append(self.schema.project_id)

        if self.schema.dataset_name:
            parts.append(self.schema.dataset_name)

        parts.append(self.schema.table_name)

        table_name = separator.join(parts)

        if use_backticks:
            return f"`{table_name}`"

        return table_name

    def _validate_optimization_columns_exist(self) -> List[str]:
        """
        Validate that all optimization hint columns exist in schema.

        Returns:
            List of error messages
        """
        errors = []
        col_names = set(self.schema.column_names())

        opt = self.schema.optimization
        if not opt:
            return errors

        # Check partition columns
        for col in opt.partition_columns:
            if col not in col_names:
                errors.append(f"Partition column '{col}' not found in schema")

        # Check cluster columns
        for col in opt.cluster_columns:
            if col not in col_names:
                errors.append(f"Cluster column '{col}' not found in schema")

        # Check sort columns
        for col in opt.sort_columns:
            if col not in col_names:
                errors.append(f"Sort column '{col}' not found in schema")

        # Check distribution column
        if opt.distribution_column and opt.distribution_column not in col_names:
            errors.append(f"Distribution column '{opt.distribution_column}' not found in schema")

        return errors

    def _get_column_comment_sql(self, full_table: str, column_name: str, description: str) -> str:
        """
        Generate SQL comment for a column (platform-specific).

        This is a helper method that subclasses can override.

        Args:
            full_table: Fully qualified table name
            column_name: Column name
            description: Column description

        Returns:
            SQL COMMENT statement
        """
        # Default: SQL standard
        desc = description.replace("'", "''")
        return f"COMMENT ON COLUMN {full_table}.{column_name} IS '{desc}';"


__all__ = ['SchemaRenderer']
