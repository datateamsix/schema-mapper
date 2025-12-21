"""
Load pattern definitions and configurations.

This module defines the various incremental load patterns supported by schema-mapper
and their configuration options.
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


class LoadPattern(Enum):
    """
    Supported incremental load patterns.

    Each pattern defines how new data is merged with existing data
    in the target table.
    """

    # Basic patterns
    FULL_REFRESH = "full_refresh"
    """Truncate table and reload all data (CREATE OR REPLACE / TRUNCATE + INSERT)"""

    APPEND_ONLY = "append"
    """Simple INSERT - add new records only, no updates or deletes"""

    # Merge patterns
    UPSERT = "upsert"
    """MERGE operation - INSERT new records, UPDATE existing records"""

    DELETE_INSERT = "delete_insert"
    """DELETE matching records then INSERT - transactional alternative to MERGE"""

    # Incremental patterns
    INCREMENTAL_TIMESTAMP = "incremental_timestamp"
    """Load only records newer than max timestamp in target table"""

    INCREMENTAL_APPEND = "incremental_append"
    """Append only records not already in target (based on primary key)"""

    # SCD patterns
    SCD_TYPE1 = "scd_type1"
    """Slowly Changing Dimension Type 1 - overwrite changed records"""

    SCD_TYPE2 = "scd_type2"
    """Slowly Changing Dimension Type 2 - maintain full history with versions"""

    # CDC pattern
    CDC_MERGE = "cdc"
    """Change Data Capture - process I/U/D operations from CDC stream"""


class MergeStrategy(Enum):
    """Strategy for handling matched records in MERGE operations."""

    UPDATE_ALL = "update_all"
    """Update all columns when records match"""

    UPDATE_CHANGED = "update_changed"
    """Only update columns that have changed (requires hash/comparison)"""

    UPDATE_SELECTIVE = "update_selective"
    """Update only specified columns"""

    UPDATE_NONE = "update_none"
    """Don't update matched records (INSERT only)"""


class DeleteStrategy(Enum):
    """Strategy for handling records that exist in target but not in source."""

    HARD_DELETE = "hard_delete"
    """DELETE records from table"""

    SOFT_DELETE = "soft_delete"
    """Set is_deleted flag or similar"""

    IGNORE = "ignore"
    """Don't delete records (default for most patterns)"""


@dataclass
class IncrementalConfig:
    """
    Configuration for incremental load operations.

    This class defines all the parameters needed to generate incremental
    load DDL for various load patterns across different platforms.

    Example:
        >>> config = IncrementalConfig(
        ...     load_pattern=LoadPattern.UPSERT,
        ...     primary_keys=['user_id'],
        ...     merge_strategy=MergeStrategy.UPDATE_ALL
        ... )
        >>> config.validate()
    """

    # Required
    load_pattern: LoadPattern
    primary_keys: List[str]

    # Optional - Merge configuration
    merge_strategy: MergeStrategy = MergeStrategy.UPDATE_ALL
    update_columns: Optional[List[str]] = None  # For UPDATE_SELECTIVE

    # Optional - Timestamp-based incremental
    incremental_column: Optional[str] = None  # Timestamp column for filtering
    lookback_window: Optional[str] = None  # e.g., "2 hours", "1 day"

    # Optional - SCD Type 2
    effective_date_column: Optional[str] = "effective_from"
    expiration_date_column: Optional[str] = "effective_to"
    is_current_column: Optional[str] = "is_current"
    hash_columns: Optional[List[str]] = None  # Columns to track for changes

    # Optional - CDC
    operation_column: Optional[str] = "_op"  # I/U/D indicator
    sequence_column: Optional[str] = "_seq"  # For ordering CDC events

    # Optional - Delete handling
    delete_strategy: DeleteStrategy = DeleteStrategy.IGNORE
    soft_delete_column: Optional[str] = "is_deleted"

    # Optional - Advanced
    staging_table: Optional[str] = None  # Custom staging table name
    partition_column: Optional[str] = None  # Only merge affected partitions
    cluster_columns: Optional[List[str]] = None

    # Validation
    enable_validation: bool = True  # Pre-merge validation checks
    dry_run: bool = False  # Generate DDL but don't execute

    # Metadata
    created_by_column: Optional[str] = "created_at"
    updated_by_column: Optional[str] = "updated_at"

    def validate(self) -> None:
        """
        Validate configuration for consistency and completeness.

        Raises:
            ValueError: If configuration is invalid
        """
        # Check if pattern requires primary keys
        metadata = LOAD_PATTERN_METADATA.get(self.load_pattern)
        if metadata and metadata.requires_primary_key:
            if not self.primary_keys:
                raise ValueError("primary_keys cannot be empty")

        if self.load_pattern == LoadPattern.INCREMENTAL_TIMESTAMP:
            if not self.incremental_column:
                raise ValueError("incremental_column required for INCREMENTAL_TIMESTAMP")

        if self.load_pattern == LoadPattern.SCD_TYPE2:
            if not self.hash_columns:
                raise ValueError("hash_columns required for SCD_TYPE2")
            if not self.effective_date_column:
                raise ValueError("effective_date_column required for SCD_TYPE2")
            if not self.expiration_date_column:
                raise ValueError("expiration_date_column required for SCD_TYPE2")
            if not self.is_current_column:
                raise ValueError("is_current_column required for SCD_TYPE2")

        if self.load_pattern == LoadPattern.CDC_MERGE:
            if not self.operation_column:
                raise ValueError("operation_column required for CDC_MERGE")

        if self.merge_strategy == MergeStrategy.UPDATE_SELECTIVE:
            if not self.update_columns:
                raise ValueError("update_columns required for UPDATE_SELECTIVE")

        if self.delete_strategy == DeleteStrategy.SOFT_DELETE:
            if not self.soft_delete_column:
                raise ValueError("soft_delete_column required for SOFT_DELETE")


@dataclass
class LoadPatternMetadata:
    """
    Metadata about a load pattern.

    Provides descriptive information about each load pattern including
    use cases, requirements, and complexity.
    """
    pattern: LoadPattern
    name: str
    description: str
    use_cases: List[str]
    requires_primary_key: bool
    supports_delete: bool
    is_transactional: bool
    complexity: str  # "simple", "medium", "advanced"


# Pattern metadata registry
LOAD_PATTERN_METADATA: Dict[LoadPattern, LoadPatternMetadata] = {
    LoadPattern.FULL_REFRESH: LoadPatternMetadata(
        pattern=LoadPattern.FULL_REFRESH,
        name="Full Refresh",
        description="Replace entire table contents",
        use_cases=["Dimension tables", "Small lookup tables", "Complete refreshes"],
        requires_primary_key=False,
        supports_delete=True,
        is_transactional=True,
        complexity="simple"
    ),
    LoadPattern.APPEND_ONLY: LoadPatternMetadata(
        pattern=LoadPattern.APPEND_ONLY,
        name="Append Only",
        description="Insert new records without updates",
        use_cases=["Event logs", "Immutable data", "Audit trails"],
        requires_primary_key=False,
        supports_delete=False,
        is_transactional=False,
        complexity="simple"
    ),
    LoadPattern.UPSERT: LoadPatternMetadata(
        pattern=LoadPattern.UPSERT,
        name="Upsert (Merge)",
        description="Insert new records, update existing records",
        use_cases=["Customer data", "Product catalogs", "Most transactional data"],
        requires_primary_key=True,
        supports_delete=False,
        is_transactional=True,
        complexity="medium"
    ),
    LoadPattern.DELETE_INSERT: LoadPatternMetadata(
        pattern=LoadPattern.DELETE_INSERT,
        name="Delete-Insert",
        description="Delete matching records then insert",
        use_cases=["Redshift upserts", "Transactional alternative to MERGE"],
        requires_primary_key=True,
        supports_delete=True,
        is_transactional=True,
        complexity="medium"
    ),
    LoadPattern.INCREMENTAL_TIMESTAMP: LoadPatternMetadata(
        pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
        name="Incremental Timestamp",
        description="Load only records newer than max timestamp",
        use_cases=["Event streams", "Time-series data", "Append-only with filtering"],
        requires_primary_key=False,
        supports_delete=False,
        is_transactional=False,
        complexity="medium"
    ),
    LoadPattern.INCREMENTAL_APPEND: LoadPatternMetadata(
        pattern=LoadPattern.INCREMENTAL_APPEND,
        name="Incremental Append",
        description="Append only records not already in target",
        use_cases=["Avoiding duplicates", "Idempotent loads"],
        requires_primary_key=True,
        supports_delete=False,
        is_transactional=False,
        complexity="medium"
    ),
    LoadPattern.SCD_TYPE1: LoadPatternMetadata(
        pattern=LoadPattern.SCD_TYPE1,
        name="Slowly Changing Dimension Type 1",
        description="Overwrite changed records (no history)",
        use_cases=["Current state only", "No history required"],
        requires_primary_key=True,
        supports_delete=False,
        is_transactional=True,
        complexity="medium"
    ),
    LoadPattern.SCD_TYPE2: LoadPatternMetadata(
        pattern=LoadPattern.SCD_TYPE2,
        name="Slowly Changing Dimension Type 2",
        description="Maintain full history with versioning",
        use_cases=["Historical tracking", "Audit requirements", "Dimensional modeling"],
        requires_primary_key=True,
        supports_delete=False,
        is_transactional=True,
        complexity="advanced"
    ),
    LoadPattern.CDC_MERGE: LoadPatternMetadata(
        pattern=LoadPattern.CDC_MERGE,
        name="Change Data Capture",
        description="Process I/U/D operations from CDC stream",
        use_cases=["Database replication", "CDC pipelines", "Real-time sync"],
        requires_primary_key=True,
        supports_delete=True,
        is_transactional=True,
        complexity="advanced"
    ),
}


def get_pattern_metadata(pattern: LoadPattern) -> LoadPatternMetadata:
    """
    Get metadata for a load pattern.

    Args:
        pattern: The load pattern to get metadata for

    Returns:
        LoadPatternMetadata for the pattern

    Example:
        >>> metadata = get_pattern_metadata(LoadPattern.UPSERT)
        >>> print(metadata.name)
        'Upsert (Merge)'
    """
    return LOAD_PATTERN_METADATA.get(pattern)


def list_patterns_for_use_case(use_case: str) -> List[LoadPattern]:
    """
    Find suitable patterns for a use case.

    Args:
        use_case: Use case description to search for

    Returns:
        List of matching LoadPattern enums

    Example:
        >>> patterns = list_patterns_for_use_case("event")
        >>> print([p.value for p in patterns])
        ['append', 'incremental_timestamp']
    """
    matches = []
    for pattern, metadata in LOAD_PATTERN_METADATA.items():
        if any(use_case.lower() in uc.lower() for uc in metadata.use_cases):
            matches.append(pattern)
    return matches


def get_all_patterns() -> List[LoadPattern]:
    """
    Get all available load patterns.

    Returns:
        List of all LoadPattern enums
    """
    return list(LoadPattern)


def get_simple_patterns() -> List[LoadPattern]:
    """
    Get simple load patterns suitable for beginners.

    Returns:
        List of simple LoadPattern enums
    """
    return [
        pattern for pattern, metadata in LOAD_PATTERN_METADATA.items()
        if metadata.complexity == "simple"
    ]


def get_advanced_patterns() -> List[LoadPattern]:
    """
    Get advanced load patterns requiring more configuration.

    Returns:
        List of advanced LoadPattern enums
    """
    return [
        pattern for pattern, metadata in LOAD_PATTERN_METADATA.items()
        if metadata.complexity == "advanced"
    ]
