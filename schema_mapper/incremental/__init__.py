"""
Incremental load DDL generation module.

This module provides production-ready incremental load patterns across all
supported platforms (BigQuery, Snowflake, Redshift, SQL Server, PostgreSQL).

Key Components:
    - LoadPattern: Enum of supported incremental load patterns
    - IncrementalConfig: Configuration for incremental loads
    - IncrementalDDLGenerator: Base class for platform-specific generators
    - get_incremental_generator: Factory function to get platform generator

Example:
    >>> from schema_mapper.incremental import (
    ...     LoadPattern, IncrementalConfig, get_incremental_generator
    ... )
    >>>
    >>> # Create config for UPSERT pattern
    >>> config = IncrementalConfig(
    ...     load_pattern=LoadPattern.UPSERT,
    ...     primary_keys=['user_id']
    ... )
    >>>
    >>> # Get platform-specific generator
    >>> generator = get_incremental_generator('bigquery')
    >>>
    >>> # Generate DDL
    >>> ddl = generator.generate_incremental_ddl(
    ...     schema=schema,
    ...     table_name='users',
    ...     config=config
    ... )
"""

from .patterns import (
    LoadPattern,
    MergeStrategy,
    DeleteStrategy,
    IncrementalConfig,
    LoadPatternMetadata,
    get_pattern_metadata,
    list_patterns_for_use_case,
    get_all_patterns,
    get_simple_patterns,
    get_advanced_patterns,
)

from .incremental_base import (
    IncrementalDDLGenerator,
    PlatformCapabilities,
)

from .key_detection import (
    KeyCandidate,
    PrimaryKeyDetector,
    detect_primary_keys,
    suggest_primary_keys,
    validate_primary_keys,
    analyze_key_columns,
    get_composite_key_suggestions,
)

__all__ = [
    # Enums
    'LoadPattern',
    'MergeStrategy',
    'DeleteStrategy',

    # Config
    'IncrementalConfig',
    'LoadPatternMetadata',

    # Base classes
    'IncrementalDDLGenerator',
    'PlatformCapabilities',

    # Key Detection
    'KeyCandidate',
    'PrimaryKeyDetector',

    # Functions
    'get_incremental_generator',
    'get_pattern_metadata',
    'list_patterns_for_use_case',
    'get_all_patterns',
    'get_simple_patterns',
    'get_advanced_patterns',
    'detect_primary_keys',
    'suggest_primary_keys',
    'validate_primary_keys',
    'analyze_key_columns',
    'get_composite_key_suggestions',
]


def get_incremental_generator(platform: str) -> IncrementalDDLGenerator:
    """
    Factory function to get platform-specific incremental DDL generator.

    Args:
        platform: Platform name ('bigquery', 'snowflake', 'redshift',
                 'sqlserver', 'postgresql')

    Returns:
        Platform-specific IncrementalDDLGenerator instance

    Raises:
        ValueError: If platform is not supported

    Example:
        >>> generator = get_incremental_generator('bigquery')
        >>> print(generator.platform)
        'bigquery'
    """
    platform = platform.lower()

    # Import platform-specific generators
    # NOTE: These will be implemented in PROMPT 2-6
    # For now, we provide the factory structure

    if platform == 'bigquery':
        from .platform_generators.bigquery import BigQueryIncrementalGenerator
        return BigQueryIncrementalGenerator()

    elif platform == 'snowflake':
        from .platform_generators.snowflake import SnowflakeIncrementalGenerator
        return SnowflakeIncrementalGenerator()

    elif platform == 'redshift':
        from .platform_generators.redshift import RedshiftIncrementalGenerator
        return RedshiftIncrementalGenerator()

    elif platform == 'sqlserver':
        from .platform_generators.sqlserver import SQLServerIncrementalGenerator
        return SQLServerIncrementalGenerator()

    elif platform == 'postgresql':
        from .platform_generators.postgresql import PostgreSQLIncrementalGenerator
        return PostgreSQLIncrementalGenerator()

    else:
        raise ValueError(
            f"Unsupported platform: {platform}. "
            f"Supported platforms: bigquery, snowflake, redshift, sqlserver, postgresql"
        )
