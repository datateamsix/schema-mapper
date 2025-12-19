"""
DDL mapping configurations for different database platforms.

This module contains platform-specific DDL capabilities, constraints, and
syntax patterns for clustering, partitioning, distribution, and other
table optimization features.
"""

from typing import Dict, List, Optional, Any, Literal
from dataclasses import dataclass, field
from enum import Enum


class PartitionType(Enum):
    """Types of partitioning supported across platforms."""
    RANGE = "range"
    LIST = "list"
    HASH = "hash"
    TIME = "time"  # BigQuery-specific
    INGESTION_TIME = "ingestion_time"  # BigQuery-specific
    NONE = "none"


class DistributionStyle(Enum):
    """Distribution styles for Redshift."""
    KEY = "key"
    ALL = "all"
    EVEN = "even"
    AUTO = "auto"


@dataclass
class PlatformCapabilities:
    """Defines what features a platform supports."""
    supports_partitioning: bool = False
    supports_clustering: bool = False
    supports_distribution: bool = False
    supports_sort_keys: bool = False
    supports_indexes: bool = False

    # Partition-specific capabilities
    partition_types: List[PartitionType] = field(default_factory=list)
    max_partition_columns: int = 0
    requires_partition_column_type: Optional[List[str]] = None

    # Clustering-specific capabilities
    max_cluster_columns: int = 0
    cluster_order_matters: bool = True

    # Distribution-specific capabilities (Redshift)
    distribution_styles: List[DistributionStyle] = field(default_factory=list)

    # Constraints
    partition_ddl_separate: bool = False  # True if partitions created separately (PostgreSQL)
    supports_partition_expressions: bool = False  # True if can partition on expressions


@dataclass
class ClusteringConfig:
    """Configuration for clustering keys."""
    columns: List[str] = field(default_factory=list)
    # Platform-specific options can be added here


@dataclass
class PartitionConfig:
    """Configuration for partitioning."""
    column: Optional[str] = None
    partition_type: PartitionType = PartitionType.NONE

    # Range partitioning (SQL Server, PostgreSQL)
    range_values: Optional[List[str]] = None  # For SQL Server partition function
    range_start: Optional[str] = None  # For PostgreSQL
    range_end: Optional[str] = None
    range_interval: Optional[str] = None  # e.g., "1 MONTH"

    # BigQuery-specific
    expiration_days: Optional[int] = None
    require_partition_filter: bool = False


@dataclass
class DistributionConfig:
    """Configuration for distribution (Redshift)."""
    style: DistributionStyle = DistributionStyle.AUTO
    key_column: Optional[str] = None


@dataclass
class SortKeyConfig:
    """Configuration for sort keys (Redshift)."""
    columns: List[str] = field(default_factory=list)
    compound: bool = True  # True for COMPOUND (default), False for INTERLEAVED


@dataclass
class DDLOptions:
    """Complete DDL generation options."""
    clustering: Optional[ClusteringConfig] = None
    partitioning: Optional[PartitionConfig] = None
    distribution: Optional[DistributionConfig] = None
    sort_keys: Optional[SortKeyConfig] = None

    # General options
    if_not_exists: bool = False
    transient: bool = False  # Snowflake
    temporary: bool = False


# Platform capabilities configuration
PLATFORM_CAPABILITIES: Dict[str, PlatformCapabilities] = {
    'bigquery': PlatformCapabilities(
        supports_partitioning=True,
        supports_clustering=True,
        supports_distribution=False,
        supports_sort_keys=False,
        supports_indexes=False,
        partition_types=[
            PartitionType.TIME,
            PartitionType.INGESTION_TIME,
            PartitionType.RANGE
        ],
        max_partition_columns=1,
        requires_partition_column_type=['DATE', 'TIMESTAMP', 'DATETIME'],
        max_cluster_columns=4,
        cluster_order_matters=True,
        partition_ddl_separate=False,
        supports_partition_expressions=False,
    ),

    'snowflake': PlatformCapabilities(
        supports_partitioning=False,  # Auto micro-partitioning
        supports_clustering=True,
        supports_distribution=False,
        supports_sort_keys=False,
        supports_indexes=False,
        partition_types=[],
        max_partition_columns=0,
        max_cluster_columns=4,
        cluster_order_matters=True,
        partition_ddl_separate=False,
        supports_partition_expressions=True,
    ),

    'redshift': PlatformCapabilities(
        supports_partitioning=False,
        supports_clustering=False,
        supports_distribution=True,
        supports_sort_keys=True,
        supports_indexes=False,
        partition_types=[],
        max_partition_columns=0,
        max_cluster_columns=0,
        distribution_styles=[
            DistributionStyle.AUTO,
            DistributionStyle.KEY,
            DistributionStyle.ALL,
            DistributionStyle.EVEN
        ],
        partition_ddl_separate=False,
        supports_partition_expressions=False,
    ),

    'sqlserver': PlatformCapabilities(
        supports_partitioning=True,
        supports_clustering=True,  # Via clustered indexes
        supports_distribution=False,
        supports_sort_keys=False,
        supports_indexes=True,
        partition_types=[PartitionType.RANGE, PartitionType.LIST],
        max_partition_columns=1,
        requires_partition_column_type=None,  # Flexible
        max_cluster_columns=16,
        cluster_order_matters=True,
        partition_ddl_separate=True,  # Partition function/scheme created separately
        supports_partition_expressions=False,
    ),

    'postgresql': PlatformCapabilities(
        supports_partitioning=True,
        supports_clustering=False,  # Use indexes instead
        supports_distribution=False,
        supports_sort_keys=False,
        supports_indexes=True,
        partition_types=[
            PartitionType.RANGE,
            PartitionType.LIST,
            PartitionType.HASH
        ],
        max_partition_columns=4,
        requires_partition_column_type=None,
        max_cluster_columns=0,
        partition_ddl_separate=True,  # Child partitions created separately
        supports_partition_expressions=True,
    ),
}


# DDL template patterns for each platform
DDL_TEMPLATES: Dict[str, Dict[str, str]] = {
    'bigquery': {
        'partition_by_date': 'PARTITION BY {column}',
        'partition_by_timestamp': 'PARTITION BY DATE({column})',
        'partition_by_range': 'PARTITION BY RANGE_BUCKET({column}, GENERATE_ARRAY({start}, {end}, {interval}))',
        'partition_by_ingestion': 'PARTITION BY _PARTITIONDATE',
        'cluster_by': 'CLUSTER BY {columns}',
        'partition_expiration': 'OPTIONS(partition_expiration_days={days})',
        'require_partition_filter': 'OPTIONS(require_partition_filter=true)',
    },

    'snowflake': {
        'cluster_by': 'CLUSTER BY ({columns})',
        'cluster_by_expression': 'CLUSTER BY ({expressions})',
        'transient': 'TRANSIENT',
    },

    'redshift': {
        'diststyle_key': 'DISTSTYLE KEY',
        'diststyle_all': 'DISTSTYLE ALL',
        'diststyle_even': 'DISTSTYLE EVEN',
        'diststyle_auto': 'DISTSTYLE AUTO',
        'distkey': 'DISTKEY ({column})',
        'sortkey_compound': 'SORTKEY ({columns})',
        'sortkey_interleaved': 'INTERLEAVED SORTKEY ({columns})',
    },

    'sqlserver': {
        'partition_function': '''CREATE PARTITION FUNCTION {function_name} ({column_type})
AS RANGE {boundary} FOR VALUES ({values})''',
        'partition_scheme': '''CREATE PARTITION SCHEME {scheme_name}
AS PARTITION {function_name}
{filegroup_spec}''',
        'table_on_partition': 'ON {scheme_name}({column})',
        'clustered_index': '''CREATE CLUSTERED INDEX {index_name}
ON {table_name} ({columns})''',
    },

    'postgresql': {
        'partition_by_range': 'PARTITION BY RANGE ({column})',
        'partition_by_list': 'PARTITION BY LIST ({column})',
        'partition_by_hash': 'PARTITION BY HASH ({column})',
        'create_partition_range': '''CREATE TABLE {partition_name}
PARTITION OF {parent_table}
FOR VALUES FROM ({start}) TO ({end})''',
        'create_partition_list': '''CREATE TABLE {partition_name}
PARTITION OF {parent_table}
FOR VALUES IN ({values})''',
        'create_partition_hash': '''CREATE TABLE {partition_name}
PARTITION OF {parent_table}
FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})''',
        'index_for_clustering': 'CREATE INDEX {index_name} ON {table_name} ({columns})',
    },
}


# Validation constraints
VALIDATION_RULES: Dict[str, Dict[str, Any]] = {
    'bigquery': {
        'max_cluster_columns': 4,
        'partition_column_types': ['DATE', 'TIMESTAMP', 'DATETIME', 'INT64'],
        'cluster_column_types': ['all'],  # Can cluster on any type
    },

    'snowflake': {
        'max_cluster_columns': 4,
        'cluster_column_types': ['all'],
    },

    'redshift': {
        'max_sortkey_columns': 400,
        'distkey_limit': 1,  # Only one column for DISTKEY
    },

    'sqlserver': {
        'max_partition_columns': 1,
        'max_clustered_index_columns': 16,
    },

    'postgresql': {
        'max_partition_columns': 32,
        'partition_column_types': ['all'],
    },
}


def get_platform_capabilities(platform: str) -> PlatformCapabilities:
    """
    Get capabilities for a specific platform.

    Args:
        platform: Target database platform

    Returns:
        PlatformCapabilities instance

    Raises:
        ValueError: If platform is not supported
    """
    platform_lower = platform.lower()
    if platform_lower not in PLATFORM_CAPABILITIES:
        raise ValueError(
            f"Unsupported platform: {platform}. "
            f"Supported platforms: {', '.join(PLATFORM_CAPABILITIES.keys())}"
        )
    return PLATFORM_CAPABILITIES[platform_lower]


def get_ddl_templates(platform: str) -> Dict[str, str]:
    """
    Get DDL templates for a specific platform.

    Args:
        platform: Target database platform

    Returns:
        Dictionary of template name to template string

    Raises:
        ValueError: If platform is not supported
    """
    platform_lower = platform.lower()
    if platform_lower not in DDL_TEMPLATES:
        raise ValueError(
            f"No DDL templates for platform: {platform}. "
            f"Supported platforms: {', '.join(DDL_TEMPLATES.keys())}"
        )
    return DDL_TEMPLATES[platform_lower]


def validate_ddl_options(platform: str, options: DDLOptions) -> List[str]:
    """
    Validate DDL options against platform capabilities.

    Args:
        platform: Target database platform
        options: DDL options to validate

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    capabilities = get_platform_capabilities(platform)
    rules = VALIDATION_RULES.get(platform.lower(), {})

    # Validate clustering
    if options.clustering and options.clustering.columns:
        if not capabilities.supports_clustering:
            errors.append(f"{platform} does not support clustering")
        elif len(options.clustering.columns) > capabilities.max_cluster_columns:
            errors.append(
                f"{platform} supports max {capabilities.max_cluster_columns} "
                f"cluster columns, got {len(options.clustering.columns)}"
            )

    # Validate partitioning
    if options.partitioning and options.partitioning.partition_type != PartitionType.NONE:
        if not capabilities.supports_partitioning:
            errors.append(f"{platform} does not support partitioning")
        elif options.partitioning.partition_type not in capabilities.partition_types:
            errors.append(
                f"{platform} does not support {options.partitioning.partition_type.value} partitioning. "
                f"Supported types: {[pt.value for pt in capabilities.partition_types]}"
            )

    # Validate distribution (Redshift)
    if options.distribution:
        if not capabilities.supports_distribution:
            errors.append(f"{platform} does not support distribution configuration")
        elif options.distribution.style not in capabilities.distribution_styles:
            errors.append(
                f"{platform} does not support {options.distribution.style.value} distribution style"
            )
        elif options.distribution.style == DistributionStyle.KEY and not options.distribution.key_column:
            errors.append("DISTKEY column must be specified for DISTSTYLE KEY")

    # Validate sort keys (Redshift)
    if options.sort_keys and options.sort_keys.columns:
        if not capabilities.supports_sort_keys:
            errors.append(f"{platform} does not support sort keys")
        elif 'max_sortkey_columns' in rules:
            if len(options.sort_keys.columns) > rules['max_sortkey_columns']:
                errors.append(
                    f"{platform} supports max {rules['max_sortkey_columns']} "
                    f"sort key columns, got {len(options.sort_keys.columns)}"
                )

    return errors


def is_platform_supported(platform: str) -> bool:
    """Check if a platform is supported."""
    return platform.lower() in PLATFORM_CAPABILITIES


# Export all public classes and functions
__all__ = [
    'PartitionType',
    'DistributionStyle',
    'PlatformCapabilities',
    'ClusteringConfig',
    'PartitionConfig',
    'DistributionConfig',
    'SortKeyConfig',
    'DDLOptions',
    'PLATFORM_CAPABILITIES',
    'DDL_TEMPLATES',
    'VALIDATION_RULES',
    'get_platform_capabilities',
    'get_ddl_templates',
    'validate_ddl_options',
    'is_platform_supported',
]
