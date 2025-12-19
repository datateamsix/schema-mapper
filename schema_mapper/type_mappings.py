"""
Type mapping configurations for different database platforms.

This module contains the type conversion mappings from pandas/numpy dtypes
to target database types for each supported platform.
"""

from typing import Dict

# Type mapping configurations
TYPE_MAPPINGS: Dict[str, Dict[str, str]] = {
    'bigquery': {
        'int64': 'INTEGER',
        'int32': 'INTEGER',
        'int16': 'INTEGER',
        'int8': 'INTEGER',
        'uint64': 'INTEGER',
        'uint32': 'INTEGER',
        'uint16': 'INTEGER',
        'uint8': 'INTEGER',
        'Int64': 'INTEGER',
        'Int32': 'INTEGER',
        'Int16': 'INTEGER',
        'Int8': 'INTEGER',
        'float64': 'FLOAT',
        'float32': 'FLOAT',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'object': 'STRING',
        'string': 'STRING',
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64[ns, UTC]': 'TIMESTAMP',
        'timedelta64[ns]': 'STRING',
        'category': 'STRING',
        'complex128': 'STRING',
        'complex64': 'STRING',
    },
    'snowflake': {
        'int64': 'NUMBER(38,0)',
        'int32': 'NUMBER(38,0)',
        'int16': 'NUMBER(38,0)',
        'int8': 'NUMBER(38,0)',
        'uint64': 'NUMBER(38,0)',
        'uint32': 'NUMBER(38,0)',
        'uint16': 'NUMBER(38,0)',
        'uint8': 'NUMBER(38,0)',
        'Int64': 'NUMBER(38,0)',
        'Int32': 'NUMBER(38,0)',
        'Int16': 'NUMBER(38,0)',
        'Int8': 'NUMBER(38,0)',
        'float64': 'FLOAT',
        'float32': 'FLOAT',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'object': 'VARCHAR(16777216)',
        'string': 'VARCHAR(16777216)',
        'datetime64[ns]': 'TIMESTAMP_NTZ',
        'datetime64[ns, UTC]': 'TIMESTAMP_TZ',
        'timedelta64[ns]': 'VARCHAR(100)',
        'category': 'VARCHAR(16777216)',
        'complex128': 'VARCHAR(100)',
        'complex64': 'VARCHAR(100)',
    },
    'redshift': {
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'int16': 'SMALLINT',
        'int8': 'SMALLINT',
        'uint64': 'BIGINT',
        'uint32': 'BIGINT',
        'uint16': 'INTEGER',
        'uint8': 'SMALLINT',
        'Int64': 'BIGINT',
        'Int32': 'INTEGER',
        'Int16': 'SMALLINT',
        'Int8': 'SMALLINT',
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'object': 'VARCHAR(65535)',
        'string': 'VARCHAR(65535)',
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64[ns, UTC]': 'TIMESTAMPTZ',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'category': 'VARCHAR(256)',
        'timedelta64[ns]': 'VARCHAR(100)',
        'complex128': 'VARCHAR(100)',
        'complex64': 'VARCHAR(100)',
    },
    'sqlserver': {
        'int64': 'BIGINT',
        'int32': 'INT',
        'int16': 'SMALLINT',
        'int8': 'TINYINT',
        'uint64': 'BIGINT',
        'uint32': 'BIGINT',
        'uint16': 'INT',
        'uint8': 'SMALLINT',
        'Int64': 'BIGINT',
        'Int32': 'INT',
        'Int16': 'SMALLINT',
        'Int8': 'TINYINT',
        'float64': 'FLOAT',
        'float32': 'REAL',
        'object': 'NVARCHAR(MAX)',
        'string': 'NVARCHAR(MAX)',
        'datetime64[ns]': 'DATETIME2',
        'datetime64[ns, UTC]': 'DATETIMEOFFSET',
        'bool': 'BIT',
        'boolean': 'BIT',
        'category': 'NVARCHAR(255)',
        'timedelta64[ns]': 'NVARCHAR(100)',
        'complex128': 'NVARCHAR(100)',
        'complex64': 'NVARCHAR(100)',
    },
    'postgresql': {
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'int16': 'SMALLINT',
        'int8': 'SMALLINT',
        'uint64': 'BIGINT',
        'uint32': 'BIGINT',
        'uint16': 'INTEGER',
        'uint8': 'SMALLINT',
        'Int64': 'BIGINT',
        'Int32': 'INTEGER',
        'Int16': 'SMALLINT',
        'Int8': 'SMALLINT',
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'object': 'TEXT',
        'string': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64[ns, UTC]': 'TIMESTAMPTZ',
        'bool': 'BOOLEAN',
        'boolean': 'BOOLEAN',
        'category': 'VARCHAR(255)',
        'timedelta64[ns]': 'INTERVAL',
        'complex128': 'TEXT',
        'complex64': 'TEXT',
    },
}

# Supported platforms
SUPPORTED_PLATFORMS = list(TYPE_MAPPINGS.keys())


def get_type_mapping(platform: str) -> Dict[str, str]:
    """
    Get type mapping for a specific platform.
    
    Args:
        platform: Target database platform
        
    Returns:
        Dictionary mapping pandas dtypes to database types
        
    Raises:
        ValueError: If platform is not supported
    """
    platform_lower = platform.lower()
    if platform_lower not in TYPE_MAPPINGS:
        raise ValueError(
            f"Unsupported platform: {platform}. "
            f"Supported platforms: {', '.join(SUPPORTED_PLATFORMS)}"
        )
    return TYPE_MAPPINGS[platform_lower]


def is_platform_supported(platform: str) -> bool:
    """Check if a platform is supported."""
    return platform.lower() in SUPPORTED_PLATFORMS
