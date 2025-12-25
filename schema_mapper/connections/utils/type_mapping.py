"""
Type mapping utilities for converting database-specific types to canonical LogicalTypes.

Provides bidirectional mapping between platform-specific types and canonical LogicalTypes,
enabling schema introspection and cross-platform type conversion.
"""

from typing import Dict, Optional, Tuple
import logging

from ...canonical import LogicalType

logger = logging.getLogger(__name__)


# ==================== BIGQUERY TYPE MAPPING ====================

BIGQUERY_TO_LOGICAL: Dict[str, LogicalType] = {
    # Numeric types
    'INT64': LogicalType.BIGINT,
    'INTEGER': LogicalType.INTEGER,
    'SMALLINT': LogicalType.INTEGER,
    'TINYINT': LogicalType.INTEGER,
    'BYTEINT': LogicalType.INTEGER,
    'BIGINT': LogicalType.BIGINT,
    'FLOAT64': LogicalType.FLOAT,
    'FLOAT': LogicalType.FLOAT,
    'NUMERIC': LogicalType.DECIMAL,
    'DECIMAL': LogicalType.DECIMAL,
    'BIGNUMERIC': LogicalType.DECIMAL,
    'BIGDECIMAL': LogicalType.DECIMAL,

    # String types
    'STRING': LogicalType.STRING,
    'VARCHAR': LogicalType.STRING,
    'TEXT': LogicalType.TEXT,

    # Binary types
    'BYTES': LogicalType.BINARY,

    # Boolean
    'BOOL': LogicalType.BOOLEAN,
    'BOOLEAN': LogicalType.BOOLEAN,

    # Date/Time types
    'DATE': LogicalType.DATE,
    'DATETIME': LogicalType.TIMESTAMP,
    'TIMESTAMP': LogicalType.TIMESTAMP,
    'TIME': LogicalType.STRING,  # BigQuery TIME as string in canonical

    # Structured types
    'JSON': LogicalType.JSON,
    'STRUCT': LogicalType.JSON,  # Map STRUCT to JSON
    'ARRAY': LogicalType.JSON,   # Map ARRAY to JSON
    'RECORD': LogicalType.JSON,
}


# ==================== SNOWFLAKE TYPE MAPPING ====================

SNOWFLAKE_TO_LOGICAL: Dict[str, LogicalType] = {
    # Numeric types
    'NUMBER': LogicalType.DECIMAL,  # Default NUMBER mapping
    'DECIMAL': LogicalType.DECIMAL,
    'NUMERIC': LogicalType.DECIMAL,
    'INT': LogicalType.INTEGER,
    'INTEGER': LogicalType.INTEGER,
    'BIGINT': LogicalType.BIGINT,
    'SMALLINT': LogicalType.INTEGER,
    'TINYINT': LogicalType.INTEGER,
    'BYTEINT': LogicalType.INTEGER,
    'FLOAT': LogicalType.FLOAT,
    'FLOAT4': LogicalType.FLOAT,
    'FLOAT8': LogicalType.FLOAT,
    'DOUBLE': LogicalType.FLOAT,
    'DOUBLE PRECISION': LogicalType.FLOAT,
    'REAL': LogicalType.FLOAT,

    # String types
    'VARCHAR': LogicalType.STRING,
    'CHAR': LogicalType.STRING,
    'CHARACTER': LogicalType.STRING,
    'STRING': LogicalType.TEXT,
    'TEXT': LogicalType.TEXT,

    # Binary types
    'BINARY': LogicalType.BINARY,
    'VARBINARY': LogicalType.BINARY,

    # Boolean
    'BOOLEAN': LogicalType.BOOLEAN,

    # Date/Time types
    'DATE': LogicalType.DATE,
    'DATETIME': LogicalType.TIMESTAMP,
    'TIME': LogicalType.STRING,
    'TIMESTAMP': LogicalType.TIMESTAMP,
    'TIMESTAMP_NTZ': LogicalType.TIMESTAMP,
    'TIMESTAMP_LTZ': LogicalType.TIMESTAMPTZ,
    'TIMESTAMP_TZ': LogicalType.TIMESTAMPTZ,

    # Structured types
    'VARIANT': LogicalType.JSON,
    'OBJECT': LogicalType.JSON,
    'ARRAY': LogicalType.JSON,
}


# ==================== POSTGRESQL TYPE MAPPING ====================

POSTGRESQL_TO_LOGICAL: Dict[str, LogicalType] = {
    # Numeric types
    'SMALLINT': LogicalType.INTEGER,
    'INT2': LogicalType.INTEGER,
    'INTEGER': LogicalType.INTEGER,
    'INT': LogicalType.INTEGER,
    'INT4': LogicalType.INTEGER,
    'BIGINT': LogicalType.BIGINT,
    'INT8': LogicalType.BIGINT,
    'DECIMAL': LogicalType.DECIMAL,
    'NUMERIC': LogicalType.DECIMAL,
    'REAL': LogicalType.FLOAT,
    'FLOAT4': LogicalType.FLOAT,
    'DOUBLE PRECISION': LogicalType.FLOAT,
    'FLOAT8': LogicalType.FLOAT,
    'FLOAT': LogicalType.FLOAT,

    # String types
    'VARCHAR': LogicalType.STRING,
    'CHARACTER VARYING': LogicalType.STRING,
    'CHAR': LogicalType.STRING,
    'CHARACTER': LogicalType.STRING,
    'TEXT': LogicalType.TEXT,

    # Binary types
    'BYTEA': LogicalType.BINARY,

    # Boolean
    'BOOLEAN': LogicalType.BOOLEAN,
    'BOOL': LogicalType.BOOLEAN,

    # Date/Time types
    'DATE': LogicalType.DATE,
    'TIMESTAMP': LogicalType.TIMESTAMP,
    'TIMESTAMP WITHOUT TIME ZONE': LogicalType.TIMESTAMP,
    'TIMESTAMP WITH TIME ZONE': LogicalType.TIMESTAMPTZ,
    'TIMESTAMPTZ': LogicalType.TIMESTAMPTZ,
    'TIME': LogicalType.STRING,
    'TIME WITHOUT TIME ZONE': LogicalType.STRING,
    'TIME WITH TIME ZONE': LogicalType.STRING,
    'TIMETZ': LogicalType.STRING,

    # Structured types
    'JSON': LogicalType.JSON,
    'JSONB': LogicalType.JSON,
    'ARRAY': LogicalType.JSON,

    # UUID
    'UUID': LogicalType.STRING,
}


# ==================== REDSHIFT TYPE MAPPING ====================

REDSHIFT_TO_LOGICAL: Dict[str, LogicalType] = {
    # Numeric types
    'SMALLINT': LogicalType.INTEGER,
    'INT2': LogicalType.INTEGER,
    'INTEGER': LogicalType.INTEGER,
    'INT': LogicalType.INTEGER,
    'INT4': LogicalType.INTEGER,
    'BIGINT': LogicalType.BIGINT,
    'INT8': LogicalType.BIGINT,
    'DECIMAL': LogicalType.DECIMAL,
    'NUMERIC': LogicalType.DECIMAL,
    'REAL': LogicalType.FLOAT,
    'FLOAT4': LogicalType.FLOAT,
    'DOUBLE PRECISION': LogicalType.FLOAT,
    'FLOAT8': LogicalType.FLOAT,
    'FLOAT': LogicalType.FLOAT,

    # String types
    'VARCHAR': LogicalType.STRING,
    'CHARACTER VARYING': LogicalType.STRING,
    'CHAR': LogicalType.STRING,
    'CHARACTER': LogicalType.STRING,
    'TEXT': LogicalType.TEXT,

    # Binary types
    'VARBYTE': LogicalType.BINARY,

    # Boolean
    'BOOLEAN': LogicalType.BOOLEAN,
    'BOOL': LogicalType.BOOLEAN,

    # Date/Time types
    'DATE': LogicalType.DATE,
    'TIMESTAMP': LogicalType.TIMESTAMP,
    'TIMESTAMP WITHOUT TIME ZONE': LogicalType.TIMESTAMP,
    'TIMESTAMP WITH TIME ZONE': LogicalType.TIMESTAMPTZ,
    'TIMESTAMPTZ': LogicalType.TIMESTAMPTZ,
    'TIME': LogicalType.STRING,
    'TIMETZ': LogicalType.STRING,

    # Structured types
    'SUPER': LogicalType.JSON,  # Redshift SUPER type
}


# ==================== SQL SERVER TYPE MAPPING ====================

SQLSERVER_TO_LOGICAL: Dict[str, LogicalType] = {
    # Numeric types
    'TINYINT': LogicalType.INTEGER,
    'SMALLINT': LogicalType.INTEGER,
    'INT': LogicalType.INTEGER,
    'BIGINT': LogicalType.BIGINT,
    'DECIMAL': LogicalType.DECIMAL,
    'NUMERIC': LogicalType.DECIMAL,
    'MONEY': LogicalType.DECIMAL,
    'SMALLMONEY': LogicalType.DECIMAL,
    'FLOAT': LogicalType.FLOAT,
    'REAL': LogicalType.FLOAT,

    # String types
    'CHAR': LogicalType.STRING,
    'VARCHAR': LogicalType.STRING,
    'NCHAR': LogicalType.STRING,
    'NVARCHAR': LogicalType.STRING,
    'TEXT': LogicalType.TEXT,
    'NTEXT': LogicalType.TEXT,

    # Binary types
    'BINARY': LogicalType.BINARY,
    'VARBINARY': LogicalType.BINARY,
    'IMAGE': LogicalType.BINARY,

    # Boolean (SQL Server uses BIT)
    'BIT': LogicalType.BOOLEAN,

    # Date/Time types
    'DATE': LogicalType.DATE,
    'DATETIME': LogicalType.TIMESTAMP,
    'DATETIME2': LogicalType.TIMESTAMP,
    'SMALLDATETIME': LogicalType.TIMESTAMP,
    'DATETIMEOFFSET': LogicalType.TIMESTAMPTZ,
    'TIME': LogicalType.STRING,

    # Structured types (SQL Server 2016+)
    'JSON': LogicalType.JSON,  # Actually stored as NVARCHAR

    # UUID
    'UNIQUEIDENTIFIER': LogicalType.STRING,
}


# ==================== TYPE MAPPING FUNCTIONS ====================

def map_to_logical_type(
    platform_type: str,
    platform: str,
    precision: Optional[int] = None,
    scale: Optional[int] = None
) -> LogicalType:
    """
    Map platform-specific type to canonical LogicalType.

    Args:
        platform_type: Database-specific type (e.g., 'INT64', 'NUMBER', 'VARCHAR')
        platform: Platform name ('bigquery', 'snowflake', etc.)
        precision: Numeric precision (for DECIMAL types)
        scale: Numeric scale (for DECIMAL types)

    Returns:
        Corresponding LogicalType

    Examples:
        >>> map_to_logical_type('INT64', 'bigquery')
        LogicalType.BIGINT

        >>> map_to_logical_type('NUMBER', 'snowflake', precision=38, scale=0)
        LogicalType.BIGINT  # NUMBER(38,0) maps to BIGINT

        >>> map_to_logical_type('VARCHAR', 'postgresql')
        LogicalType.STRING
    """
    # Normalize type string
    platform_type = platform_type.upper().strip()
    platform = platform.lower().strip()

    # Get platform-specific mapping
    type_map = _get_type_map(platform)

    # Handle special cases for Snowflake NUMBER
    if platform == 'snowflake' and platform_type == 'NUMBER':
        return _map_snowflake_number(precision, scale)

    # Direct mapping
    if platform_type in type_map:
        return type_map[platform_type]

    # Handle parameterized types (e.g., VARCHAR(255))
    # Extract base type
    if '(' in platform_type:
        base_type = platform_type.split('(')[0].strip()
        if base_type in type_map:
            return type_map[base_type]

    # Default fallback
    logger.warning(
        f"Unknown {platform} type: {platform_type}. "
        f"Defaulting to STRING. Please add mapping if needed."
    )
    return LogicalType.STRING


def _get_type_map(platform: str) -> Dict[str, LogicalType]:
    """Get type mapping for platform."""
    maps = {
        'bigquery': BIGQUERY_TO_LOGICAL,
        'snowflake': SNOWFLAKE_TO_LOGICAL,
        'postgresql': POSTGRESQL_TO_LOGICAL,
        'redshift': REDSHIFT_TO_LOGICAL,
        'sqlserver': SQLSERVER_TO_LOGICAL,
    }

    if platform not in maps:
        raise ValueError(f"Unknown platform: {platform}")

    return maps[platform]


def _map_snowflake_number(precision: Optional[int], scale: Optional[int]) -> LogicalType:
    """
    Map Snowflake NUMBER to appropriate LogicalType based on precision/scale.

    Snowflake NUMBER rules:
    - NUMBER(38, 0) → BIGINT
    - NUMBER(18, 0) → BIGINT
    - NUMBER(9, 0) → INTEGER
    - NUMBER(p, s) where s > 0 → DECIMAL
    - NUMBER (no precision/scale) → DECIMAL

    Args:
        precision: Numeric precision
        scale: Numeric scale

    Returns:
        Appropriate LogicalType
    """
    if scale is None or scale == 0:
        # Integer type - determine size by precision
        if precision is None:
            # NUMBER with no precision/scale → DECIMAL
            return LogicalType.DECIMAL
        elif precision <= 9:
            return LogicalType.INTEGER
        else:
            return LogicalType.BIGINT
    else:
        # Has decimal places → DECIMAL
        return LogicalType.DECIMAL


def extract_precision_scale(type_string: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract precision and scale from type string.

    Args:
        type_string: Type with precision/scale (e.g., 'DECIMAL(10,2)', 'NUMBER(38,0)')

    Returns:
        Tuple of (precision, scale)

    Examples:
        >>> extract_precision_scale('DECIMAL(10, 2)')
        (10, 2)

        >>> extract_precision_scale('NUMBER(38, 0)')
        (38, 0)

        >>> extract_precision_scale('VARCHAR(255)')
        (255, None)

        >>> extract_precision_scale('INT')
        (None, None)
    """
    import re

    # Pattern for TYPE(precision, scale) or TYPE(precision)
    match = re.match(r'[A-Z]+\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)', type_string, re.IGNORECASE)

    if match:
        precision = int(match.group(1))
        scale = int(match.group(2)) if match.group(2) else None
        return (precision, scale)

    return (None, None)


def infer_max_length(platform_type: str) -> Optional[int]:
    """
    Infer max_length from type string for STRING/VARCHAR types.

    Args:
        platform_type: Type string (e.g., 'VARCHAR(255)', 'STRING')

    Returns:
        Max length or None

    Examples:
        >>> infer_max_length('VARCHAR(255)')
        255

        >>> infer_max_length('STRING')
        None
    """
    precision, _ = extract_precision_scale(platform_type)

    # For VARCHAR/CHAR types, precision is the max length
    if precision is not None:
        base_type = platform_type.split('(')[0].upper().strip()
        if base_type in ('VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR', 'CHARACTER VARYING'):
            return precision

    return None
