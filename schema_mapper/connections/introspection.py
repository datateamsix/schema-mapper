"""
SQL-based introspection utilities for databases that support INFORMATION_SCHEMA.

Provides reusable SQL queries and parsing functions for introspecting
database schemas across Snowflake, PostgreSQL, Redshift, and SQL Server.
"""

from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


# ==================== INFORMATION_SCHEMA QUERIES ====================

def build_columns_query(
    table_name: str,
    schema_name: str,
    database_name: Optional[str] = None,
    platform: str = 'snowflake'
) -> str:
    """
    Build query to retrieve column information from INFORMATION_SCHEMA.

    Args:
        table_name: Table name
        schema_name: Schema/dataset name
        database_name: Database name (optional, platform-specific)
        platform: Platform name ('snowflake', 'postgresql', 'redshift', 'sqlserver')

    Returns:
        SQL query string

    Examples:
        >>> query = build_columns_query('users', 'public', platform='postgresql')
        >>> # Executes: SELECT column_name, data_type, ... FROM information_schema.columns ...
    """
    # Base query for most platforms
    base_query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            ordinal_position
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
          AND table_schema = '{schema_name}'
    """

    # Platform-specific adjustments
    if platform == 'snowflake':
        # Snowflake is case-sensitive, upper case by default
        query = base_query.format(
            table_name=table_name.upper(),
            schema_name=schema_name.upper()
        )
        if database_name:
            query += f" AND table_catalog = '{database_name.upper()}'"
        query += " ORDER BY ordinal_position"

    elif platform == 'postgresql':
        # PostgreSQL is case-sensitive, lower case by default
        query = base_query.format(
            table_name=table_name.lower(),
            schema_name=schema_name.lower()
        )
        query += " ORDER BY ordinal_position"

    elif platform == 'redshift':
        # Redshift (PostgreSQL-based)
        query = base_query.format(
            table_name=table_name.lower(),
            schema_name=schema_name.lower()
        )
        query += " ORDER BY ordinal_position"

    elif platform == 'sqlserver':
        # SQL Server
        query = base_query.format(
            table_name=table_name,
            schema_name=schema_name
        )
        if database_name:
            query = query.replace(
                "FROM information_schema.columns",
                f"FROM {database_name}.information_schema.columns"
            )
        query += " ORDER BY ordinal_position"

    else:
        raise ValueError(f"Unknown platform: {platform}")

    return query


def build_table_exists_query(
    table_name: str,
    schema_name: str,
    database_name: Optional[str] = None,
    platform: str = 'snowflake'
) -> str:
    """
    Build query to check if table exists.

    Args:
        table_name: Table name
        schema_name: Schema name
        database_name: Database name (optional)
        platform: Platform name

    Returns:
        SQL query that returns 1 if table exists, 0 otherwise

    Examples:
        >>> query = build_table_exists_query('users', 'public', platform='snowflake')
        >>> # Returns: SELECT COUNT(*) FROM information_schema.tables WHERE ...
    """
    base_query = """
        SELECT COUNT(*) as table_count
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
          AND table_schema = '{schema_name}'
    """

    if platform == 'snowflake':
        query = base_query.format(
            table_name=table_name.upper(),
            schema_name=schema_name.upper()
        )
        if database_name:
            query += f" AND table_catalog = '{database_name.upper()}'"

    elif platform in ('postgresql', 'redshift'):
        query = base_query.format(
            table_name=table_name.lower(),
            schema_name=schema_name.lower()
        )

    elif platform == 'sqlserver':
        query = base_query.format(
            table_name=table_name,
            schema_name=schema_name
        )
        if database_name:
            query = query.replace(
                "FROM information_schema.tables",
                f"FROM {database_name}.information_schema.tables"
            )

    else:
        raise ValueError(f"Unknown platform: {platform}")

    return query


def build_list_tables_query(
    schema_name: str,
    database_name: Optional[str] = None,
    platform: str = 'snowflake'
) -> str:
    """
    Build query to list tables in schema.

    Args:
        schema_name: Schema name
        database_name: Database name (optional)
        platform: Platform name

    Returns:
        SQL query to list table names

    Examples:
        >>> query = build_list_tables_query('public', platform='postgresql')
    """
    base_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """

    if platform == 'snowflake':
        query = base_query.format(schema_name=schema_name.upper())
        if database_name:
            query = query.replace(
                "WHERE table_schema",
                f"WHERE table_catalog = '{database_name.upper()}' AND table_schema"
            )

    elif platform in ('postgresql', 'redshift'):
        query = base_query.format(schema_name=schema_name.lower())

    elif platform == 'sqlserver':
        query = base_query.format(schema_name=schema_name)
        if database_name:
            query = query.replace(
                "FROM information_schema.tables",
                f"FROM {database_name}.information_schema.tables"
            )

    else:
        raise ValueError(f"Unknown platform: {platform}")

    return query


def build_table_comment_query(
    table_name: str,
    schema_name: str,
    database_name: Optional[str] = None,
    platform: str = 'snowflake'
) -> Optional[str]:
    """
    Build query to get table description/comment.

    Args:
        table_name: Table name
        schema_name: Schema name
        database_name: Database name (optional)
        platform: Platform name

    Returns:
        SQL query or None if platform doesn't support table comments via SQL

    Examples:
        >>> query = build_table_comment_query('users', 'public', platform='postgresql')
    """
    if platform == 'snowflake':
        # Snowflake: Use SHOW TABLES or information_schema
        return f"""
            SHOW TABLES LIKE '{table_name.upper()}'
            IN SCHEMA {schema_name.upper()}
        """

    elif platform == 'postgresql':
        # PostgreSQL: Use pg_catalog
        return f"""
            SELECT obj_description(
                (SELECT oid FROM pg_class WHERE relname = '{table_name.lower()}'),
                'pg_class'
            ) AS table_comment
        """

    elif platform == 'sqlserver':
        # SQL Server: Extended properties
        return f"""
            SELECT value AS table_comment
            FROM sys.extended_properties
            WHERE major_id = OBJECT_ID('{schema_name}.{table_name}')
              AND minor_id = 0
              AND name = 'MS_Description'
        """

    else:
        return None


# ==================== RESULT PARSING ====================

def parse_column_row(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    """
    Parse column information from INFORMATION_SCHEMA row.

    Args:
        row: Row from INFORMATION_SCHEMA.COLUMNS query
        platform: Platform name

    Returns:
        Dictionary with standardized column information

    Examples:
        >>> row = {
        ...     'column_name': 'USER_ID',
        ...     'data_type': 'NUMBER',
        ...     'is_nullable': 'YES',
        ...     'numeric_precision': 38,
        ...     'numeric_scale': 0
        ... }
        >>> parsed = parse_column_row(row, 'snowflake')
        >>> print(parsed['name'])  # 'user_id'
    """
    # Normalize column name (preserve case sensitivity per platform)
    column_name = row.get('column_name') or row.get('COLUMN_NAME')
    if platform == 'snowflake':
        # Snowflake returns uppercase, convert to lowercase for canonical
        column_name = column_name.lower() if column_name else None
    elif platform in ('postgresql', 'redshift'):
        # Already lowercase
        pass
    elif platform == 'sqlserver':
        # Preserve case
        pass

    # Data type
    data_type = row.get('data_type') or row.get('DATA_TYPE')

    # Nullability
    is_nullable_str = row.get('is_nullable') or row.get('IS_NULLABLE')
    nullable = (is_nullable_str.upper() == 'YES') if is_nullable_str else True

    # Numeric precision and scale
    precision = row.get('numeric_precision') or row.get('NUMERIC_PRECISION')
    scale = row.get('numeric_scale') or row.get('NUMERIC_SCALE')

    # Character maximum length
    max_length = row.get('character_maximum_length') or row.get('CHARACTER_MAXIMUM_LENGTH')

    # Default value
    column_default = row.get('column_default') or row.get('COLUMN_DEFAULT')

    # Ordinal position
    ordinal = row.get('ordinal_position') or row.get('ORDINAL_POSITION')

    return {
        'name': column_name,
        'data_type': data_type,
        'nullable': nullable,
        'precision': precision,
        'scale': scale,
        'max_length': max_length,
        'default': column_default,
        'ordinal_position': ordinal,
    }


def parse_table_exists_result(result) -> bool:
    """
    Parse result from table exists query.

    Args:
        result: Query result (cursor or list of rows)

    Returns:
        True if table exists

    Examples:
        >>> result = [{'table_count': 1}]
        >>> parse_table_exists_result(result)
        True
    """
    try:
        # Handle different result formats
        if hasattr(result, 'fetchone'):
            # Cursor object
            row = result.fetchone()
            if row:
                count = row[0] if isinstance(row, (list, tuple)) else row.get('table_count', 0)
                return count > 0
        elif isinstance(result, (list, tuple)) and len(result) > 0:
            # List of rows
            row = result[0]
            if isinstance(row, dict):
                count = row.get('table_count', 0) or row.get('TABLE_COUNT', 0)
            else:
                count = row[0] if row else 0
            return count > 0
    except Exception as e:
        logger.error(f"Error parsing table exists result: {e}")

    return False


def parse_list_tables_result(result) -> List[str]:
    """
    Parse result from list tables query.

    Args:
        result: Query result

    Returns:
        List of table names

    Examples:
        >>> result = [{'table_name': 'users'}, {'table_name': 'orders'}]
        >>> parse_list_tables_result(result)
        ['users', 'orders']
    """
    table_names = []

    try:
        # Handle cursor
        if hasattr(result, 'fetchall'):
            rows = result.fetchall()
        else:
            rows = result

        for row in rows:
            if isinstance(row, dict):
                table_name = row.get('table_name') or row.get('TABLE_NAME')
            elif isinstance(row, (list, tuple)):
                table_name = row[0]
            else:
                table_name = str(row)

            if table_name:
                # Normalize case for Snowflake
                table_names.append(table_name.lower())

    except Exception as e:
        logger.error(f"Error parsing list tables result: {e}")

    return table_names


# ==================== SNOWFLAKE-SPECIFIC HELPERS ====================

def build_snowflake_clustering_query(
    table_name: str,
    schema_name: str,
    database_name: Optional[str] = None
) -> str:
    """
    Build query to get Snowflake clustering keys.

    Args:
        table_name: Table name
        schema_name: Schema name
        database_name: Database name (optional)

    Returns:
        SQL query to get clustering information

    Examples:
        >>> query = build_snowflake_clustering_query('events', 'analytics')
    """
    table_name = table_name.upper()
    schema_name = schema_name.upper()

    if database_name:
        database_name = database_name.upper()
        return f"""
            SHOW TABLES LIKE '{table_name}' IN SCHEMA {database_name}.{schema_name}
        """
    else:
        return f"""
            SHOW TABLES LIKE '{table_name}' IN SCHEMA {schema_name}
        """


def parse_snowflake_clustering_result(result) -> Optional[List[str]]:
    """
    Parse clustering keys from Snowflake SHOW TABLES result.

    Args:
        result: Result from SHOW TABLES query

    Returns:
        List of clustering column names or None

    Examples:
        >>> result = [{'clustering_key': '(USER_ID, EVENT_DATE)'}]
        >>> parse_snowflake_clustering_result(result)
        ['user_id', 'event_date']
    """
    try:
        if hasattr(result, 'fetchall'):
            rows = result.fetchall()
        else:
            rows = result

        if rows and len(rows) > 0:
            row = rows[0]
            if isinstance(row, dict):
                clustering_key = row.get('clustering_key') or row.get('CLUSTERING_KEY')
            else:
                # SHOW TABLES returns many columns, clustering_key might be at index
                # This is platform-specific and may need adjustment
                clustering_key = None

            if clustering_key and clustering_key != '':
                # Parse: "LINEAR(COLUMN1, COLUMN2)" or "(COLUMN1, COLUMN2)"
                import re
                match = re.search(r'\((.*?)\)', clustering_key)
                if match:
                    columns_str = match.group(1)
                    columns = [col.strip().lower() for col in columns_str.split(',')]
                    return columns

    except Exception as e:
        logger.error(f"Error parsing Snowflake clustering result: {e}")

    return None


# ==================== UTILITY FUNCTIONS ====================

def normalize_identifier(identifier: str, platform: str) -> str:
    """
    Normalize identifier (table/column name) for platform.

    Args:
        identifier: Table or column name
        platform: Platform name

    Returns:
        Normalized identifier

    Examples:
        >>> normalize_identifier('UserTable', 'snowflake')
        'USERTABLE'

        >>> normalize_identifier('UserTable', 'postgresql')
        'usertable'
    """
    if platform == 'snowflake':
        return identifier.upper()
    elif platform in ('postgresql', 'redshift'):
        return identifier.lower()
    elif platform == 'sqlserver':
        return identifier  # Preserve case
    else:
        return identifier
