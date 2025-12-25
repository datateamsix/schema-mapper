"""
Custom exceptions for database connections.

This module defines a hierarchy of exceptions for handling connection-related errors
across all supported database platforms.
"""


class ConnectionError(Exception):
    """
    Base exception for all connection-related errors.

    This is the parent class for all connection-specific exceptions in schema-mapper.
    """

    def __init__(self, message: str, platform: str = None, details: dict = None):
        """
        Initialize connection error.

        Args:
            message: Human-readable error message
            platform: Platform name (e.g., 'bigquery', 'snowflake')
            details: Additional error context
        """
        self.platform = platform
        self.details = details or {}

        # Build comprehensive error message
        full_message = message
        if platform:
            full_message = f"[{platform}] {message}"

        super().__init__(full_message)


class ConfigurationError(ConnectionError):
    """
    Invalid or missing configuration.

    Raised when:
    - Required configuration parameters are missing
    - Configuration values are invalid
    - Configuration file cannot be parsed
    """
    pass


class AuthenticationError(ConnectionError):
    """
    Authentication failed.

    Raised when:
    - Invalid credentials (username/password)
    - Service account key not found or invalid
    - Token expired or invalid
    - Insufficient permissions
    """
    pass


class NetworkError(ConnectionError):
    """
    Network connectivity issue.

    Raised when:
    - Cannot reach database server
    - Connection timeout
    - DNS resolution failure
    - Firewall blocking connection
    """
    pass


class TableNotFoundError(ConnectionError):
    """
    Requested table does not exist.

    Raised when attempting to:
    - Introspect a non-existent table
    - Query a table that doesn't exist
    - Get schema for missing table
    """

    def __init__(
        self,
        message: str,
        table_name: str = None,
        schema_name: str = None,
        database_name: str = None,
        platform: str = None
    ):
        """
        Initialize table not found error.

        Args:
            message: Error message
            table_name: Name of missing table
            schema_name: Schema/dataset name
            database_name: Database/project name
            platform: Platform name
        """
        self.table_name = table_name
        self.schema_name = schema_name
        self.database_name = database_name

        details = {
            'table': table_name,
            'schema': schema_name,
            'database': database_name
        }

        super().__init__(message, platform=platform, details=details)


class ExecutionError(ConnectionError):
    """
    Query or DDL execution failed.

    Raised when:
    - DDL statement has syntax error
    - Query execution fails
    - Transaction fails to commit
    - Constraint violation during execution
    """

    def __init__(
        self,
        message: str,
        query: str = None,
        platform: str = None,
        original_error: Exception = None
    ):
        """
        Initialize execution error.

        Args:
            message: Error message
            query: SQL query that failed
            platform: Platform name
            original_error: Original exception from database driver
        """
        self.query = query
        self.original_error = original_error

        details = {
            'query': query[:500] if query else None,  # Truncate long queries
            'original_error': str(original_error) if original_error else None
        }

        super().__init__(message, platform=platform, details=details)


class TransactionError(ConnectionError):
    """
    Transaction operation failed.

    Raised when:
    - Cannot begin transaction
    - Commit fails
    - Rollback fails
    - Deadlock detected
    - Transaction timeout
    """
    pass


class PoolExhaustedError(ConnectionError):
    """
    Connection pool has no available connections.

    Raised when:
    - All pool connections are in use
    - Cannot create new connection (max pool size reached)
    - Timeout waiting for available connection
    """

    def __init__(
        self,
        message: str,
        pool_size: int = None,
        active_connections: int = None,
        platform: str = None
    ):
        """
        Initialize pool exhausted error.

        Args:
            message: Error message
            pool_size: Maximum pool size
            active_connections: Number of active connections
            platform: Platform name
        """
        details = {
            'pool_size': pool_size,
            'active_connections': active_connections
        }

        super().__init__(message, platform=platform, details=details)


class IntrospectionError(ConnectionError):
    """
    Table introspection failed.

    Raised when:
    - Cannot read table schema from database
    - Type mapping fails
    - Metadata query fails
    """
    pass


class ValidationError(ConnectionError):
    """
    Validation failed.

    Raised when:
    - Credential validation fails
    - Configuration validation fails
    - Parameter validation fails
    """
    pass


class RetryExhaustedError(ConnectionError):
    """
    All retry attempts failed.

    Raised when:
    - Maximum retries exceeded
    - Transient errors persist after retries
    - Backoff timeout exceeded
    """

    def __init__(
        self,
        message: str,
        attempts: int = None,
        last_error: Exception = None,
        platform: str = None
    ):
        """
        Initialize retry exhausted error.

        Args:
            message: Error message
            attempts: Number of retry attempts made
            last_error: Last exception encountered
            platform: Platform name
        """
        self.attempts = attempts
        self.last_error = last_error

        details = {
            'attempts': attempts,
            'last_error': str(last_error) if last_error else None
        }

        super().__init__(message, platform=platform, details=details)


# Platform-specific error detection helpers
def is_transient_error(error: Exception, platform: str) -> bool:
    """
    Determine if an error is transient and retry-able.

    Transient errors are temporary failures that may succeed on retry:
    - Network timeouts
    - Connection resets
    - Rate limiting
    - Temporary service unavailability

    Args:
        error: Exception to check
        platform: Platform name

    Returns:
        True if error is transient and should be retried
    """
    error_str = str(error).lower()
    error_type = type(error).__name__

    # Common transient error patterns across all platforms
    common_transient_patterns = [
        'timeout',
        'timed out',
        'connection reset',
        'connection refused',
        'connection aborted',
        'connection closed',
        'broken pipe',
        'network',
        'temporarily unavailable',
        'too many connections',
        'rate limit',
        'throttle',
        'service unavailable',
        'try again',
    ]

    # Check common patterns
    if any(pattern in error_str for pattern in common_transient_patterns):
        return True

    # Platform-specific detection
    if platform == 'bigquery':
        # BigQuery-specific transient errors
        if 'rateLimitExceeded' in error_str:
            return True
        if 'backendError' in error_str:
            return True
        if '503' in error_str:  # Service unavailable
            return True

    elif platform == 'snowflake':
        # Snowflake-specific transient errors
        if '390144' in error_str:  # Session no longer exists
            return True
        if '390201' in error_str:  # Connection timeout
            return True
        if '604' in error_str:  # Query timeout
            return True

    elif platform in ('postgresql', 'redshift'):
        # PostgreSQL/Redshift-specific
        if 'deadlock detected' in error_str:
            return True
        if 'could not serialize' in error_str:
            return True
        if 'connection not open' in error_str:
            return True

    elif platform == 'sqlserver':
        # SQL Server-specific
        if '40613' in error_str:  # Database unavailable
            return True
        if '40501' in error_str:  # Service busy
            return True
        if 'deadlock' in error_str:
            return True

    return False


def format_error_context(error: Exception, context: dict = None) -> str:
    """
    Format error with contextual information for better debugging.

    Args:
        error: Exception to format
        context: Additional context (query, table, etc.)

    Returns:
        Formatted error message with context
    """
    lines = [str(error)]

    if context:
        lines.append("\nContext:")
        for key, value in context.items():
            if value is not None:
                # Truncate long values
                if isinstance(value, str) and len(value) > 200:
                    value = value[:200] + '...'
                lines.append(f"  {key}: {value}")

    return '\n'.join(lines)
