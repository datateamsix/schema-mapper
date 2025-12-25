"""
Base connection class for all database platforms.

Defines the abstract interface that all platform-specific connectors must implement.
Provides common functionality for connection lifecycle, transaction management,
and integration with canonical schema.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from datetime import datetime
import logging

from ..canonical import CanonicalSchema
from .exceptions import (
    ConnectionError,
    ConfigurationError,
    TableNotFoundError,
    ExecutionError,
    TransactionError
)


class ConnectionState(Enum):
    """Connection lifecycle states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


class BaseConnection(ABC):
    """
    Abstract base class for all database connections.

    Responsibilities:
    - Connection lifecycle management (connect, disconnect, test)
    - Credential validation
    - Table introspection → CanonicalSchema
    - DDL execution
    - Transaction support (where applicable)
    - Integration with renderers and generators

    Design Goals:
    - Single mental model across platforms
    - Canonical schema as lingua franca
    - Rich error handling with platform-specific context
    - Easy testing via mocks

    All platform-specific connectors (BigQuery, Snowflake, etc.) must inherit
    from this class and implement all abstract methods.

    Examples:
        Basic usage:
        >>> config = {'project': 'my-project'}
        >>> conn = BigQueryConnection(config)
        >>> conn.connect()
        >>> if conn.test_connection():
        ...     schema = conn.get_target_schema('users', schema_name='public')
        >>> conn.disconnect()

        Context manager:
        >>> with BigQueryConnection(config) as conn:
        ...     schema = conn.get_target_schema('users')
        ...     conn.execute_ddl(ddl)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize connection with configuration.

        Args:
            config: Platform-specific configuration dictionary
                   Required keys vary by platform (see platform docs)

        Raises:
            ConfigurationError: If configuration is invalid
            ValidationError: If credentials are invalid
        """
        self.config = config
        self.state = ConnectionState.DISCONNECTED
        self._connection = None
        self._cursor = None
        self._transaction_active = False
        self._savepoint_counter = 0  # For savepoint naming

        # Transaction statistics
        self._transaction_stats = {
            'total_transactions': 0,
            'total_commits': 0,
            'total_rollbacks': 0,
            'total_savepoints': 0,
            'active_transaction_start': None,
        }

        # Set up logging
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Validate configuration on initialization
        try:
            self._validate_config()
        except Exception as e:
            self.state = ConnectionState.ERROR
            raise ConfigurationError(
                f"Configuration validation failed: {e}",
                platform=self.platform_name(),
                details={'config_keys': list(config.keys())}
            )

    # ==================== LIFECYCLE METHODS ====================

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish database connection.

        This method should:
        1. Set state to CONNECTING
        2. Establish connection using platform-specific client
        3. Store connection in self._connection
        4. Set state to CONNECTED on success
        5. Set state to ERROR on failure

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If credentials are invalid
            NetworkError: If cannot reach database

        Examples:
            >>> conn.connect()
            True
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close database connection gracefully.

        This method should:
        1. Rollback any active transaction
        2. Close cursor if open
        3. Close connection
        4. Set state to DISCONNECTED

        Examples:
            >>> conn.disconnect()
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test if connection is alive and working.

        Executes a lightweight query (e.g., SELECT 1) to verify connection.
        Does not raise exceptions on failure.

        Returns:
            True if connection is healthy, False otherwise

        Examples:
            >>> if conn.test_connection():
            ...     print("Connection OK")
        """
        pass

    # ==================== INTROSPECTION METHODS ====================

    @abstractmethod
    def table_exists(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> bool:
        """
        Check if table exists in database.

        Args:
            table_name: Table name
            schema_name: Schema/dataset name (optional)
            database_name: Database/project name (optional)

        Returns:
            True if table exists, False otherwise

        Examples:
            >>> conn.table_exists('users', schema_name='public')
            True
        """
        pass

    @abstractmethod
    def get_target_schema(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> CanonicalSchema:
        """
        Introspect existing table and return CanonicalSchema.

        This is the key method that enables bidirectional schema flow:
        - Read table structure from database
        - Convert platform types to LogicalTypes
        - Extract optimization metadata (clustering, partitioning)
        - Return CanonicalSchema that can be used anywhere

        The returned CanonicalSchema can be:
        - Used with renderers to generate DDL for other platforms
        - Used with generators for incremental loads
        - Validated against DataFrames
        - Stored in version control

        Args:
            table_name: Table name
            schema_name: Schema/dataset name (optional, uses default if not provided)
            database_name: Database/project name (optional, uses default if not provided)

        Returns:
            CanonicalSchema representing the table

        Raises:
            TableNotFoundError: If table doesn't exist
            IntrospectionError: If cannot read table schema
            ConnectionError: If not connected

        Examples:
            >>> schema = conn.get_target_schema('users', schema_name='public')
            >>> print(f"Table has {len(schema.columns)} columns")
            >>> # Use schema with different renderer
            >>> from schema_mapper.renderers import RendererFactory
            >>> renderer = RendererFactory.get_renderer('snowflake', schema)
            >>> print(renderer.to_ddl())  # Convert to Snowflake DDL
        """
        pass

    @abstractmethod
    def list_tables(
        self,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> List[str]:
        """
        List all tables in schema/database.

        Args:
            schema_name: Schema/dataset name (optional, uses default if not provided)
            database_name: Database/project name (optional, uses default if not provided)

        Returns:
            List of table names

        Examples:
            >>> tables = conn.list_tables(schema_name='public')
            >>> print(f"Found {len(tables)} tables")
        """
        pass

    # ==================== EXECUTION METHODS ====================

    @abstractmethod
    def execute_ddl(self, ddl: str) -> bool:
        """
        Execute DDL statement (CREATE, ALTER, DROP).

        Args:
            ddl: DDL statement to execute

        Returns:
            True if successful

        Raises:
            ExecutionError: If DDL execution fails
            ConnectionError: If not connected

        Examples:
            >>> ddl = "CREATE TABLE users (id INT, name VARCHAR(100))"
            >>> conn.execute_ddl(ddl)
            True
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """
        Execute query and return results.

        Args:
            query: SQL query to execute

        Returns:
            Query results in platform-specific format
            (e.g., BigQuery QueryJob.result(), Snowflake cursor)

        Raises:
            ExecutionError: If query execution fails
            ConnectionError: If not connected

        Examples:
            >>> results = conn.execute_query("SELECT COUNT(*) FROM users")
        """
        pass

    # ==================== CONVENIENCE METHODS ====================

    def create_table_from_schema(
        self,
        canonical_schema: CanonicalSchema,
        if_not_exists: bool = True
    ) -> bool:
        """
        Create table from CanonicalSchema.

        This method integrates with the renderer architecture:
        1. Get renderer for this platform
        2. Generate DDL from CanonicalSchema
        3. Execute DDL

        This enables workflows like:
        - DataFrame → CanonicalSchema → Connection → Create table
        - Snowflake table → CanonicalSchema → BigQuery connection → Create table

        Args:
            canonical_schema: CanonicalSchema to create
            if_not_exists: Add IF NOT EXISTS clause (default: True)

        Returns:
            True if successful

        Raises:
            ExecutionError: If table creation fails
            ConnectionError: If not connected

        Examples:
            >>> from schema_mapper import infer_canonical_schema
            >>> import pandas as pd
            >>> df = pd.read_csv('events.csv')
            >>> schema = infer_canonical_schema(df, table_name='events')
            >>> conn.create_table_from_schema(schema)
            True
        """
        from ..renderers.factory import RendererFactory

        try:
            # Get renderer for this platform
            renderer = RendererFactory.get_renderer(
                self.platform_name(),
                canonical_schema
            )

            # Generate DDL (renderer handles if_not_exists internally)
            ddl = renderer.to_ddl()

            # Execute DDL
            return self.execute_ddl(ddl)

        except Exception as e:
            raise ExecutionError(
                f"Failed to create table from canonical schema: {e}",
                platform=self.platform_name(),
                original_error=e
            )

    def execute_incremental_ddl(
        self,
        canonical_schema: CanonicalSchema,
        config: Any,  # IncrementalConfig
        staging_table: str = None
    ) -> bool:
        """
        Execute incremental load DDL (MERGE, UPSERT, etc.).

        Integrates with incremental load generators:
        1. Get incremental generator for this platform
        2. Generate incremental DDL
        3. Execute DDL

        Args:
            canonical_schema: Schema for target table
            config: IncrementalConfig with load pattern and keys
            staging_table: Staging table name (optional)

        Returns:
            True if successful

        Raises:
            ExecutionError: If execution fails
            ConnectionError: If not connected

        Examples:
            >>> from schema_mapper.incremental import IncrementalConfig, LoadPattern
            >>> config = IncrementalConfig(
            ...     load_pattern=LoadPattern.UPSERT,
            ...     primary_keys=['user_id']
            ... )
            >>> conn.execute_incremental_ddl(schema, config)
            True
        """
        from ..incremental import get_incremental_generator

        try:
            # Get incremental generator
            generator = get_incremental_generator(self.platform_name())

            # Generate incremental DDL
            ddl = generator.generate(
                canonical_schema=canonical_schema,
                config=config,
                staging_table=staging_table
            )

            # Execute
            return self.execute_ddl(ddl)

        except Exception as e:
            raise ExecutionError(
                f"Failed to execute incremental load: {e}",
                platform=self.platform_name(),
                original_error=e
            )

    # ==================== TRANSACTION SUPPORT ====================

    @abstractmethod
    def begin_transaction(self) -> None:
        """
        Begin transaction.

        For platforms that support transactions (PostgreSQL, Snowflake, SQL Server).
        For platforms without transactions (BigQuery), this may log a warning.

        Raises:
            TransactionError: If transaction fails to start
            ConnectionError: If not connected

        Examples:
            >>> conn.begin_transaction()
            >>> try:
            ...     conn.execute_ddl(ddl1)
            ...     conn.execute_ddl(ddl2)
            ...     conn.commit()
            ... except Exception:
            ...     conn.rollback()
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """
        Commit transaction.

        Raises:
            TransactionError: If commit fails
            ConnectionError: If not connected

        Examples:
            >>> conn.commit()
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """
        Rollback transaction.

        Raises:
            TransactionError: If rollback fails
            ConnectionError: If not connected

        Examples:
            >>> conn.rollback()
        """
        pass

    @contextmanager
    def transaction(self, isolation_level: Optional[str] = None):
        """
        Transaction context manager.

        Automatically begins, commits, or rolls back transaction.
        Provides a clean interface for transactional operations.

        Args:
            isolation_level: Optional isolation level (platform-specific)

        Yields:
            self (for chaining operations)

        Raises:
            TransactionError: If transaction fails
            ConnectionError: If not connected

        Examples:
            >>> with conn.transaction():
            ...     conn.execute_ddl("CREATE TABLE users (id INT)")
            ...     conn.execute_ddl("CREATE TABLE orders (id INT)")
            >>> # Auto-committed

            >>> with conn.transaction(isolation_level='serializable'):
            ...     conn.execute_ddl(ddl)
            ...     # If exception occurs, auto-rollback

            >>> # Explicit control
            >>> with conn.transaction() as txn:
            ...     txn.execute_ddl(ddl1)
            ...     if some_condition:
            ...         raise Exception("Rollback!")
            ...     txn.execute_ddl(ddl2)
        """
        self.require_connection()

        # Track transaction start
        transaction_start = datetime.now()
        self._transaction_stats['active_transaction_start'] = transaction_start

        # Begin transaction with optional isolation level
        if isolation_level:
            # Check if platform supports isolation levels
            begin_method = getattr(self, 'begin_transaction')
            import inspect
            sig = inspect.signature(begin_method)
            if 'isolation_level' in sig.parameters:
                self.begin_transaction(isolation_level=isolation_level)
            else:
                self.logger.warning(
                    f"{self.platform_name()} does not support isolation levels, "
                    "using default transaction semantics"
                )
                self.begin_transaction()
        else:
            self.begin_transaction()

        self._transaction_stats['total_transactions'] += 1

        try:
            yield self
            # Success - commit
            self.commit()
            self._transaction_stats['total_commits'] += 1

            # Log transaction duration
            duration = (datetime.now() - transaction_start).total_seconds()
            self.logger.info(f"Transaction committed (duration: {duration:.2f}s)")

        except Exception as e:
            # Failure - rollback
            self.logger.warning(f"Transaction failed, rolling back: {e}")
            try:
                self.rollback()
                self._transaction_stats['total_rollbacks'] += 1
            except Exception as rollback_error:
                self.logger.error(f"Rollback failed: {rollback_error}")
            raise

        finally:
            self._transaction_stats['active_transaction_start'] = None

    def get_transaction_stats(self) -> Dict[str, Any]:
        """
        Get transaction statistics for this connection.

        Returns:
            Dictionary with transaction metrics

        Examples:
            >>> stats = conn.get_transaction_stats()
            >>> print(f"Commits: {stats['total_commits']}")
            >>> print(f"Rollbacks: {stats['total_rollbacks']}")
        """
        stats = self._transaction_stats.copy()

        # Calculate active transaction duration if applicable
        if stats['active_transaction_start']:
            duration = (datetime.now() - stats['active_transaction_start']).total_seconds()
            stats['active_transaction_duration_seconds'] = duration
        else:
            stats['active_transaction_duration_seconds'] = None

        # Calculate success rate
        total_completed = stats['total_commits'] + stats['total_rollbacks']
        if total_completed > 0:
            stats['commit_success_rate'] = stats['total_commits'] / total_completed
        else:
            stats['commit_success_rate'] = None

        return stats

    # ==================== SAVEPOINT SUPPORT ====================
    # Default implementations (no-op for platforms that don't support savepoints)
    # PostgreSQL and SQL Server override these methods

    def savepoint(self, name: Optional[str] = None) -> str:
        """
        Create a savepoint within a transaction.

        Savepoints allow partial rollback within a transaction.
        Supported by PostgreSQL and SQL Server.

        Args:
            name: Savepoint name (auto-generated if not provided)

        Returns:
            Savepoint name

        Raises:
            TransactionError: If savepoint creation fails
            NotImplementedError: If platform doesn't support savepoints

        Examples:
            >>> conn.begin_transaction()
            >>> sp1 = conn.savepoint('before_risky_operation')
            >>> try:
            ...     conn.execute_ddl(risky_ddl)
            ... except Exception:
            ...     conn.rollback_to_savepoint(sp1)
            >>> conn.commit()
        """
        raise NotImplementedError(
            f"{self.platform_name()} does not support savepoints"
        )

    def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to a savepoint.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If rollback fails
            NotImplementedError: If platform doesn't support savepoints

        Examples:
            >>> conn.rollback_to_savepoint('sp1')
        """
        raise NotImplementedError(
            f"{self.platform_name()} does not support savepoints"
        )

    def release_savepoint(self, name: str) -> None:
        """
        Release a savepoint (remove it without rolling back).

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If release fails
            NotImplementedError: If platform doesn't support savepoints

        Examples:
            >>> conn.release_savepoint('sp1')
        """
        raise NotImplementedError(
            f"{self.platform_name()} does not support savepoints"
        )

    # ==================== METADATA ====================

    @abstractmethod
    def platform_name(self) -> str:
        """
        Return platform name.

        Returns:
            Platform identifier (e.g., 'bigquery', 'snowflake', 'postgresql')

        Examples:
            >>> conn.platform_name()
            'bigquery'
        """
        pass

    @abstractmethod
    def _validate_config(self) -> None:
        """
        Validate configuration dictionary.

        Called during __init__. Should check that all required fields are present
        and have valid values.

        Raises:
            ConfigurationError: If config is invalid
            ValidationError: If credentials are invalid

        Examples:
            >>> def _validate_config(self):
            ...     if 'project' not in self.config:
            ...         raise ConfigurationError("Missing 'project' in config")
        """
        pass

    # ==================== CONTEXT MANAGER ====================

    def __enter__(self):
        """
        Context manager entry.

        Automatically connects when entering context.

        Returns:
            self

        Examples:
            >>> with BigQueryConnection(config) as conn:
            ...     schema = conn.get_target_schema('users')
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit.

        Automatically disconnects when exiting context.
        Rolls back transaction if exception occurred.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback

        Returns:
            False (don't suppress exceptions)
        """
        try:
            if exc_type and self._transaction_active:
                # Exception occurred, rollback
                try:
                    self.rollback()
                except Exception as e:
                    self.logger.error(f"Error during rollback: {e}")
        finally:
            self.disconnect()

        return False  # Don't suppress exceptions

    # ==================== STATE CHECKING ====================

    def is_connected(self) -> bool:
        """
        Check if connection is active.

        Returns:
            True if connected

        Examples:
            >>> if conn.is_connected():
            ...     conn.execute_query("SELECT 1")
        """
        return self.state == ConnectionState.CONNECTED

    def require_connection(self) -> None:
        """
        Raise error if not connected.

        Raises:
            ConnectionError: If not connected

        Examples:
            >>> def execute_query(self, query):
            ...     self.require_connection()
            ...     # Execute query
        """
        if not self.is_connected():
            raise ConnectionError(
                "Not connected to database. Call connect() first.",
                platform=self.platform_name()
            )

    # ==================== UTILITY METHODS ====================

    def __repr__(self) -> str:
        """String representation of connection."""
        return (
            f"<{self.__class__.__name__} "
            f"platform={self.platform_name()} "
            f"state={self.state.value}>"
        )
