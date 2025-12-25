"""
Snowflake database connection implementation.

Provides connection lifecycle, table introspection via INFORMATION_SCHEMA,
transaction support, and DDL execution for Snowflake Data Warehouse.
"""

from typing import Optional, Dict, Any, List
import logging

try:
    import snowflake.connector
    from snowflake.connector import ProgrammingError, DatabaseError
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    snowflake = None
    ProgrammingError = Exception
    DatabaseError = Exception

from ..base import BaseConnection, ConnectionState
from ..exceptions import (
    ConnectionError,
    AuthenticationError,
    TableNotFoundError,
    ExecutionError,
    IntrospectionError,
    TransactionError,
)
from ..utils.retry import retry_on_transient_error
from ..utils.validation import validate_credentials
from ..utils.type_mapping import map_to_logical_type, extract_precision_scale
from ..introspection import (
    build_columns_query,
    build_table_exists_query,
    build_list_tables_query,
    build_snowflake_clustering_query,
    parse_column_row,
    parse_table_exists_result,
    parse_list_tables_result,
    parse_snowflake_clustering_result,
)
from ...canonical import CanonicalSchema, ColumnDefinition, OptimizationHints

logger = logging.getLogger(__name__)


class SnowflakeConnection(BaseConnection):
    """
    Snowflake database connection.

    Features:
    - Connection via snowflake-connector-python
    - Table introspection via INFORMATION_SCHEMA
    - Full transaction support (BEGIN, COMMIT, ROLLBACK)
    - Type mapping: Snowflake → LogicalType
    - Clustering metadata extraction
    - DDL execution

    Configuration:
        user: Snowflake username (required)
        password: Snowflake password (required)
        account: Account identifier (required, e.g., 'abc123.us-east-1')
        warehouse: Warehouse name (optional but recommended)
        database: Database name (optional but recommended)
        schema: Schema name (optional, default: PUBLIC)
        role: Role name (optional)

    Examples:
        >>> config = {
        ...     'user': 'admin',
        ...     'password': 'secret',
        ...     'account': 'abc123.us-east-1',
        ...     'warehouse': 'ANALYTICS_WH',
        ...     'database': 'ANALYTICS',
        ...     'schema': 'PUBLIC'
        ... }
        >>> conn = SnowflakeConnection(config)
        >>> conn.connect()
        >>> schema = conn.get_target_schema('customers')
        >>> conn.disconnect()

        With transactions:
        >>> with SnowflakeConnection(config) as conn:
        ...     conn.begin_transaction()
        ...     try:
        ...         conn.execute_ddl(ddl1)
        ...         conn.execute_ddl(ddl2)
        ...         conn.commit()
        ...     except Exception:
        ...         conn.rollback()
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Snowflake connection.

        Args:
            config: Configuration dictionary with Snowflake credentials

        Raises:
            ConfigurationError: If configuration is invalid
            ImportError: If snowflake-connector-python not installed
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is not installed. "
                "Install with: pip install schema-mapper[snowflake]"
            )

        super().__init__(config)

        # Snowflake-specific configuration
        self.user = config.get('user')
        self.password = config.get('password')
        self.account = config.get('account')
        self.warehouse = config.get('warehouse')
        self.database = config.get('database')
        self.schema = config.get('schema', 'PUBLIC')
        self.role = config.get('role')

    def platform_name(self) -> str:
        """Return platform name."""
        return 'snowflake'

    def _validate_config(self) -> None:
        """
        Validate Snowflake configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        validate_credentials(self.config, 'snowflake')

    @retry_on_transient_error(max_retries=3, platform='snowflake')
    def connect(self) -> bool:
        """
        Establish Snowflake connection.

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If credentials are invalid
        """
        try:
            self.state = ConnectionState.CONNECTING
            self.logger.info(
                f"Connecting to Snowflake account: {self.account}, "
                f"warehouse: {self.warehouse}, database: {self.database}"
            )

            # Build connection parameters
            conn_params = {
                'user': self.user,
                'password': self.password,
                'account': self.account,
            }

            # Add optional parameters
            if self.warehouse:
                conn_params['warehouse'] = self.warehouse
            if self.database:
                conn_params['database'] = self.database
            if self.schema:
                conn_params['schema'] = self.schema
            if self.role:
                conn_params['role'] = self.role

            # Establish connection
            self._connection = snowflake.connector.connect(**conn_params)

            # Create cursor
            self._cursor = self._connection.cursor()

            self.state = ConnectionState.CONNECTED
            self.logger.info(f"Successfully connected to Snowflake account: {self.account}")
            return True

        except ProgrammingError as e:
            self.state = ConnectionState.ERROR
            # Check if authentication error
            if 'Incorrect username or password' in str(e):
                raise AuthenticationError(
                    f"Authentication failed: {e}",
                    platform=self.platform_name()
                )
            raise ConnectionError(
                f"Failed to connect to Snowflake: {e}",
                platform=self.platform_name()
            )
        except Exception as e:
            self.state = ConnectionState.ERROR
            raise ConnectionError(
                f"Failed to connect to Snowflake: {e}",
                platform=self.platform_name()
            )

    def disconnect(self) -> None:
        """Close Snowflake connection."""
        try:
            # Rollback any pending transaction
            if self._transaction_active:
                try:
                    self.rollback()
                except Exception as e:
                    self.logger.error(f"Error rolling back transaction: {e}")

            # Close cursor
            if self._cursor:
                self._cursor.close()
                self._cursor = None

            # Close connection
            if self._connection:
                self._connection.close()
                self._connection = None

            self.state = ConnectionState.DISCONNECTED
            self.logger.info("Disconnected from Snowflake")

        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")

    def test_connection(self) -> bool:
        """
        Test if Snowflake connection is working.

        Returns:
            True if connection is healthy
        """
        try:
            self.require_connection()
            # Simple query to test connection
            self._cursor.execute("SELECT 1")
            self._cursor.fetchone()
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def table_exists(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> bool:
        """
        Check if Snowflake table exists.

        Args:
            table_name: Table name
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (uses configured database if not provided)

        Returns:
            True if table exists

        Examples:
            >>> conn.table_exists('customers', schema_name='PUBLIC')
            True
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema
            database = database_name or self.database

            # Build and execute query
            query = build_table_exists_query(
                table_name,
                schema,
                database,
                platform='snowflake'
            )

            self._cursor.execute(query)
            result = self._cursor.fetchall()

            return parse_table_exists_result(result)

        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}")
            return False

    def get_target_schema(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> CanonicalSchema:
        """
        Introspect Snowflake table and return CanonicalSchema.

        Queries INFORMATION_SCHEMA to get:
        - Column names and types
        - Nullability
        - Precision and scale
        - Clustering keys

        Args:
            table_name: Table name
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (uses configured database if not provided)

        Returns:
            CanonicalSchema representing the table

        Raises:
            TableNotFoundError: If table doesn't exist
            IntrospectionError: If cannot read table schema

        Examples:
            >>> schema = conn.get_target_schema('orders', schema_name='PUBLIC')
            >>> print(f"Table has {len(schema.columns)} columns")
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema
            database = database_name or self.database

            # Check if table exists
            if not self.table_exists(table_name, schema, database):
                raise TableNotFoundError(
                    f"Table not found: {database}.{schema}.{table_name}",
                    table_name=table_name,
                    schema_name=schema,
                    database_name=database,
                    platform=self.platform_name()
                )

            # Query column information
            query = build_columns_query(table_name, schema, database, platform='snowflake')
            self._cursor.execute(query)
            rows = self._cursor.fetchall()

            if not rows:
                raise IntrospectionError(
                    f"No columns found for table {table_name}",
                    platform=self.platform_name()
                )

            # Convert rows to ColumnDefinitions
            columns = []
            for row in rows:
                # Convert row to dict (Snowflake cursor returns tuples by default)
                row_dict = {}
                for idx, col_desc in enumerate(self._cursor.description):
                    row_dict[col_desc[0]] = row[idx]

                columns.append(self._convert_sf_row_to_column(row_dict))

            # Extract optimization hints (clustering)
            optimization = self._extract_optimization_hints(table_name, schema, database)

            # Create canonical schema
            canonical = CanonicalSchema(
                table_name=table_name,
                dataset_name=schema,
                project_id=database,
                columns=columns,
                optimization=optimization,
                created_from="Snowflake"
            )

            self.logger.info(
                f"Introspected table {database}.{schema}.{table_name}: "
                f"{len(columns)} columns, "
                f"clustering={optimization.cluster_columns}"
            )

            return canonical

        except (TableNotFoundError, IntrospectionError):
            raise
        except Exception as e:
            raise IntrospectionError(
                f"Failed to introspect table {table_name}: {e}",
                platform=self.platform_name()
            )

    def _convert_sf_row_to_column(self, row_dict: Dict[str, Any]) -> ColumnDefinition:
        """
        Convert INFORMATION_SCHEMA row to ColumnDefinition.

        Args:
            row_dict: Row dictionary from INFORMATION_SCHEMA.COLUMNS

        Returns:
            ColumnDefinition
        """
        # Parse row
        col_info = parse_column_row(row_dict, 'snowflake')

        # Map Snowflake type to LogicalType
        logical_type = map_to_logical_type(
            col_info['data_type'],
            'snowflake',
            precision=col_info['precision'],
            scale=col_info['scale']
        )

        return ColumnDefinition(
            name=col_info['name'],
            logical_type=logical_type,
            nullable=col_info['nullable'],
            max_length=col_info['max_length'],
            precision=col_info['precision'],
            scale=col_info['scale'],
            original_name=col_info['name'].upper()  # Snowflake stores as uppercase
        )

    def _extract_optimization_hints(
        self,
        table_name: str,
        schema_name: str,
        database_name: Optional[str]
    ) -> OptimizationHints:
        """
        Extract clustering keys from Snowflake table.

        Args:
            table_name: Table name
            schema_name: Schema name
            database_name: Database name

        Returns:
            OptimizationHints with clustering information
        """
        hints = OptimizationHints()

        try:
            # Query clustering keys
            query = build_snowflake_clustering_query(table_name, schema_name, database_name)
            self._cursor.execute(query)
            result = self._cursor.fetchall()

            # Parse clustering keys
            cluster_columns = parse_snowflake_clustering_result(result)
            if cluster_columns:
                hints.cluster_columns = cluster_columns

        except Exception as e:
            self.logger.warning(f"Could not extract clustering keys: {e}")

        return hints

    def list_tables(
        self,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> List[str]:
        """
        List tables in Snowflake schema.

        Args:
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (uses configured database if not provided)

        Returns:
            List of table names

        Examples:
            >>> tables = conn.list_tables(schema_name='PUBLIC')
            >>> print(f"Found {len(tables)} tables")
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema
            database = database_name or self.database

            # Build and execute query
            query = build_list_tables_query(schema, database, platform='snowflake')
            self._cursor.execute(query)
            result = self._cursor.fetchall()

            table_names = parse_list_tables_result(result)

            self.logger.info(f"Listed {len(table_names)} tables in {database}.{schema}")
            return table_names

        except Exception as e:
            self.logger.error(f"Error listing tables: {e}")
            raise

    def execute_ddl(self, ddl: str) -> bool:
        """
        Execute DDL statement in Snowflake.

        Args:
            ddl: DDL statement (CREATE, ALTER, DROP)

        Returns:
            True if successful

        Raises:
            ExecutionError: If DDL execution fails

        Examples:
            >>> ddl = "CREATE TABLE PUBLIC.users (id INTEGER, name VARCHAR(100))"
            >>> conn.execute_ddl(ddl)
            True
        """
        try:
            self.require_connection()
            self.logger.info(f"Executing DDL: {ddl[:100]}...")

            self._cursor.execute(ddl)

            self.logger.info("DDL executed successfully")
            return True

        except ProgrammingError as e:
            raise ExecutionError(
                f"DDL execution failed: {e}",
                query=ddl,
                platform=self.platform_name(),
                original_error=e
            )
        except Exception as e:
            raise ExecutionError(
                f"Unexpected error during DDL execution: {e}",
                query=ddl,
                platform=self.platform_name(),
                original_error=e
            )

    def execute_query(self, query: str) -> Any:
        """
        Execute query in Snowflake.

        Args:
            query: SQL query

        Returns:
            Cursor with results

        Examples:
            >>> result = conn.execute_query("SELECT COUNT(*) FROM PUBLIC.users")
            >>> for row in result:
            ...     print(row)
        """
        try:
            self.require_connection()
            self.logger.debug(f"Executing query: {query[:100]}...")

            self._cursor.execute(query)
            return self._cursor

        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise

    def begin_transaction(self) -> None:
        """
        Begin Snowflake transaction.

        Raises:
            TransactionError: If transaction fails to start
        """
        try:
            self.require_connection()

            if self._transaction_active:
                self.logger.warning("Transaction already active")
                return

            self._cursor.execute("BEGIN")
            self._transaction_active = True
            self.logger.info("Transaction started")

        except Exception as e:
            raise TransactionError(
                f"Failed to begin transaction: {e}",
                platform=self.platform_name()
            )

    def commit(self) -> None:
        """
        Commit Snowflake transaction.

        Raises:
            TransactionError: If commit fails
        """
        try:
            self.require_connection()

            if not self._transaction_active:
                self.logger.warning("No active transaction to commit")
                return

            self._connection.commit()
            self._transaction_active = False
            self.logger.info("Transaction committed")

        except Exception as e:
            raise TransactionError(
                f"Failed to commit transaction: {e}",
                platform=self.platform_name()
            )

    def rollback(self) -> None:
        """
        Rollback Snowflake transaction.

        Raises:
            TransactionError: If rollback fails
        """
        try:
            self.require_connection()

            if not self._transaction_active:
                self.logger.warning("No active transaction to rollback")
                return

            self._connection.rollback()
            self._transaction_active = False
            self.logger.info("Transaction rolled back")

        except Exception as e:
            raise TransactionError(
                f"Failed to rollback transaction: {e}",
                platform=self.platform_name()
            )
