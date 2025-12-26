"""
Snowflake database connection implementation.

Provides connection lifecycle, table introspection via INFORMATION_SCHEMA,
transaction support, and DDL execution for Snowflake Data Warehouse.
"""

from typing import Optional, Dict, Any, List
import logging
import pandas as pd

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

    def get_tables(
        self,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get detailed table information as a pandas DataFrame.

        Args:
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (uses configured database if not provided)

        Returns:
            pandas DataFrame with columns: table_name, table_type, created, rows,
            size_mb, comment

        Examples:
            >>> tables = conn.get_tables(schema_name='PUBLIC')
            >>> print(tables)
              table_name table_type                 created    rows  size_mb comment
            0      USERS      TABLE  2024-01-01 10:00:00  150000    245.5  User data
            1     EVENTS      TABLE  2024-01-05 11:00:00 5000000   8920.3  Event tracking
            2  USER_VIEW       VIEW  2024-01-10 12:00:00       0      0.0  User summary
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema
            database = database_name or self.database

            self.logger.info(f"Getting table details for {database}.{schema}")

            # Query to get table details from INFORMATION_SCHEMA
            query = f"""
                SELECT
                    TABLE_NAME,
                    TABLE_TYPE,
                    CREATED,
                    ROW_COUNT as rows,
                    ROUND(BYTES / (1024 * 1024), 2) as size_mb,
                    COMMENT
                FROM {database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema}'
                ORDER BY TABLE_NAME
            """

            self._cursor.execute(query)
            columns = [desc[0].lower() for desc in self._cursor.description]
            data = self._cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)

            self.logger.info(f"Retrieved details for {len(df)} tables")

            return df

        except Exception as e:
            self.logger.error(f"Error getting table details: {e}")
            raise

    def get_schemas(self, database_name: Optional[str] = None) -> pd.DataFrame:
        """
        List all schemas in Snowflake database and return as a pandas DataFrame.

        Args:
            database_name: Database name (uses configured database if not provided)

        Returns:
            pandas DataFrame with columns: schema_name, owner, created, comment

        Examples:
            >>> schemas = conn.get_schemas()
            >>> print(schemas)
              schema_name     owner                 created comment
            0      PUBLIC  SYSADMIN  2024-01-01 10:00:00  Public schema
            1   ANALYTICS  ANALYST   2024-02-01 09:00:00  Analytics data
        """
        try:
            self.require_connection()
            database = database_name or self.database

            self.logger.info(f"Listing schemas in database: {database}")

            # Query to list schemas
            query = f"""
                SELECT
                    SCHEMA_NAME,
                    SCHEMA_OWNER as owner,
                    CREATED,
                    COMMENT
                FROM {database}.INFORMATION_SCHEMA.SCHEMATA
                ORDER BY SCHEMA_NAME
            """

            self._cursor.execute(query)
            columns = [desc[0] for desc in self._cursor.description]
            data = self._cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)

            self.logger.info(f"Found {len(df)} schemas in database {database}")

            return df

        except Exception as e:
            self.logger.error(f"Error listing schemas: {e}")
            raise

    def get_database_tree(
        self,
        database_name: Optional[str] = None,
        include_table_counts: bool = True,
        format: str = 'dict'
    ) -> Any:
        """
        Get hierarchical structure of database → schemas → tables.

        Args:
            database_name: Database name (uses configured database if not provided)
            include_table_counts: Include count of tables in each schema (default: True)
            format: Output format - 'dict' for nested dictionary, 'dataframe' for flat table

        Returns:
            Dictionary or DataFrame with database structure

        Examples:
            >>> # Get as nested dictionary (JSON-serializable)
            >>> tree = conn.get_database_tree(format='dict')
            >>> print(tree)
            {
                'database': 'MYDB',
                'schemas': [
                    {
                        'schema_name': 'PUBLIC',
                        'table_count': 5,
                        'tables': ['USERS', 'EVENTS', 'SESSIONS', 'PRODUCTS', 'ORDERS']
                    },
                    {
                        'schema_name': 'ANALYTICS',
                        'table_count': 3,
                        'tables': ['DAILY_STATS', 'MONTHLY_SUMMARY', 'REPORTS']
                    }
                ]
            }

            >>> # Get as flattened DataFrame
            >>> tree_df = conn.get_database_tree(format='dataframe')
            >>> print(tree_df)
              database schema_name  table_count                    tables
            0     MYDB      PUBLIC            5  USERS, EVENTS, ...
            1     MYDB   ANALYTICS            3  DAILY_STATS, ...
        """
        try:
            self.require_connection()
            database = database_name or self.database

            self.logger.info(f"Building database tree for: {database}")

            # Get schemas
            schemas_df = self.get_schemas(database_name=database)

            tree_data = []
            for _, schema_row in schemas_df.iterrows():
                schema_name = schema_row['SCHEMA_NAME']

                schema_info = {
                    'schema_name': schema_name,
                }

                # Get tables for this schema
                try:
                    tables = self.list_tables(schema_name=schema_name, database_name=database)

                    if include_table_counts:
                        schema_info['table_count'] = len(tables)

                    schema_info['tables'] = tables

                except Exception as e:
                    self.logger.warning(f"Could not list tables for {schema_name}: {e}")
                    schema_info['table_count'] = 0
                    schema_info['tables'] = []

                tree_data.append(schema_info)

            if format == 'dict':
                result = {
                    'database': database,
                    'schema_count': len(tree_data),
                    'schemas': tree_data
                }
                self.logger.info(f"Database tree built: {len(tree_data)} schemas")
                return result

            elif format == 'dataframe':
                # Flatten to DataFrame
                df_data = []
                for schema_info in tree_data:
                    row = {
                        'database': database,
                        'schema_name': schema_info['schema_name'],
                    }
                    if include_table_counts:
                        row['table_count'] = schema_info['table_count']
                    row['tables'] = ', '.join(schema_info['tables']) if schema_info['tables'] else ''
                    df_data.append(row)

                df = pd.DataFrame(df_data)
                self.logger.info(f"Database tree built: {len(df)} schemas")
                return df

            else:
                raise ValueError(f"Invalid format: {format}. Must be 'dict' or 'dataframe'")

        except Exception as e:
            self.logger.error(f"Error building database tree: {e}")
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

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute query in Snowflake and return results as a pandas DataFrame.

        Args:
            query: SQL query

        Returns:
            pandas DataFrame with query results

        Raises:
            ExecutionError: If query execution fails
            ConnectionError: If not connected

        Examples:
            >>> result = conn.execute_query("SELECT COUNT(*) FROM PUBLIC.users")
            >>> print(result)
            >>> # result is a pandas DataFrame
            >>> print(type(result))
            <class 'pandas.core.frame.DataFrame'>
        """
        try:
            self.require_connection()
            self.logger.debug(f"Executing query: {query[:100]}...")

            self._cursor.execute(query)
            # Fetch all results and convert to DataFrame
            columns = [desc[0] for desc in self._cursor.description]
            data = self._cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)

            self.logger.debug(f"Query returned {len(df)} rows")
            return df

        except (ProgrammingError, DatabaseError) as e:
            self.logger.error(f"Query execution failed: {e}")
            raise ExecutionError(
                f"Query execution failed: {e}",
                query=query,
                platform=self.platform_name(),
                original_error=e
            )
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise ExecutionError(
                f"Unexpected error during query execution: {e}",
                query=query,
                platform=self.platform_name(),
                original_error=e
            )

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
