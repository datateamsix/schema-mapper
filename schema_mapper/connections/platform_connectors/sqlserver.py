"""
Microsoft SQL Server database connection implementation.

Provides connection lifecycle, table introspection via INFORMATION_SCHEMA and sys tables,
transaction support with snapshot isolation, and DDL execution for SQL Server.
"""

from typing import Optional, Dict, Any, List
import logging
import pandas as pd

try:
    import pyodbc
    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLSERVER_AVAILABLE = False
    pyodbc = None

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
    parse_column_row,
    parse_table_exists_result,
    parse_list_tables_result,
)
from ...canonical import CanonicalSchema, ColumnDefinition, OptimizationHints

logger = logging.getLogger(__name__)


class SQLServerConnection(BaseConnection):
    """
    Microsoft SQL Server database connection.

    Features:
    - Connection via pyodbc
    - Table introspection via INFORMATION_SCHEMA and sys tables
    - Full transaction support with snapshot isolation
    - Type mapping: SQL Server → LogicalType
    - Clustered/nonclustered index metadata extraction
    - Columnstore index detection
    - DDL execution with batch support

    Configuration:
        server: SQL Server hostname or IP (required)
        port: SQL Server port (default: 1433)
        database: Database name (required)
        user: SQL Server username (required, unless using Windows auth)
        password: SQL Server password (required, unless using Windows auth)
        schema: Schema name (optional, default: dbo)
        driver: ODBC driver name (optional, default: auto-detect)
        trusted_connection: Use Windows authentication (optional, default: False)
        encrypt: Enable encryption (optional, default: True)
        trust_server_certificate: Trust server certificate (optional, default: False)

    Examples:
        >>> config = {
        ...     'server': 'localhost',
        ...     'port': 1433,
        ...     'database': 'analytics',
        ...     'user': 'sa',
        ...     'password': 'secret',
        ...     'schema': 'dbo'
        ... }
        >>> conn = SQLServerConnection(config)
        >>> conn.connect()
        >>> schema = conn.get_target_schema('customers')
        >>> conn.disconnect()

        With transactions:
        >>> with SQLServerConnection(config) as conn:
        ...     conn.begin_transaction(isolation_level='snapshot')
        ...     try:
        ...         conn.execute_ddl(ddl1)
        ...         conn.execute_ddl(ddl2)
        ...         conn.commit()
        ...     except Exception:
        ...         conn.rollback()

        Windows Authentication:
        >>> config = {
        ...     'server': 'localhost',
        ...     'database': 'analytics',
        ...     'trusted_connection': True
        ... }
        >>> conn = SQLServerConnection(config)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQL Server connection.

        Args:
            config: Configuration dictionary with SQL Server credentials

        Raises:
            ConfigurationError: If configuration is invalid
            ImportError: If pyodbc not installed
        """
        if not SQLSERVER_AVAILABLE:
            raise ImportError(
                "pyodbc is not installed. "
                "Install with: pip install schema-mapper[sqlserver]"
            )

        super().__init__(config)

        # SQL Server-specific configuration
        self.server = config.get('server')
        self.port = config.get('port', 1433)
        self.database = config.get('database')
        self.user = config.get('user')
        self.password = config.get('password')
        self.schema = config.get('schema', 'dbo')
        self.driver = config.get('driver')  # Auto-detect if not provided
        self.trusted_connection = config.get('trusted_connection', False)
        self.encrypt = config.get('encrypt', True)
        self.trust_server_certificate = config.get('trust_server_certificate', False)

    def platform_name(self) -> str:
        """Return platform name."""
        return 'sqlserver'

    def _validate_config(self) -> None:
        """
        Validate SQL Server configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        validate_credentials(self.config, 'sqlserver')

    def _get_odbc_driver(self) -> str:
        """
        Get available ODBC driver for SQL Server.

        Returns:
            ODBC driver name

        Raises:
            ConnectionError: If no suitable driver found
        """
        if self.driver:
            return self.driver

        # Try to find available SQL Server drivers
        drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]

        if not drivers:
            raise ConnectionError(
                "No SQL Server ODBC driver found. "
                "Install ODBC Driver 17 for SQL Server or newer.",
                platform=self.platform_name()
            )

        # Prefer newer drivers
        preferred_order = [
            'ODBC Driver 18 for SQL Server',
            'ODBC Driver 17 for SQL Server',
            'ODBC Driver 13 for SQL Server',
            'SQL Server Native Client 11.0',
            'SQL Server',
        ]

        for preferred in preferred_order:
            if preferred in drivers:
                return preferred

        # Return first available driver
        return drivers[0]

    @retry_on_transient_error(max_retries=3, platform='sqlserver')
    def connect(self) -> bool:
        """
        Establish SQL Server connection.

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If credentials are invalid
        """
        try:
            self.state = ConnectionState.CONNECTING
            self.logger.info(
                f"Connecting to SQL Server: {self.server}:{self.port}/{self.database}"
            )

            # Get ODBC driver
            driver = self._get_odbc_driver()
            self.logger.debug(f"Using ODBC driver: {driver}")

            # Build connection string
            conn_string_parts = [
                f"DRIVER={{{driver}}}",
                f"SERVER={self.server},{self.port}",
                f"DATABASE={self.database}",
            ]

            if self.trusted_connection:
                # Windows Authentication
                conn_string_parts.append("Trusted_Connection=yes")
            else:
                # SQL Server Authentication
                conn_string_parts.append(f"UID={self.user}")
                conn_string_parts.append(f"PWD={self.password}")

            # Add encryption settings
            if self.encrypt:
                conn_string_parts.append("Encrypt=yes")
            if self.trust_server_certificate:
                conn_string_parts.append("TrustServerCertificate=yes")

            conn_string = ";".join(conn_string_parts)

            # Establish connection
            self._connection = pyodbc.connect(conn_string)

            # Create cursor
            self._cursor = self._connection.cursor()

            self.state = ConnectionState.CONNECTED
            self.logger.info(f"Successfully connected to SQL Server: {self.server}:{self.port}/{self.database}")
            return True

        except pyodbc.Error as e:
            self.state = ConnectionState.ERROR
            # Check if authentication error
            error_str = str(e).lower()
            if 'login failed' in error_str or 'authentication failed' in error_str:
                raise AuthenticationError(
                    f"Authentication failed: {e}",
                    platform=self.platform_name()
                )
            raise ConnectionError(
                f"Failed to connect to SQL Server: {e}",
                platform=self.platform_name()
            )
        except Exception as e:
            self.state = ConnectionState.ERROR
            raise ConnectionError(
                f"Failed to connect to SQL Server: {e}",
                platform=self.platform_name()
            )

    def disconnect(self) -> None:
        """Close SQL Server connection."""
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
            self.logger.info("Disconnected from SQL Server")

        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")

    def test_connection(self) -> bool:
        """
        Test if SQL Server connection is working.

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
        Check if SQL Server table exists.

        Args:
            table_name: Table name
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (uses configured database if not provided)

        Returns:
            True if table exists

        Examples:
            >>> conn.table_exists('customers', schema_name='dbo')
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
                platform='sqlserver'
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
        Introspect SQL Server table and return CanonicalSchema.

        Queries INFORMATION_SCHEMA and sys tables to get:
        - Column names and types
        - Nullability
        - Precision and scale
        - Clustered/nonclustered indexes
        - Columnstore indexes

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
            >>> schema = conn.get_target_schema('orders', schema_name='dbo')
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
            query = build_columns_query(table_name, schema, database, platform='sqlserver')
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
                # Convert row to dict
                row_dict = {}
                for idx, col_desc in enumerate(self._cursor.description):
                    row_dict[col_desc[0]] = row[idx]

                columns.append(self._convert_sqlserver_row_to_column(row_dict))

            # Extract optimization hints (indexes, columnstore)
            optimization = self._extract_optimization_hints(table_name, schema, database)

            # Get table description
            description = self._get_table_description(table_name, schema, database)

            # Create canonical schema
            canonical = CanonicalSchema(
                table_name=table_name,
                dataset_name=schema,
                project_id=database,
                columns=columns,
                optimization=optimization,
                description=description,
                created_from="SQL Server"
            )

            self.logger.info(
                f"Introspected table {database}.{schema}.{table_name}: "
                f"{len(columns)} columns, "
                f"indexes={len(optimization.cluster_columns or [])}"
            )

            return canonical

        except (TableNotFoundError, IntrospectionError):
            raise
        except Exception as e:
            raise IntrospectionError(
                f"Failed to introspect table {table_name}: {e}",
                platform=self.platform_name()
            )

    def _convert_sqlserver_row_to_column(self, row_dict: Dict[str, Any]) -> ColumnDefinition:
        """
        Convert INFORMATION_SCHEMA row to ColumnDefinition.

        Args:
            row_dict: Row dictionary from INFORMATION_SCHEMA.COLUMNS

        Returns:
            ColumnDefinition
        """
        # Parse row
        col_info = parse_column_row(row_dict, 'sqlserver')

        # Map SQL Server type to LogicalType
        logical_type = map_to_logical_type(
            col_info['data_type'],
            'sqlserver',
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
            original_name=col_info['name']
        )

    def _extract_optimization_hints(
        self,
        table_name: str,
        schema_name: str,
        database_name: str
    ) -> OptimizationHints:
        """
        Extract clustered index and columnstore info from SQL Server table.

        Args:
            table_name: Table name
            schema_name: Schema name
            database_name: Database name

        Returns:
            OptimizationHints with SQL Server-specific optimization information
        """
        hints = OptimizationHints()

        try:
            # Switch to the correct database
            if database_name:
                self._cursor.execute(f"USE [{database_name}]")

            # Query for clustered index columns
            index_query = """
                SELECT
                    i.name AS index_name,
                    i.type_desc AS index_type,
                    COL_NAME(ic.object_id, ic.column_id) AS column_name,
                    ic.key_ordinal AS column_position
                FROM
                    sys.indexes i
                    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    INNER JOIN sys.tables t ON i.object_id = t.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE
                    t.name = ?
                    AND s.name = ?
                    AND i.type IN (1, 5, 6)  -- 1=CLUSTERED, 5=CLUSTERED COLUMNSTORE, 6=NONCLUSTERED COLUMNSTORE
                    AND ic.is_included_column = 0
                ORDER BY
                    i.is_primary_key DESC,
                    i.type,
                    ic.key_ordinal
            """

            self._cursor.execute(index_query, (table_name, schema_name))
            index_rows = self._cursor.fetchall()

            # Extract clustered index columns
            clustered_columns = []
            for row in index_rows:
                index_type = row[1]  # index_type
                column_name = row[2]  # column_name

                # Only include clustered index columns
                if 'CLUSTERED' in index_type and column_name:
                    clustered_columns.append(column_name)

            if clustered_columns:
                hints.cluster_columns = clustered_columns

            # Check for partitioning
            partition_query = """
                SELECT
                    c.name AS column_name
                FROM
                    sys.tables t
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    INNER JOIN sys.indexes i ON t.object_id = i.object_id
                    INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                    INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE
                    t.name = ?
                    AND s.name = ?
                    AND ic.partition_ordinal > 0
            """

            self._cursor.execute(partition_query, (table_name, schema_name))
            partition_result = self._cursor.fetchone()

            if partition_result:
                partition_column = partition_result[0]
                hints.partition_columns = [partition_column]

        except Exception as e:
            self.logger.warning(f"Could not extract optimization hints: {e}")

        return hints

    def _get_table_description(
        self,
        table_name: str,
        schema_name: str,
        database_name: str
    ) -> Optional[str]:
        """
        Get table description from extended properties.

        Args:
            table_name: Table name
            schema_name: Schema name
            database_name: Database name

        Returns:
            Table description or None
        """
        try:
            # Switch to the correct database
            if database_name:
                self._cursor.execute(f"USE [{database_name}]")

            query = """
                SELECT
                    ep.value AS table_description
                FROM
                    sys.tables t
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id
                        AND ep.minor_id = 0
                        AND ep.name = 'MS_Description'
                WHERE
                    t.name = ?
                    AND s.name = ?
            """

            self._cursor.execute(query, (table_name, schema_name))
            result = self._cursor.fetchone()

            if result and result[0]:
                return result[0]

        except Exception as e:
            self.logger.warning(f"Could not get table description: {e}")

        return None

    def list_tables(
        self,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> List[str]:
        """
        List tables in SQL Server schema.

        Args:
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (uses configured database if not provided)

        Returns:
            List of table names

        Examples:
            >>> tables = conn.list_tables(schema_name='dbo')
            >>> print(f"Found {len(tables)} tables")
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema
            database = database_name or self.database

            # Switch to the correct database
            if database:
                self._cursor.execute(f"USE [{database}]")

            # Build and execute query
            query = build_list_tables_query(schema, database, platform='sqlserver')
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
            pandas DataFrame with columns: table_name, table_type, rows, size_mb

        Examples:
            >>> tables = conn.get_tables(schema_name='dbo')
            >>> print(tables)
              table_name table_type    rows  size_mb
            0      users      TABLE  150000    245.5
            1     events      TABLE 5000000   8920.3
            2  user_view       VIEW       0      0.0
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema
            database = database_name or self.database

            self.logger.info(f"Getting table details for {database}.{schema}")

            # Query to get table details
            query = f"""
                SELECT
                    t.name as table_name,
                    t.type_desc as table_type,
                    SUM(p.rows) as rows,
                    ROUND(SUM(a.total_pages) * 8.0 / 1024, 2) as size_mb
                FROM [{database}].sys.tables t
                INNER JOIN [{database}].sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN [{database}].sys.partitions p ON t.object_id = p.object_id
                INNER JOIN [{database}].sys.allocation_units a ON p.partition_id = a.container_id
                WHERE s.name = '{schema}'
                  AND p.index_id IN (0, 1)
                GROUP BY t.name, t.type_desc
                UNION ALL
                SELECT
                    v.name as table_name,
                    'VIEW' as table_type,
                    0 as rows,
                    0.0 as size_mb
                FROM [{database}].sys.views v
                INNER JOIN [{database}].sys.schemas s ON v.schema_id = s.schema_id
                WHERE s.name = '{schema}'
                ORDER BY table_name
            """

            self._cursor.execute(query)
            columns = [desc[0] for desc in self._cursor.description]
            data = self._cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)

            self.logger.info(f"Retrieved details for {len(df)} tables")

            return df

        except Exception as e:
            self.logger.error(f"Error getting table details: {e}")
            raise

    def get_schemas(self, database_name: Optional[str] = None) -> pd.DataFrame:
        """
        List all schemas in SQL Server database and return as a pandas DataFrame.

        Args:
            database_name: Database name (uses configured database if not provided)

        Returns:
            pandas DataFrame with columns: schema_name, schema_id

        Examples:
            >>> schemas = conn.get_schemas()
            >>> print(schemas)
              schema_name  schema_id
            0         dbo          1
            1   analytics          5
        """
        try:
            self.require_connection()
            database = database_name or self.database

            self.logger.info(f"Listing schemas in database: {database}")

            # Query to list schemas
            query = f"""
                SELECT
                    name as schema_name,
                    schema_id
                FROM [{database}].sys.schemas
                WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
                ORDER BY name
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
            >>> tree = conn.get_database_tree(format='dict')
            >>> tree_df = conn.get_database_tree(format='dataframe')
        """
        try:
            self.require_connection()
            database = database_name or self.database

            self.logger.info(f"Building database tree for: {database}")

            # Get schemas
            schemas_df = self.get_schemas(database_name=database)

            tree_data = []
            for _, schema_row in schemas_df.iterrows():
                schema_name = schema_row['schema_name']

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
        Execute DDL statement in SQL Server.

        Supports batched DDL with GO separators.

        Args:
            ddl: DDL statement (CREATE, ALTER, DROP)

        Returns:
            True if successful

        Raises:
            ExecutionError: If DDL execution fails

        Examples:
            >>> ddl = "CREATE TABLE dbo.users (id BIGINT, name NVARCHAR(100))"
            >>> conn.execute_ddl(ddl)
            True

            >>> # Batched DDL with GO
            >>> ddl = '''
            ...     CREATE TABLE dbo.users (id INT);
            ...     GO
            ...     CREATE INDEX idx_users ON dbo.users(id);
            ... '''
            >>> conn.execute_ddl(ddl)
            True
        """
        try:
            self.require_connection()
            self.logger.info(f"Executing DDL: {ddl[:100]}...")

            # Split on GO for batch execution
            batches = [batch.strip() for batch in ddl.split('GO') if batch.strip()]

            for batch in batches:
                self._cursor.execute(batch)

            # Auto-commit if not in transaction
            if not self._transaction_active:
                self._connection.commit()

            self.logger.info("DDL executed successfully")
            return True

        except pyodbc.Error as e:
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
        Execute query in SQL Server and return results as a pandas DataFrame.

        Args:
            query: SQL query

        Returns:
            pandas DataFrame with query results

        Raises:
            ExecutionError: If query execution fails
            ConnectionError: If not connected

        Examples:
            >>> result = conn.execute_query("SELECT COUNT(*) FROM dbo.users")
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

        except pyodbc.Error as e:
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

    def begin_transaction(self, isolation_level: Optional[str] = None) -> None:
        """
        Begin SQL Server transaction with optional isolation level.

        Args:
            isolation_level: Isolation level ('read_uncommitted', 'read_committed',
                           'repeatable_read', 'serializable', 'snapshot')

        Raises:
            TransactionError: If transaction fails to start
        """
        try:
            self.require_connection()

            if self._transaction_active:
                self.logger.warning("Transaction already active")
                return

            # Set isolation level if provided
            if isolation_level:
                self._set_isolation_level(isolation_level)

            # Begin transaction
            self._cursor.execute("BEGIN TRANSACTION")
            self._transaction_active = True
            self.logger.info(f"Transaction started (isolation: {isolation_level or 'default'})")

        except Exception as e:
            raise TransactionError(
                f"Failed to begin transaction: {e}",
                platform=self.platform_name()
            )

    def commit(self) -> None:
        """
        Commit SQL Server transaction.

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
        Rollback SQL Server transaction.

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

    # ==================== SAVEPOINT SUPPORT ====================

    def savepoint(self, name: Optional[str] = None) -> str:
        """
        Create a savepoint within a SQL Server transaction.

        Args:
            name: Savepoint name (auto-generated if not provided)

        Returns:
            Savepoint name

        Raises:
            TransactionError: If savepoint creation fails
            ConnectionError: If not connected

        Examples:
            >>> conn.begin_transaction()
            >>> sp1 = conn.savepoint('before_update')
            >>> try:
            ...     conn.execute_ddl("UPDATE users SET active = 1")
            ... except Exception:
            ...     conn.rollback_to_savepoint(sp1)
            >>> conn.commit()
        """
        try:
            self.require_connection()

            if not self._transaction_active:
                raise TransactionError(
                    "Cannot create savepoint without active transaction",
                    platform=self.platform_name()
                )

            # Generate name if not provided
            if name is None:
                self._savepoint_counter += 1
                name = f"sp_{self._savepoint_counter}"

            # Create savepoint (SQL Server syntax: SAVE TRANSACTION)
            self._cursor.execute(f"SAVE TRANSACTION {name}")
            self._transaction_stats['total_savepoints'] += 1
            self.logger.debug(f"Created savepoint: {name}")

            return name

        except TransactionError:
            raise
        except Exception as e:
            raise TransactionError(
                f"Failed to create savepoint: {e}",
                platform=self.platform_name()
            )

    def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to a SQL Server savepoint.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If rollback fails
            ConnectionError: If not connected

        Examples:
            >>> conn.rollback_to_savepoint('sp1')
        """
        try:
            self.require_connection()

            if not self._transaction_active:
                raise TransactionError(
                    "Cannot rollback to savepoint without active transaction",
                    platform=self.platform_name()
                )

            # SQL Server syntax: ROLLBACK TRANSACTION
            self._cursor.execute(f"ROLLBACK TRANSACTION {name}")
            self.logger.info(f"Rolled back to savepoint: {name}")

        except TransactionError:
            raise
        except Exception as e:
            raise TransactionError(
                f"Failed to rollback to savepoint: {e}",
                platform=self.platform_name()
            )

    def release_savepoint(self, name: str) -> None:
        """
        Release a SQL Server savepoint.

        Note: SQL Server does not have an explicit RELEASE SAVEPOINT command.
        Savepoints are automatically released when the transaction commits.
        This method is a no-op for SQL Server but included for API consistency.

        Args:
            name: Savepoint name

        Examples:
            >>> conn.release_savepoint('sp1')  # No-op in SQL Server
        """
        # SQL Server doesn't have RELEASE SAVEPOINT
        # Savepoints are automatically cleared on COMMIT
        self.logger.debug(
            f"SQL Server does not support explicit savepoint release. "
            f"Savepoint '{name}' will be released automatically on commit."
        )

    # ==================== PRIVATE METHODS ====================

    def _set_isolation_level(self, level: str) -> None:
        """
        Set transaction isolation level.

        Args:
            level: Isolation level name

        Raises:
            ValueError: If isolation level is invalid
        """
        level_map = {
            'read_uncommitted': 'READ UNCOMMITTED',
            'read_committed': 'READ COMMITTED',
            'repeatable_read': 'REPEATABLE READ',
            'serializable': 'SERIALIZABLE',
            'snapshot': 'SNAPSHOT',
        }

        level_lower = level.lower()
        if level_lower not in level_map:
            raise ValueError(
                f"Invalid isolation level: {level}. "
                f"Valid options: {', '.join(level_map.keys())}"
            )

        self._cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {level_map[level_lower]}")
        self.logger.debug(f"Set isolation level to {level}")
