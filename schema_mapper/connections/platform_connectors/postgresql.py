"""
PostgreSQL database connection implementation.

Provides connection lifecycle, table introspection via INFORMATION_SCHEMA and pg_catalog,
transaction support with isolation levels, and DDL execution for PostgreSQL.
"""

from typing import Optional, Dict, Any, List
import logging
import pandas as pd

try:
    import psycopg2
    from psycopg2 import ProgrammingError, OperationalError, DatabaseError
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_SERIALIZABLE
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    psycopg2 = None
    ProgrammingError = Exception
    OperationalError = Exception
    DatabaseError = Exception
    ISOLATION_LEVEL_AUTOCOMMIT = None
    ISOLATION_LEVEL_READ_COMMITTED = None
    ISOLATION_LEVEL_REPEATABLE_READ = None
    ISOLATION_LEVEL_SERIALIZABLE = None

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


class PostgreSQLConnection(BaseConnection):
    """
    PostgreSQL database connection.

    Features:
    - Connection via psycopg2-binary
    - Table introspection via INFORMATION_SCHEMA and pg_catalog
    - Full transaction support with isolation levels
    - Type mapping: PostgreSQL → LogicalType
    - Index and partition metadata extraction
    - DDL execution with COPY command support

    Configuration:
        host: PostgreSQL host (required, default: localhost)
        port: PostgreSQL port (default: 5432)
        database: Database name (required)
        user: PostgreSQL username (required)
        password: PostgreSQL password (required)
        schema: Schema name (optional, default: public)
        sslmode: SSL mode (optional, default: prefer)
        isolation_level: Transaction isolation level (optional, default: read_committed)

    Examples:
        >>> config = {
        ...     'host': 'localhost',
        ...     'port': 5432,
        ...     'database': 'analytics',
        ...     'user': 'admin',
        ...     'password': 'secret',
        ...     'schema': 'public'
        ... }
        >>> conn = PostgreSQLConnection(config)
        >>> conn.connect()
        >>> schema = conn.get_target_schema('customers')
        >>> conn.disconnect()

        With transactions:
        >>> with PostgreSQLConnection(config) as conn:
        ...     conn.begin_transaction(isolation_level='repeatable_read')
        ...     try:
        ...         conn.execute_ddl(ddl1)
        ...         conn.execute_ddl(ddl2)
        ...         conn.commit()
        ...     except Exception:
        ...         conn.rollback()
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL connection.

        Args:
            config: Configuration dictionary with PostgreSQL credentials

        Raises:
            ConfigurationError: If configuration is invalid
            ImportError: If psycopg2-binary not installed
        """
        if not POSTGRESQL_AVAILABLE:
            raise ImportError(
                "psycopg2-binary is not installed. "
                "Install with: pip install schema-mapper[postgresql]"
            )

        super().__init__(config)

        # PostgreSQL-specific configuration
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 5432)
        self.database = config.get('database')
        self.user = config.get('user')
        self.password = config.get('password')
        self.schema = config.get('schema', 'public')
        self.sslmode = config.get('sslmode', 'prefer')
        self.isolation_level = config.get('isolation_level', 'read_committed')

    def platform_name(self) -> str:
        """Return platform name."""
        return 'postgresql'

    def _validate_config(self) -> None:
        """
        Validate PostgreSQL configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        validate_credentials(self.config, 'postgresql')

    @retry_on_transient_error(max_retries=3, platform='postgresql')
    def connect(self) -> bool:
        """
        Establish PostgreSQL connection.

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If credentials are invalid
        """
        try:
            self.state = ConnectionState.CONNECTING
            self.logger.info(
                f"Connecting to PostgreSQL: {self.host}:{self.port}/{self.database}"
            )

            # Build connection parameters
            conn_params = {
                'host': self.host,
                'port': self.port,
                'database': self.database,
                'user': self.user,
                'password': self.password,
                'sslmode': self.sslmode,
            }

            # Establish connection
            self._connection = psycopg2.connect(**conn_params)

            # Set isolation level
            self._set_isolation_level(self.isolation_level)

            # Create cursor
            self._cursor = self._connection.cursor()

            self.state = ConnectionState.CONNECTED
            self.logger.info(f"Successfully connected to PostgreSQL: {self.host}:{self.port}/{self.database}")
            return True

        except OperationalError as e:
            self.state = ConnectionState.ERROR
            # Check if authentication error
            if 'authentication failed' in str(e).lower() or 'password authentication failed' in str(e).lower():
                raise AuthenticationError(
                    f"Authentication failed: {e}",
                    platform=self.platform_name()
                )
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {e}",
                platform=self.platform_name()
            )
        except Exception as e:
            self.state = ConnectionState.ERROR
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {e}",
                platform=self.platform_name()
            )

    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
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
            self.logger.info("Disconnected from PostgreSQL")

        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")

    def test_connection(self) -> bool:
        """
        Test if PostgreSQL connection is working.

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
        Check if PostgreSQL table exists.

        Args:
            table_name: Table name
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (ignored for PostgreSQL, uses configured database)

        Returns:
            True if table exists

        Examples:
            >>> conn.table_exists('customers', schema_name='public')
            True
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema

            # Build and execute query
            query = build_table_exists_query(
                table_name,
                schema,
                platform='postgresql'
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
        Introspect PostgreSQL table and return CanonicalSchema.

        Queries INFORMATION_SCHEMA and pg_catalog to get:
        - Column names and types
        - Nullability
        - Precision and scale
        - Indexes and partitioning information

        Args:
            table_name: Table name
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (ignored for PostgreSQL)

        Returns:
            CanonicalSchema representing the table

        Raises:
            TableNotFoundError: If table doesn't exist
            IntrospectionError: If cannot read table schema

        Examples:
            >>> schema = conn.get_target_schema('orders', schema_name='public')
            >>> print(f"Table has {len(schema.columns)} columns")
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema

            # Check if table exists
            if not self.table_exists(table_name, schema):
                raise TableNotFoundError(
                    f"Table not found: {schema}.{table_name}",
                    table_name=table_name,
                    schema_name=schema,
                    platform=self.platform_name()
                )

            # Query column information
            query = build_columns_query(table_name, schema, platform='postgresql')
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

                columns.append(self._convert_pg_row_to_column(row_dict))

            # Extract optimization hints (indexes, partitioning)
            optimization = self._extract_optimization_hints(table_name, schema)

            # Get table description
            description = self._get_table_description(table_name, schema)

            # Create canonical schema
            canonical = CanonicalSchema(
                table_name=table_name,
                dataset_name=schema,
                project_id=self.database,
                columns=columns,
                optimization=optimization,
                description=description,
                created_from="PostgreSQL"
            )

            self.logger.info(
                f"Introspected table {schema}.{table_name}: "
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

    def _convert_pg_row_to_column(self, row_dict: Dict[str, Any]) -> ColumnDefinition:
        """
        Convert INFORMATION_SCHEMA row to ColumnDefinition.

        Args:
            row_dict: Row dictionary from INFORMATION_SCHEMA.COLUMNS

        Returns:
            ColumnDefinition
        """
        # Parse row
        col_info = parse_column_row(row_dict, 'postgresql')

        # Map PostgreSQL type to LogicalType
        logical_type = map_to_logical_type(
            col_info['data_type'],
            'postgresql',
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
        schema_name: str
    ) -> OptimizationHints:
        """
        Extract indexes and partitioning info from PostgreSQL table.

        Args:
            table_name: Table name
            schema_name: Schema name

        Returns:
            OptimizationHints with index and partition information
        """
        hints = OptimizationHints()

        try:
            # Query for indexes (use for clustering hints)
            index_query = """
                SELECT
                    i.relname AS index_name,
                    a.attname AS column_name,
                    ix.indisclustered AS is_clustered
                FROM
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a,
                    pg_namespace n
                WHERE
                    t.oid = ix.indrelid
                    AND i.oid = ix.indexrelid
                    AND a.attrelid = t.oid
                    AND a.attnum = ANY(ix.indkey)
                    AND t.relkind = 'r'
                    AND t.relname = %s
                    AND n.oid = t.relnamespace
                    AND n.nspname = %s
                ORDER BY
                    i.relname,
                    array_position(ix.indkey, a.attnum)
            """

            self._cursor.execute(index_query, (table_name, schema_name))
            index_rows = self._cursor.fetchall()

            # Extract clustered index columns
            clustered_columns = []
            for row in index_rows:
                if row[2]:  # is_clustered
                    clustered_columns.append(row[1])  # column_name

            if clustered_columns:
                hints.cluster_columns = clustered_columns

            # Query for partitioning info (PostgreSQL 10+)
            partition_query = """
                SELECT
                    pg_get_partkeydef(c.oid) AS partition_key
                FROM
                    pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE
                    c.relname = %s
                    AND n.nspname = %s
                    AND c.relkind = 'p'
            """

            self._cursor.execute(partition_query, (table_name, schema_name))
            partition_result = self._cursor.fetchone()

            if partition_result and partition_result[0]:
                # Parse partition key (e.g., "RANGE (created_date)")
                partition_key = partition_result[0]
                # Extract column name from partition key definition
                import re
                match = re.search(r'\(([^)]+)\)', partition_key)
                if match:
                    partition_columns = [col.strip() for col in match.group(1).split(',')]
                    hints.partition_columns = partition_columns

        except Exception as e:
            self.logger.warning(f"Could not extract optimization hints: {e}")

        return hints

    def _get_table_description(
        self,
        table_name: str,
        schema_name: str
    ) -> Optional[str]:
        """
        Get table description/comment from pg_catalog.

        Args:
            table_name: Table name
            schema_name: Schema name

        Returns:
            Table description or None
        """
        try:
            query = """
                SELECT
                    obj_description(c.oid, 'pg_class') AS table_comment
                FROM
                    pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE
                    c.relname = %s
                    AND n.nspname = %s
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
        List tables in PostgreSQL schema.

        Args:
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (ignored for PostgreSQL)

        Returns:
            List of table names

        Examples:
            >>> tables = conn.list_tables(schema_name='public')
            >>> print(f"Found {len(tables)} tables")
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema

            # Build and execute query
            query = build_list_tables_query(schema, platform='postgresql')
            self._cursor.execute(query)
            result = self._cursor.fetchall()

            table_names = parse_list_tables_result(result)

            self.logger.info(f"Listed {len(table_names)} tables in {schema}")
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
            database_name: Database name (ignored for PostgreSQL)

        Returns:
            pandas DataFrame with columns: table_name, table_type, size_mb

        Examples:
            >>> tables = conn.get_tables(schema_name='public')
            >>> print(tables)
              table_name table_type  size_mb
            0      users      TABLE    245.5
            1     events      TABLE   8920.3
            2  user_view       VIEW      0.0
        """
        try:
            self.require_connection()

            schema = schema_name or self.schema

            self.logger.info(f"Getting table details for schema: {schema}")

            # Query to get table details
            query = f"""
                SELECT
                    t.tablename as table_name,
                    'TABLE' as table_type,
                    ROUND(pg_total_relation_size(quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::numeric / (1024 * 1024), 2) as size_mb
                FROM pg_catalog.pg_tables t
                WHERE t.schemaname = '{schema}'
                UNION ALL
                SELECT
                    v.viewname as table_name,
                    'VIEW' as table_type,
                    0.0 as size_mb
                FROM pg_catalog.pg_views v
                WHERE v.schemaname = '{schema}'
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
        List all schemas in PostgreSQL database and return as a pandas DataFrame.

        Args:
            database_name: Database name (ignored for PostgreSQL, uses current database)

        Returns:
            pandas DataFrame with columns: schema_name, schema_owner

        Examples:
            >>> schemas = conn.get_schemas()
            >>> print(schemas)
              schema_name schema_owner
            0      public     postgres
            1   analytics      analyst
        """
        try:
            self.require_connection()

            self.logger.info("Listing schemas in PostgreSQL database")

            # Query to list schemas
            query = """
                SELECT
                    schema_name,
                    schema_owner
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
            """

            self._cursor.execute(query)
            columns = [desc[0] for desc in self._cursor.description]
            data = self._cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)

            self.logger.info(f"Found {len(df)} schemas")

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
            database_name: Database name (ignored for PostgreSQL, uses current database)
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

            self.logger.info("Building database tree for PostgreSQL")

            # Get schemas
            schemas_df = self.get_schemas()

            tree_data = []
            for _, schema_row in schemas_df.iterrows():
                schema_name = schema_row['schema_name']

                schema_info = {
                    'schema_name': schema_name,
                }

                # Get tables for this schema
                try:
                    tables = self.list_tables(schema_name=schema_name)

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
                    'database': self.database or 'current',
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
                        'database': self.database or 'current',
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
        Execute DDL statement in PostgreSQL.

        Args:
            ddl: DDL statement (CREATE, ALTER, DROP)

        Returns:
            True if successful

        Raises:
            ExecutionError: If DDL execution fails

        Examples:
            >>> ddl = "CREATE TABLE public.users (id BIGINT, name VARCHAR(100))"
            >>> conn.execute_ddl(ddl)
            True
        """
        try:
            self.require_connection()
            self.logger.info(f"Executing DDL: {ddl[:100]}...")

            self._cursor.execute(ddl)

            # Auto-commit if not in transaction
            if not self._transaction_active:
                self._connection.commit()

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
        Execute query in PostgreSQL and return results as a pandas DataFrame.

        Args:
            query: SQL query

        Returns:
            pandas DataFrame with query results

        Raises:
            ExecutionError: If query execution fails
            ConnectionError: If not connected

        Examples:
            >>> result = conn.execute_query("SELECT COUNT(*) FROM public.users")
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

        except (ProgrammingError, OperationalError, DatabaseError) as e:
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
        Begin PostgreSQL transaction with optional isolation level.

        Args:
            isolation_level: Isolation level ('autocommit', 'read_committed',
                           'repeatable_read', 'serializable')

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

            # PostgreSQL transactions start automatically, just track state
            self._transaction_active = True
            self.logger.info(f"Transaction started (isolation: {isolation_level or 'default'})")

        except Exception as e:
            raise TransactionError(
                f"Failed to begin transaction: {e}",
                platform=self.platform_name()
            )

    def commit(self) -> None:
        """
        Commit PostgreSQL transaction.

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
        Rollback PostgreSQL transaction.

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
        Create a savepoint within a PostgreSQL transaction.

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
            ...     conn.execute_ddl("UPDATE users SET active = true")
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

            # Create savepoint
            self._cursor.execute(f"SAVEPOINT {name}")
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
        Rollback to a PostgreSQL savepoint.

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

            self._cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")
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
        Release a PostgreSQL savepoint.

        Args:
            name: Savepoint name

        Raises:
            TransactionError: If release fails
            ConnectionError: If not connected

        Examples:
            >>> conn.release_savepoint('sp1')
        """
        try:
            self.require_connection()

            if not self._transaction_active:
                raise TransactionError(
                    "Cannot release savepoint without active transaction",
                    platform=self.platform_name()
                )

            self._cursor.execute(f"RELEASE SAVEPOINT {name}")
            self.logger.debug(f"Released savepoint: {name}")

        except TransactionError:
            raise
        except Exception as e:
            raise TransactionError(
                f"Failed to release savepoint: {e}",
                platform=self.platform_name()
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
            'autocommit': ISOLATION_LEVEL_AUTOCOMMIT,
            'read_committed': ISOLATION_LEVEL_READ_COMMITTED,
            'repeatable_read': ISOLATION_LEVEL_REPEATABLE_READ,
            'serializable': ISOLATION_LEVEL_SERIALIZABLE,
        }

        level_lower = level.lower()
        if level_lower not in level_map:
            raise ValueError(
                f"Invalid isolation level: {level}. "
                f"Valid options: {', '.join(level_map.keys())}"
            )

        self._connection.set_isolation_level(level_map[level_lower])
        self.logger.debug(f"Set isolation level to {level}")
