"""
Amazon Redshift database connection implementation.

Provides connection lifecycle, table introspection via INFORMATION_SCHEMA and pg_catalog,
transaction support, and DDL execution for Amazon Redshift.
"""

from typing import Optional, Dict, Any, List
import logging
import pandas as pd

try:
    import redshift_connector
    REDSHIFT_AVAILABLE = True
    USING_REDSHIFT_CONNECTOR = True
except ImportError:
    REDSHIFT_AVAILABLE = False
    USING_REDSHIFT_CONNECTOR = False
    redshift_connector = None
    # Fallback to psycopg2 if redshift-connector not available
    try:
        import psycopg2
        from psycopg2 import ProgrammingError, OperationalError, DatabaseError
        REDSHIFT_AVAILABLE = True
        USING_REDSHIFT_CONNECTOR = False
    except ImportError:
        psycopg2 = None
        ProgrammingError = Exception
        OperationalError = Exception
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
    parse_column_row,
    parse_table_exists_result,
    parse_list_tables_result,
)
from ...canonical import CanonicalSchema, ColumnDefinition, OptimizationHints

logger = logging.getLogger(__name__)


class RedshiftConnection(BaseConnection):
    """
    Amazon Redshift database connection.

    Features:
    - Connection via redshift-connector (preferred) or psycopg2 (fallback)
    - Table introspection via INFORMATION_SCHEMA and pg_catalog
    - Full transaction support
    - Type mapping: Redshift → LogicalType
    - DISTKEY, SORTKEY, and SUPER type metadata extraction
    - DDL execution with COPY command support

    Configuration:
        host: Redshift cluster endpoint (required)
        port: Redshift port (default: 5439)
        database: Database name (required)
        user: Redshift username (required)
        password: Redshift password (required)
        schema: Schema name (optional, default: public)
        sslmode: SSL mode (optional, default: prefer)
        region: AWS region (optional, for redshift-connector)

    Examples:
        >>> config = {
        ...     'host': 'my-cluster.abc123.us-west-2.redshift.amazonaws.com',
        ...     'port': 5439,
        ...     'database': 'analytics',
        ...     'user': 'admin',
        ...     'password': 'secret',
        ...     'schema': 'public'
        ... }
        >>> conn = RedshiftConnection(config)
        >>> conn.connect()
        >>> schema = conn.get_target_schema('customers')
        >>> conn.disconnect()

        With transactions:
        >>> with RedshiftConnection(config) as conn:
        ...     conn.begin_transaction()
        ...     try:
        ...         conn.execute_ddl(ddl1)
        ...         conn.execute_ddl(ddl2)
        ...         conn.commit()
        ...     except Exception:
        ...         conn.rollback()

        COPY command:
        >>> copy_sql = '''
        ...     COPY public.sales
        ...     FROM 's3://mybucket/sales.csv'
        ...     IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
        ...     CSV
        ... '''
        >>> conn.execute_ddl(copy_sql)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Redshift connection.

        Args:
            config: Configuration dictionary with Redshift credentials

        Raises:
            ConfigurationError: If configuration is invalid
            ImportError: If neither redshift-connector nor psycopg2 installed
        """
        if not REDSHIFT_AVAILABLE:
            raise ImportError(
                "redshift-connector or psycopg2-binary is not installed. "
                "Install with: pip install schema-mapper[redshift]"
            )

        super().__init__(config)

        # Redshift-specific configuration
        self.host = config.get('host')
        self.port = config.get('port', 5439)
        self.database = config.get('database')
        self.user = config.get('user')
        self.password = config.get('password')
        self.schema = config.get('schema', 'public')
        self.sslmode = config.get('sslmode', 'prefer')
        self.region = config.get('region')  # For redshift-connector

        self._using_redshift_connector = USING_REDSHIFT_CONNECTOR

    def platform_name(self) -> str:
        """Return platform name."""
        return 'redshift'

    def _validate_config(self) -> None:
        """
        Validate Redshift configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        validate_credentials(self.config, 'redshift')

    @retry_on_transient_error(max_retries=3, platform='redshift')
    def connect(self) -> bool:
        """
        Establish Redshift connection.

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If credentials are invalid
        """
        try:
            self.state = ConnectionState.CONNECTING
            self.logger.info(
                f"Connecting to Redshift: {self.host}:{self.port}/{self.database} "
                f"(using {'redshift-connector' if self._using_redshift_connector else 'psycopg2'})"
            )

            if self._using_redshift_connector:
                # Use redshift-connector
                conn_params = {
                    'host': self.host,
                    'port': self.port,
                    'database': self.database,
                    'user': self.user,
                    'password': self.password,
                }

                # Add optional parameters
                if self.region:
                    conn_params['region'] = self.region
                if self.sslmode:
                    conn_params['ssl'] = (self.sslmode != 'disable')

                self._connection = redshift_connector.connect(**conn_params)

            else:
                # Use psycopg2 fallback
                conn_params = {
                    'host': self.host,
                    'port': self.port,
                    'database': self.database,
                    'user': self.user,
                    'password': self.password,
                    'sslmode': self.sslmode,
                }

                self._connection = psycopg2.connect(**conn_params)

            # Create cursor
            self._cursor = self._connection.cursor()

            self.state = ConnectionState.CONNECTED
            self.logger.info(f"Successfully connected to Redshift: {self.host}:{self.port}/{self.database}")
            return True

        except (OperationalError, DatabaseError) as e:
            self.state = ConnectionState.ERROR
            # Check if authentication error
            error_str = str(e).lower()
            if 'authentication failed' in error_str or 'password authentication failed' in error_str:
                raise AuthenticationError(
                    f"Authentication failed: {e}",
                    platform=self.platform_name()
                )
            raise ConnectionError(
                f"Failed to connect to Redshift: {e}",
                platform=self.platform_name()
            )
        except Exception as e:
            self.state = ConnectionState.ERROR
            raise ConnectionError(
                f"Failed to connect to Redshift: {e}",
                platform=self.platform_name()
            )

    def disconnect(self) -> None:
        """Close Redshift connection."""
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
            self.logger.info("Disconnected from Redshift")

        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")

    def test_connection(self) -> bool:
        """
        Test if Redshift connection is working.

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
        Check if Redshift table exists.

        Args:
            table_name: Table name
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (ignored for Redshift, uses configured database)

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
                platform='redshift'
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
        Introspect Redshift table and return CanonicalSchema.

        Queries INFORMATION_SCHEMA and Redshift-specific system tables to get:
        - Column names and types
        - Nullability
        - Precision and scale
        - DISTKEY, SORTKEY, and encoding information

        Args:
            table_name: Table name
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (ignored for Redshift)

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
            query = build_columns_query(table_name, schema, platform='redshift')
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

                columns.append(self._convert_redshift_row_to_column(row_dict))

            # Extract optimization hints (DISTKEY, SORTKEY)
            optimization = self._extract_optimization_hints(table_name, schema)

            # Create canonical schema
            canonical = CanonicalSchema(
                table_name=table_name,
                dataset_name=schema,
                project_id=self.database,
                columns=columns,
                optimization=optimization,
                created_from="Redshift"
            )

            self.logger.info(
                f"Introspected table {schema}.{table_name}: "
                f"{len(columns)} columns, "
                f"sortkeys={len(optimization.cluster_columns or [])}"
            )

            return canonical

        except (TableNotFoundError, IntrospectionError):
            raise
        except Exception as e:
            raise IntrospectionError(
                f"Failed to introspect table {table_name}: {e}",
                platform=self.platform_name()
            )

    def _convert_redshift_row_to_column(self, row_dict: Dict[str, Any]) -> ColumnDefinition:
        """
        Convert INFORMATION_SCHEMA row to ColumnDefinition.

        Args:
            row_dict: Row dictionary from INFORMATION_SCHEMA.COLUMNS

        Returns:
            ColumnDefinition
        """
        # Parse row
        col_info = parse_column_row(row_dict, 'redshift')

        # Map Redshift type to LogicalType
        logical_type = map_to_logical_type(
            col_info['data_type'],
            'redshift',
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
        Extract DISTKEY, SORTKEY, and encoding info from Redshift table.

        Args:
            table_name: Table name
            schema_name: Schema name

        Returns:
            OptimizationHints with Redshift-specific optimization information
        """
        hints = OptimizationHints()

        try:
            # Query for SORTKEY columns (use for cluster_columns)
            sortkey_query = """
                SELECT
                    "column" AS column_name,
                    sortkey AS sort_order
                FROM
                    pg_table_def
                WHERE
                    schemaname = %s
                    AND tablename = %s
                    AND sortkey > 0
                ORDER BY
                    sortkey
            """

            self._cursor.execute(sortkey_query, (schema_name, table_name))
            sortkey_rows = self._cursor.fetchall()

            # Extract SORTKEY columns
            sortkey_columns = []
            for row in sortkey_rows:
                sortkey_columns.append(row[0])  # column_name

            if sortkey_columns:
                hints.cluster_columns = sortkey_columns

            # Query for DISTKEY column
            distkey_query = """
                SELECT
                    "column" AS column_name
                FROM
                    pg_table_def
                WHERE
                    schemaname = %s
                    AND tablename = %s
                    AND distkey = true
            """

            self._cursor.execute(distkey_query, (schema_name, table_name))
            distkey_result = self._cursor.fetchone()

            if distkey_result:
                # Store DISTKEY in partition_columns (conceptually similar)
                hints.partition_columns = [distkey_result[0]]

        except Exception as e:
            self.logger.warning(f"Could not extract optimization hints: {e}")

        return hints

    def list_tables(
        self,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> List[str]:
        """
        List tables in Redshift schema.

        Args:
            schema_name: Schema name (uses configured schema if not provided)
            database_name: Database name (ignored for Redshift)

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
            query = build_list_tables_query(schema, platform='redshift')
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
            database_name: Database name (ignored for Redshift)

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
                    CASE
                        WHEN t.tablename IS NOT NULL THEN 'TABLE'
                        WHEN v.viewname IS NOT NULL THEN 'VIEW'
                    END as table_type,
                    ROUND(COALESCE(s.size, 0) / 1024.0, 2) as size_mb
                FROM pg_catalog.pg_tables t
                FULL OUTER JOIN pg_catalog.pg_views v
                    ON t.tablename = v.viewname AND t.schemaname = v.schemaname
                LEFT JOIN (
                    SELECT
                        tablename,
                        SUM(size) as size
                    FROM svv_table_info
                    WHERE schema = '{schema}'
                    GROUP BY tablename
                ) s ON COALESCE(t.tablename, v.viewname) = s.tablename
                WHERE COALESCE(t.schemaname, v.schemaname) = '{schema}'
                ORDER BY COALESCE(t.tablename, v.viewname)
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
        List all schemas in Redshift database and return as a pandas DataFrame.

        Args:
            database_name: Database name (ignored for Redshift, uses current database)

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

            self.logger.info("Listing schemas in Redshift database")

            # Query to list schemas
            query = """
                SELECT
                    schema_name,
                    schema_owner
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
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
            database_name: Database name (ignored for Redshift, uses current database)
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

            self.logger.info("Building database tree for Redshift")

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
        Execute DDL statement in Redshift.

        Supports standard DDL (CREATE, ALTER, DROP) and COPY commands.

        Args:
            ddl: DDL statement or COPY command

        Returns:
            True if successful

        Raises:
            ExecutionError: If DDL execution fails

        Examples:
            >>> ddl = "CREATE TABLE public.users (id BIGINT, name VARCHAR(100))"
            >>> conn.execute_ddl(ddl)
            True

            >>> # COPY command
            >>> copy = '''
            ...     COPY public.sales
            ...     FROM 's3://mybucket/sales.csv'
            ...     IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
            ...     CSV
            ... '''
            >>> conn.execute_ddl(copy)
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

        except (ProgrammingError, DatabaseError) as e:
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
        Execute query in Redshift and return results as a pandas DataFrame.

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

    def begin_transaction(self) -> None:
        """
        Begin Redshift transaction.

        Raises:
            TransactionError: If transaction fails to start
        """
        try:
            self.require_connection()

            if self._transaction_active:
                self.logger.warning("Transaction already active")
                return

            # Redshift transactions start automatically, just track state
            self._transaction_active = True
            self.logger.info("Transaction started")

        except Exception as e:
            raise TransactionError(
                f"Failed to begin transaction: {e}",
                platform=self.platform_name()
            )

    def commit(self) -> None:
        """
        Commit Redshift transaction.

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
        Rollback Redshift transaction.

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

    def execute_copy_command(
        self,
        table_name: str,
        s3_path: str,
        iam_role: str,
        schema_name: Optional[str] = None,
        file_format: str = 'CSV',
        options: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Execute Redshift COPY command to load data from S3.

        Convenience method for COPY command construction.

        Args:
            table_name: Target table name
            s3_path: S3 path (e.g., 's3://mybucket/data.csv')
            iam_role: IAM role ARN for S3 access
            schema_name: Schema name (optional, uses configured schema)
            file_format: File format ('CSV', 'JSON', 'PARQUET', etc.)
            options: Additional COPY options (e.g., {'DELIMITER': ',', 'IGNOREHEADER': 1})

        Returns:
            True if successful

        Raises:
            ExecutionError: If COPY command fails

        Examples:
            >>> conn.execute_copy_command(
            ...     table_name='sales',
            ...     s3_path='s3://mybucket/sales.csv',
            ...     iam_role='arn:aws:iam::123456789012:role/MyRedshiftRole',
            ...     file_format='CSV',
            ...     options={'DELIMITER': ',', 'IGNOREHEADER': 1}
            ... )
            True
        """
        schema = schema_name or self.schema
        full_table_name = f"{schema}.{table_name}"

        # Build COPY command
        copy_cmd = f"COPY {full_table_name}\n"
        copy_cmd += f"FROM '{s3_path}'\n"
        copy_cmd += f"IAM_ROLE '{iam_role}'\n"
        copy_cmd += f"{file_format}\n"

        # Add options
        if options:
            for key, value in options.items():
                if isinstance(value, str):
                    copy_cmd += f"{key} '{value}'\n"
                elif isinstance(value, bool):
                    if value:
                        copy_cmd += f"{key}\n"
                else:
                    copy_cmd += f"{key} {value}\n"

        return self.execute_ddl(copy_cmd)
