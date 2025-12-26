"""
BigQuery database connection implementation.

Provides connection lifecycle, table introspection, and DDL execution
for Google Cloud BigQuery.
"""

from typing import Optional, Dict, Any, List
import logging
import pandas as pd

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound, GoogleCloudError
    from google.oauth2 import service_account
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    bigquery = None
    NotFound = Exception
    GoogleCloudError = Exception
    service_account = None

from ..base import BaseConnection, ConnectionState
from ..exceptions import (
    ConnectionError,
    AuthenticationError,
    TableNotFoundError,
    ExecutionError,
    IntrospectionError,
)
from ..utils.retry import retry_on_transient_error
from ..utils.validation import validate_credentials
from ..utils.type_mapping import map_to_logical_type, extract_precision_scale
from ...canonical import CanonicalSchema, ColumnDefinition, OptimizationHints

logger = logging.getLogger(__name__)


class BigQueryConnection(BaseConnection):
    """
    BigQuery database connection.

    Features:
    - Connection via google-cloud-bigquery client
    - Table introspection with clustering/partitioning metadata
    - Type mapping: BigQuery → LogicalType
    - DDL execution with dataset.table format
    - Query execution with pandas DataFrame results
    - Integration with BigQueryRenderer

    Configuration:
        project: GCP project ID (required)
        credentials_path: Path to service account JSON (optional, uses default credentials if not provided)
        location: Dataset location (default: US)
        dataset: Default dataset name (optional)

    Important:
        BigQuery requires fully-qualified table names in queries. Use the format
        `dataset.table` (e.g., FROM analytics.users) or `project.dataset.table`.

    Examples:
        >>> config = {
        ...     'project': 'my-gcp-project',
        ...     'credentials_path': '/path/to/service-account.json',
        ...     'location': 'US'
        ... }
        >>> conn = BigQueryConnection(config)
        >>> conn.connect()
        >>> schema = conn.get_target_schema('users', schema_name='analytics')
        >>>
        >>> # Execute query (returns pandas DataFrame)
        >>> df = conn.execute_query("SELECT * FROM analytics.users LIMIT 10")
        >>> print(type(df))  # <class 'pandas.core.frame.DataFrame'>
        >>> conn.disconnect()

        Context manager:
        >>> with BigQueryConnection(config) as conn:
        ...     tables = conn.list_tables(schema_name='analytics')
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize BigQuery connection.

        Args:
            config: Configuration dictionary with BigQuery credentials

        Raises:
            ConfigurationError: If configuration is invalid
            ImportError: If google-cloud-bigquery not installed
        """
        if not BIGQUERY_AVAILABLE:
            raise ImportError(
                "google-cloud-bigquery is not installed. "
                "Install with: pip install schema-mapper[bigquery]"
            )

        super().__init__(config)

        # BigQuery-specific configuration
        self.project_id = config.get('project')
        self.location = config.get('location', 'US')
        self.credentials_path = config.get('credentials_path')
        self.default_dataset = config.get('dataset')

    def platform_name(self) -> str:
        """Return platform name."""
        return 'bigquery'

    def _validate_config(self) -> None:
        """
        Validate BigQuery configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        validate_credentials(self.config, 'bigquery')

    @retry_on_transient_error(max_retries=3, platform='bigquery')
    def connect(self) -> bool:
        """
        Establish BigQuery client connection.

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If credentials are invalid
        """
        try:
            self.state = ConnectionState.CONNECTING
            self.logger.info(f"Connecting to BigQuery project: {self.project_id}")

            # Load credentials if path provided
            if self.credentials_path:
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        self.credentials_path
                    )
                    self._connection = bigquery.Client(
                        project=self.project_id,
                        credentials=credentials,
                        location=self.location
                    )
                    self.logger.info("Using service account credentials")
                except Exception as e:
                    raise AuthenticationError(
                        f"Failed to load service account credentials: {e}",
                        platform=self.platform_name(),
                        details={'credentials_path': self.credentials_path}
                    )
            else:
                # Use default credentials (from environment)
                try:
                    self._connection = bigquery.Client(
                        project=self.project_id,
                        location=self.location
                    )
                    self.logger.info("Using default credentials")
                except Exception as e:
                    raise AuthenticationError(
                        f"Failed to use default credentials: {e}. "
                        "Set GOOGLE_APPLICATION_CREDENTIALS or provide credentials_path.",
                        platform=self.platform_name()
                    )

            self.state = ConnectionState.CONNECTED
            self.logger.info(f"Successfully connected to BigQuery project: {self.project_id}")
            return True

        except (AuthenticationError, ConnectionError):
            self.state = ConnectionState.ERROR
            raise
        except Exception as e:
            self.state = ConnectionState.ERROR
            raise ConnectionError(
                f"Failed to connect to BigQuery: {e}",
                platform=self.platform_name()
            )

    def disconnect(self) -> None:
        """
        Close BigQuery client connection.

        BigQuery client doesn't require explicit disconnection,
        but we clean up the reference.
        """
        if self._connection:
            self._connection.close()
            self._connection = None
        self.state = ConnectionState.DISCONNECTED
        self.logger.info("Disconnected from BigQuery")

    def test_connection(self) -> bool:
        """
        Test if BigQuery connection is working.

        Tests by listing datasets (lightweight operation).

        Returns:
            True if connection is healthy
        """
        try:
            self.require_connection()
            # List datasets (limit 1 for speed)
            list(self._connection.list_datasets(max_results=1))
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
        Check if BigQuery table exists.

        Args:
            table_name: Table name
            schema_name: Dataset name (uses default if not provided)
            database_name: Project ID (uses configured project if not provided)

        Returns:
            True if table exists

        Examples:
            >>> conn.table_exists('users', schema_name='analytics')
            True
        """
        try:
            self.require_connection()
            table_id = self._build_table_id(table_name, schema_name, database_name)
            self._connection.get_table(table_id)
            return True
        except NotFound:
            return False
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
        Introspect BigQuery table and return CanonicalSchema.

        Converts BigQuery schema to canonical representation:
        - Maps BigQuery types to LogicalTypes
        - Extracts clustering and partitioning metadata
        - Preserves nullability and descriptions

        Args:
            table_name: Table name
            schema_name: Dataset name (uses default if not provided)
            database_name: Project ID (uses configured project if not provided)

        Returns:
            CanonicalSchema representing the table

        Raises:
            TableNotFoundError: If table doesn't exist
            IntrospectionError: If cannot read table schema

        Examples:
            >>> schema = conn.get_target_schema('events', schema_name='analytics')
            >>> print(f"Table has {len(schema.columns)} columns")
            >>> # Use with different renderer
            >>> from schema_mapper.renderers import RendererFactory
            >>> renderer = RendererFactory.get_renderer('snowflake', schema)
            >>> print(renderer.to_ddl())  # BigQuery → Snowflake DDL!
        """
        try:
            self.require_connection()
            table_id = self._build_table_id(table_name, schema_name, database_name)

            # Get table metadata
            try:
                table = self._connection.get_table(table_id)
            except NotFound:
                raise TableNotFoundError(
                    f"Table not found: {table_id}",
                    table_name=table_name,
                    schema_name=schema_name or self.default_dataset,
                    database_name=database_name or self.project_id,
                    platform=self.platform_name()
                )

            # Convert BigQuery schema to canonical columns
            columns = []
            for field in table.schema:
                columns.append(self._convert_bq_field_to_column(field))

            # Extract optimization hints
            optimization = self._extract_optimization_hints(table)

            # Create canonical schema
            canonical = CanonicalSchema(
                table_name=table_name,
                dataset_name=schema_name or table.dataset_id,
                project_id=database_name or self.project_id,
                columns=columns,
                optimization=optimization,
                description=table.description,
                created_from="BigQuery"
            )

            self.logger.info(
                f"Introspected table {table_id}: "
                f"{len(columns)} columns, "
                f"clustering={optimization.cluster_columns}, "
                f"partitioning={optimization.partition_columns}"
            )

            return canonical

        except TableNotFoundError:
            raise
        except Exception as e:
            raise IntrospectionError(
                f"Failed to introspect table {table_name}: {e}",
                platform=self.platform_name()
            )

    def _convert_bq_field_to_column(self, field) -> ColumnDefinition:
        """
        Convert BigQuery field to ColumnDefinition.

        Args:
            field: BigQuery SchemaField

        Returns:
            ColumnDefinition
        """
        # Map BigQuery type to LogicalType
        bq_type = field.field_type
        precision, scale = extract_precision_scale(bq_type)

        logical_type = map_to_logical_type(
            bq_type,
            'bigquery',
            precision=precision,
            scale=scale
        )

        # Determine nullability (BigQuery default is NULLABLE)
        nullable = (field.mode != 'REQUIRED')

        # Extract max_length for STRING types
        max_length = None
        if bq_type in ('STRING', 'BYTES') and hasattr(field, 'max_length'):
            max_length = field.max_length

        return ColumnDefinition(
            name=field.name,
            logical_type=logical_type,
            nullable=nullable,
            description=field.description,
            max_length=max_length,
            precision=precision,
            scale=scale,
            original_name=field.name
        )

    def _extract_optimization_hints(self, table) -> OptimizationHints:
        """
        Extract clustering and partitioning metadata from BigQuery table.

        Args:
            table: BigQuery Table object

        Returns:
            OptimizationHints
        """
        hints = OptimizationHints()

        # Extract clustering
        if table.clustering_fields:
            hints.cluster_columns = list(table.clustering_fields)

        # Extract partitioning
        if table.time_partitioning:
            if table.time_partitioning.field:
                # Column-based partitioning
                hints.partition_columns = [table.time_partitioning.field]
            else:
                # Ingestion-time partitioning (_PARTITIONTIME or _PARTITIONDATE)
                # We don't add a partition column for ingestion-time partitioning
                # as it's not a real column
                pass

            # Extract partition expiration
            if table.time_partitioning.expiration_ms:
                hints.partition_expiration_days = (
                    table.time_partitioning.expiration_ms // (1000 * 60 * 60 * 24)
                )

            # Extract require partition filter
            if hasattr(table.time_partitioning, 'require_partition_filter'):
                hints.require_partition_filter = table.time_partitioning.require_partition_filter

        # Extract range partitioning
        if table.range_partitioning:
            if table.range_partitioning.field:
                hints.partition_columns = [table.range_partitioning.field]

        return hints

    def list_tables(
        self,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> List[str]:
        """
        List tables in BigQuery dataset.

        Args:
            schema_name: Dataset name (uses default if not provided)
            database_name: Project ID (uses configured project if not provided)

        Returns:
            List of table names

        Examples:
            >>> tables = conn.list_tables(schema_name='analytics')
            >>> print(f"Found {len(tables)} tables")
        """
        try:
            self.require_connection()

            dataset_id = schema_name or self.default_dataset
            if not dataset_id:
                raise ValueError(
                    "dataset_name (schema_name) required for list_tables. "
                    "Provide schema_name parameter or set default_dataset in config."
                )

            project = database_name or self.project_id
            full_dataset_id = f"{project}.{dataset_id}"

            tables = self._connection.list_tables(full_dataset_id)
            table_names = [table.table_id for table in tables]

            self.logger.info(f"Listed {len(table_names)} tables in {full_dataset_id}")
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
            schema_name: Dataset name (uses default if not provided)
            database_name: Project ID (uses configured project if not provided)

        Returns:
            pandas DataFrame with columns: table_name, table_type, created, modified,
            num_rows, size_mb, description

        Examples:
            >>> tables = conn.get_tables(schema_name='analytics')
            >>> print(tables)
              table_name table_type                 created                modified  num_rows  size_mb description
            0      users      TABLE  2024-01-01 10:00:00  2024-01-15 14:30:00    150000    245.5  User data
            1     events      TABLE  2024-01-05 11:00:00  2024-01-20 16:45:00   5000000   8920.3  Event tracking
            2  user_view       VIEW  2024-01-10 12:00:00  2024-01-10 12:00:00         0      0.0  User summary
        """
        try:
            self.require_connection()

            dataset_id = schema_name or self.default_dataset
            if not dataset_id:
                raise ValueError(
                    "dataset_name (schema_name) required for get_tables. "
                    "Provide schema_name parameter or set default_dataset in config."
                )

            project = database_name or self.project_id
            full_dataset_id = f"{project}.{dataset_id}"

            self.logger.info(f"Getting table details for {full_dataset_id}")

            # List tables with metadata
            tables = list(self._connection.list_tables(full_dataset_id))

            table_data = []
            for table_ref in tables:
                # Get full table metadata
                full_table = self._connection.get_table(table_ref)

                # Calculate size in MB
                size_bytes = full_table.num_bytes or 0
                size_mb = size_bytes / (1024 * 1024)

                table_data.append({
                    'table_name': full_table.table_id,
                    'table_type': full_table.table_type,  # TABLE, VIEW, EXTERNAL, MATERIALIZED_VIEW
                    'created': full_table.created,
                    'modified': full_table.modified,
                    'num_rows': full_table.num_rows or 0,
                    'size_mb': round(size_mb, 2),
                    'description': full_table.description or ''
                })

            df = pd.DataFrame(table_data)
            self.logger.info(f"Retrieved details for {len(df)} tables")

            return df

        except Exception as e:
            self.logger.error(f"Error getting table details: {e}")
            raise

    def get_datasets(self, database_name: Optional[str] = None) -> pd.DataFrame:
        """
        List all datasets in BigQuery project and return as a pandas DataFrame.

        Args:
            database_name: Project ID (uses configured project if not provided)

        Returns:
            pandas DataFrame with columns: dataset_id, location, description, created, modified

        Examples:
            >>> datasets = conn.get_datasets()
            >>> print(datasets)
               dataset_id  location         description                 created                modified
            0   analytics        US  Analytics data           2024-01-01 10:00:00  2024-01-15 14:30:00
            1   marketing        US  Marketing campaigns      2024-02-01 09:00:00  2024-02-20 16:45:00
        """
        try:
            self.require_connection()
            project = database_name or self.project_id

            self.logger.info(f"Listing datasets in project: {project}")

            # List datasets
            datasets = list(self._connection.list_datasets(project=project))

            # Convert to DataFrame
            dataset_data = []
            for dataset in datasets:
                # Get full dataset info for additional metadata
                full_dataset = self._connection.get_dataset(dataset.reference)
                dataset_data.append({
                    'dataset_id': dataset.dataset_id,
                    'location': full_dataset.location,
                    'description': full_dataset.description or '',
                    'created': full_dataset.created,
                    'modified': full_dataset.modified
                })

            df = pd.DataFrame(dataset_data)
            self.logger.info(f"Found {len(df)} datasets in project {project}")

            return df

        except Exception as e:
            self.logger.error(f"Error listing datasets: {e}")
            raise

    def get_project_tree(
        self,
        database_name: Optional[str] = None,
        include_table_counts: bool = True,
        format: str = 'dict'
    ) -> Any:
        """
        Get hierarchical structure of project → datasets → tables.

        Args:
            database_name: Project ID (uses configured project if not provided)
            include_table_counts: Include count of tables in each dataset (default: True)
            format: Output format - 'dict' for nested dictionary, 'dataframe' for flat table

        Returns:
            Dictionary or DataFrame with project structure

        Examples:
            >>> # Get as nested dictionary (JSON-serializable)
            >>> tree = conn.get_project_tree(format='dict')
            >>> print(tree)
            {
                'project_id': 'my-project',
                'datasets': [
                    {
                        'dataset_id': 'analytics',
                        'location': 'US',
                        'table_count': 5,
                        'tables': ['users', 'events', 'sessions', 'products', 'orders']
                    },
                    {
                        'dataset_id': 'marketing',
                        'location': 'US',
                        'table_count': 3,
                        'tables': ['campaigns', 'leads', 'conversions']
                    }
                ]
            }

            >>> # Get as flattened DataFrame for easy viewing
            >>> tree_df = conn.get_project_tree(format='dataframe')
            >>> print(tree_df)
               project_id dataset_id  location  table_count                    tables
            0  my-project  analytics        US            5  [users, events, ...]
            1  my-project  marketing        US            3  [campaigns, ...]
        """
        try:
            self.require_connection()
            project = database_name or self.project_id

            self.logger.info(f"Building project tree for: {project}")

            # Get datasets
            datasets = list(self._connection.list_datasets(project=project))

            tree_data = []
            for dataset in datasets:
                dataset_info = {
                    'dataset_id': dataset.dataset_id,
                    'location': dataset.location or 'US',
                }

                # Get tables for this dataset
                try:
                    full_dataset_id = f"{project}.{dataset.dataset_id}"
                    tables = list(self._connection.list_tables(full_dataset_id))
                    table_names = [table.table_id for table in tables]

                    if include_table_counts:
                        dataset_info['table_count'] = len(table_names)

                    dataset_info['tables'] = table_names

                except Exception as e:
                    self.logger.warning(f"Could not list tables for {dataset.dataset_id}: {e}")
                    dataset_info['table_count'] = 0
                    dataset_info['tables'] = []

                tree_data.append(dataset_info)

            if format == 'dict':
                result = {
                    'project_id': project,
                    'dataset_count': len(tree_data),
                    'datasets': tree_data
                }
                self.logger.info(f"Project tree built: {len(tree_data)} datasets")
                return result

            elif format == 'dataframe':
                # Flatten to DataFrame
                df_data = []
                for dataset_info in tree_data:
                    row = {
                        'project_id': project,
                        'dataset_id': dataset_info['dataset_id'],
                        'location': dataset_info['location'],
                    }
                    if include_table_counts:
                        row['table_count'] = dataset_info['table_count']
                    row['tables'] = ', '.join(dataset_info['tables']) if dataset_info['tables'] else ''
                    df_data.append(row)

                df = pd.DataFrame(df_data)
                self.logger.info(f"Project tree built: {len(df)} datasets")
                return df

            else:
                raise ValueError(f"Invalid format: {format}. Must be 'dict' or 'dataframe'")

        except Exception as e:
            self.logger.error(f"Error building project tree: {e}")
            raise

    def execute_ddl(self, ddl: str) -> bool:
        """
        Execute DDL statement in BigQuery.

        Args:
            ddl: DDL statement (CREATE, ALTER, DROP)

        Returns:
            True if successful

        Raises:
            ExecutionError: If DDL execution fails

        Examples:
            >>> ddl = "CREATE TABLE analytics.users (id INT64, name STRING)"
            >>> conn.execute_ddl(ddl)
            True
        """
        try:
            self.require_connection()
            self.logger.info(f"Executing DDL: {ddl[:100]}...")

            # Execute query
            query_job = self._connection.query(ddl)

            # Wait for completion
            query_job.result()

            self.logger.info("DDL executed successfully")
            return True

        except GoogleCloudError as e:
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
        Execute query in BigQuery and return results as a pandas DataFrame.

        Note: BigQuery queries require fully-qualified table names in the format
        `dataset.table` (e.g., FROM analytics.users) or `project.dataset.table`.

        Args:
            query: SQL query (must use dataset.table format for table references)

        Returns:
            pandas DataFrame with query results

        Raises:
            ExecutionError: If query execution fails
            ConnectionError: If not connected

        Examples:
            >>> # Query with dataset.table format
            >>> result = conn.execute_query("SELECT COUNT(*) FROM analytics.users")
            >>> print(result)
            >>> # result is a pandas DataFrame
            >>> print(type(result))
            <class 'pandas.core.frame.DataFrame'>
        """
        try:
            self.require_connection()
            self.logger.debug(f"Executing query: {query[:100]}...")

            query_job = self._connection.query(query)
            # Convert RowIterator to pandas DataFrame
            result = query_job.result()
            df = result.to_dataframe()

            self.logger.debug(f"Query returned {len(df)} rows")
            return df

        except GoogleCloudError as e:
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
        BigQuery doesn't support traditional transactions.

        Logs a warning.
        """
        self.logger.warning(
            "BigQuery doesn't support traditional transactions. "
            "All statements are auto-committed."
        )

    def commit(self) -> None:
        """BigQuery auto-commits all statements."""
        pass

    def rollback(self) -> None:
        """
        BigQuery doesn't support rollback.

        Logs a warning.
        """
        self.logger.warning("BigQuery doesn't support rollback.")

    def _build_table_id(
        self,
        table_name: str,
        schema_name: Optional[str],
        database_name: Optional[str]
    ) -> str:
        """
        Build fully-qualified BigQuery table ID.

        Format: project.dataset.table

        Args:
            table_name: Table name
            schema_name: Dataset name (optional, uses default)
            database_name: Project ID (optional, uses configured project)

        Returns:
            Fully-qualified table ID

        Raises:
            ValueError: If dataset name cannot be determined
        """
        project = database_name or self.project_id
        dataset = schema_name or self.default_dataset

        if not dataset:
            raise ValueError(
                "Dataset name required. "
                "Provide schema_name parameter or set default_dataset in config."
            )

        return f"{project}.{dataset}.{table_name}"
