"""
Connection factory for creating platform-specific database connections.

Provides a unified interface for creating connections across all supported platforms
using the factory pattern with lazy loading.
"""

import logging
from typing import Optional, Dict, Any, Type, Union

from .base import BaseConnection
from .config import ConnectionConfig
from .exceptions import ConfigurationError, ValidationError

logger = logging.getLogger(__name__)


class ConnectionFactory:
    """
    Factory for creating platform-specific database connections.

    Uses lazy loading to import platform connectors only when needed.
    Supports both ConnectionConfig objects and raw dictionaries.

    Examples:
        From YAML config:
        >>> config = ConnectionConfig('connections.yaml')
        >>> conn = ConnectionFactory.get_connection('snowflake', config)
        >>> with conn:
        ...     schema = conn.get_target_schema('users')

        From dictionary:
        >>> config_dict = {
        ...     'project': 'my-project',
        ...     'credentials_path': '/path/to/key.json'
        ... }
        >>> conn = ConnectionFactory.get_connection('bigquery', config_dict)

        Using default target:
        >>> config = ConnectionConfig('connections.yaml')  # target: snowflake
        >>> conn = ConnectionFactory.get_connection_from_config(config)
        >>> print(conn.platform_name())  # snowflake
    """

    # Registry of platform connectors (lazily populated)
    _connectors: Dict[str, Type[BaseConnection]] = {}

    @classmethod
    def get_connection(
        cls,
        target: str,
        config: Optional[Union[ConnectionConfig, Dict[str, Any]]] = None
    ) -> BaseConnection:
        """
        Get platform-specific connection.

        Args:
            target: Platform name ('bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver')
            config: Either ConnectionConfig object or configuration dict

        Returns:
            Platform-specific connection instance

        Raises:
            ValueError: If target platform not supported
            TypeError: If config type is invalid
            ConfigurationError: If configuration is invalid

        Examples:
            >>> conn = ConnectionFactory.get_connection('bigquery', {
            ...     'project': 'my-project'
            ... })

            >>> config = ConnectionConfig('connections.yaml')
            >>> conn = ConnectionFactory.get_connection('snowflake', config)
        """
        target = target.lower().strip()

        # Lazy load connectors
        if not cls._connectors:
            cls._load_connectors()

        # Validate target
        if target not in cls._connectors:
            available = ', '.join(cls.supported_platforms())
            raise ValueError(
                f"Unsupported platform: {target}. "
                f"Supported platforms: {available}"
            )

        # Extract configuration dict
        if config is None:
            raise ConfigurationError(
                f"Configuration required for {target} connection"
            )
        elif isinstance(config, ConnectionConfig):
            try:
                config_dict = config.get_connection_config(target)
            except ConfigurationError as e:
                raise ConfigurationError(
                    f"Cannot get {target} configuration from ConnectionConfig: {e}",
                    platform=target
                )
        elif isinstance(config, dict):
            config_dict = config
        else:
            raise TypeError(
                f"config must be ConnectionConfig or dict, got {type(config).__name__}"
            )

        # Validate configuration dict
        if not isinstance(config_dict, dict):
            raise ConfigurationError(
                f"Configuration for {target} must be a dictionary",
                platform=target
            )

        # Instantiate connector
        logger.info(f"Creating {target} connection")
        try:
            connector_class = cls._connectors[target]
            return connector_class(config_dict)
        except Exception as e:
            logger.error(f"Failed to create {target} connection: {e}")
            raise

    @classmethod
    def get_connection_from_config(
        cls,
        config: ConnectionConfig,
        target: Optional[str] = None
    ) -> BaseConnection:
        """
        Get connection using default target from config.

        Convenience method when config has a default target set.

        Args:
            config: ConnectionConfig with default target
            target: Optional platform override (uses config default if not provided)

        Returns:
            Platform-specific connection instance

        Raises:
            ConfigurationError: If no default target and target not provided

        Examples:
            >>> config = ConnectionConfig('connections.yaml')  # target: snowflake
            >>> conn = ConnectionFactory.get_connection_from_config(config)
            >>> print(conn.platform_name())  # snowflake

            >>> # Override default target
            >>> conn = ConnectionFactory.get_connection_from_config(config, target='bigquery')
        """
        if target is None:
            target = config.get_default_target()

        return cls.get_connection(target, config)

    @classmethod
    def _load_connectors(cls) -> None:
        """
        Lazy load platform connectors.

        Imports platform-specific connector classes only when needed.
        This avoids requiring all platform dependencies if user only uses one platform.
        """
        logger.debug("Loading platform connectors...")

        # Import platform connectors
        # Note: These will only succeed if the platform's dependencies are installed
        connectors = {}

        try:
            from .platform_connectors.bigquery import BigQueryConnection
            connectors['bigquery'] = BigQueryConnection
        except ImportError as e:
            logger.warning(
                f"BigQuery connector not available: {e}. "
                "Install with: pip install schema-mapper[bigquery]"
            )

        try:
            from .platform_connectors.snowflake import SnowflakeConnection
            connectors['snowflake'] = SnowflakeConnection
        except ImportError as e:
            logger.warning(
                f"Snowflake connector not available: {e}. "
                "Install with: pip install schema-mapper[snowflake]"
            )

        try:
            from .platform_connectors.redshift import RedshiftConnection
            connectors['redshift'] = RedshiftConnection
        except ImportError as e:
            logger.warning(
                f"Redshift connector not available: {e}. "
                "Install with: pip install schema-mapper[redshift]"
            )

        try:
            from .platform_connectors.postgresql import PostgreSQLConnection
            connectors['postgresql'] = PostgreSQLConnection
        except ImportError as e:
            logger.warning(
                f"PostgreSQL connector not available: {e}. "
                "Install with: pip install schema-mapper[postgresql]"
            )

        try:
            from .platform_connectors.sqlserver import SQLServerConnection
            connectors['sqlserver'] = SQLServerConnection
        except ImportError as e:
            logger.warning(
                f"SQL Server connector not available: {e}. "
                "Install with: pip install schema-mapper[sqlserver]"
            )

        if not connectors:
            raise ImportError(
                "No database connectors available. "
                "Install at least one platform: "
                "pip install schema-mapper[bigquery] or pip install schema-mapper[all]"
            )

        cls._connectors = connectors
        logger.info(f"Loaded connectors: {', '.join(connectors.keys())}")

    @classmethod
    def supported_platforms(cls) -> list:
        """
        Get list of supported platforms.

        Returns:
            List of platform names

        Examples:
            >>> platforms = ConnectionFactory.supported_platforms()
            >>> print(f"Supported: {', '.join(platforms)}")
        """
        if not cls._connectors:
            cls._load_connectors()
        return list(cls._connectors.keys())

    @classmethod
    def is_platform_supported(cls, platform: str) -> bool:
        """
        Check if platform is supported and dependencies are installed.

        Args:
            platform: Platform name

        Returns:
            True if platform is supported

        Examples:
            >>> if ConnectionFactory.is_platform_supported('bigquery'):
            ...     conn = ConnectionFactory.get_connection('bigquery', config)
        """
        if not cls._connectors:
            cls._load_connectors()
        return platform.lower() in cls._connectors

    @classmethod
    def create_pool(
        cls,
        target: str,
        config: Optional[Union[ConnectionConfig, Dict[str, Any]]] = None,
        min_size: int = 2,
        max_size: int = 10,
        max_idle_seconds: int = 300,
        max_lifetime_seconds: int = 3600,
        validate_on_checkout: bool = True,
        wait_timeout: int = 30,
    ):
        """
        Create a connection pool for a platform.

        Args:
            target: Platform name
            config: Configuration for connections
            min_size: Minimum pool size (default: 2)
            max_size: Maximum pool size (default: 10)
            max_idle_seconds: Max idle time before cleanup (default: 300)
            max_lifetime_seconds: Max connection lifetime (default: 3600)
            validate_on_checkout: Validate connections before use (default: True)
            wait_timeout: Max wait time for connection (default: 30)

        Returns:
            ConnectionPool instance

        Raises:
            ValueError: If target platform not supported
            ConfigurationError: If configuration is invalid

        Examples:
            >>> pool = ConnectionFactory.create_pool(
            ...     'bigquery',
            ...     config={'project': 'my-project'},
            ...     min_size=2,
            ...     max_size=10
            ... )

            >>> with pool.get_connection() as conn:
            ...     schema = conn.get_target_schema('users')

            >>> pool.close()
        """
        from .utils.pooling import ConnectionPool

        # Create connection factory function
        def connection_factory():
            return cls.get_connection(target, config)

        # Create pool
        pool = ConnectionPool(
            connection_factory=connection_factory,
            min_size=min_size,
            max_size=max_size,
            max_idle_seconds=max_idle_seconds,
            max_lifetime_seconds=max_lifetime_seconds,
            validate_on_checkout=validate_on_checkout,
            wait_timeout=wait_timeout,
        )

        return pool

    @classmethod
    def register_connector(
        cls,
        platform: str,
        connector_class: Type[BaseConnection]
    ) -> None:
        """
        Register a custom connector (for extensibility).

        Allows users to register custom platform connectors without modifying
        the factory code.

        Args:
            platform: Platform name
            connector_class: Connector class (must inherit from BaseConnection)

        Raises:
            TypeError: If connector_class doesn't inherit from BaseConnection

        Examples:
            >>> class MyCustomConnection(BaseConnection):
            ...     # Implementation
            ...     pass
            >>> ConnectionFactory.register_connector('mycustom', MyCustomConnection)
            >>> conn = ConnectionFactory.get_connection('mycustom', config)
        """
        if not issubclass(connector_class, BaseConnection):
            raise TypeError(
                f"Connector class must inherit from BaseConnection, "
                f"got {connector_class.__name__}"
            )

        platform = platform.lower()

        if platform in cls._connectors:
            logger.warning(f"Overriding existing connector for platform: {platform}")

        cls._connectors[platform] = connector_class
        logger.info(f"Registered connector for platform: {platform}")

    @classmethod
    def clear_connectors(cls) -> None:
        """
        Clear connector registry (mainly for testing).

        Examples:
            >>> ConnectionFactory.clear_connectors()
            >>> ConnectionFactory._load_connectors()
        """
        cls._connectors.clear()
        logger.debug("Cleared connector registry")


def get_connection(
    target: str,
    config: Optional[Union[ConnectionConfig, Dict[str, Any], str]] = None
) -> BaseConnection:
    """
    Convenience function to get database connection.

    Wrapper around ConnectionFactory.get_connection() for simpler imports.

    Args:
        target: Platform name ('bigquery', 'snowflake', etc.)
        config: ConnectionConfig, dict, or path to YAML config file

    Returns:
        Platform-specific connection instance

    Examples:
        >>> from schema_mapper.connections import get_connection
        >>> conn = get_connection('bigquery', {'project': 'my-project'})

        >>> # From YAML file
        >>> conn = get_connection('snowflake', 'config/connections.yaml')
    """
    # If config is a string, treat as YAML path
    if isinstance(config, str):
        config = ConnectionConfig(config)

    return ConnectionFactory.get_connection(target, config)
