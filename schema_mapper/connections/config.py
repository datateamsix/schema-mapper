"""
Configuration management for database connections.

Supports YAML configuration files with environment variable interpolation
and .env file loading for secure credential management.
"""

import os
import yaml
import logging
from pathlib import Path
from string import Template
from typing import Dict, Any, Optional

from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ConnectionConfig:
    """
    Configuration manager for database connections.

    Features:
    - Load YAML configuration files
    - Environment variable interpolation (${VAR_NAME})
    - .env file support via python-dotenv
    - Validation of required fields
    - Multi-environment support (dev, staging, prod)

    Examples:
        From YAML file:
        >>> config = ConnectionConfig('config/connections.yaml')
        >>> bq_config = config.get_connection_config('bigquery')

        From dictionary (for testing):
        >>> config_dict = {
        ...     'connections': {
        ...         'bigquery': {'project': 'my-project'}
        ...     }
        ... }
        >>> config = ConnectionConfig.from_dict(config_dict)

        With .env file:
        >>> config = ConnectionConfig(
        ...     'config/connections.yaml',
        ...     env_file='.env.production'
        ... )
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        env_file: Optional[str] = None,
        auto_load_env: bool = True
    ):
        """
        Initialize configuration.

        Args:
            config_path: Path to YAML config file (optional)
            env_file: Path to .env file (default: .env)
            auto_load_env: Automatically load .env if it exists (default: True)

        Raises:
            ConfigurationError: If config file cannot be loaded

        Examples:
            >>> config = ConnectionConfig('config/connections.yaml')
            >>> config = ConnectionConfig(env_file='.env.production')
        """
        self.config_path = config_path
        self.env_file = env_file or '.env'
        self._config: Dict[str, Any] = {}

        # Load .env file first (so variables are available for YAML interpolation)
        if auto_load_env:
            self._load_env_file()

        # Load YAML config if provided
        if config_path:
            self._load_yaml_config()

    def _load_env_file(self) -> None:
        """
        Load environment variables from .env file.

        Silently skips if file doesn't exist.

        Examples:
            .env file:
            ```
            SNOWFLAKE_USER=admin
            SNOWFLAKE_PASSWORD=secret123
            ```
        """
        if not Path(self.env_file).exists():
            logger.debug(f".env file not found: {self.env_file}")
            return

        try:
            # Use python-dotenv if available
            try:
                from dotenv import load_dotenv
                load_dotenv(self.env_file, override=False)
                logger.info(f"Loaded environment from: {self.env_file}")
            except ImportError:
                # Fallback to manual parsing if python-dotenv not installed
                logger.warning(
                    "python-dotenv not installed. "
                    "Install with: pip install python-dotenv"
                )
                self._manual_load_env()

        except Exception as e:
            logger.error(f"Error loading .env file: {e}")
            raise ConfigurationError(f"Failed to load .env file: {e}")

    def _manual_load_env(self) -> None:
        """Manually parse .env file (fallback if python-dotenv not available)."""
        try:
            with open(self.env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    # Parse KEY=VALUE
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        # Remove quotes if present
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        # Set env var if not already set
                        if key not in os.environ:
                            os.environ[key] = value
            logger.info(f"Manually loaded .env file: {self.env_file}")
        except Exception as e:
            logger.error(f"Error manually parsing .env file: {e}")

    def _load_yaml_config(self) -> None:
        """
        Load and parse YAML configuration file.

        Raises:
            ConfigurationError: If config file cannot be loaded or parsed

        Examples:
            YAML file:
            ```yaml
            target: snowflake
            connections:
              snowflake:
                user: ${SNOWFLAKE_USER}
                password: ${SNOWFLAKE_PASSWORD}
                account: abc123
            ```
        """
        if not self.config_path:
            return

        config_file = Path(self.config_path)

        if not config_file.exists():
            raise ConfigurationError(
                f"Configuration file not found: {self.config_path}",
                details={'absolute_path': str(config_file.absolute())}
            )

        try:
            with open(config_file, 'r') as f:
                config_content = f.read()

            # Interpolate environment variables
            config_content = self._interpolate_env_vars(config_content)

            # Parse YAML
            self._config = yaml.safe_load(config_content) or {}

            logger.info(f"Loaded configuration from: {self.config_path}")

        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Invalid YAML in config file: {e}",
                details={'file': self.config_path}
            )
        except Exception as e:
            raise ConfigurationError(
                f"Error loading config file: {e}",
                details={'file': self.config_path}
            )

    def _interpolate_env_vars(self, content: str) -> str:
        """
        Replace ${VAR_NAME} with environment variable values.

        Supports:
        - ${VAR_NAME} - Required variable (error if not set)
        - ${VAR_NAME:-default} - Optional with default value

        Args:
            content: Configuration content with ${VAR} placeholders

        Returns:
            Content with variables replaced

        Examples:
            >>> content = "password: ${DB_PASSWORD}"
            >>> # If DB_PASSWORD=secret123
            >>> interpolated = self._interpolate_env_vars(content)
            >>> # Returns: "password: secret123"

            >>> content = "port: ${DB_PORT:-5432}"
            >>> # If DB_PORT not set, uses default
            >>> interpolated = self._interpolate_env_vars(content)
            >>> # Returns: "port: 5432"

        Raises:
            ConfigurationError: If required variable not set
        """
        import re

        # Pattern for ${VAR_NAME} or ${VAR_NAME:-default}
        pattern = r'\$\{([^}:]+)(?::-(.*?))?\}'

        def replace_var(match):
            """Replace environment variable placeholder with actual value."""
            var_name = match.group(1)
            default_value = match.group(2)

            value = os.getenv(var_name)

            if value is None:
                if default_value is not None:
                    # Use default
                    logger.debug(
                        f"Environment variable {var_name} not set, "
                        f"using default: {default_value}"
                    )
                    return default_value
                else:
                    # Required variable not set
                    raise ConfigurationError(
                        f"Environment variable not set: {var_name}. "
                        f"Set it in environment or .env file.",
                        details={'variable': var_name}
                    )

            return value

        try:
            return re.sub(pattern, replace_var, content)
        except ConfigurationError:
            raise
        except Exception as e:
            raise ConfigurationError(f"Error interpolating environment variables: {e}")

    def get_connection_config(self, target: str) -> Dict[str, Any]:
        """
        Get connection configuration for a target platform.

        Args:
            target: Platform name (e.g., 'snowflake', 'bigquery')

        Returns:
            Configuration dict for that platform

        Raises:
            ConfigurationError: If target not found in config

        Examples:
            >>> config = ConnectionConfig('connections.yaml')
            >>> sf_config = config.get_connection_config('snowflake')
            >>> print(sf_config['user'])
        """
        if not self._config:
            raise ConfigurationError(
                "No configuration loaded. "
                "Provide config_path or use from_dict()."
            )

        connections = self._config.get('connections', {})

        if target not in connections:
            available = ', '.join(connections.keys())
            raise ConfigurationError(
                f"No configuration for target: {target}. "
                f"Available targets: {available}",
                details={'available_targets': list(connections.keys())}
            )

        return connections[target]

    def get_default_target(self) -> str:
        """
        Get default target platform from config.

        Returns:
            Default platform name

        Raises:
            ConfigurationError: If no default target set

        Examples:
            >>> config = ConnectionConfig('connections.yaml')
            >>> target = config.get_default_target()
            >>> print(f"Default: {target}")
        """
        if not self._config:
            raise ConfigurationError("No configuration loaded")

        target = self._config.get('target')

        if not target:
            raise ConfigurationError(
                "No default target set in configuration. "
                "Add 'target: platform_name' to YAML config."
            )

        return target

    def list_targets(self) -> list:
        """
        List all configured targets.

        Returns:
            List of platform names

        Examples:
            >>> config = ConnectionConfig('connections.yaml')
            >>> targets = config.list_targets()
            >>> print(f"Configured: {', '.join(targets)}")
        """
        if not self._config:
            return []

        return list(self._config.get('connections', {}).keys())

    def has_target(self, target: str) -> bool:
        """
        Check if target is configured.

        Args:
            target: Platform name

        Returns:
            True if target configured

        Examples:
            >>> if config.has_target('bigquery'):
            ...     bq_config = config.get_connection_config('bigquery')
        """
        return target in self.list_targets()

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ConnectionConfig':
        """
        Create config from dictionary (useful for testing).

        Args:
            config_dict: Configuration dictionary

        Returns:
            ConnectionConfig instance

        Examples:
            >>> config_dict = {
            ...     'target': 'bigquery',
            ...     'connections': {
            ...         'bigquery': {'project': 'my-project'}
            ...     }
            ... }
            >>> config = ConnectionConfig.from_dict(config_dict)
        """
        instance = cls(auto_load_env=False)
        instance._config = config_dict
        return instance

    @classmethod
    def from_env_only(cls, platform: str, env_prefix: str = None) -> 'ConnectionConfig':
        """
        Create config from environment variables only (no YAML).

        Args:
            platform: Platform name
            env_prefix: Prefix for env vars (e.g., 'SNOWFLAKE_')

        Returns:
            ConnectionConfig instance

        Examples:
            >>> # With env vars: SNOWFLAKE_USER=admin, SNOWFLAKE_PASSWORD=secret
            >>> config = ConnectionConfig.from_env_only('snowflake', 'SNOWFLAKE_')
            >>> sf_config = config.get_connection_config('snowflake')
        """
        instance = cls(auto_load_env=True)

        if env_prefix is None:
            env_prefix = f"{platform.upper()}_"

        # Collect env vars with prefix
        conn_config = {}
        for key, value in os.environ.items():
            if key.startswith(env_prefix):
                config_key = key[len(env_prefix):].lower()
                conn_config[config_key] = value

        instance._config = {
            'target': platform,
            'connections': {
                platform: conn_config
            }
        }

        return instance

    def to_dict(self) -> Dict[str, Any]:
        """
        Get configuration as dictionary.

        Returns:
            Configuration dict

        Examples:
            >>> config_dict = config.to_dict()
        """
        return self._config.copy()

    def __repr__(self) -> str:
        """String representation."""
        targets = self.list_targets()
        return f"<ConnectionConfig targets={targets}>"
