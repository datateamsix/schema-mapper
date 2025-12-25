"""
Credential and configuration validation utilities.

Provides functions to validate connection configurations before attempting
to establish database connections.
"""

import os
import re
from pathlib import Path
from typing import Dict, Any, List, Optional

from ..exceptions import ValidationError, ConfigurationError


def validate_required_fields(
    config: Dict[str, Any],
    required_fields: List[str],
    platform: str = None
) -> None:
    """
    Validate that required fields are present in configuration.

    Args:
        config: Configuration dictionary
        required_fields: List of required field names
        platform: Platform name for error messages

    Raises:
        ConfigurationError: If required fields are missing
    """
    missing_fields = []

    for field in required_fields:
        if field not in config or config[field] is None or config[field] == '':
            missing_fields.append(field)

    if missing_fields:
        raise ConfigurationError(
            f"Missing required configuration fields: {', '.join(missing_fields)}",
            platform=platform,
            details={'missing_fields': missing_fields}
        )


def validate_credentials(
    config: Dict[str, Any],
    platform: str
) -> None:
    """
    Validate platform-specific credentials.

    Args:
        config: Configuration dictionary
        platform: Platform name ('bigquery', 'snowflake', etc.)

    Raises:
        ValidationError: If credentials are invalid
        ConfigurationError: If required fields are missing
    """
    validators = {
        'bigquery': _validate_bigquery_credentials,
        'snowflake': _validate_snowflake_credentials,
        'postgresql': _validate_postgresql_credentials,
        'redshift': _validate_redshift_credentials,
        'sqlserver': _validate_sqlserver_credentials,
    }

    if platform not in validators:
        raise ValidationError(
            f"Unknown platform: {platform}. "
            f"Supported platforms: {', '.join(validators.keys())}",
            platform=platform
        )

    # Run platform-specific validation
    validators[platform](config, platform)


def _validate_bigquery_credentials(config: Dict[str, Any], platform: str) -> None:
    """Validate BigQuery credentials."""
    # Project ID is required
    validate_required_fields(config, ['project'], platform)

    # Validate project ID format (alphanumeric, hyphens, underscores)
    project_id = config['project']
    if not re.match(r'^[a-z0-9-_]+$', project_id):
        raise ValidationError(
            f"Invalid BigQuery project ID format: {project_id}. "
            "Must contain only lowercase letters, numbers, hyphens, and underscores.",
            platform=platform
        )

    # If credentials_path provided, validate file exists
    if 'credentials_path' in config and config['credentials_path']:
        creds_path = Path(config['credentials_path'])
        if not creds_path.exists():
            raise ValidationError(
                f"Credentials file not found: {config['credentials_path']}",
                platform=platform,
                details={'path': str(creds_path.absolute())}
            )
        if not creds_path.is_file():
            raise ValidationError(
                f"Credentials path is not a file: {config['credentials_path']}",
                platform=platform
            )

    # Validate location if provided
    if 'location' in config and config['location']:
        valid_locations = ['US', 'EU', 'us-central1', 'europe-west1', 'asia-east1']
        if config['location'] not in valid_locations:
            # Just warn, don't fail (BigQuery accepts many locations)
            import logging
            logging.getLogger(__name__).warning(
                f"Unusual BigQuery location: {config['location']}. "
                f"Common locations: {', '.join(valid_locations)}"
            )


def _validate_snowflake_credentials(config: Dict[str, Any], platform: str) -> None:
    """Validate Snowflake credentials."""
    # Required fields
    validate_required_fields(
        config,
        ['user', 'password', 'account'],
        platform
    )

    # Validate account format (should be like 'abc123' or 'abc123.us-east-1')
    account = config['account']
    if not re.match(r'^[a-zA-Z0-9_.-]+$', account):
        raise ValidationError(
            f"Invalid Snowflake account format: {account}",
            platform=platform
        )

    # Warehouse, database, schema are recommended but not required
    recommended = ['warehouse', 'database', 'schema']
    missing_recommended = [f for f in recommended if f not in config or not config[f]]

    if missing_recommended:
        import logging
        logging.getLogger(__name__).warning(
            f"Missing recommended Snowflake configuration: {', '.join(missing_recommended)}. "
            "You may need to specify these when querying tables."
        )


def _validate_postgresql_credentials(config: Dict[str, Any], platform: str) -> None:
    """Validate PostgreSQL credentials."""
    # Required fields
    validate_required_fields(
        config,
        ['host', 'database', 'user', 'password'],
        platform
    )

    # Validate port if provided
    if 'port' in config:
        port = config['port']
        try:
            port_int = int(port)
            if not (1 <= port_int <= 65535):
                raise ValueError
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid port number: {port}. Must be between 1 and 65535.",
                platform=platform
            )

    # Set default port if not provided
    if 'port' not in config:
        config['port'] = 5432


def _validate_redshift_credentials(config: Dict[str, Any], platform: str) -> None:
    """Validate Redshift credentials."""
    # Required fields (same as PostgreSQL)
    validate_required_fields(
        config,
        ['host', 'database', 'user', 'password'],
        platform
    )

    # Validate port
    if 'port' in config:
        port = config['port']
        try:
            port_int = int(port)
            if not (1 <= port_int <= 65535):
                raise ValueError
        except (ValueError, TypeError):
            raise ValidationError(
                f"Invalid port number: {port}",
                platform=platform
            )
    else:
        # Redshift default port
        config['port'] = 5439

    # Validate host format (should be Redshift cluster endpoint)
    host = config['host']
    if not host.endswith('.redshift.amazonaws.com') and not host.endswith('.redshift-serverless.amazonaws.com'):
        import logging
        logging.getLogger(__name__).warning(
            f"Host {host} doesn't look like a Redshift endpoint. "
            "Expected format: *.redshift.amazonaws.com"
        )


def _validate_sqlserver_credentials(config: Dict[str, Any], platform: str) -> None:
    """Validate SQL Server credentials."""
    # Required fields
    validate_required_fields(
        config,
        ['server', 'database', 'user', 'password'],
        platform
    )

    # Validate driver if provided
    if 'driver' in config and config['driver']:
        valid_drivers = [
            'ODBC Driver 17 for SQL Server',
            'ODBC Driver 18 for SQL Server',
            'SQL Server',
            'SQL Server Native Client 11.0'
        ]
        if config['driver'] not in valid_drivers:
            import logging
            logging.getLogger(__name__).warning(
                f"Unusual ODBC driver: {config['driver']}. "
                f"Common drivers: {', '.join(valid_drivers)}"
            )
    else:
        # Set default driver
        config['driver'] = 'ODBC Driver 18 for SQL Server'

    # Validate server format
    server = config['server']
    # Can be hostname, IP, or hostname,port or hostname\instance
    if not re.match(r'^[a-zA-Z0-9._,-\\]+$', server):
        raise ValidationError(
            f"Invalid SQL Server server format: {server}",
            platform=platform
        )


def validate_file_path(
    path: str,
    must_exist: bool = True,
    must_be_file: bool = True,
    extensions: Optional[List[str]] = None
) -> Path:
    """
    Validate file path.

    Args:
        path: File path to validate
        must_exist: Path must exist
        must_be_file: Path must be a file (not directory)
        extensions: Allowed file extensions (e.g., ['.json', '.yaml'])

    Returns:
        Validated Path object

    Raises:
        ValidationError: If validation fails
    """
    try:
        file_path = Path(path).resolve()
    except Exception as e:
        raise ValidationError(f"Invalid file path: {path}. Error: {e}")

    if must_exist and not file_path.exists():
        raise ValidationError(
            f"File not found: {path}",
            details={'absolute_path': str(file_path)}
        )

    if must_be_file and file_path.exists() and not file_path.is_file():
        raise ValidationError(f"Path is not a file: {path}")

    if extensions and file_path.suffix not in extensions:
        raise ValidationError(
            f"Invalid file extension: {file_path.suffix}. "
            f"Expected one of: {', '.join(extensions)}"
        )

    return file_path


def sanitize_connection_string(connection_string: str) -> str:
    """
    Sanitize connection string for logging (mask passwords).

    Args:
        connection_string: Connection string with potential secrets

    Returns:
        Sanitized string with masked passwords

    Examples:
        >>> sanitize_connection_string("user=admin password=secret123 host=db.com")
        'user=admin password=*** host=db.com'
    """
    # Mask password values
    sanitized = re.sub(
        r'(password|pwd|pass|secret|token|key)=([^\s;]+)',
        r'\1=***',
        connection_string,
        flags=re.IGNORECASE
    )
    return sanitized


def validate_table_name(table_name: str, platform: str = None) -> None:
    """
    Validate table name format.

    Args:
        table_name: Table name to validate
        platform: Platform name for platform-specific rules

    Raises:
        ValidationError: If table name is invalid
    """
    if not table_name or not table_name.strip():
        raise ValidationError("Table name cannot be empty")

    # Common rules across platforms
    if len(table_name) > 128:
        raise ValidationError(
            f"Table name too long: {len(table_name)} characters. "
            "Maximum is typically 128."
        )

    # Check for valid characters (alphanumeric, underscore, some platforms allow hyphens)
    if platform == 'bigquery':
        # BigQuery allows alphanumeric and underscores
        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
            raise ValidationError(
                f"Invalid BigQuery table name: {table_name}. "
                "Must contain only letters, numbers, and underscores."
            )
    else:
        # Most SQL databases: alphanumeric and underscore
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            raise ValidationError(
                f"Invalid table name: {table_name}. "
                "Must start with a letter or underscore and contain only letters, numbers, and underscores."
            )


def is_env_var_set(var_name: str) -> bool:
    """
    Check if environment variable is set and non-empty.

    Args:
        var_name: Environment variable name

    Returns:
        True if set and non-empty
    """
    value = os.getenv(var_name)
    return value is not None and value.strip() != ''
