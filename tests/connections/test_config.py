"""Tests for schema_mapper.connections.config module."""

import os
import pytest
import yaml
from pathlib import Path

from schema_mapper.connections.config import ConnectionConfig
from schema_mapper.connections.exceptions import ConfigurationError


class TestConnectionConfig:
    """Test ConnectionConfig class."""

    def test_from_dict(self, mock_connection_config):
        """Test creating config from dictionary."""
        config = ConnectionConfig.from_dict(mock_connection_config)

        assert config.list_targets() == ['bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver']
        assert config.get_default_target() == 'bigquery'

    def test_from_dict_get_connection_config(self, mock_connection_config):
        """Test getting connection config for specific platform."""
        config = ConnectionConfig.from_dict(mock_connection_config)

        bq_config = config.get_connection_config('bigquery')
        assert bq_config['project'] == 'test-project'
        assert bq_config['location'] == 'US'

        sf_config = config.get_connection_config('snowflake')
        assert sf_config['account'] == 'test-account'
        assert sf_config['warehouse'] == 'TEST_WH'

    def test_get_connection_config_missing_target(self, mock_connection_config):
        """Test error when target not configured."""
        config = ConnectionConfig.from_dict(mock_connection_config)

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_connection_config('oracle')

        assert 'No configuration for target: oracle' in str(exc_info.value)
        assert 'Available targets' in str(exc_info.value)

    def test_has_target(self, mock_connection_config):
        """Test checking if target is configured."""
        config = ConnectionConfig.from_dict(mock_connection_config)

        assert config.has_target('bigquery') is True
        assert config.has_target('snowflake') is True
        assert config.has_target('oracle') is False

    def test_list_targets(self, mock_connection_config):
        """Test listing all configured targets."""
        config = ConnectionConfig.from_dict(mock_connection_config)

        targets = config.list_targets()
        assert isinstance(targets, list)
        assert len(targets) == 5
        assert 'bigquery' in targets
        assert 'snowflake' in targets

    def test_get_default_target(self, mock_connection_config):
        """Test getting default target."""
        config = ConnectionConfig.from_dict(mock_connection_config)
        assert config.get_default_target() == 'bigquery'

    def test_get_default_target_missing(self):
        """Test error when no default target set."""
        config = ConnectionConfig.from_dict({'connections': {}})

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_default_target()

        assert 'No default target' in str(exc_info.value)

    def test_to_dict(self, mock_connection_config):
        """Test converting config to dictionary."""
        config = ConnectionConfig.from_dict(mock_connection_config)
        config_dict = config.to_dict()

        assert config_dict['target'] == 'bigquery'
        assert 'connections' in config_dict
        assert len(config_dict['connections']) == 5

    def test_repr(self, mock_connection_config):
        """Test string representation."""
        config = ConnectionConfig.from_dict(mock_connection_config)
        repr_str = repr(config)

        assert 'ConnectionConfig' in repr_str
        assert 'targets=' in repr_str


class TestConnectionConfigFromYAML:
    """Test loading configuration from YAML files."""

    def test_load_from_yaml(self, temp_config_file):
        """Test loading config from YAML file."""
        config = ConnectionConfig(temp_config_file, auto_load_env=False)

        # Use set comparison since dict key order may vary
        assert set(config.list_targets()) == {'bigquery', 'snowflake', 'postgresql', 'redshift', 'sqlserver'}
        assert config.get_default_target() == 'bigquery'

    def test_load_yaml_file_not_found(self):
        """Test error when YAML file not found."""
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionConfig('nonexistent.yaml', auto_load_env=False)

        assert 'not found' in str(exc_info.value).lower()

    def test_load_yaml_invalid_syntax(self, tmp_path):
        """Test error with invalid YAML syntax."""
        invalid_yaml = tmp_path / "invalid.yaml"
        with open(invalid_yaml, 'w') as f:
            f.write("invalid: [unclosed")

        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionConfig(str(invalid_yaml), auto_load_env=False)

        assert 'Invalid YAML' in str(exc_info.value)

    def test_empty_yaml_file(self, tmp_path):
        """Test loading empty YAML file."""
        empty_yaml = tmp_path / "empty.yaml"
        empty_yaml.write_text("")

        config = ConnectionConfig(str(empty_yaml), auto_load_env=False)
        assert config.list_targets() == []


class TestEnvironmentVariableInterpolation:
    """Test environment variable interpolation in YAML."""

    def test_interpolate_required_env_var(self, tmp_path, monkeypatch):
        """Test interpolating required environment variable."""
        monkeypatch.setenv('TEST_USER', 'admin')
        monkeypatch.setenv('TEST_PASSWORD', 'secret123')

        yaml_content = {
            'target': 'postgresql',
            'connections': {
                'postgresql': {
                    'user': '${TEST_USER}',
                    'password': '${TEST_PASSWORD}'
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(yaml_content, f)

        config = ConnectionConfig(str(config_file), auto_load_env=False)
        pg_config = config.get_connection_config('postgresql')

        assert pg_config['user'] == 'admin'
        assert pg_config['password'] == 'secret123'

    def test_interpolate_missing_required_env_var(self, tmp_path):
        """Test error when required environment variable not set."""
        yaml_content = {
            'target': 'postgresql',
            'connections': {
                'postgresql': {
                    'user': '${MISSING_VAR}'
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(yaml_content, f)

        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionConfig(str(config_file), auto_load_env=False)

        assert 'not set: MISSING_VAR' in str(exc_info.value)

    def test_interpolate_env_var_with_default(self, tmp_path, monkeypatch):
        """Test environment variable with default value."""
        # Set one var, leave other to use default
        monkeypatch.setenv('DB_HOST', 'custom-host')

        yaml_content = {
            'target': 'postgresql',
            'connections': {
                'postgresql': {
                    'host': '${DB_HOST}',
                    'port': '${DB_PORT:-5432}',  # Default to 5432
                    'database': '${DB_NAME:-testdb}'  # Default to testdb
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            # Write raw YAML with ${} syntax
            f.write(yaml.dump(yaml_content))

        config = ConnectionConfig(str(config_file), auto_load_env=False)
        pg_config = config.get_connection_config('postgresql')

        assert pg_config['host'] == 'custom-host'
        # YAML parser converts string numbers to int, but interpolation should preserve as string
        assert str(pg_config['port']) == '5432'  # Used default
        assert pg_config['database'] == 'testdb'  # Used default

    def test_multiple_env_vars_in_one_value(self, tmp_path, monkeypatch):
        """Test multiple environment variables in single value."""
        monkeypatch.setenv('DB_HOST', 'myhost')
        monkeypatch.setenv('DB_PORT', '5432')

        yaml_content = {
            'target': 'postgresql',
            'connections': {
                'postgresql': {
                    'connection_string': 'postgresql://${DB_HOST}:${DB_PORT}/mydb'
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            f.write(yaml.dump(yaml_content))

        config = ConnectionConfig(str(config_file), auto_load_env=False)
        pg_config = config.get_connection_config('postgresql')

        assert pg_config['connection_string'] == 'postgresql://myhost:5432/mydb'


class TestDotEnvFile:
    """Test .env file loading."""

    def test_load_env_file(self, tmp_path, temp_env_file):
        """Test loading .env file."""
        yaml_content = {
            'target': 'snowflake',
            'connections': {
                'snowflake': {
                    'user': '${SNOWFLAKE_USER}',
                    'password': '${SNOWFLAKE_PASSWORD}'
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(yaml_content, f)

        config = ConnectionConfig(str(config_file), env_file=temp_env_file)
        sf_config = config.get_connection_config('snowflake')

        assert sf_config['user'] == 'test_user'
        assert sf_config['password'] == 'test_password'

    def test_env_file_not_found(self, tmp_path):
        """Test graceful handling when .env file doesn't exist."""
        # Should not raise error, just skip loading
        config = ConnectionConfig(env_file='nonexistent.env', auto_load_env=True)
        assert config._config == {}

    def test_auto_load_env_disabled(self, tmp_path):
        """Test disabling auto-loading of .env."""
        config = ConnectionConfig(auto_load_env=False)
        # Should not load .env even if it exists
        assert config._config == {}


class TestFromEnvOnly:
    """Test creating config from environment variables only."""

    def test_from_env_only_with_prefix(self, monkeypatch):
        """Test creating config from env vars with prefix."""
        monkeypatch.setenv('SNOWFLAKE_USER', 'admin')
        monkeypatch.setenv('SNOWFLAKE_PASSWORD', 'secret')
        monkeypatch.setenv('SNOWFLAKE_ACCOUNT', 'abc123')
        monkeypatch.setenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')

        config = ConnectionConfig.from_env_only('snowflake', 'SNOWFLAKE_')
        sf_config = config.get_connection_config('snowflake')

        assert sf_config['user'] == 'admin'
        assert sf_config['password'] == 'secret'
        assert sf_config['account'] == 'abc123'
        assert sf_config['warehouse'] == 'COMPUTE_WH'

    def test_from_env_only_auto_prefix(self, monkeypatch):
        """Test auto-generating prefix from platform name."""
        monkeypatch.setenv('BIGQUERY_PROJECT', 'my-project')
        monkeypatch.setenv('BIGQUERY_LOCATION', 'US')

        config = ConnectionConfig.from_env_only('bigquery')
        bq_config = config.get_connection_config('bigquery')

        assert bq_config['project'] == 'my-project'
        assert bq_config['location'] == 'US'

    def test_from_env_only_case_insensitive(self, monkeypatch):
        """Test that env var keys are lowercased."""
        monkeypatch.setenv('PG_HOST', 'localhost')
        monkeypatch.setenv('PG_PORT', '5432')
        monkeypatch.setenv('PG_DATABASE', 'testdb')

        config = ConnectionConfig.from_env_only('postgresql', 'PG_')
        pg_config = config.get_connection_config('postgresql')

        # Keys should be lowercase
        assert pg_config['host'] == 'localhost'
        assert pg_config['port'] == '5432'
        assert pg_config['database'] == 'testdb'


class TestConfigValidation:
    """Test configuration validation."""

    def test_get_connection_config_no_config_loaded(self):
        """Test error when trying to get config without loading any."""
        config = ConnectionConfig(auto_load_env=False)

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_connection_config('bigquery')

        assert 'No configuration loaded' in str(exc_info.value)

    def test_get_default_target_no_config(self):
        """Test error when getting default target with no config."""
        config = ConnectionConfig(auto_load_env=False)

        with pytest.raises(ConfigurationError) as exc_info:
            config.get_default_target()

        assert 'No configuration loaded' in str(exc_info.value)

    def test_list_targets_empty_config(self):
        """Test listing targets with empty config."""
        config = ConnectionConfig(auto_load_env=False)
        assert config.list_targets() == []


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_yaml_with_nested_structure(self, tmp_path, monkeypatch):
        """Test YAML with nested configuration."""
        monkeypatch.setenv('DB_PASSWORD', 'secret')

        yaml_content = {
            'target': 'bigquery',
            'connections': {
                'bigquery': {
                    'project': 'my-project',
                    'options': {
                        'timeout': 300,
                        'retries': 3
                    }
                }
            },
            'pooling': {
                'enabled': True,
                'default': {
                    'min_size': 2,
                    'max_size': 10
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(yaml_content, f)

        config = ConnectionConfig(str(config_file), auto_load_env=False)
        bq_config = config.get_connection_config('bigquery')

        assert bq_config['options']['timeout'] == 300
        assert bq_config['options']['retries'] == 3

    def test_env_var_with_special_characters(self, tmp_path, monkeypatch):
        """Test environment variables with special characters."""
        monkeypatch.setenv('PASSWORD_WITH_SPECIAL', 'p@ssw0rd!#$%')

        yaml_content = {
            'target': 'postgresql',
            'connections': {
                'postgresql': {
                    'password': '${PASSWORD_WITH_SPECIAL}'
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(yaml_content, f)

        config = ConnectionConfig(str(config_file), auto_load_env=False)
        pg_config = config.get_connection_config('postgresql')

        assert pg_config['password'] == 'p@ssw0rd!#$%'

    def test_numeric_values_preserved(self, tmp_path):
        """Test that numeric values in YAML are preserved."""
        yaml_content = {
            'target': 'postgresql',
            'connections': {
                'postgresql': {
                    'port': 5432,  # Integer
                    'timeout': 30.5,  # Float
                    'max_connections': 100
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(yaml_content, f)

        config = ConnectionConfig(str(config_file), auto_load_env=False)
        pg_config = config.get_connection_config('postgresql')

        assert pg_config['port'] == 5432
        assert isinstance(pg_config['port'], int)
        assert pg_config['timeout'] == 30.5
        assert isinstance(pg_config['timeout'], float)

    def test_boolean_values_preserved(self, tmp_path):
        """Test that boolean values in YAML are preserved."""
        yaml_content = {
            'target': 'bigquery',
            'connections': {
                'bigquery': {
                    'use_legacy_sql': False,
                    'require_partition_filter': True
                }
            }
        }

        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(yaml_content, f)

        config = ConnectionConfig(str(config_file), auto_load_env=False)
        bq_config = config.get_connection_config('bigquery')

        assert bq_config['use_legacy_sql'] is False
        assert bq_config['require_partition_filter'] is True
