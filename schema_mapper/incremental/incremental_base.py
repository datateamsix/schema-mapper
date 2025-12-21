"""
Abstract base classes for incremental load DDL generation.

This module provides the base interface that all platform-specific
incremental DDL generators must implement.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Tuple
import pandas as pd

from .patterns import LoadPattern, IncrementalConfig


class IncrementalDDLGenerator(ABC):
    """
    Abstract base class for platform-specific incremental DDL generators.

    Each platform (BigQuery, Snowflake, etc.) implements this interface
    to generate platform-specific incremental load DDL.

    Attributes:
        platform: Name of the target platform
    """

    def __init__(self, platform: str):
        """
        Initialize the generator for a specific platform.

        Args:
            platform: Platform name (e.g., 'bigquery', 'snowflake')
        """
        self.platform = platform

    @abstractmethod
    def supports_pattern(self, pattern: LoadPattern) -> bool:
        """
        Check if platform supports this load pattern.

        Args:
            pattern: Load pattern to check

        Returns:
            True if platform supports the pattern
        """
        pass

    @abstractmethod
    def generate_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate MERGE (UPSERT) DDL for the platform.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental load configuration
            dataset_name: Dataset/schema name (platform-specific)
            project_id: Project ID (for platforms that need it)
            **kwargs: Additional platform-specific arguments

        Returns:
            DDL statement(s) as string
        """
        pass

    @abstractmethod
    def generate_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate APPEND (INSERT) DDL.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental load configuration
            dataset_name: Dataset/schema name (platform-specific)
            project_id: Project ID (for platforms that need it)
            **kwargs: Additional platform-specific arguments

        Returns:
            DDL statement(s) as string
        """
        pass

    @abstractmethod
    def generate_full_refresh_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate FULL REFRESH DDL.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental load configuration
            dataset_name: Dataset/schema name (platform-specific)
            project_id: Project ID (for platforms that need it)
            **kwargs: Additional platform-specific arguments

        Returns:
            DDL statement(s) as string
        """
        pass

    @abstractmethod
    def generate_scd2_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SCD Type 2 DDL.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental load configuration
            dataset_name: Dataset/schema name (platform-specific)
            project_id: Project ID (for platforms that need it)
            **kwargs: Additional platform-specific arguments

        Returns:
            DDL statement(s) as string
        """
        pass

    @abstractmethod
    def generate_staging_table_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        staging_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate staging table DDL (needed for most patterns).

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Base table name
            staging_name: Custom staging table name (optional)
            dataset_name: Dataset/schema name (platform-specific)
            project_id: Project ID (for platforms that need it)
            **kwargs: Additional platform-specific arguments

        Returns:
            DDL statement(s) as string
        """
        pass

    @abstractmethod
    def get_max_timestamp_query(
        self,
        table_name: str,
        timestamp_column: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate query to get max timestamp from target table.

        Args:
            table_name: Target table name
            timestamp_column: Timestamp column name
            dataset_name: Dataset/schema name (platform-specific)
            project_id: Project ID (for platforms that need it)
            **kwargs: Additional platform-specific arguments

        Returns:
            SQL query string
        """
        pass

    def generate_incremental_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Main entry point - generates DDL based on load pattern.

        Routes to appropriate method based on config.load_pattern.

        Args:
            schema: Schema definition from SchemaMapper
            table_name: Target table name
            config: Incremental load configuration
            dataset_name: Dataset/schema name (platform-specific)
            project_id: Project ID (for platforms that need it)
            **kwargs: Additional platform-specific arguments

        Returns:
            DDL statement(s) as string

        Raises:
            ValueError: If pattern is not supported by platform
            NotImplementedError: If pattern is not yet implemented
        """
        # Validate config
        config.validate()

        # Check platform support
        if not self.supports_pattern(config.load_pattern):
            raise ValueError(
                f"{self.platform} does not support {config.load_pattern.value}"
            )

        # Route to appropriate generator
        if config.load_pattern == LoadPattern.UPSERT:
            return self.generate_merge_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.APPEND_ONLY:
            return self.generate_append_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.FULL_REFRESH:
            return self.generate_full_refresh_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.SCD_TYPE2:
            return self.generate_scd2_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.INCREMENTAL_TIMESTAMP:
            return self.generate_incremental_timestamp_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.INCREMENTAL_APPEND:
            return self.generate_incremental_append_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.DELETE_INSERT:
            return self.generate_delete_insert_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.CDC_MERGE:
            return self.generate_cdc_merge_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        elif config.load_pattern == LoadPattern.SCD_TYPE1:
            return self.generate_scd1_ddl(
                schema, table_name, config, dataset_name, project_id, **kwargs
            )

        else:
            raise NotImplementedError(
                f"Load pattern {config.load_pattern.value} not yet implemented for {self.platform}"
            )

    # Optional methods with default implementations
    def generate_incremental_timestamp_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental timestamp-based load DDL.

        Default implementation uses APPEND with timestamp filter.
        Can be overridden for platform-specific optimizations.
        """
        raise NotImplementedError(
            f"INCREMENTAL_TIMESTAMP not implemented for {self.platform}"
        )

    def generate_incremental_append_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental append DDL (append only new records).

        Default implementation uses NOT EXISTS check.
        Can be overridden for platform-specific optimizations.
        """
        raise NotImplementedError(
            f"INCREMENTAL_APPEND not implemented for {self.platform}"
        )

    def generate_delete_insert_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate DELETE + INSERT DDL.

        Useful for platforms without MERGE support (e.g., older Redshift).
        """
        raise NotImplementedError(
            f"DELETE_INSERT not implemented for {self.platform}"
        )

    def generate_cdc_merge_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate CDC (Change Data Capture) merge DDL.

        Processes I/U/D operations from CDC stream.
        """
        raise NotImplementedError(
            f"CDC_MERGE not implemented for {self.platform}"
        )

    def generate_scd1_ddl(
        self,
        schema: List[Dict],
        table_name: str,
        config: IncrementalConfig,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate SCD Type 1 DDL (overwrite changes).

        Similar to UPSERT but specifically for dimension tables.
        """
        raise NotImplementedError(
            f"SCD_TYPE1 not implemented for {self.platform}"
        )

    # Helper methods for subclasses
    def _format_column_list(
        self,
        columns: List[str],
        prefix: str = "",
        quote: bool = False
    ) -> str:
        """
        Format column list for SQL.

        Args:
            columns: List of column names
            prefix: Optional table alias prefix
            quote: Whether to quote column names

        Returns:
            Comma-separated column list
        """
        if quote:
            columns = [f'`{col}`' if self.platform == 'bigquery' else f'"{col}"' for col in columns]

        if prefix:
            return ", ".join([f"{prefix}.{col}" for col in columns])
        return ", ".join(columns)

    def _get_non_key_columns(
        self,
        schema: List[Dict],
        primary_keys: List[str]
    ) -> List[str]:
        """
        Get all columns except primary keys.

        Args:
            schema: Schema definition
            primary_keys: List of primary key column names

        Returns:
            List of non-key column names
        """
        all_columns = [field['name'] for field in schema]
        return [col for col in all_columns if col not in primary_keys]

    def _build_join_condition(
        self,
        primary_keys: List[str],
        target_alias: str = "target",
        source_alias: str = "source",
        quote: bool = False
    ) -> str:
        """
        Build JOIN ON condition for primary keys.

        Args:
            primary_keys: List of primary key column names
            target_alias: Alias for target table
            source_alias: Alias for source table
            quote: Whether to quote column names

        Returns:
            JOIN condition SQL string
        """
        if quote:
            quote_char = '`' if self.platform == 'bigquery' else '"'
            conditions = [
                f"{target_alias}.{quote_char}{key}{quote_char} = {source_alias}.{quote_char}{key}{quote_char}"
                for key in primary_keys
            ]
        else:
            conditions = [
                f"{target_alias}.{key} = {source_alias}.{key}"
                for key in primary_keys
            ]
        return " AND ".join(conditions)

    def _get_update_set_clause(
        self,
        columns: List[str],
        source_alias: str = "source",
        target_alias: str = "target",
        quote: bool = False
    ) -> str:
        """
        Build UPDATE SET clause for non-key columns.

        Args:
            columns: List of column names to update
            source_alias: Alias for source table
            target_alias: Alias for target table (unused, kept for compatibility)
            quote: Whether to quote column names

        Returns:
            UPDATE SET clause SQL string
        """
        if quote:
            quote_char = '`' if self.platform == 'bigquery' else '"'
            updates = [
                f"{quote_char}{col}{quote_char} = {source_alias}.{quote_char}{col}{quote_char}"
                for col in columns
            ]
        else:
            updates = [
                f"{col} = {source_alias}.{col}"
                for col in columns
            ]
        return ", ".join(updates)

    def _build_full_table_name(
        self,
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """
        Build fully qualified table name.

        Args:
            table_name: Table name
            dataset_name: Dataset/schema name
            project_id: Project ID (BigQuery only)

        Returns:
            Fully qualified table name
        """
        if self.platform == 'bigquery':
            if project_id and dataset_name:
                return f"`{project_id}.{dataset_name}.{table_name}`"
            elif dataset_name:
                return f"`{dataset_name}.{table_name}`"
            else:
                return f"`{table_name}`"
        else:
            # For other platforms (schema.table format)
            if dataset_name:
                return f'"{dataset_name}"."{table_name}"'
            else:
                return f'"{table_name}"'


class PlatformCapabilities:
    """
    Define what each platform supports for incremental loads.

    This class provides a centralized registry of platform capabilities
    to help determine which patterns and features are available.
    """

    _CAPABILITIES = {
        'bigquery': {
            'native_merge': True,
            'merge_with_delete': True,
            'transactional': False,
            'staging_required': True,
            'supports_cte': True,
            'supports_temp_tables': True,
            'partition_pruning': True,
            'clustering': True,
            'max_statement_length': None,  # No hard limit
            'quote_character': '`',
        },
        'snowflake': {
            'native_merge': True,
            'merge_with_delete': True,
            'transactional': True,
            'staging_required': True,
            'supports_cte': True,
            'supports_temp_tables': True,
            'partition_pruning': False,
            'clustering': True,
            'max_statement_length': None,
            'quote_character': '"',
        },
        'redshift': {
            'native_merge': False,  # No MERGE statement in older versions
            'merge_with_delete': True,
            'transactional': True,
            'staging_required': True,
            'supports_cte': True,
            'supports_temp_tables': True,
            'partition_pruning': False,
            'clustering': False,
            'max_statement_length': 16777216,  # 16 MB
            'quote_character': '"',
        },
        'sqlserver': {
            'native_merge': True,
            'merge_with_delete': True,
            'transactional': True,
            'staging_required': True,
            'supports_cte': True,
            'supports_temp_tables': True,
            'partition_pruning': True,
            'clustering': False,
            'max_statement_length': None,
            'quote_character': '"',
        },
        'postgresql': {
            'native_merge': True,  # UPSERT via ON CONFLICT
            'merge_with_delete': False,  # ON CONFLICT doesn't support DELETE
            'transactional': True,
            'staging_required': True,
            'supports_cte': True,
            'supports_temp_tables': True,
            'partition_pruning': True,
            'clustering': False,
            'max_statement_length': None,
            'quote_character': '"',
        },
    }

    @classmethod
    def get_capabilities(cls, platform: str) -> Dict[str, any]:
        """
        Get capabilities for a platform.

        Args:
            platform: Platform name

        Returns:
            Dictionary of platform capabilities

        Example:
            >>> caps = PlatformCapabilities.get_capabilities('bigquery')
            >>> print(caps['native_merge'])
            True
        """
        return cls._CAPABILITIES.get(platform.lower(), {})

    @classmethod
    def supports_feature(cls, platform: str, feature: str) -> bool:
        """
        Check if a platform supports a specific feature.

        Args:
            platform: Platform name
            feature: Feature name (e.g., 'native_merge')

        Returns:
            True if feature is supported
        """
        caps = cls.get_capabilities(platform)
        return caps.get(feature, False)
