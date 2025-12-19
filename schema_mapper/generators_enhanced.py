"""
Enhanced DDL generation with clustering, partitioning, and distribution support.

This module extends the base DDL generators to support platform-specific
table optimization features using ddl_mappings configurations.
"""

from typing import List, Dict, Optional
from abc import ABC, abstractmethod
import logging

from .ddl_mappings import (
    DDLOptions,
    ClusteringConfig,
    PartitionConfig,
    DistributionConfig,
    SortKeyConfig,
    PartitionType,
    DistributionStyle,
    get_platform_capabilities,
    get_ddl_templates,
    validate_ddl_options,
)

logger = logging.getLogger(__name__)


class EnhancedDDLGenerator(ABC):
    """Abstract base class for enhanced DDL generators with clustering/partitioning support."""

    def __init__(self, platform: str):
        """
        Initialize generator.

        Args:
            platform: Database platform name
        """
        self.platform = platform
        self.capabilities = get_platform_capabilities(platform)
        self.templates = get_ddl_templates(platform)

    @abstractmethod
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        ddl_options: Optional[DDLOptions] = None
    ) -> str:
        """
        Generate DDL statement with optional clustering/partitioning.

        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Dataset/schema name
            project_id: Project ID (platform-specific)
            ddl_options: DDL optimization options

        Returns:
            DDL statement
        """
        pass

    def _validate_options(self, options: Optional[DDLOptions]) -> List[str]:
        """Validate DDL options against platform capabilities."""
        if not options:
            return []
        return validate_ddl_options(self.platform, options)


class EnhancedBigQueryDDLGenerator(EnhancedDDLGenerator):
    """Generate DDL for BigQuery with partitioning and clustering support."""

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        ddl_options: Optional[DDLOptions] = None
    ) -> str:
        """
        Generate BigQuery CREATE TABLE statement with partitioning/clustering.

        Example output:
            CREATE TABLE `project.dataset.events` (
              event_id INT64,
              user_id INT64,
              event_date DATE
            )
            PARTITION BY event_date
            CLUSTER BY user_id, event_type
            OPTIONS(
              partition_expiration_days=365,
              require_partition_filter=true
            );
        """
        # Validate options
        if ddl_options:
            errors = self._validate_options(ddl_options)
            if errors:
                raise ValueError(f"Invalid DDL options: {', '.join(errors)}")

        # Build full table name
        if project_id and dataset_name:
            full_table = f"`{project_id}.{dataset_name}.{table_name}`"
        elif dataset_name:
            full_table = f"`{dataset_name}.{table_name}`"
        else:
            full_table = f"`{table_name}`"

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            if field.get('description'):
                desc = field['description'].replace('"', '\\"')
                col_def += f" OPTIONS(description=\"{desc}\")"
            col_defs.append(col_def)

        # Build DDL
        ddl_parts = [f"CREATE TABLE {full_table} ("]
        ddl_parts.append(",\n".join(col_defs))
        ddl_parts.append(")")

        # Add partitioning
        if ddl_options and ddl_options.partitioning:
            partition_clause = self._build_partition_clause(ddl_options.partitioning, schema)
            if partition_clause:
                ddl_parts.append(partition_clause)

        # Add clustering
        if ddl_options and ddl_options.clustering and ddl_options.clustering.columns:
            cluster_cols = ", ".join(ddl_options.clustering.columns)
            ddl_parts.append(self.templates['cluster_by'].format(columns=cluster_cols))

        # Add options
        options_parts = []
        if ddl_options and ddl_options.partitioning:
            if ddl_options.partitioning.expiration_days:
                options_parts.append(
                    f"partition_expiration_days={ddl_options.partitioning.expiration_days}"
                )
            if ddl_options.partitioning.require_partition_filter:
                options_parts.append("require_partition_filter=true")

        if options_parts:
            ddl_parts.append(f"OPTIONS(\n  {',\n  '.join(options_parts)}\n)")

        ddl = "\n".join(ddl_parts) + ";"

        logger.info(f"Generated BigQuery DDL for {full_table}")
        return ddl

    def _build_partition_clause(self, partition_config: PartitionConfig, schema: List[Dict]) -> Optional[str]:
        """Build PARTITION BY clause for BigQuery."""
        if not partition_config.column:
            return None

        # Find column type
        col_type = None
        for field in schema:
            if field['name'] == partition_config.column:
                col_type = field['type']
                break

        if not col_type:
            logger.warning(f"Partition column {partition_config.column} not found in schema")
            return None

        # Choose template based on column type
        if col_type == 'DATE':
            return self.templates['partition_by_date'].format(column=partition_config.column)
        elif col_type == 'TIMESTAMP':
            return self.templates['partition_by_timestamp'].format(column=partition_config.column)
        elif col_type == 'INT64' and partition_config.partition_type == PartitionType.RANGE:
            # Range partitioning on integer
            if partition_config.range_start and partition_config.range_end and partition_config.range_interval:
                return self.templates['partition_by_range'].format(
                    column=partition_config.column,
                    start=partition_config.range_start,
                    end=partition_config.range_end,
                    interval=partition_config.range_interval
                )

        return self.templates['partition_by_date'].format(column=partition_config.column)


class EnhancedSnowflakeDDLGenerator(EnhancedDDLGenerator):
    """Generate DDL for Snowflake with clustering support."""

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        ddl_options: Optional[DDLOptions] = None
    ) -> str:
        """
        Generate Snowflake CREATE TABLE statement with clustering.

        Example output:
            CREATE TRANSIENT TABLE analytics.events (
              event_id NUMBER,
              user_id NUMBER,
              event_date DATE
            )
            CLUSTER BY (event_date, user_id);
        """
        # Validate options
        if ddl_options:
            errors = self._validate_options(ddl_options)
            if errors:
                raise ValueError(f"Invalid DDL options: {', '.join(errors)}")

        full_table = f"{dataset_name}.{table_name}" if dataset_name else table_name

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                col_def += f" COMMENT '{desc}'"
            col_defs.append(col_def)

        # Build DDL
        create_clause = "CREATE"
        if ddl_options and ddl_options.transient:
            create_clause += " TRANSIENT"

        ddl_parts = [f"{create_clause} TABLE {full_table} ("]
        ddl_parts.append(",\n".join(col_defs))
        ddl_parts.append(")")

        # Add clustering
        if ddl_options and ddl_options.clustering and ddl_options.clustering.columns:
            cluster_cols = ", ".join(ddl_options.clustering.columns)
            ddl_parts.append(self.templates['cluster_by'].format(columns=cluster_cols))

        ddl = "\n".join(ddl_parts) + ";"

        logger.info(f"Generated Snowflake DDL for {full_table}")
        return ddl


class EnhancedRedshiftDDLGenerator(EnhancedDDLGenerator):
    """Generate DDL for Redshift with distribution and sort keys."""

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        ddl_options: Optional[DDLOptions] = None
    ) -> str:
        """
        Generate Redshift CREATE TABLE statement with distribution/sort keys.

        Example output:
            CREATE TABLE analytics.events (
              event_id BIGINT,
              user_id BIGINT,
              event_date DATE
            )
            DISTSTYLE KEY
            DISTKEY (user_id)
            SORTKEY (event_date, event_ts);
        """
        # Validate options
        if ddl_options:
            errors = self._validate_options(ddl_options)
            if errors:
                raise ValueError(f"Invalid DDL options: {', '.join(errors)}")

        full_table = f"{dataset_name}.{table_name}" if dataset_name else table_name

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f'  "{field["name"]}" {field["type"]}'
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            col_defs.append(col_def)

        # Build DDL
        ddl_parts = [f"CREATE TABLE {full_table} ("]
        ddl_parts.append(",\n".join(col_defs))
        ddl_parts.append(")")

        # Add distribution style
        if ddl_options and ddl_options.distribution:
            dist_config = ddl_options.distribution

            if dist_config.style == DistributionStyle.KEY:
                ddl_parts.append(self.templates['diststyle_key'])
                if dist_config.key_column:
                    ddl_parts.append(self.templates['distkey'].format(column=dist_config.key_column))
            elif dist_config.style == DistributionStyle.ALL:
                ddl_parts.append(self.templates['diststyle_all'])
            elif dist_config.style == DistributionStyle.EVEN:
                ddl_parts.append(self.templates['diststyle_even'])
            elif dist_config.style == DistributionStyle.AUTO:
                ddl_parts.append(self.templates['diststyle_auto'])

        # Add sort keys
        if ddl_options and ddl_options.sort_keys and ddl_options.sort_keys.columns:
            sort_cols = ", ".join(ddl_options.sort_keys.columns)
            if ddl_options.sort_keys.compound:
                ddl_parts.append(self.templates['sortkey_compound'].format(columns=sort_cols))
            else:
                ddl_parts.append(self.templates['sortkey_interleaved'].format(columns=sort_cols))

        ddl = "\n".join(ddl_parts) + ";"

        # Add column comments separately
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"COMMENT ON COLUMN {full_table}.{field['name']} IS '{desc}';"
                comments.append(comment)

        if comments:
            ddl += "\n\n-- Column comments\n" + "\n".join(comments)

        logger.info(f"Generated Redshift DDL for {full_table}")
        return ddl


class EnhancedPostgreSQLDDLGenerator(EnhancedDDLGenerator):
    """Generate DDL for PostgreSQL with partitioning and indexes."""

    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        ddl_options: Optional[DDLOptions] = None
    ) -> str:
        """
        Generate PostgreSQL CREATE TABLE statement with partitioning.

        Example output:
            CREATE TABLE analytics.events (
              event_id BIGINT,
              user_id BIGINT,
              event_date DATE
            ) PARTITION BY RANGE (event_date);

            -- Index for clustering behavior
            CREATE INDEX idx_events_cluster ON analytics.events (event_date, user_id);
        """
        # Validate options
        if ddl_options:
            errors = self._validate_options(ddl_options)
            if errors:
                raise ValueError(f"Invalid DDL options: {', '.join(errors)}")

        full_table = f'"{dataset_name}"."{table_name}"' if dataset_name else f'"{table_name}"'

        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f'  "{field["name"]}" {field["type"]}'
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            col_defs.append(col_def)

        # Build DDL
        ddl_parts = [f"CREATE TABLE {full_table} ("]
        ddl_parts.append(",\n".join(col_defs))
        ddl_parts.append(")")

        # Add partitioning
        if ddl_options and ddl_options.partitioning and ddl_options.partitioning.column:
            partition_type = ddl_options.partitioning.partition_type
            column = ddl_options.partitioning.column

            if partition_type == PartitionType.RANGE:
                ddl_parts.append(self.templates['partition_by_range'].format(column=column))
            elif partition_type == PartitionType.LIST:
                ddl_parts.append(self.templates['partition_by_list'].format(column=column))
            elif partition_type == PartitionType.HASH:
                ddl_parts.append(self.templates['partition_by_hash'].format(column=column))

        ddl = "\n".join(ddl_parts) + ";"

        # Add index for clustering behavior (PostgreSQL doesn't have CLUSTER BY)
        if ddl_options and ddl_options.clustering and ddl_options.clustering.columns:
            index_name = f"idx_{table_name}_cluster"
            index_cols = ", ".join(ddl_options.clustering.columns)
            simple_table = f"{dataset_name}.{table_name}" if dataset_name else table_name
            ddl += f"\n\n-- Index for clustering behavior\n"
            ddl += self.templates['index_for_clustering'].format(
                index_name=index_name,
                table_name=simple_table,
                columns=index_cols
            ) + ";"

        # Add column comments
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"COMMENT ON COLUMN {full_table}.{field['name']} IS '{desc}';"
                comments.append(comment)

        if comments:
            ddl += "\n\n-- Column comments\n" + "\n".join(comments)

        logger.info(f"Generated PostgreSQL DDL for {full_table}")
        return ddl


# Factory function
def get_enhanced_ddl_generator(platform: str) -> EnhancedDDLGenerator:
    """
    Get the appropriate enhanced DDL generator for a platform.

    Args:
        platform: Target database platform

    Returns:
        EnhancedDDLGenerator instance

    Raises:
        ValueError: If platform is not supported
    """
    generators = {
        'bigquery': EnhancedBigQueryDDLGenerator,
        'snowflake': EnhancedSnowflakeDDLGenerator,
        'redshift': EnhancedRedshiftDDLGenerator,
        'postgresql': EnhancedPostgreSQLDDLGenerator,
    }

    platform_lower = platform.lower()
    if platform_lower not in generators:
        raise ValueError(
            f"No enhanced DDL generator for platform: {platform}. "
            f"Supported: {', '.join(generators.keys())}"
        )

    return generators[platform_lower](platform_lower)
