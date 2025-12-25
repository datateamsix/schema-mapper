"""
Snowflake renderer - DDL-driven with auto micro-partitioning.

Snowflake characteristics:
- No user-defined partitioning (automatic micro-partitions)
- Clustering for query optimization (up to 4 columns)
- Transient tables for staging
- DDL-only (no JSON schema support)
"""

from typing import Dict, List, Optional
from .base import SchemaRenderer
from ..canonical import CanonicalSchema, LogicalType


class SnowflakeRenderer(SchemaRenderer):
    """
    Snowflake renderer with clustering support.

    Capabilities:
    - Clustering: Up to 4 columns
    - Transient tables
    - Auto micro-partitioning (no user config needed)
    - DDL execution via snowsql
    """

    TYPE_MAPPING = {
        LogicalType.INTEGER: 'NUMBER(38,0)',
        LogicalType.BIGINT: 'NUMBER(38,0)',
        LogicalType.FLOAT: 'FLOAT',
        LogicalType.DECIMAL: 'NUMBER',
        LogicalType.STRING: 'VARCHAR(16777216)',
        LogicalType.TEXT: 'VARCHAR(16777216)',
        LogicalType.BOOLEAN: 'BOOLEAN',
        LogicalType.DATE: 'DATE',
        LogicalType.TIMESTAMP: 'TIMESTAMP_NTZ',
        LogicalType.TIMESTAMPTZ: 'TIMESTAMP_TZ',
        LogicalType.JSON: 'VARIANT',
        LogicalType.BINARY: 'BINARY',
    }

    def platform_name(self) -> str:
        return 'snowflake'

    def validate(self) -> List[str]:
        """Validate against Snowflake capabilities."""
        errors = []

        # Validate optimization columns exist
        errors.extend(self._validate_optimization_columns_exist())

        opt = self.schema.optimization
        if not opt:
            return errors

        # Check clustering limits
        if opt.cluster_columns and len(opt.cluster_columns) > 4:
            errors.append(f"Snowflake supports max 4 clustering columns, got {len(opt.cluster_columns)}")

        # Partitioning not supported (auto micro-partitioning)
        if opt.partition_columns:
            errors.append(
                "Snowflake does not support user-defined partitioning "
                "(uses automatic micro-partitioning). Use clustering instead."
            )

        # Distribution and sort keys not supported
        if opt.distribution_column:
            errors.append("Snowflake does not support distribution keys")

        if opt.sort_columns:
            errors.append("Snowflake does not support sort keys (use clustering instead)")

        return errors

    def to_physical_types(self) -> Dict[str, str]:
        """Convert logical types to Snowflake types."""
        result = {}
        for col in self.schema.columns:
            sf_type = self.TYPE_MAPPING.get(col.logical_type, 'VARCHAR(16777216)')

            # Handle NUMBER with precision/scale
            if col.logical_type == LogicalType.DECIMAL:
                if col.precision and col.scale:
                    sf_type = f'NUMBER({col.precision},{col.scale})'
                elif col.precision:
                    sf_type = f'NUMBER({col.precision})'

            # Handle VARCHAR with max_length
            elif col.logical_type in (LogicalType.STRING, LogicalType.TEXT):
                if col.max_length and col.max_length <= 16777216:
                    sf_type = f'VARCHAR({col.max_length})'

            result[col.name] = sf_type

        return result

    def to_ddl(self) -> str:
        """Generate Snowflake CREATE TABLE DDL with clustering."""
        from ..generators import get_ddl_generator

        generator = get_ddl_generator('snowflake')

        # Convert canonical schema to Snowflake schema format
        sf_schema = self._to_snowflake_schema_format()

        # Extract optimization hints
        opt = self.schema.optimization
        cluster_by = None
        transient = False

        if opt:
            if opt.cluster_columns:
                cluster_by = opt.cluster_columns
            transient = opt.transient

        return generator.generate(
            schema=sf_schema,
            table_name=self.schema.table_name,
            dataset_name=self.schema.dataset_name,
            cluster_by=cluster_by,
            transient=transient
        )

    def to_cli_create(self) -> str:
        """Generate snowsql command to create table."""
        ddl = self.to_ddl()

        # Escape for shell
        ddl_escaped = ddl.replace("'", "'\\''")

        return f"snowsql -q '{ddl_escaped}'"

    def to_cli_load(self, data_path: str, **kwargs) -> str:
        """
        Generate Snowflake COPY INTO command.

        Args:
            data_path: Path to CSV file (local or stage)
            **kwargs: Additional options
                - stage_name: Internal stage name (default: table stage)
                - file_format: CSV (default), JSON, PARQUET
                - on_error: CONTINUE, ABORT_STATEMENT (default), SKIP_FILE
        """
        stage_name = kwargs.get('stage_name', f'%{self.schema.table_name}')
        file_format = kwargs.get('file_format', 'CSV')
        on_error = kwargs.get('on_error', 'ABORT_STATEMENT')

        full_table = f"{self.schema.dataset_name}.{self.schema.table_name}" if self.schema.dataset_name else self.schema.table_name

        lines = [
            '# Upload file to Snowflake stage',
            f"snowsql -q \"PUT file://{data_path} @{stage_name};\"",
            '',
            '# Load data from stage into table',
            f"snowsql -q \"",
            f"COPY INTO {full_table}",
            f"FROM @{stage_name}",
            f"FILE_FORMAT = (TYPE = {file_format}"
        ]

        if file_format == 'CSV':
            lines.append("               SKIP_HEADER = 1")
            lines.append("               FIELD_OPTIONALLY_ENCLOSED_BY = '\\\"')")
        else:
            lines.append(")")

        lines.extend([
            f"ON_ERROR = {on_error};\"",
            '',
            '# Verify load',
            f"snowsql -q \"SELECT COUNT(*) as row_count FROM {full_table};\""
        ])

        return '\n'.join(lines)

    def _to_snowflake_schema_format(self) -> List[Dict]:
        """Convert canonical columns to Snowflake schema format."""
        schema = []
        physical_types = self.to_physical_types()

        for col in self.schema.columns:
            field = {
                'name': col.name,
                'type': physical_types[col.name],
                'mode': 'NULLABLE' if col.nullable else 'REQUIRED'
            }

            if col.description:
                field['description'] = col.description

            schema.append(field)

        return schema



__all__ = ['SnowflakeRenderer']
