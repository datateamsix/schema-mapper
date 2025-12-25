"""
BigQuery renderer - the only platform with native JSON schema support.

BigQuery is unique:
- Supports both JSON schemas (for bq load) and DDL
- Native partitioning and clustering
- Schema can be specified at load time or DDL time
"""

import json
from typing import Dict, List, Optional
from .base import SchemaRenderer
from ..canonical import CanonicalSchema, LogicalType, ColumnDefinition


class BigQueryRenderer(SchemaRenderer):
    """
    BigQuery renderer with JSON schema and DDL support.

    Capabilities:
    - Partitioning: DATE, TIMESTAMP, INTEGER RANGE
    - Clustering: Up to 4 columns
    - Native JSON schema for bq load command
    - Partition expiration and filtering
    """

    TYPE_MAPPING = {
        LogicalType.INTEGER: 'INT64',
        LogicalType.BIGINT: 'INT64',
        LogicalType.FLOAT: 'FLOAT64',
        LogicalType.DECIMAL: 'NUMERIC',
        LogicalType.STRING: 'STRING',
        LogicalType.TEXT: 'STRING',
        LogicalType.BOOLEAN: 'BOOL',
        LogicalType.DATE: 'DATE',
        LogicalType.TIMESTAMP: 'TIMESTAMP',
        LogicalType.TIMESTAMPTZ: 'TIMESTAMP',
        LogicalType.JSON: 'JSON',
        LogicalType.BINARY: 'BYTES',
    }

    def platform_name(self) -> str:
        return 'bigquery'

    def validate(self) -> List[str]:
        """Validate against BigQuery capabilities."""
        errors = []

        # Validate optimization columns exist
        errors.extend(self._validate_optimization_columns_exist())

        opt = self.schema.optimization
        if not opt:
            return errors

        # Check clustering limits
        if opt.cluster_columns and len(opt.cluster_columns) > 4:
            errors.append(f"BigQuery supports max 4 clustering columns, got {len(opt.cluster_columns)}")

        # Check partitioning limits
        if opt.partition_columns and len(opt.partition_columns) > 1:
            errors.append(f"BigQuery supports max 1 partition column, got {len(opt.partition_columns)}")

        # Validate partition column type
        if opt.partition_columns:
            partition_col = opt.partition_columns[0]
            col_def = self.schema.get_column(partition_col)
            if col_def:
                valid_types = [LogicalType.DATE, LogicalType.TIMESTAMP, LogicalType.TIMESTAMPTZ, LogicalType.INTEGER]
                if col_def.logical_type not in valid_types:
                    errors.append(
                        f"BigQuery partition column must be DATE, TIMESTAMP, or INTEGER, "
                        f"got {col_def.logical_type.value}"
                    )

        # Distribution and sort keys not supported
        if opt.distribution_column:
            errors.append("BigQuery does not support distribution keys")

        if opt.sort_columns:
            errors.append("BigQuery does not support sort keys (use clustering instead)")

        return errors

    def supports_json_schema(self) -> bool:
        """BigQuery natively supports JSON schemas."""
        return True

    def to_physical_types(self) -> Dict[str, str]:
        """Convert logical types to BigQuery types."""
        result = {}
        for col in self.schema.columns:
            bq_type = self.TYPE_MAPPING.get(col.logical_type, 'STRING')

            # Handle NUMERIC with precision/scale
            if col.logical_type == LogicalType.DECIMAL and col.precision and col.scale:
                bq_type = f'NUMERIC({col.precision},{col.scale})'

            result[col.name] = bq_type

        return result

    def to_schema_json(self) -> str:
        """
        Generate BigQuery JSON schema for bq load command.

        This is the native format for:
        - bq load command
        - BigQuery UI
        - API calls
        """
        fields = []
        physical_types = self.to_physical_types()

        for col in self.schema.columns:
            field = {
                "name": col.name,
                "type": physical_types[col.name],
                "mode": "NULLABLE" if col.nullable else "REQUIRED"
            }

            if col.description:
                field["description"] = col.description

            fields.append(field)

        return json.dumps(fields, indent=2)

    def to_ddl(self) -> str:
        """
        Generate BigQuery CREATE TABLE DDL with partitioning/clustering.

        Uses the unified DDL generator with simplified API.
        """
        from ..generators import get_ddl_generator

        generator = get_ddl_generator('bigquery')

        # Convert canonical schema to BigQuery schema format
        bq_schema = self._to_bigquery_schema_format()

        # Extract optimization hints
        opt = self.schema.optimization
        cluster_by = None
        partition_by = None
        partition_type = 'time'
        partition_expiration_days = None
        require_partition_filter = False

        if opt and opt.has_optimizations():
            # Clustering
            if opt.cluster_columns:
                cluster_by = opt.cluster_columns

            # Partitioning
            if opt.partition_columns:
                partition_by = opt.partition_columns[0]
                col_def = self.schema.get_column(partition_by)

                # Determine partition type from column type
                if col_def and col_def.logical_type == LogicalType.INTEGER:
                    partition_type = 'range'
                else:
                    partition_type = 'time'

                partition_expiration_days = opt.partition_expiration_days
                require_partition_filter = opt.require_partition_filter

        return generator.generate(
            schema=bq_schema,
            table_name=self.schema.table_name,
            dataset_name=self.schema.dataset_name,
            project_id=self.schema.project_id,
            cluster_by=cluster_by,
            partition_by=partition_by,
            partition_type=partition_type,
            partition_expiration_days=partition_expiration_days,
            require_partition_filter=require_partition_filter
        )

    def to_cli_create(self) -> str:
        """
        Generate bq CLI command to create table.

        Strategy:
        - If partitioning/clustering: Use DDL via bq query
        - Otherwise: Use simpler bq mk --table with JSON schema
        """
        opt = self.schema.optimization

        # If we have partitioning or clustering, must use DDL
        if (opt and (opt.partition_columns or opt.cluster_columns)):
            ddl = self.to_ddl()
            # Escape quotes for shell
            ddl_escaped = ddl.replace('"', '\\"').replace('`', '\\`')
            return f'bq query --use_legacy_sql=false "{ddl_escaped}"'

        # Otherwise, can use simpler schema-based creation
        full_table = self._get_full_table_name_for_bq()
        schema_json = self.to_schema_json()

        # Create temp file approach (more reliable than inline)
        return f'''# Save schema to temporary file
cat > /tmp/bq_schema.json << 'EOF'
{schema_json}
EOF

# Create table with schema
bq mk --table --schema=/tmp/bq_schema.json {full_table}

# Cleanup
rm /tmp/bq_schema.json'''

    def to_cli_load(self, data_path: str, **kwargs) -> str:
        """
        Generate bq load command.

        Args:
            data_path: Path to CSV file
            **kwargs: Additional options
                - source_format: CSV (default), JSON, PARQUET
                - skip_leading_rows: Number of header rows to skip (default: 1)
                - write_disposition: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY (default)
        """
        source_format = kwargs.get('source_format', 'CSV')
        skip_leading_rows = kwargs.get('skip_leading_rows', 1)
        write_disposition = kwargs.get('write_disposition', 'WRITE_EMPTY')

        full_table = self._get_full_table_name_for_bq()
        schema_json = self.to_schema_json()

        cmd_parts = [
            '# Save schema to temporary file',
            "cat > /tmp/bq_schema.json << 'EOF'",
            schema_json,
            'EOF',
            '',
            '# Load data into BigQuery',
            f'bq load \\',
            f'  --source_format={source_format} \\',
        ]

        if source_format == 'CSV':
            cmd_parts.append(f'  --skip_leading_rows={skip_leading_rows} \\')

        cmd_parts.extend([
            f'  --write_disposition={write_disposition} \\',
            f'  --schema=/tmp/bq_schema.json \\',
            f'  {full_table} \\',
            f'  {data_path}',
            '',
            '# Cleanup',
            'rm /tmp/bq_schema.json'
        ])

        return '\n'.join(cmd_parts)

    def _get_full_table_name_for_bq(self) -> str:
        """Get BigQuery-style table name (project:dataset.table)."""
        parts = []

        if self.schema.project_id:
            parts.append(f"{self.schema.project_id}:")

        if self.schema.dataset_name:
            parts.append(f"{self.schema.dataset_name}.")

        parts.append(self.schema.table_name)

        return ''.join(parts)

    def _to_bigquery_schema_format(self) -> List[Dict]:
        """Convert canonical columns to BigQuery schema format."""
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



__all__ = ['BigQueryRenderer']
