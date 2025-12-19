"""
PostgreSQL renderer - declarative partitioning with indexes.

PostgreSQL characteristics:
- Native declarative partitioning (RANGE, LIST, HASH)
- No native clustering (use indexes)
- Child partitions created separately
- Standard SQL DDL
"""

from typing import Dict, List, Optional
from .base import SchemaRenderer
from ..canonical import CanonicalSchema, LogicalType


class PostgreSQLRenderer(SchemaRenderer):
    """
    PostgreSQL renderer with partitioning and index support.

    Capabilities:
    - Partitioning: RANGE, LIST, HASH (up to 32 columns)
    - Indexes for clustering behavior
    - Expression-based partitioning
    - Standard psql commands
    """

    TYPE_MAPPING = {
        LogicalType.INTEGER: 'INTEGER',
        LogicalType.BIGINT: 'BIGINT',
        LogicalType.FLOAT: 'DOUBLE PRECISION',
        LogicalType.DECIMAL: 'NUMERIC',
        LogicalType.STRING: 'VARCHAR(255)',
        LogicalType.TEXT: 'TEXT',
        LogicalType.BOOLEAN: 'BOOLEAN',
        LogicalType.DATE: 'DATE',
        LogicalType.TIMESTAMP: 'TIMESTAMP',
        LogicalType.TIMESTAMPTZ: 'TIMESTAMPTZ',
        LogicalType.JSON: 'JSONB',
        LogicalType.BINARY: 'BYTEA',
    }

    def platform_name(self) -> str:
        return 'postgresql'

    def validate(self) -> List[str]:
        """Validate against PostgreSQL capabilities."""
        errors = []

        # Validate optimization columns exist
        errors.extend(self._validate_optimization_columns_exist())

        opt = self.schema.optimization
        if not opt:
            return errors

        # Check partition column limits
        if opt.partition_columns and len(opt.partition_columns) > 32:
            errors.append(f"PostgreSQL supports max 32 partition columns, got {len(opt.partition_columns)}")

        # Distribution and sort keys not supported
        if opt.distribution_column:
            errors.append("PostgreSQL does not support distribution keys")

        if opt.sort_columns:
            errors.append("PostgreSQL does not support sort keys (use clustering or indexes instead)")

        return errors

    def to_physical_types(self) -> Dict[str, str]:
        """Convert logical types to PostgreSQL types."""
        result = {}
        for col in self.schema.columns:
            pg_type = self.TYPE_MAPPING.get(col.logical_type, 'TEXT')

            # Handle NUMERIC with precision/scale
            if col.logical_type == LogicalType.DECIMAL:
                if col.precision and col.scale:
                    pg_type = f'NUMERIC({col.precision},{col.scale})'
                elif col.precision:
                    pg_type = f'NUMERIC({col.precision})'

            # Handle VARCHAR with max_length
            elif col.logical_type == LogicalType.STRING:
                if col.max_length:
                    pg_type = f'VARCHAR({col.max_length})'

            result[col.name] = pg_type

        return result

    def to_ddl(self) -> str:
        """Generate PostgreSQL CREATE TABLE DDL with partitioning."""
        from ..generators_enhanced import get_enhanced_ddl_generator
        from ..ddl_mappings import DDLOptions, ClusteringConfig, PartitionConfig, PartitionType

        generator = get_enhanced_ddl_generator('postgresql')

        # Convert canonical schema to PostgreSQL schema format
        pg_schema = self._to_postgresql_schema_format()

        # Build DDL options
        ddl_options = self._build_ddl_options()

        ddl = generator.generate(
            schema=pg_schema,
            table_name=self.schema.table_name,
            dataset_name=self.schema.dataset_name,
            ddl_options=ddl_options
        )

        # Add partition creation hints
        if self.schema.optimization and self.schema.optimization.partition_columns:
            ddl += "\n\n-- Note: Create child partitions separately"
            ddl += "\n-- Example for RANGE partitioning:"
            full_table = f'"{self.schema.dataset_name}"."{self.schema.table_name}"' if self.schema.dataset_name else f'"{self.schema.table_name}"'
            partition_col = self.schema.optimization.partition_columns[0]
            ddl += f"\n-- CREATE TABLE {self.schema.table_name}_2024_q1 PARTITION OF {full_table}"
            ddl += f"\n--   FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');"

        return ddl

    def to_cli_create(self) -> str:
        """Generate psql command to create table."""
        ddl = self.to_ddl()

        return f'''# Save DDL to temporary file
cat > /tmp/postgres_ddl.sql << 'EOF'
{ddl}
EOF

# Execute DDL
psql -h localhost -U your_user -d your_db -f /tmp/postgres_ddl.sql

# Cleanup
rm /tmp/postgres_ddl.sql

# Note: Replace connection details with your actual PostgreSQL server'''

    def to_cli_load(self, data_path: str, **kwargs) -> str:
        """
        Generate PostgreSQL COPY command to load from CSV.

        Args:
            data_path: Path to CSV file
            **kwargs: Additional options
                - delimiter: Field delimiter (default: ',')
                - null_string: String representing NULL (default: '')
                - header: Whether CSV has header (default: True)
        """
        delimiter = kwargs.get('delimiter', ',')
        null_string = kwargs.get('null_string', '')
        header = kwargs.get('header', True)

        full_table = f'"{self.schema.dataset_name}"."{self.schema.table_name}"' if self.schema.dataset_name else f'"{self.schema.table_name}"'

        copy_sql = f"""\\COPY {full_table} FROM '{data_path}'
WITH (
  FORMAT CSV,
  DELIMITER '{delimiter}',
  NULL '{null_string}',
  HEADER {str(header).upper()}
);"""

        return f'''# Load data into PostgreSQL via COPY
psql -h localhost -U your_user -d your_db -c "
{copy_sql}
"

# Verify load
psql -h localhost -U your_user -d your_db -c "
SELECT COUNT(*) as row_count FROM {full_table};
"

# Note: Replace connection details with your actual PostgreSQL server'''

    def _to_postgresql_schema_format(self) -> List[Dict]:
        """Convert canonical columns to PostgreSQL schema format."""
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

    def _build_ddl_options(self) -> Optional['DDLOptions']:
        """Build DDLOptions from canonical optimization hints."""
        from ..ddl_mappings import DDLOptions, ClusteringConfig, PartitionConfig, PartitionType

        opt = self.schema.optimization
        if not opt:
            return None

        ddl_options = DDLOptions()

        # Partitioning
        if opt.partition_columns:
            # Default to RANGE partitioning (most common)
            ddl_options.partitioning = PartitionConfig(
                column=opt.partition_columns[0],
                partition_type=PartitionType.RANGE
            )

        # Clustering (via indexes)
        if opt.cluster_columns:
            ddl_options.clustering = ClusteringConfig(columns=opt.cluster_columns)

        return ddl_options if (ddl_options.partitioning or ddl_options.clustering) else None


__all__ = ['PostgreSQLRenderer']
