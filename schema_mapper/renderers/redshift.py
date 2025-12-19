"""
Redshift renderer - distribution and sort key optimization.

Redshift characteristics:
- No native partitioning
- Distribution styles: AUTO, KEY, ALL, EVEN
- Sort keys: COMPOUND or INTERLEAVED
- Load via COPY from S3
"""

from typing import Dict, List, Optional
from .base import SchemaRenderer
from ..canonical import CanonicalSchema, LogicalType


class RedshiftRenderer(SchemaRenderer):
    """
    Redshift renderer with distribution and sort key support.

    Capabilities:
    - Distribution: AUTO, KEY, ALL, EVEN
    - Sort keys: COMPOUND or INTERLEAVED (up to 400 columns)
    - COPY command from S3
    - No partitioning (use date-based tables instead)
    """

    TYPE_MAPPING = {
        LogicalType.INTEGER: 'INTEGER',
        LogicalType.BIGINT: 'BIGINT',
        LogicalType.FLOAT: 'DOUBLE PRECISION',
        LogicalType.DECIMAL: 'DECIMAL',
        LogicalType.STRING: 'VARCHAR(256)',
        LogicalType.TEXT: 'VARCHAR(65535)',
        LogicalType.BOOLEAN: 'BOOLEAN',
        LogicalType.DATE: 'DATE',
        LogicalType.TIMESTAMP: 'TIMESTAMP',
        LogicalType.TIMESTAMPTZ: 'TIMESTAMPTZ',
        LogicalType.JSON: 'SUPER',  # Redshift SUPER type for semi-structured data
        LogicalType.BINARY: 'VARBYTE',
    }

    def platform_name(self) -> str:
        return 'redshift'

    def validate(self) -> List[str]:
        """Validate against Redshift capabilities."""
        errors = []

        # Validate optimization columns exist
        errors.extend(self._validate_optimization_columns_exist())

        opt = self.schema.optimization
        if not opt:
            return errors

        # Check sort key limits
        if opt.sort_columns and len(opt.sort_columns) > 400:
            errors.append(f"Redshift supports max 400 sort key columns, got {len(opt.sort_columns)}")

        # Partitioning not supported
        if opt.partition_columns:
            errors.append(
                "Redshift does not support native partitioning. "
                "Use date-based table names or sort keys instead."
            )

        # Clustering not supported (use sort keys)
        if opt.cluster_columns:
            errors.append("Redshift does not support clustering (use sort keys instead)")

        return errors

    def to_physical_types(self) -> Dict[str, str]:
        """Convert logical types to Redshift types."""
        result = {}
        for col in self.schema.columns:
            rs_type = self.TYPE_MAPPING.get(col.logical_type, 'VARCHAR(256)')

            # Handle DECIMAL with precision/scale
            if col.logical_type == LogicalType.DECIMAL:
                if col.precision and col.scale:
                    rs_type = f'DECIMAL({col.precision},{col.scale})'
                elif col.precision:
                    rs_type = f'DECIMAL({col.precision})'

            # Handle VARCHAR with max_length
            elif col.logical_type in (LogicalType.STRING, LogicalType.TEXT):
                if col.max_length:
                    # Redshift VARCHAR max is 65535
                    max_len = min(col.max_length, 65535)
                    rs_type = f'VARCHAR({max_len})'

            result[col.name] = rs_type

        return result

    def to_ddl(self) -> str:
        """Generate Redshift CREATE TABLE DDL with distribution/sort keys."""
        from ..generators_enhanced import get_enhanced_ddl_generator
        from ..ddl_mappings import DDLOptions, DistributionConfig, SortKeyConfig, DistributionStyle

        generator = get_enhanced_ddl_generator('redshift')

        # Convert canonical schema to Redshift schema format
        rs_schema = self._to_redshift_schema_format()

        # Build DDL options
        ddl_options = self._build_ddl_options()

        return generator.generate(
            schema=rs_schema,
            table_name=self.schema.table_name,
            dataset_name=self.schema.dataset_name,
            ddl_options=ddl_options
        )

    def to_cli_create(self) -> str:
        """Generate psql command to create table."""
        ddl = self.to_ddl()

        # Save DDL to temp file and execute
        return f'''# Save DDL to temporary file
cat > /tmp/redshift_ddl.sql << 'EOF'
{ddl}
EOF

# Execute DDL
psql -h your-cluster.redshift.amazonaws.com -U your_user -d your_db -f /tmp/redshift_ddl.sql

# Cleanup
rm /tmp/redshift_ddl.sql

# Note: Replace connection details with your actual Redshift cluster'''

    def to_cli_load(self, data_path: str, **kwargs) -> str:
        """
        Generate Redshift COPY command to load from S3.

        Args:
            data_path: S3 path (s3://bucket/path/) or local path
            **kwargs: Additional options
                - iam_role: IAM role ARN (required for S3 access)
                - region: AWS region
                - date_format: Date format string (default: 'auto')
                - time_format: Time format string (default: 'auto')
                - delimiter: Field delimiter (default: ',')
        """
        iam_role = kwargs.get('iam_role', 'arn:aws:iam::ACCOUNT:role/RedshiftCopyRole')
        region = kwargs.get('region', 'us-east-1')
        date_format = kwargs.get('date_format', 'auto')
        time_format = kwargs.get('time_format', 'auto')
        delimiter = kwargs.get('delimiter', ',')

        full_table = f"{self.schema.dataset_name}.{self.schema.table_name}" if self.schema.dataset_name else self.schema.table_name

        # Check if path is already S3
        if not data_path.startswith('s3://'):
            s3_path = f"s3://your-bucket/data/{self.schema.table_name}/"
            upload_step = f'''# Upload local file to S3
aws s3 cp {data_path} {s3_path}
'''
        else:
            s3_path = data_path
            upload_step = '# Data already in S3\n'

        copy_sql = f"""COPY {full_table}
FROM '{s3_path}'
IAM_ROLE '{iam_role}'
REGION '{region}'
CSV
IGNOREHEADER 1
DELIMITER '{delimiter}'
DATEFORMAT '{date_format}'
TIMEFORMAT '{time_format}'
REMOVEQUOTES
EMPTYASNULL
BLANKSASNULL
MAXERROR 100;"""

        return f'''{upload_step}
# Load data into Redshift via COPY
psql -h your-cluster.redshift.amazonaws.com -U your_user -d your_db -c "
{copy_sql}
"

# Verify load
psql -h your-cluster.redshift.amazonaws.com -U your_user -d your_db -c "
SELECT COUNT(*) as row_count FROM {full_table};
"

# Note: Replace connection details and IAM role with your actual values'''

    def _to_redshift_schema_format(self) -> List[Dict]:
        """Convert canonical columns to Redshift schema format."""
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
        from ..ddl_mappings import DDLOptions, DistributionConfig, SortKeyConfig, DistributionStyle

        opt = self.schema.optimization
        if not opt:
            return None

        ddl_options = DDLOptions()

        # Distribution
        if opt.distribution_column:
            ddl_options.distribution = DistributionConfig(
                style=DistributionStyle.KEY,
                key_column=opt.distribution_column
            )
        else:
            # Default to AUTO if not specified
            ddl_options.distribution = DistributionConfig(style=DistributionStyle.AUTO)

        # Sort keys (use cluster_columns or sort_columns)
        sort_cols = opt.sort_columns or opt.cluster_columns
        if sort_cols:
            ddl_options.sort_keys = SortKeyConfig(
                columns=sort_cols,
                compound=True  # Default to COMPOUND (better for most use cases)
            )

        return ddl_options if (ddl_options.distribution or ddl_options.sort_keys) else None


__all__ = ['RedshiftRenderer']
