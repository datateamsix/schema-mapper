"""
Command-line interface for schema-mapper (Renderer Architecture).

This CLI is built on the canonical schema + renderer pattern:
1. Infer canonical schema from CSV
2. Use platform-specific renderers
3. Output DDL, JSON, CLI commands, or canonical schema
"""

import argparse
import sys
import pandas as pd
import json
from pathlib import Path

from . import __version__
from .canonical import infer_canonical_schema, canonical_schema_to_dict
from .renderers import RendererFactory


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Schema Mapper - Universal Data Warehouse Schema Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate BigQuery DDL with partitioning/clustering
  schema-mapper events.csv --platform bigquery --ddl \\
    --table-name events --dataset-name analytics \\
    --partition-by event_date --cluster-by "user_id,event_type"

  # Generate for all platforms
  schema-mapper events.csv --platform all --ddl --table-name events

  # Output canonical schema (for version control)
  schema-mapper events.csv --canonical --table-name events -o schema.json

  # Generate CLI commands to create and load
  schema-mapper events.csv --platform bigquery --cli-create --table-name events
  schema-mapper events.csv --platform bigquery --cli-load events_clean.csv

  # Snowflake with clustering
  schema-mapper events.csv --platform snowflake --ddl \\
    --cluster-by "event_date,user_id" --transient

  # Redshift with distribution and sort keys
  schema-mapper events.csv --platform redshift --ddl \\
    --distribution-key user_id --sort-keys "event_date,event_ts"

Supported platforms: bigquery, snowflake, redshift, postgresql
        """
    )

    # Input/Output
    parser.add_argument('input', help='Input CSV file')
    parser.add_argument('--output', '-o', help='Output file path (default: stdout)')

    # Platform
    parser.add_argument(
        '--platform',
        choices=['bigquery', 'snowflake', 'redshift', 'postgresql', 'all'],
        default='bigquery',
        help='Target database platform (default: bigquery)'
    )

    # Table Identity
    parser.add_argument('--table-name', default='table', help='Table name')
    parser.add_argument('--dataset-name', help='Dataset/schema name')
    parser.add_argument('--project-id', help='Project ID (BigQuery only)')

    # Output Format
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument('--ddl', action='store_true', help='Generate DDL (default)')
    output_group.add_argument('--json-schema', action='store_true', help='Generate JSON schema (BigQuery only)')
    output_group.add_argument('--cli-create', action='store_true', help='Generate CLI command to create table')
    output_group.add_argument('--cli-load', help='Generate CLI command to load data from file')
    output_group.add_argument('--canonical', action='store_true', help='Output canonical schema as JSON')

    # Clustering
    parser.add_argument(
        '--cluster-by',
        help='Comma-separated clustering columns (e.g., "user_id,event_type")'
    )

    # Partitioning
    parser.add_argument('--partition-by', help='Partition column')
    parser.add_argument(
        '--partition-type',
        choices=['time', 'range', 'list', 'hash'],
        help='Partition type (default: auto-detect from column type)'
    )
    parser.add_argument(
        '--partition-expiration-days',
        type=int,
        help='Auto-delete partitions after N days (BigQuery)'
    )
    parser.add_argument(
        '--require-partition-filter',
        action='store_true',
        help='Require partition filter in queries (BigQuery)'
    )

    # Distribution (Redshift)
    parser.add_argument('--distribution-key', help='Distribution column (Redshift)')
    parser.add_argument(
        '--distribution-style',
        choices=['auto', 'key', 'all', 'even'],
        default='auto',
        help='Distribution style (Redshift, default: auto)'
    )

    # Sort Keys (Redshift)
    parser.add_argument(
        '--sort-keys',
        help='Comma-separated sort key columns (Redshift)'
    )
    parser.add_argument(
        '--sort-interleaved',
        action='store_true',
        help='Use INTERLEAVED sort keys instead of COMPOUND (Redshift)'
    )

    # Table Options
    parser.add_argument('--transient', action='store_true', help='Create transient table (Snowflake)')

    # Data Processing
    parser.add_argument('--no-standardize', action='store_true', help='Do not standardize column names')
    parser.add_argument('--no-auto-cast', action='store_true', help='Do not auto-cast types')
    parser.add_argument('--validate', action='store_true', help='Validate data and show issues')

    # Prepare data
    parser.add_argument('--prepare', action='store_true', help='Prepare and clean data (output CSV)')

    # Version
    parser.add_argument('--version', action='version', version=f'schema-mapper {__version__}')

    args = parser.parse_args()

    # Default to DDL if no output format specified
    if not any([args.ddl, args.json_schema, args.cli_create, args.cli_load, args.canonical, args.prepare]):
        args.ddl = True

    # Read input file
    try:
        df = pd.read_csv(args.input)
        print(f"📊 Loaded {len(df):,} rows, {len(df.columns)} columns", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error reading input file: {e}", file=sys.stderr)
        sys.exit(1)

    # Validate if requested
    if args.validate:
        from .validators import validate_dataframe
        issues = validate_dataframe(df)
        if issues['errors']:
            print("\n❌ Validation Errors:", file=sys.stderr)
            for error in issues['errors']:
                print(f"  • {error}", file=sys.stderr)
        if issues['warnings']:
            print("\n⚠️  Validation Warnings:", file=sys.stderr)
            for warning in issues['warnings']:
                print(f"  • {warning}", file=sys.stderr)
        if issues['errors']:
            sys.exit(1)

    standardize = not args.no_standardize
    auto_cast = not args.no_auto_cast

    # If just preparing data, do that and exit
    if args.prepare:
        prepare_data(df, args, standardize, auto_cast)
        return

    # Parse optimization options
    cluster_columns = parse_column_list(args.cluster_by) if args.cluster_by else []
    partition_columns = [args.partition_by] if args.partition_by else []
    sort_columns = parse_column_list(args.sort_keys) if args.sort_keys else []

    # Infer canonical schema
    try:
        canonical = infer_canonical_schema(
            df,
            table_name=args.table_name,
            dataset_name=args.dataset_name,
            project_id=args.project_id,
            partition_columns=partition_columns,
            cluster_columns=cluster_columns,
            sort_columns=sort_columns,
            distribution_column=args.distribution_key,
            partition_expiration_days=args.partition_expiration_days,
            require_partition_filter=args.require_partition_filter,
            transient=args.transient,
            standardize_columns=standardize,
            auto_cast=auto_cast
        )

        print(f"✅ Canonical schema created: {len(canonical.columns)} columns", file=sys.stderr)

    except Exception as e:
        print(f"❌ Error creating canonical schema: {e}", file=sys.stderr)
        sys.exit(1)

    # Validate canonical schema
    errors = canonical.validate()
    if errors:
        print(f"\n❌ Schema validation errors:", file=sys.stderr)
        for error in errors:
            print(f"  • {error}", file=sys.stderr)
        sys.exit(1)

    # Output canonical schema if requested
    if args.canonical:
        output_canonical_schema(canonical, args.output)
        return

    # Determine platforms
    platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql'] if args.platform == 'all' else [args.platform]

    # Generate output for each platform
    results = {}
    for platform in platforms:
        try:
            result = generate_for_platform(platform, canonical, args)
            if result:
                results[platform] = result
        except ValueError as e:
            print(f"\n⚠️  {platform}: {e}", file=sys.stderr)
            if len(platforms) == 1:
                sys.exit(1)
            continue

    # Output results
    output_results(results, args.output, platforms)


def parse_column_list(column_str: str) -> list:
    """Parse comma-separated column list."""
    return [col.strip() for col in column_str.split(',') if col.strip()]


def prepare_data(df, args, standardize, auto_cast):
    """Prepare and clean data, output as CSV."""
    from .core import SchemaMapper

    platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql'] if args.platform == 'all' else [args.platform]

    for platform in platforms:
        mapper = SchemaMapper(platform)
        df_prepared = mapper.prepare_dataframe(df, standardize, auto_cast)

        if args.output:
            output_path = Path(args.output)
            if len(platforms) > 1:
                stem = output_path.stem
                suffix = output_path.suffix
                output_path = output_path.parent / f"{stem}_{platform}{suffix}"
            df_prepared.to_csv(output_path, index=False)
            print(f"✅ {platform}: Saved to {output_path}", file=sys.stderr)
        else:
            print(f"\n{'='*60}", file=sys.stderr)
            print(f"{platform.upper()}", file=sys.stderr)
            print('='*60, file=sys.stderr)
            print(df_prepared.to_csv(index=False))


def output_canonical_schema(canonical, output_path):
    """Output canonical schema as JSON."""
    schema_dict = canonical_schema_to_dict(canonical)
    schema_json = json.dumps(schema_dict, indent=2)

    if output_path:
        with open(output_path, 'w') as f:
            f.write(schema_json)
        print(f"✅ Canonical schema saved to {output_path}", file=sys.stderr)
    else:
        print(schema_json)


def generate_for_platform(platform: str, canonical, args):
    """Generate output for a specific platform."""
    # Get renderer
    renderer = RendererFactory.get_renderer(platform, canonical)

    # Generate requested output
    if args.ddl:
        return renderer.to_ddl()
    elif args.json_schema:
        if renderer.supports_json_schema():
            return renderer.to_schema_json()
        else:
            raise ValueError(f"{platform} does not support JSON schemas (only BigQuery does)")
    elif args.cli_create:
        return renderer.to_cli_create()
    elif args.cli_load:
        return renderer.to_cli_load(args.cli_load)

    return None


def output_results(results, output_path, platforms):
    """Output results to file or stdout."""
    if output_path:
        # Save to file(s)
        for platform, content in results.items():
            out_path = Path(output_path)

            if len(platforms) > 1:
                # Multiple platforms - add platform suffix
                stem = out_path.stem
                suffix = out_path.suffix
                out_path = out_path.parent / f"{stem}_{platform}{suffix}"

            with open(out_path, 'w') as f:
                f.write(content)

            print(f"✅ {platform}: {out_path}", file=sys.stderr)
    else:
        # Print to stdout
        for platform, content in results.items():
            if len(platforms) > 1:
                print(f"\n{'='*60}")
                print(f"{platform.upper()}")
                print('='*60)
            print(content)


if __name__ == '__main__':
    main()
