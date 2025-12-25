"""
Example 8: Metadata & Data Dictionary Framework

Demonstrates the metadata-first approach where schema + metadata = single source of truth:
1. Infer schema from DataFrame
2. Enrich with business metadata (descriptions, owner, PII flags, tags)
3. Validate metadata completeness
4. Save to YAML (version control alongside code)
5. Load from YAML
6. Export data dictionaries (Markdown, CSV, JSON)
7. Deploy with metadata intact

This implements the framework from metadata_data_dictionary_framework.md

Time: ~20 minutes
Prerequisites: None (fully self-contained)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from schema_mapper import (
    infer_canonical_schema,
    load_schema_from_yaml,
    save_schema_to_yaml,
    LogicalType,
    ColumnDefinition,
)


def create_sample_data() -> pd.DataFrame:
    """Create sample user events dataset."""
    base_date = datetime(2025, 1, 1)

    return pd.DataFrame({
        'event_id': ['evt_001', 'evt_002', 'evt_003', 'evt_004', 'evt_005'],
        'user_id': [1001, 1002, 1001, 1003, 1002],
        'email': [
            'alice@example.com',
            'bob@example.com',
            'alice@example.com',
            'charlie@example.com',
            'bob@example.com'
        ],
        'event_type': ['page_view', 'button_click', 'page_view', 'signup', 'purchase'],
        'event_date': [
            base_date + timedelta(days=i) for i in range(5)
        ],
        'revenue': [0.0, 0.0, 0.0, 0.0, 99.99],
        'session_duration_seconds': [145, 67, 234, 89, 156],
        'created_at': [
            base_date + timedelta(days=i, hours=12) for i in range(5)
        ]
    })


def main():
    """Run metadata and data dictionary example."""

    print("=" * 80)
    print("Example 8: Metadata & Data Dictionary Framework")
    print("=" * 80)

    # ========================================================================
    # Step 1: Infer Schema from DataFrame
    # ========================================================================

    print("\n[Step 1] Creating sample data and inferring schema...")

    df = create_sample_data()

    print(f"   Sample data: {len(df)} rows, {len(df.columns)} columns")
    print(f"\n   Preview:")
    print("  ", df.head(3).to_string().replace('\n', '\n   '))

    # Infer canonical schema
    schema = infer_canonical_schema(
        df,
        table_name='user_events',
        dataset_name='analytics',
        partition_columns=['event_date'],
        cluster_columns=['user_id', 'event_type']
    )

    print(f"\n   [OK] Inferred schema: {len(schema.columns)} columns")

    # ========================================================================
    # Step 2: Enrich with Business Metadata
    # ========================================================================

    print("\n[Step 2] Enriching schema with business metadata...")

    # Table-level metadata
    schema.description = "User interaction events captured from web and mobile clients"
    schema.owner = "analytics"
    schema.domain = "product_analytics"
    schema.tags = ['events', 'core', 'user_behavior']

    # Column-level metadata
    metadata_enrichment = {
        'event_id': {
            'description': 'Unique identifier for the event',
            'source': 'client_sdk',
            'pii': False,
            'tags': ['identifier', 'primary_key']
        },
        'user_id': {
            'description': 'User identifier (hashed for privacy)',
            'source': 'client_sdk',
            'pii': False,  # Hashed, not directly identifiable
            'tags': ['identifier', 'foreign_key']
        },
        'email': {
            'description': 'User email address',
            'source': 'user_profile',
            'pii': True,  # Personally identifiable
            'tags': ['contact', 'sensitive']
        },
        'event_type': {
            'description': 'Type of user interaction (page_view, button_click, etc.)',
            'source': 'client_sdk',
            'pii': False,
            'tags': ['categorical', 'event_classification']
        },
        'event_date': {
            'description': 'Date when the event occurred (UTC)',
            'source': 'server',
            'pii': False,
            'tags': ['temporal', 'partition_key']
        },
        'revenue': {
            'description': 'Revenue generated from this event (USD)',
            'source': 'payment_processor',
            'pii': False,
            'tags': ['financial', 'metric']
        },
        'session_duration_seconds': {
            'description': 'Length of user session in seconds',
            'source': 'client_sdk',
            'pii': False,
            'tags': ['metric', 'engagement']
        },
        'created_at': {
            'description': 'Timestamp when event was recorded in database (UTC)',
            'source': 'server',
            'pii': False,
            'tags': ['temporal', 'audit']
        }
    }

    # Apply metadata to columns
    for col in schema.columns:
        metadata = metadata_enrichment.get(col.name, {})
        col.description = metadata.get('description')
        col.source = metadata.get('source')
        col.pii = metadata.get('pii', False)
        col.tags = metadata.get('tags', [])

    print(f"   [OK] Enriched {len(schema.columns)} columns with metadata")
    print(f"\n   Table Metadata:")
    print(f"      Description: {schema.description}")
    print(f"      Owner: {schema.owner}")
    print(f"      Domain: {schema.domain}")
    print(f"      Tags: {', '.join(schema.tags)}")

    print(f"\n   Sample Column Metadata:")
    email_col = schema.get_column('email')
    print(f"      Column: {email_col.name}")
    print(f"      Description: {email_col.description}")
    print(f"      PII: {'Yes' if email_col.pii else 'No'}")
    print(f"      Source: {email_col.source}")
    print(f"      Tags: {', '.join(email_col.tags)}")

    # ========================================================================
    # Step 3: Validate Metadata Completeness
    # ========================================================================

    print("\n[Step] Step 3: Validating metadata completeness...")

    # Validate that required metadata fields are populated
    errors = schema.validate_metadata(
        required_table_fields=['description', 'owner'],
        required_column_fields=['description', 'pii']
    )

    if errors:
        print(f"   [X] Metadata validation failed:")
        for error in errors:
            print(f"      - {error}")
    else:
        print(f"   [OK] All required metadata fields populated")
        print(f"      Table fields: description, owner")
        print(f"      Column fields: description, pii")

    # PII Analysis
    pii_columns = [col.name for col in schema.columns if col.pii]
    print(f"\n   Governance Analysis:")
    print(f"      Total columns: {len(schema.columns)}")
    print(f"      PII columns: {len(pii_columns)}")
    if pii_columns:
        print(f"      PII fields: {', '.join(pii_columns)}")
        print(f"      [!] These fields require special handling for compliance")

    # ========================================================================
    # Step 4: Save to YAML (Single Source of Truth)
    # ========================================================================

    print("\n[Step 4] Saving schema to YAML...")

    yaml_path = Path(__file__).parent / 'output' / 'user_events_schema.yaml'
    yaml_path.parent.mkdir(exist_ok=True)

    save_schema_to_yaml(schema, str(yaml_path))

    print(f"   [OK] Saved to: {yaml_path}")
    print(f"   [OK] This YAML file is now the single source of truth")
    print(f"   [OK] Can be versioned in git alongside code")
    print(f"   [OK] Can be deployed across multiple warehouses")

    # Show YAML preview
    print(f"\n   YAML Preview (first 15 lines):")
    with open(yaml_path, 'r') as f:
        lines = f.readlines()[:15]
        for line in lines:
            print(f"      {line.rstrip()}")
        print(f"      ... ({len(open(yaml_path).readlines())} total lines)")

    # ========================================================================
    # Step 5: Load from YAML
    # ========================================================================

    print("\n[Step 5] Loading schema from YAML...")

    # Simulate loading in a different context (e.g., deployment pipeline)
    loaded_schema = load_schema_from_yaml(str(yaml_path))

    print(f"   [OK] Loaded schema: {loaded_schema.table_name}")
    print(f"   [OK] Columns: {len(loaded_schema.columns)}")
    print(f"   [OK] Metadata preserved:")
    print(f"      - Table description: {loaded_schema.description[:50]}...")
    print(f"      - Owner: {loaded_schema.owner}")
    print(f"      - PII columns: {len([c for c in loaded_schema.columns if c.pii])}")

    # ========================================================================
    # Step 6: Export Data Dictionaries
    # ========================================================================

    print("\n[Step 6] Exporting data dictionaries...")

    output_dir = yaml_path.parent

    # Export as Markdown
    markdown_path = output_dir / 'user_events_dictionary.md'
    markdown_content = schema.export_data_dictionary(format='markdown')
    with open(markdown_path, 'w') as f:
        f.write(markdown_content)
    print(f"   [OK] Markdown: {markdown_path}")

    # Export as CSV
    csv_path = output_dir / 'user_events_dictionary.csv'
    csv_content = schema.export_data_dictionary(format='csv')
    with open(csv_path, 'w') as f:
        f.write(csv_content)
    print(f"   [OK] CSV: {csv_path}")

    # Export as JSON
    json_path = output_dir / 'user_events_dictionary.json'
    json_content = schema.export_data_dictionary(format='json')
    with open(json_path, 'w') as f:
        f.write(json_content)
    print(f"   [OK] JSON: {json_path}")

    # Show Markdown preview
    print(f"\n   Markdown Preview:")
    print("   " + "-" * 76)
    lines = markdown_content.split('\n')[:20]
    for line in lines:
        print(f"   {line}")
    print("   " + "-" * 76)

    # ========================================================================
    # Step 7: Production Deployment Workflow
    # ========================================================================

    print("\n[Step 7] Production deployment workflow...")

    print(f"\n   Typical Workflow:")
    print(f"")
    print(f"   1. Data Engineer creates DataFrame -> infer_canonical_schema()")
    print(f"   2. Engineer enriches with metadata (descriptions, PII, tags)")
    print(f"   3. Save to YAML -> save_schema_to_yaml()")
    print(f"   4. Commit YAML to git (version control)")
    print(f"   5. CI/CD pipeline:")
    print(f"      a. Load from YAML -> load_schema_from_yaml()")
    print(f"      b. Validate metadata -> schema.validate_metadata()")
    print(f"      c. Deploy to BigQuery/Snowflake/etc with metadata intact")
    print(f"      d. Generate data dictionary -> schema.export_data_dictionary()")
    print(f"      e. Publish to documentation site")
    print(f"")
    print(f"   Result:")
    print(f"      * Metadata never drifts from reality")
    print(f"      * Analysts get documentation on day one")
    print(f"      * Governance hooks exist without heavy tooling")
    print(f"      * Schema evolution is safe and auditable")

    # ========================================================================
    # Summary
    # ========================================================================

    print("\n" + "=" * 80)
    print("SUCCESS: Metadata & Data Dictionary Demo Complete")
    print("=" * 80)

    print(f"\n-- Summary:")
    print(f"   Schema: {schema.dataset_name}.{schema.table_name}")
    print(f"   Columns: {len(schema.columns)}")
    print(f"   PII columns: {len([c for c in schema.columns if c.pii])}")
    print(f"   Tags: {len(schema.tags)} table-level, {sum(len(c.tags) for c in schema.columns)} column-level")

    print(f"\n-- Generated Files:")
    print(f"   * {yaml_path.name} - YAML schema (single source of truth)")
    print(f"   * {markdown_path.name} - Markdown data dictionary")
    print(f"   * {csv_path.name} - CSV data dictionary")
    print(f"   * {json_path.name} - JSON schema export")

    print(f"\n-- Key Benefits:")
    print(f"   [OK] Schema = Structure + Meaning (not just column types)")
    print(f"   [OK] Metadata travels with schema across warehouses")
    print(f"   [OK] Documentation generated, not written twice")
    print(f"   [OK] PII governance hooks built-in")
    print(f"   [OK] Version control for institutional knowledge")

    print(f"\n-- Next Steps:")
    print(f"   * Review generated files in {output_dir}")
    print(f"   * Customize metadata for your use case")
    print(f"   * Integrate into CI/CD pipeline")
    print(f"   * Deploy to production with metadata intact")

    print(f"\n-- Framework Reference:")
    print(f"   * See metadata_data_dictionary_framework.md for full specification")
    print(f"   * This is institutional memory encoded in code")


if __name__ == '__main__':
    main()
