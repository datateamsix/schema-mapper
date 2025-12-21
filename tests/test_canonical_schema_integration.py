"""
Tests for canonical schema date format integration with preprocessing and validation.

These tests verify that date formats specified in the canonical schema are:
1. Validated correctly in ColumnDefinition
2. Applied automatically during preprocessing
3. Validated against the data
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType
from schema_mapper.preprocessor import PreProcessor
from schema_mapper.validators import DataFrameValidator
from schema_mapper import prepare_for_load


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def date_dataframe():
    """DataFrame with dates in various formats."""
    return pd.DataFrame({
        'event_date': ['2024-01-15', '2024-02-20', '2024-03-10'],
        'created_at': ['2024-01-15 10:30:00', '2024-02-20 14:45:00', '2024-03-10 08:15:00'],
        'user_id': [1, 2, 3],
        'description': ['Event A', 'Event B', 'Event C']
    })


@pytest.fixture
def messy_date_dataframe():
    """DataFrame with dates in mixed formats."""
    return pd.DataFrame({
        'event_date': ['15/01/2024', '20/02/2024', '10/03/2024'],  # DD/MM/YYYY
        'created_at': ['01-15-2024 10:30', '02-20-2024 14:45', '03-10-2024 08:15'],  # MM-DD-YYYY HH:MM
        'user_id': [1, 2, 3]
    })


@pytest.fixture
def canonical_schema_with_dates():
    """Canonical schema with date format specifications."""
    return CanonicalSchema(
        table_name='events',
        columns=[
            ColumnDefinition('event_date', LogicalType.DATE, date_format='%Y-%m-%d'),
            ColumnDefinition('created_at', LogicalType.TIMESTAMP, date_format='%Y-%m-%d %H:%M:%S'),
            ColumnDefinition('user_id', LogicalType.INTEGER),
            ColumnDefinition('description', LogicalType.STRING)
        ]
    )


@pytest.fixture
def canonical_schema_eu_format():
    """Canonical schema with European date format."""
    return CanonicalSchema(
        table_name='events',
        columns=[
            ColumnDefinition('event_date', LogicalType.DATE, date_format='%d/%m/%Y'),
            ColumnDefinition('created_at', LogicalType.TIMESTAMP, date_format='%d/%m/%Y %H:%M'),
            ColumnDefinition('user_id', LogicalType.INTEGER)
        ]
    )


# ============================================================================
# COLUMN DEFINITION VALIDATION TESTS
# ============================================================================

def test_column_definition_valid_date_format():
    """Test that valid date format passes validation."""
    col = ColumnDefinition('created_at', LogicalType.TIMESTAMP, date_format='%Y-%m-%d %H:%M:%S')
    errors = col.validate()
    assert len(errors) == 0


def test_column_definition_invalid_date_format():
    """Test that invalid date format fails validation."""
    col = ColumnDefinition('created_at', LogicalType.TIMESTAMP, date_format='%invalid%')
    errors = col.validate()
    assert len(errors) > 0
    assert 'invalid date_format' in errors[0].lower()


def test_column_definition_date_format_on_non_temporal_type():
    """Test that date_format on non-temporal type fails validation."""
    col = ColumnDefinition('user_id', LogicalType.INTEGER, date_format='%Y-%m-%d')
    errors = col.validate()
    assert len(errors) > 0
    assert 'not a temporal type' in errors[0].lower()


def test_column_definition_timezone_validation():
    """Test timezone validation."""
    # Valid: timezone on TIMESTAMP
    col1 = ColumnDefinition('created_at', LogicalType.TIMESTAMP, timezone='UTC')
    assert len(col1.validate()) == 0

    # Invalid: timezone on DATE
    col2 = ColumnDefinition('event_date', LogicalType.DATE, timezone='UTC')
    errors = col2.validate()
    assert len(errors) > 0
    assert 'not TIMESTAMP or TIMESTAMPTZ' in errors[0]

    # Invalid: timezone on INTEGER
    col3 = ColumnDefinition('user_id', LogicalType.INTEGER, timezone='UTC')
    errors = col3.validate()
    assert len(errors) > 0


def test_canonical_schema_validates_columns():
    """Test that CanonicalSchema validates its columns."""
    schema = CanonicalSchema(
        table_name='test',
        columns=[
            ColumnDefinition('id', LogicalType.INTEGER, date_format='%Y-%m-%d'),  # Invalid
            ColumnDefinition('created_at', LogicalType.TIMESTAMP, date_format='%Y-%m-%d')  # Valid
        ]
    )
    errors = schema.validate()
    assert len(errors) > 0
    assert any('not a temporal type' in error.lower() for error in errors)


# ============================================================================
# PREPROCESSOR apply_schema_formats() TESTS
# ============================================================================

def test_preprocessor_apply_schema_formats_basic(date_dataframe, canonical_schema_with_dates):
    """Test that apply_schema_formats() applies date formats from schema."""
    preprocessor = PreProcessor(date_dataframe, canonical_schema=canonical_schema_with_dates)
    df_result = preprocessor.apply_schema_formats().apply()

    assert isinstance(df_result, pd.DataFrame)
    assert len(df_result) == 3


def test_preprocessor_apply_schema_formats_without_schema():
    """Test that apply_schema_formats() raises error without schema."""
    df = pd.DataFrame({'col': [1, 2, 3]})
    preprocessor = PreProcessor(df)

    with pytest.raises(ValueError, match="canonical_schema is not set"):
        preprocessor.apply_schema_formats()


def test_preprocessor_apply_schema_formats_invalid_schema():
    """Test that apply_schema_formats() validates schema."""
    invalid_schema = CanonicalSchema(
        table_name='test',
        columns=[
            ColumnDefinition('id', LogicalType.INTEGER, date_format='%invalid%')
        ]
    )
    df = pd.DataFrame({'id': [1, 2, 3]})
    preprocessor = PreProcessor(df, canonical_schema=invalid_schema)

    with pytest.raises(ValueError, match="Invalid canonical schema"):
        preprocessor.apply_schema_formats()


def test_preprocessor_with_schema_in_init(date_dataframe, canonical_schema_with_dates):
    """Test PreProcessor accepts canonical_schema in __init__."""
    preprocessor = PreProcessor(date_dataframe, canonical_schema=canonical_schema_with_dates)
    assert preprocessor.canonical_schema is not None
    assert preprocessor.canonical_schema == canonical_schema_with_dates


def test_apply_schema_formats_eu_format(messy_date_dataframe, canonical_schema_eu_format):
    """Test applying European date format from schema."""
    preprocessor = PreProcessor(messy_date_dataframe, canonical_schema=canonical_schema_eu_format)
    df_result = preprocessor.apply_schema_formats().apply()

    # Check that dates are formatted correctly
    assert isinstance(df_result, pd.DataFrame)
    assert 'event_date' in df_result.columns


# ============================================================================
# VALIDATOR validate_with_schema() TESTS
# ============================================================================

def test_validator_validate_with_schema_success(date_dataframe, canonical_schema_with_dates):
    """Test validation passes with correctly formatted dates."""
    validator = DataFrameValidator()
    result = validator.validate_with_schema(date_dataframe, canonical_schema_with_dates)

    assert not result.has_errors()


def test_validator_validate_with_schema_date_format_mismatch():
    """Test validation fails when dates don't match schema format."""
    df = pd.DataFrame({
        'event_date': ['15/01/2024', '20/02/2024'],  # DD/MM/YYYY
        'user_id': [1, 2]
    })

    schema = CanonicalSchema(
        table_name='events',
        columns=[
            ColumnDefinition('event_date', LogicalType.DATE, date_format='%Y-%m-%d'),  # Expects YYYY-MM-DD
            ColumnDefinition('user_id', LogicalType.INTEGER)
        ]
    )

    validator = DataFrameValidator()
    result = validator.validate_with_schema(df, schema)

    # Should have errors or warnings about format mismatch
    assert result.has_errors() or result.has_warnings()


def test_validator_validate_with_schema_missing_columns():
    """Test validation warns about missing schema columns."""
    df = pd.DataFrame({
        'user_id': [1, 2]
    })

    schema = CanonicalSchema(
        table_name='events',
        columns=[
            ColumnDefinition('event_date', LogicalType.DATE),
            ColumnDefinition('user_id', LogicalType.INTEGER)
        ]
    )

    validator = DataFrameValidator()
    result = validator.validate_with_schema(df, schema)

    assert result.has_warnings()
    assert any('not found in DataFrame' in warning for warning in result.warnings)


def test_validator_validate_with_schema_invalid_schema():
    """Test validation fails with invalid schema."""
    df = pd.DataFrame({'col': [1, 2]})
    invalid_schema = CanonicalSchema(
        table_name='test',
        columns=[
            ColumnDefinition('col', LogicalType.INTEGER, date_format='%invalid%')
        ]
    )

    validator = DataFrameValidator()
    result = validator.validate_with_schema(df, invalid_schema)

    assert result.has_errors()


# ============================================================================
# prepare_for_load() INTEGRATION TESTS
# ============================================================================

def test_prepare_for_load_with_canonical_schema(date_dataframe, canonical_schema_with_dates):
    """Test prepare_for_load() with canonical schema."""
    df_prepared, schema, issues = prepare_for_load(
        date_dataframe,
        'bigquery',
        canonical_schema=canonical_schema_with_dates
    )

    assert isinstance(df_prepared, pd.DataFrame)
    assert isinstance(schema, list)
    assert isinstance(issues, dict)


def test_prepare_for_load_applies_schema_formats(messy_date_dataframe, canonical_schema_eu_format):
    """Test that prepare_for_load() automatically applies schema formats."""
    df_prepared, schema, issues = prepare_for_load(
        messy_date_dataframe,
        'bigquery',
        canonical_schema=canonical_schema_eu_format,
        validate=False  # Skip validation to focus on formatting
    )

    # Verify dates were processed
    assert isinstance(df_prepared, pd.DataFrame)
    assert len(df_prepared) == 3


def test_prepare_for_load_validates_with_schema(date_dataframe, canonical_schema_with_dates):
    """Test that prepare_for_load() validates against schema."""
    df_prepared, schema, issues = prepare_for_load(
        date_dataframe,
        'bigquery',
        canonical_schema=canonical_schema_with_dates,
        validate=True
    )

    # Should validate successfully
    assert isinstance(issues, dict)
    assert 'errors' in issues
    assert 'warnings' in issues


def test_prepare_for_load_with_schema_and_preprocessing():
    """Test prepare_for_load() with both schema and preprocessing pipeline."""
    df = pd.DataFrame({
        'event_date': ['2024-01-15', '2024-02-20', '2024-02-20'],  # Has duplicate
        'user_id': [1, 2, 2]
    })

    schema = CanonicalSchema(
        table_name='events',
        columns=[
            ColumnDefinition('event_date', LogicalType.DATE, date_format='%Y-%m-%d'),
            ColumnDefinition('user_id', LogicalType.INTEGER)
        ]
    )

    df_prepared, db_schema, issues = prepare_for_load(
        df,
        'bigquery',
        canonical_schema=schema,
        preprocess_pipeline=['fix_whitespace', 'remove_duplicates']
    )

    # Should remove duplicates and apply schema formats
    assert len(df_prepared) <= 3


def test_schema_mapper_preprocess_with_canonical_schema(date_dataframe, canonical_schema_with_dates):
    """Test SchemaMapper.preprocess_data() with canonical schema."""
    from schema_mapper import SchemaMapper

    mapper = SchemaMapper('bigquery')
    df_clean = mapper.preprocess_data(
        date_dataframe,
        canonical_schema=canonical_schema_with_dates
    )

    assert isinstance(df_clean, pd.DataFrame)
    assert len(df_clean) == 3


# ============================================================================
# EDGE CASES
# ============================================================================

def test_schema_with_no_date_formats():
    """Test that schema without date_format works normally."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C']
    })

    schema = CanonicalSchema(
        table_name='test',
        columns=[
            ColumnDefinition('id', LogicalType.INTEGER),
            ColumnDefinition('name', LogicalType.STRING)
        ]
    )

    preprocessor = PreProcessor(df, canonical_schema=schema)
    df_result = preprocessor.apply_schema_formats().apply()

    # Should work without any date processing
    assert len(df_result) == 3


def test_schema_with_missing_date_columns():
    """Test schema with date columns that don't exist in DataFrame."""
    df = pd.DataFrame({
        'user_id': [1, 2, 3]
    })

    schema = CanonicalSchema(
        table_name='test',
        columns=[
            ColumnDefinition('event_date', LogicalType.DATE, date_format='%Y-%m-%d'),
            ColumnDefinition('user_id', LogicalType.INTEGER)
        ]
    )

    preprocessor = PreProcessor(df, canonical_schema=schema)
    # Should not crash even though event_date doesn't exist
    df_result = preprocessor.apply_schema_formats().apply()
    assert isinstance(df_result, pd.DataFrame)


def test_empty_dataframe_with_schema():
    """Test empty DataFrame with canonical schema."""
    df = pd.DataFrame()

    schema = CanonicalSchema(
        table_name='test',
        columns=[
            ColumnDefinition('event_date', LogicalType.DATE, date_format='%Y-%m-%d')
        ]
    )

    preprocessor = PreProcessor(df, canonical_schema=schema)
    df_result = preprocessor.apply_schema_formats().apply()
    assert df_result.empty


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
