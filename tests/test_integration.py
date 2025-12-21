"""
Integration tests for SchemaMapper with Profiler and PreProcessor.

These tests verify that the integrated workflow works correctly with
profiling and preprocessing capabilities.
"""

import pytest
import pandas as pd
import numpy as np
from schema_mapper import SchemaMapper, prepare_for_load, create_schema


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_messy_df():
    """Sample DataFrame with messy data requiring preprocessing."""
    return pd.DataFrame({
        '  User ID  ': [1, 2, 3, 3, 4],  # Whitespace, duplicates
        'First Name': ['  Alice  ', 'Bob', None, 'Charlie', 'David'],  # Whitespace, missing
        'Email Address': ['alice@test.com', 'bob@test', 'charlie@test.com', 'charlie@test.com', 'david@test.com'],
        'Age': [25, 30, None, 35, 28],
        'Salary': [50000, 60000, 55000, 70000, 65000]
    })


@pytest.fixture
def sample_clean_df():
    """Sample clean DataFrame for basic testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'active': [True, False, True]
    })


# ============================================================================
# SchemaMapper.profile_data() TESTS
# ============================================================================

def test_schema_mapper_profile_data_detailed(sample_clean_df):
    """Test profiling data with detailed report."""
    mapper = SchemaMapper('bigquery')
    report = mapper.profile_data(sample_clean_df, detailed=True, show_progress=False)

    # Verify report structure
    assert isinstance(report, dict)
    assert 'dataset' in report
    assert 'quality' in report
    assert 'missing_values' in report

    # Verify profiler was stored
    assert mapper.profiler is not None

    # Verify dataset info
    assert report['dataset']['n_rows'] == 3
    assert report['dataset']['n_columns'] == 4


def test_schema_mapper_profile_data_summary(sample_clean_df):
    """Test profiling data with summary only."""
    mapper = SchemaMapper('bigquery')
    summary = mapper.profile_data(sample_clean_df, detailed=False, show_progress=False)

    # Verify summary is a dictionary
    assert isinstance(summary, dict)

    # Verify profiler was stored
    assert mapper.profiler is not None


def test_schema_mapper_profile_data_quality_score(sample_messy_df):
    """Test that profiling includes quality score."""
    mapper = SchemaMapper('bigquery')
    report = mapper.profile_data(sample_messy_df, detailed=True, show_progress=False)

    assert 'quality' in report
    assert 'overall_score' in report['quality']
    assert 0 <= report['quality']['overall_score'] <= 100


# ============================================================================
# SchemaMapper.preprocess_data() TESTS
# ============================================================================

def test_schema_mapper_preprocess_default_pipeline(sample_messy_df):
    """Test preprocessing with default pipeline."""
    mapper = SchemaMapper('bigquery')
    df_clean = mapper.preprocess_data(sample_messy_df)

    # Verify preprocessing was applied
    assert mapper.preprocessor is not None

    # Verify whitespace was fixed (column names)
    assert 'User ID' not in df_clean.columns or 'user_id' in df_clean.columns

    # Verify data was returned
    assert isinstance(df_clean, pd.DataFrame)
    assert len(df_clean) > 0


def test_schema_mapper_preprocess_custom_pipeline(sample_messy_df):
    """Test preprocessing with custom pipeline."""
    mapper = SchemaMapper('bigquery')
    df_clean = mapper.preprocess_data(
        sample_messy_df,
        pipeline=['fix_whitespace', 'standardize_column_names', 'remove_duplicates']
    )

    # Verify preprocessing was applied
    assert mapper.preprocessor is not None
    assert isinstance(df_clean, pd.DataFrame)

    # With duplicates removed, should have fewer rows
    # (Original has duplicate row for id=3)
    assert len(df_clean) <= len(sample_messy_df)


def test_schema_mapper_preprocess_empty_pipeline(sample_clean_df):
    """Test preprocessing with empty pipeline."""
    mapper = SchemaMapper('bigquery')
    df_result = mapper.preprocess_data(sample_clean_df, pipeline=[])

    # With empty pipeline, data should be returned as-is
    assert isinstance(df_result, pd.DataFrame)
    pd.testing.assert_frame_equal(df_result, sample_clean_df)


# ============================================================================
# prepare_for_load() INTEGRATION TESTS
# ============================================================================

def test_prepare_for_load_basic(sample_clean_df):
    """Test basic prepare_for_load without profiling or preprocessing."""
    df_prepared, schema, issues = prepare_for_load(sample_clean_df, 'bigquery')

    # Verify outputs
    assert isinstance(df_prepared, pd.DataFrame)
    assert isinstance(schema, list)
    assert isinstance(issues, dict)
    assert 'errors' in issues
    assert 'warnings' in issues

    # Verify schema
    assert len(schema) == 4  # id, name, age, active
    assert all('name' in field for field in schema)
    assert all('type' in field for field in schema)


def test_prepare_for_load_with_profiling(sample_clean_df):
    """Test prepare_for_load with profiling enabled."""
    result = prepare_for_load(sample_clean_df, 'bigquery', profile=True)

    # With profiling, should return 4 values
    assert len(result) == 4
    df_prepared, schema, issues, report = result

    # Verify profiling report
    assert isinstance(report, dict)
    assert 'quality' in report
    assert 'dataset' in report

    # Verify other outputs
    assert isinstance(df_prepared, pd.DataFrame)
    assert isinstance(schema, list)
    assert isinstance(issues, dict)


def test_prepare_for_load_with_preprocessing(sample_messy_df):
    """Test prepare_for_load with preprocessing pipeline."""
    df_prepared, schema, issues = prepare_for_load(
        sample_messy_df,
        'bigquery',
        preprocess_pipeline=['fix_whitespace', 'remove_duplicates']
    )

    # Verify data was processed
    assert isinstance(df_prepared, pd.DataFrame)

    # Should have fewer or equal rows (duplicates removed)
    assert len(df_prepared) <= len(sample_messy_df)

    # Verify schema was generated
    assert len(schema) > 0


def test_prepare_for_load_with_profiling_and_preprocessing(sample_messy_df):
    """Test prepare_for_load with both profiling and preprocessing."""
    result = prepare_for_load(
        sample_messy_df,
        'bigquery',
        profile=True,
        preprocess_pipeline=['fix_whitespace', 'standardize_column_names', 'remove_duplicates']
    )

    # Should return 4 values (with profiling)
    assert len(result) == 4
    df_prepared, schema, issues, report = result

    # Verify profiling report (on original data)
    assert isinstance(report, dict)
    assert report['dataset']['n_rows'] == len(sample_messy_df)

    # Verify preprocessing was applied
    assert isinstance(df_prepared, pd.DataFrame)

    # Verify schema
    assert isinstance(schema, list)
    assert len(schema) > 0


def test_prepare_for_load_no_validation(sample_clean_df):
    """Test prepare_for_load with validation disabled."""
    df_prepared, schema, issues = prepare_for_load(
        sample_clean_df,
        'bigquery',
        validate=False
    )

    # Issues should be empty (validation not run)
    assert issues == {'errors': [], 'warnings': []}

    # Other outputs should be valid
    assert isinstance(df_prepared, pd.DataFrame)
    assert isinstance(schema, list)


def test_prepare_for_load_different_platforms(sample_clean_df):
    """Test prepare_for_load works with different target platforms."""
    platforms = ['bigquery', 'snowflake', 'redshift', 'postgresql']

    for platform in platforms:
        df_prepared, schema, issues = prepare_for_load(sample_clean_df, platform)

        # Verify schema was generated for each platform
        assert isinstance(schema, list)
        assert len(schema) == 4

        # Verify data was prepared
        assert isinstance(df_prepared, pd.DataFrame)


# ============================================================================
# FULL WORKFLOW INTEGRATION TESTS
# ============================================================================

def test_full_etl_workflow_with_all_features(sample_messy_df):
    """Test complete ETL workflow with profiling, preprocessing, validation, and schema generation."""
    # Step 1: Create mapper
    mapper = SchemaMapper('bigquery')

    # Step 2: Profile original data
    profile_report = mapper.profile_data(sample_messy_df, detailed=True, show_progress=False)
    assert 'quality' in profile_report
    original_quality = profile_report['quality']['overall_score']

    # Step 3: Preprocess data
    df_clean = mapper.preprocess_data(
        sample_messy_df,
        pipeline=['fix_whitespace', 'standardize_column_names', 'remove_duplicates', 'handle_missing']
    )

    # Step 4: Validate cleaned data
    validation = mapper.validate_dataframe(df_clean)
    assert isinstance(validation, dict)

    # Step 5: Generate schema
    schema, mapping = mapper.generate_schema(df_clean)
    assert len(schema) > 0
    assert isinstance(mapping, dict)

    # Verify the full workflow improved data
    assert len(df_clean) > 0


def test_schema_mapper_integration_with_profiler_visualizations(sample_clean_df):
    """Test that profiler visualizations work after profiling."""
    mapper = SchemaMapper('bigquery')

    # Profile the data
    report = mapper.profile_data(sample_clean_df, detailed=True, show_progress=False)

    # Verify profiler is available
    assert mapper.profiler is not None

    # Verify we can access profiler methods
    assert hasattr(mapper.profiler, 'plot_distributions')
    assert hasattr(mapper.profiler, 'plot_correlations')


def test_prepare_for_load_preserves_data_types(sample_clean_df):
    """Test that prepare_for_load preserves appropriate data types."""
    df_prepared, schema, issues = prepare_for_load(
        sample_clean_df,
        'bigquery',
        auto_cast=True
    )

    # Verify int columns
    assert pd.api.types.is_integer_dtype(df_prepared['id'])
    assert pd.api.types.is_integer_dtype(df_prepared['age'])

    # Verify schema types match
    id_field = next(f for f in schema if f['name'] == 'id')
    assert id_field['type'] in ['INT64', 'INTEGER', 'BIGINT']


def test_sequential_operations_maintain_mapper_state():
    """Test that sequential operations maintain SchemaMapper state."""
    df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })

    mapper = SchemaMapper('bigquery')

    # Profile
    report1 = mapper.profile_data(df, show_progress=False)
    profiler1 = mapper.profiler

    # Preprocess
    df_clean = mapper.preprocess_data(df)
    preprocessor1 = mapper.preprocessor

    # Profile again (should create new profiler)
    report2 = mapper.profile_data(df_clean, show_progress=False)
    profiler2 = mapper.profiler

    # Verify state is maintained
    assert preprocessor1 is not None
    assert profiler1 is not None
    assert profiler2 is not None
    assert profiler1 != profiler2  # Different profiler instances


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

def test_schema_mapper_preprocess_invalid_operation():
    """Test that preprocessing handles invalid operations gracefully."""
    df = pd.DataFrame({'col1': [1, 2, 3]})
    mapper = SchemaMapper('bigquery')

    # Should log warning but not crash
    result = mapper.preprocess_data(df, pipeline=['invalid_operation'])

    # Should still return a DataFrame
    assert isinstance(result, pd.DataFrame)


def test_prepare_for_load_with_empty_dataframe():
    """Test prepare_for_load handles empty DataFrame."""
    df = pd.DataFrame()

    # Empty DataFrame should return validation errors but not crash
    df_prepared, schema, issues = prepare_for_load(df, 'bigquery')

    # Should have validation errors
    assert len(issues['errors']) > 0
    assert any('empty' in error.lower() for error in issues['errors'])


def test_prepare_for_load_with_validation_errors(sample_messy_df):
    """Test prepare_for_load returns validation errors when present."""
    # Create a DataFrame likely to have validation warnings
    df_with_issues = sample_messy_df.copy()

    df_prepared, schema, issues = prepare_for_load(df_with_issues, 'bigquery', validate=True)

    # Should still return valid outputs even with validation issues
    assert isinstance(df_prepared, pd.DataFrame)
    assert isinstance(schema, list)
    assert isinstance(issues, dict)


# ============================================================================
# BACKWARD COMPATIBILITY TESTS
# ============================================================================

def test_prepare_for_load_backward_compatible(sample_clean_df):
    """Test that prepare_for_load maintains backward compatibility."""
    # Old usage (without new parameters) should still work
    df_prepared, schema, issues = prepare_for_load(sample_clean_df, 'bigquery')

    assert isinstance(df_prepared, pd.DataFrame)
    assert isinstance(schema, list)
    assert isinstance(issues, dict)
    assert len(issues) == 2  # Should have 'errors' and 'warnings' keys


def test_create_schema_still_works(sample_clean_df):
    """Test that create_schema function still works without integration features."""
    schema = create_schema(sample_clean_df, 'bigquery')

    assert isinstance(schema, list)
    assert len(schema) == 4

    # With return_mapping
    schema, mapping = create_schema(sample_clean_df, 'bigquery', return_mapping=True)
    assert isinstance(mapping, dict)
