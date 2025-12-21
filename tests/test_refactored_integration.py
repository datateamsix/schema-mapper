"""
Test backward compatibility and integration after refactoring.

This test suite ensures that:
1. All existing functionality still works (backward compatibility)
2. New Profiler-enhanced features work correctly
3. Integration between validators, utils, and profiler is seamless
"""

import pytest
import pandas as pd
import numpy as np
from schema_mapper import (
    prepare_for_load,
    create_schema,
    validate_dataframe,
    Profiler
)
from schema_mapper.validators import DataFrameValidator
from schema_mapper.utils import detect_and_cast_types, standardize_column_name


class TestBackwardCompatibility:
    """Test that existing code still works after refactoring."""

    def test_prepare_for_load_backward_compatible(self):
        """Test that prepare_for_load works exactly as before."""
        df = pd.DataFrame({
            'User ID': [1, 2, 3],
            'Name': ['Alice', 'Bob', 'Charlie'],
            'Active': ['yes', 'no', 'yes']
        })

        # This should work exactly as it did before
        df_clean, schema, issues = prepare_for_load(df, target_type='bigquery')

        assert len(df_clean) == 3
        assert len(schema) == 3
        assert 'errors' in issues
        assert 'warnings' in issues
        assert isinstance(df_clean, pd.DataFrame)

    def test_create_schema_backward_compatible(self):
        """Test that create_schema works exactly as before."""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })

        schema = create_schema(df, target_type='bigquery')

        assert isinstance(schema, list)
        assert len(schema) == 2

    def test_validate_dataframe_backward_compatible(self):
        """Test that validate_dataframe works exactly as before."""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [None, None, None]  # High nulls
        })

        issues = validate_dataframe(df, high_null_threshold=50.0)

        assert 'errors' in issues
        assert 'warnings' in issues
        assert isinstance(issues, dict)

    def test_standardize_column_name_backward_compatible(self):
        """Test that standardize_column_name works exactly as before."""
        assert standardize_column_name("User ID#") == "user_id"
        assert standardize_column_name("First Name") == "first_name"
        assert standardize_column_name("123Start") == "_123start"

    def test_detect_and_cast_types_backward_compatible(self):
        """Test that detect_and_cast_types works without profiler."""
        df = pd.DataFrame({
            'dates': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'numbers': ['1', '2', '3'],
            'bools': ['yes', 'no', 'yes']
        })

        df_typed = detect_and_cast_types(df)

        # Should still work without use_profiler parameter
        assert pd.api.types.is_datetime64_any_dtype(df_typed['dates'])
        assert pd.api.types.is_integer_dtype(df_typed['numbers'])
        assert df_typed['bools'].dtype == bool


class TestValidatorEnhancements:
    """Test new validate_detailed() functionality."""

    def test_validate_detailed_returns_comprehensive_report(self):
        """Test that validate_detailed returns detailed analysis."""
        df = pd.DataFrame({
            'age': [25, 30, 35, 150, 28],  # Outlier: 150
            'email': ['a@b.com', 'c@d.com', 'invalid', None, 'e@f.com'],
            'region': ['North', 'South', 'North', 'East', 'West']
        })

        validator = DataFrameValidator()
        detailed = validator.validate_detailed(df)

        # Check structure
        assert 'quality_score' in detailed
        assert 'quality_breakdown' in detailed
        assert 'anomalies' in detailed
        assert 'missing_values' in detailed
        assert 'cardinality' in detailed
        assert 'patterns' in detailed
        assert 'duplicates' in detailed
        assert 'recommendations' in detailed

        # Check quality score is numeric
        assert isinstance(detailed['quality_score'], (int, float))
        assert 0 <= detailed['quality_score'] <= 100

        # Check recommendations is a list
        assert isinstance(detailed['recommendations'], list)
        assert len(detailed['recommendations']) > 0

    def test_validate_detailed_detects_outliers(self):
        """Test that validate_detailed detects outliers."""
        df = pd.DataFrame({
            'normal_data': [1, 2, 3, 4, 5],
            'with_outliers': [10, 12, 11, 100, 13]  # 100 is outlier
        })

        validator = DataFrameValidator()
        detailed = validator.validate_detailed(df)

        # Should detect outlier in with_outliers column
        assert 'with_outliers' in detailed['anomalies']
        assert detailed['anomalies']['with_outliers']['count'] > 0

    def test_validate_detailed_detects_missing_values(self):
        """Test that validate_detailed analyzes missing values."""
        df = pd.DataFrame({
            'complete': [1, 2, 3, 4, 5],
            'with_nulls': [1, None, None, None, 5]  # 60% null
        })

        validator = DataFrameValidator()
        detailed = validator.validate_detailed(df)

        # Should identify column with high missing values
        missing = detailed['missing_values']
        assert 'with_nulls' in missing['columns_with_missing']
        assert missing['missing_percentages']['with_nulls'] > 50

    def test_validate_detailed_provides_recommendations(self):
        """Test that recommendations are actionable."""
        df = pd.DataFrame({
            'mostly_null': [None] * 95 + [1, 2, 3, 4, 5],  # 100 rows
            'age': [25, 30, 200, 28, 32] * 20  # 100 rows with outliers
        })

        validator = DataFrameValidator()
        detailed = validator.validate_detailed(df)

        recommendations = detailed['recommendations']

        # Should recommend dropping high-null column
        assert any('mostly_null' in r for r in recommendations)

        # Should recommend investigating outliers
        assert any('outlier' in r.lower() for r in recommendations)

    def test_validator_quick_vs_detailed(self):
        """Test that quick validate() is faster than detailed."""
        df = pd.DataFrame({
            'col1': range(1000),
            'col2': np.random.randn(1000),
            'col3': ['text'] * 1000
        })

        validator = DataFrameValidator()

        import time

        # Quick validation
        start = time.time()
        result_quick = validator.validate(df)
        time_quick = time.time() - start

        # Detailed validation
        start = time.time()
        result_detailed = validator.validate_detailed(df)
        time_detailed = time.time() - start

        # Quick should be faster (though both might be very fast)
        assert time_quick < time_detailed or time_quick < 0.1  # Quick should be <100ms


class TestUtilsEnhancements:
    """Test enhanced type detection with Profiler."""

    def test_detect_and_cast_types_with_profiler_currency(self):
        """Test enhanced detection of currency values."""
        df = pd.DataFrame({
            'price': ['$19.99', '$29.99', '$39.99']
        })

        # With profiler enhancement
        df_enhanced = detect_and_cast_types(df, use_profiler=True)

        # Should detect and convert currency
        assert pd.api.types.is_numeric_dtype(df_enhanced['price'])
        assert df_enhanced['price'].iloc[0] == 19.99

    def test_detect_and_cast_types_with_profiler_percentage(self):
        """Test enhanced detection of percentage values."""
        df = pd.DataFrame({
            'completion': ['75%', '80%', '90%', '95%']
        })

        # With profiler enhancement
        df_enhanced = detect_and_cast_types(df, use_profiler=True)

        # Should detect and convert percentages
        assert pd.api.types.is_numeric_dtype(df_enhanced['completion'])
        assert df_enhanced['completion'].iloc[0] == 0.75  # 75% = 0.75

    def test_detect_and_cast_types_without_profiler_still_works(self):
        """Test that without profiler flag, function works as before."""
        df = pd.DataFrame({
            'dates': ['2024-01-01', '2024-01-02'],
            'numbers': ['123', '456']
        })

        # Without profiler (default behavior)
        df_typed = detect_and_cast_types(df, use_profiler=False)

        assert pd.api.types.is_datetime64_any_dtype(df_typed['dates'])
        assert pd.api.types.is_integer_dtype(df_typed['numbers'])


class TestProfilerIntegration:
    """Test Profiler class integration with existing components."""

    def test_profiler_works_standalone(self):
        """Test that Profiler can be used independently."""
        df = pd.DataFrame({
            'age': [25, 30, 35, 28, 32],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        profiler = Profiler(df, name="test_data")

        # Should work independently
        quality = profiler.assess_quality()
        assert 'overall_score' in quality
        assert isinstance(quality['overall_score'], (int, float))

    def test_profiler_integrates_with_validator(self):
        """Test that Profiler is used by DataFrameValidator.validate_detailed()."""
        df = pd.DataFrame({
            'col1': [1, 2, 3, 4, 5],
            'col2': ['a', 'b', 'c', 'd', 'e']
        })

        # Create profiler
        profiler = Profiler(df)
        profiler_quality = profiler.assess_quality()

        # Use validator
        validator = DataFrameValidator()
        validator_detailed = validator.validate_detailed(df)

        # Quality scores should be the same (validator uses profiler)
        assert validator_detailed['quality_score'] == profiler_quality['overall_score']

    def test_profiler_insights_used_in_type_detection(self):
        """Test that profiler insights enhance type detection."""
        df = pd.DataFrame({
            'prices': ['$10.00', '$20.00', '$30.00']
        })

        # Get profiler patterns
        profiler = Profiler(df)
        patterns = profiler.detect_patterns()

        # Use patterns for type detection
        df_typed = detect_and_cast_types(df, profiler_insights=patterns)

        # Should detect currency and convert to numeric
        assert pd.api.types.is_numeric_dtype(df_typed['prices'])


class TestEndToEndWorkflow:
    """Test complete workflows with refactored components."""

    def test_complete_etl_workflow_backward_compatible(self):
        """Test that complete ETL workflow works as before."""
        # Original messy data
        df = pd.DataFrame({
            'User ID': [1, 2, 3],
            'Email Address': ['alice@example.com', 'bob@example.com', None],
            'Is Active?': ['yes', 'no', 'yes'],
            'Signup Date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })

        # Prepare for load (backward compatible)
        df_clean, schema, issues = prepare_for_load(
            df,
            target_type='bigquery',
            standardize_columns=True,
            auto_cast=True,
            validate=True
        )

        # Should have standardized columns
        assert 'user_id' in df_clean.columns
        assert 'email_address' in df_clean.columns
        assert 'is_active' in df_clean.columns

        # Should have detected types
        assert pd.api.types.is_integer_dtype(df_clean['user_id'])
        assert df_clean['is_active'].dtype == bool
        assert pd.api.types.is_datetime64_any_dtype(df_clean['signup_date'])

        # Should have schema
        assert len(schema) == 4

        # Should have validation issues (missing email)
        assert 'warnings' in issues

    def test_complete_workflow_with_profiler_enhancements(self):
        """Test complete workflow using new Profiler features."""
        # Create dataset with quality issues
        df = pd.DataFrame({
            'customer_id': range(1, 101),
            'age': [25, 30, 35, 150, 28] * 20,  # Outlier: 150
            'purchase_amount': ['$19.99', '$29.99', '$39.99'] * 33 + ['$49.99'],
            'region': ['North', 'South', 'East', 'West'] * 25
        })

        # Step 1: Profile the data
        profiler = Profiler(df, name="sales_data")
        quality = profiler.assess_quality()

        # Step 2: Detailed validation
        validator = DataFrameValidator()
        detailed_issues = validator.validate_detailed(df)

        # Step 3: Enhanced type detection
        df_typed = detect_and_cast_types(df, use_profiler=True)

        # Step 4: Standard prepare for load
        df_clean, schema, issues = prepare_for_load(
            df_typed,
            target_type='bigquery',
            validate=True
        )

        # Verify results
        assert quality['overall_score'] > 0
        assert len(detailed_issues['recommendations']) > 0
        assert pd.api.types.is_numeric_dtype(df_typed['purchase_amount'])
        assert len(schema) == 4


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
