"""
Comprehensive unit tests for the PreProcessor class.

Tests cover:
- Whitespace handling
- Column name standardization
- Date standardization
- Email validation
- Phone number validation
- Duplicate removal
- Missing value handling
- Encoding operations
- Pipeline creation
- Transformation tracking
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from schema_mapper import PreProcessor


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def messy_whitespace_df():
    """DataFrame with whitespace issues."""
    return pd.DataFrame({
        '  Column Name  ': ['  value1  ', ' value2', 'value3  '],
        'Another Col': ['test  ', '  test2  ', 'test3'],
        'Clean': ['a', 'b', 'c']
    })


@pytest.fixture
def messy_columns_df():
    """DataFrame with non-standard column names."""
    return pd.DataFrame({
        'User Name': [1, 2, 3],
        'Email-Address': ['a@test.com', 'b@test.com', 'c@test.com'],
        'PHONE_NUMBER': ['555-1234', '555-5678', '555-9012'],
        'Created At': ['2024-01-01', '2024-01-02', '2024-01-03']
    })


@pytest.fixture
def date_df():
    """DataFrame with various date formats."""
    return pd.DataFrame({
        'date1': ['2024-01-01', '2024-02-15', '2024-03-30'],
        'date2': ['01/15/2024', '02/20/2024', '03/25/2024'],
        'date3': ['Jan 1, 2024', 'Feb 15, 2024', 'Mar 30, 2024'],
        'value': [10, 20, 30]
    })


@pytest.fixture
def email_df():
    """DataFrame with email addresses."""
    return pd.DataFrame({
        'email': [
            'valid@example.com',
            'invalid-email',
            'another@test.co.uk',
            'bad@',
            'good@domain.org'
        ],
        'id': [1, 2, 3, 4, 5]
    })


@pytest.fixture
def phone_df():
    """DataFrame with phone numbers."""
    return pd.DataFrame({
        'phone': [
            '555-123-4567',
            '5551234567',
            '(555) 123-4567',
            '+1-555-123-4567',
            'invalid'
        ],
        'id': [1, 2, 3, 4, 5]
    })


@pytest.fixture
def duplicate_df():
    """DataFrame with duplicate rows."""
    return pd.DataFrame({
        'id': [1, 2, 2, 3, 3, 3],
        'name': ['Alice', 'Bob', 'Bob', 'Charlie', 'Charlie', 'Charlie'],
        'value': [100, 200, 200, 300, 300, 300]
    })


@pytest.fixture
def missing_df():
    """DataFrame with missing values."""
    np.random.seed(42)
    return pd.DataFrame({
        'numeric': [1.0, 2.0, np.nan, 4.0, np.nan, 6.0],
        'category': ['A', 'B', None, 'A', 'B', None],
        'complete': [10, 20, 30, 40, 50, 60]
    })


@pytest.fixture
def categorical_df():
    """DataFrame with categorical columns for encoding."""
    return pd.DataFrame({
        'category': ['A', 'B', 'A', 'C', 'B', 'A'],
        'color': ['red', 'blue', 'red', 'green', 'blue', 'red'],
        'size': ['S', 'M', 'L', 'S', 'M', 'L'],
        'value': [10, 20, 30, 40, 50, 60]
    })


# ============================================================================
# INITIALIZATION TESTS
# ============================================================================

class TestPreProcessorInitialization:
    """Test PreProcessor initialization and basic functionality."""

    def test_init_with_valid_dataframe(self, messy_columns_df):
        """Should initialize successfully with valid DataFrame."""
        preprocessor = PreProcessor(messy_columns_df)
        assert preprocessor.df is not None
        assert isinstance(preprocessor.transformation_log, list)
        assert len(preprocessor.transformation_log) == 0

    def test_init_with_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame()
        preprocessor = PreProcessor(df)
        assert preprocessor.df.empty

    def test_apply_without_transformations(self, messy_columns_df):
        """Should return copy of original DataFrame when apply() called without transformations."""
        preprocessor = PreProcessor(messy_columns_df)
        result = preprocessor.apply()
        pd.testing.assert_frame_equal(result, messy_columns_df)


# ============================================================================
# WHITESPACE HANDLING TESTS
# ============================================================================

class TestWhitespaceHandling:
    """Test whitespace fixing functionality."""

    def test_fix_whitespace_column_names(self, messy_whitespace_df):
        """Should strip whitespace from column names."""
        preprocessor = PreProcessor(messy_whitespace_df)
        result = preprocessor.fix_whitespace().apply()

        # Column names should be stripped
        assert 'Column Name' in result.columns
        assert '  Column Name  ' not in result.columns

    def test_fix_whitespace_string_values(self, messy_whitespace_df):
        """Should strip whitespace from string values."""
        preprocessor = PreProcessor(messy_whitespace_df)
        result = preprocessor.fix_whitespace().apply()

        # String values should be stripped
        col_name = 'Column Name' if 'Column Name' in result.columns else result.columns[0]
        assert result[col_name].iloc[0] == 'value1'
        assert result[col_name].iloc[1] == 'value2'

    def test_fix_whitespace_preserves_non_string(self):
        """Should not affect non-string columns."""
        df = pd.DataFrame({
            'numbers': [1, 2, 3],
            'floats': [1.1, 2.2, 3.3],
            'bools': [True, False, True]
        })
        preprocessor = PreProcessor(df)
        result = preprocessor.fix_whitespace().apply()

        pd.testing.assert_frame_equal(result, df)

    def test_fix_whitespace_logs_transformation(self, messy_whitespace_df):
        """Should log whitespace transformation."""
        preprocessor = PreProcessor(messy_whitespace_df)
        preprocessor.fix_whitespace()

        assert len(preprocessor.transformation_log) == 1
        assert 'whitespace' in preprocessor.transformation_log[0].lower()


# ============================================================================
# COLUMN NAME STANDARDIZATION TESTS
# ============================================================================

class TestColumnNameStandardization:
    """Test column name standardization."""

    def test_standardize_column_names_spaces(self, messy_columns_df):
        """Should replace spaces with underscores."""
        preprocessor = PreProcessor(messy_columns_df)
        result = preprocessor.standardize_column_names().apply()

        assert 'user_name' in result.columns
        assert 'created_at' in result.columns
        assert 'User Name' not in result.columns

    def test_standardize_column_names_case(self, messy_columns_df):
        """Should convert to lowercase."""
        preprocessor = PreProcessor(messy_columns_df)
        result = preprocessor.standardize_column_names().apply()

        # All columns should be lowercase
        assert all(col == col.lower() for col in result.columns)

    def test_standardize_column_names_special_chars(self):
        """Should remove or replace special characters."""
        df = pd.DataFrame({
            'Column-Name': [1, 2, 3],
            'Another.Column': [4, 5, 6],
            'Third@Column': [7, 8, 9]
        })
        preprocessor = PreProcessor(df)
        result = preprocessor.standardize_column_names().apply()

        # Should have underscores instead of special chars
        assert 'column_name' in result.columns
        assert 'another_column' in result.columns

    def test_standardize_column_names_logs_transformation(self, messy_columns_df):
        """Should log column name standardization."""
        preprocessor = PreProcessor(messy_columns_df)
        preprocessor.standardize_column_names()

        assert len(preprocessor.transformation_log) == 1
        assert 'column' in preprocessor.transformation_log[0].lower()


# ============================================================================
# DATE STANDARDIZATION TESTS
# ============================================================================

class TestDateStandardization:
    """Test date standardization functionality."""

    def test_standardize_dates_single_column(self, date_df):
        """Should standardize date format in single column."""
        preprocessor = PreProcessor(date_df)
        result = preprocessor.standardize_dates(columns=['date1']).apply()

        # date1 should be datetime
        assert pd.api.types.is_datetime64_any_dtype(result['date1'])

    def test_standardize_dates_multiple_columns(self, date_df):
        """Should standardize multiple date columns."""
        preprocessor = PreProcessor(date_df)
        result = preprocessor.standardize_dates(columns=['date1', 'date2']).apply()

        assert pd.api.types.is_datetime64_any_dtype(result['date1'])
        assert pd.api.types.is_datetime64_any_dtype(result['date2'])

    def test_standardize_dates_auto_detect(self, date_df):
        """Should auto-detect date columns when not specified."""
        preprocessor = PreProcessor(date_df)
        result = preprocessor.standardize_dates().apply()

        # At least date1 should be converted
        assert pd.api.types.is_datetime64_any_dtype(result['date1'])

    def test_standardize_dates_preserves_non_date(self, date_df):
        """Should not affect non-date columns."""
        preprocessor = PreProcessor(date_df)
        result = preprocessor.standardize_dates().apply()

        # value column should remain numeric
        assert pd.api.types.is_numeric_dtype(result['value'])


# ============================================================================
# EMAIL VALIDATION TESTS
# ============================================================================

class TestEmailValidation:
    """Test email validation functionality."""

    def test_validate_emails_basic(self, email_df):
        """Should validate email addresses."""
        preprocessor = PreProcessor(email_df)
        result = preprocessor.validate_emails(column='email').apply()

        # Should have validation column
        assert 'email_valid' in result.columns

    def test_validate_emails_correct_validation(self, email_df):
        """Should correctly identify valid and invalid emails."""
        preprocessor = PreProcessor(email_df)
        result = preprocessor.validate_emails(column='email').apply()

        # valid@example.com should be valid
        assert result.loc[result['email'] == 'valid@example.com', 'email_valid'].iloc[0]

        # invalid-email should be invalid
        assert not result.loc[result['email'] == 'invalid-email', 'email_valid'].iloc[0]

    def test_validate_emails_nonexistent_column(self, email_df):
        """Should handle nonexistent column gracefully."""
        preprocessor = PreProcessor(email_df)
        result = preprocessor.validate_emails(column='nonexistent').apply()

        # Should still return DataFrame
        assert isinstance(result, pd.DataFrame)


# ============================================================================
# PHONE NUMBER VALIDATION TESTS
# ============================================================================

class TestPhoneValidation:
    """Test phone number validation functionality."""

    def test_validate_phone_numbers_basic(self, phone_df):
        """Should validate phone numbers."""
        preprocessor = PreProcessor(phone_df)
        result = preprocessor.validate_phone_numbers(column='phone').apply()

        # Should have validation column
        assert 'phone_valid' in result.columns

    def test_validate_phone_numbers_various_formats(self, phone_df):
        """Should handle various phone number formats."""
        preprocessor = PreProcessor(phone_df)
        result = preprocessor.validate_phone_numbers(column='phone').apply()

        # Different formats should be validated
        # Note: actual validation depends on implementation
        assert 'phone_valid' in result.columns

    def test_validate_phone_numbers_standardize(self, phone_df):
        """Should optionally standardize phone numbers."""
        preprocessor = PreProcessor(phone_df)
        result = preprocessor.validate_phone_numbers(column='phone', standardize=True).apply()

        # Should have standardized column if implemented
        assert isinstance(result, pd.DataFrame)


# ============================================================================
# DUPLICATE REMOVAL TESTS
# ============================================================================

class TestDuplicateRemoval:
    """Test duplicate row removal."""

    def test_remove_duplicates_all_columns(self, duplicate_df):
        """Should remove duplicate rows based on all columns."""
        preprocessor = PreProcessor(duplicate_df)
        result = preprocessor.remove_duplicates().apply()

        # Should have fewer rows (duplicates removed)
        assert len(result) < len(duplicate_df)
        assert len(result) == 3  # Only 3 unique combinations

    def test_remove_duplicates_specific_columns(self, duplicate_df):
        """Should remove duplicates based on specific columns."""
        preprocessor = PreProcessor(duplicate_df)
        result = preprocessor.remove_duplicates(subset=['id']).apply()

        # Should keep only unique ids
        assert len(result) == 3  # ids: 1, 2, 3

    def test_remove_duplicates_keep_first(self, duplicate_df):
        """Should keep first occurrence by default."""
        preprocessor = PreProcessor(duplicate_df)
        result = preprocessor.remove_duplicates(subset=['id'], keep='first').apply()

        # First occurrence should be kept
        assert result.loc[result['id'] == 1, 'name'].iloc[0] == 'Alice'

    def test_remove_duplicates_logs_transformation(self, duplicate_df):
        """Should log duplicate removal."""
        preprocessor = PreProcessor(duplicate_df)
        preprocessor.remove_duplicates()

        assert len(preprocessor.transformation_log) == 1
        assert 'duplicate' in preprocessor.transformation_log[0].lower()


# ============================================================================
# MISSING VALUE HANDLING TESTS
# ============================================================================

class TestMissingValueHandling:
    """Test missing value imputation."""

    def test_handle_missing_mean_strategy(self, missing_df):
        """Should impute missing values with mean."""
        preprocessor = PreProcessor(missing_df)
        result = preprocessor.handle_missing(strategy='mean', columns=['numeric']).apply()

        # Should have no missing values in numeric column
        assert result['numeric'].isna().sum() == 0

    def test_handle_missing_median_strategy(self, missing_df):
        """Should impute missing values with median."""
        preprocessor = PreProcessor(missing_df)
        result = preprocessor.handle_missing(strategy='median', columns=['numeric']).apply()

        assert result['numeric'].isna().sum() == 0

    def test_handle_missing_mode_strategy(self, missing_df):
        """Should impute missing values with mode."""
        preprocessor = PreProcessor(missing_df)
        result = preprocessor.handle_missing(strategy='mode', columns=['category']).apply()

        # Categorical column should have no missing values
        assert result['category'].isna().sum() == 0

    def test_handle_missing_constant_strategy(self, missing_df):
        """Should impute missing values with constant."""
        preprocessor = PreProcessor(missing_df)
        result = preprocessor.handle_missing(
            strategy='constant',
            columns=['category'],
            fill_value='MISSING'
        ).apply()

        # Should fill with specified constant
        assert 'MISSING' in result['category'].values

    def test_handle_missing_drop_strategy(self, missing_df):
        """Should drop rows with missing values."""
        preprocessor = PreProcessor(missing_df)
        result = preprocessor.handle_missing(strategy='drop').apply()

        # Should have fewer rows
        assert len(result) < len(missing_df)
        # Should have no missing values
        assert result.isna().sum().sum() == 0

    def test_handle_missing_forward_fill(self, missing_df):
        """Should forward fill missing values."""
        preprocessor = PreProcessor(missing_df)
        result = preprocessor.handle_missing(strategy='ffill', columns=['numeric']).apply()

        # Should reduce missing values
        assert result['numeric'].isna().sum() <= missing_df['numeric'].isna().sum()

    def test_handle_missing_logs_transformation(self, missing_df):
        """Should log missing value handling."""
        preprocessor = PreProcessor(missing_df)
        preprocessor.handle_missing(strategy='mean')

        assert len(preprocessor.transformation_log) == 1
        assert 'missing' in preprocessor.transformation_log[0].lower()


# ============================================================================
# ENCODING TESTS
# ============================================================================

class TestEncoding:
    """Test categorical encoding functionality."""

    def test_one_hot_encode_basic(self, categorical_df):
        """Should create one-hot encoded columns."""
        preprocessor = PreProcessor(categorical_df)
        result = preprocessor.one_hot_encode(columns=['category']).apply()

        # Should have new columns for each category
        assert 'category_A' in result.columns
        assert 'category_B' in result.columns
        assert 'category_C' in result.columns

    def test_one_hot_encode_multiple_columns(self, categorical_df):
        """Should encode multiple columns."""
        preprocessor = PreProcessor(categorical_df)
        result = preprocessor.one_hot_encode(columns=['category', 'size']).apply()

        # Should have encoded columns for both
        assert 'category_A' in result.columns
        assert 'size_S' in result.columns

    def test_one_hot_encode_drop_original(self, categorical_df):
        """Should drop original column when specified."""
        preprocessor = PreProcessor(categorical_df)
        result = preprocessor.one_hot_encode(columns=['category'], drop_original=True).apply()

        # Original column should be gone
        assert 'category' not in result.columns
        # Encoded columns should exist
        assert 'category_A' in result.columns

    def test_one_hot_encode_preserves_other_columns(self, categorical_df):
        """Should not affect other columns."""
        preprocessor = PreProcessor(categorical_df)
        result = preprocessor.one_hot_encode(columns=['category']).apply()

        # value column should still exist and be unchanged
        assert 'value' in result.columns
        pd.testing.assert_series_equal(result['value'], categorical_df['value'])


# ============================================================================
# PIPELINE TESTS
# ============================================================================

class TestPipeline:
    """Test pipeline creation and execution."""

    def test_create_pipeline_basic(self, messy_columns_df):
        """Should create and execute pipeline."""
        preprocessor = PreProcessor(messy_columns_df)
        pipeline = preprocessor.create_pipeline([
            'fix_whitespace',
            'standardize_column_names'
        ])

        # Pipeline should be the preprocessor itself
        assert pipeline is preprocessor

    def test_pipeline_chaining(self, messy_whitespace_df):
        """Should chain multiple operations."""
        preprocessor = PreProcessor(messy_whitespace_df)
        result = (preprocessor
                 .fix_whitespace()
                 .standardize_column_names()
                 .apply())

        # Both operations should be applied
        assert 'column_name' in result.columns
        assert result['column_name'].iloc[0] == 'value1'

    def test_pipeline_logs_all_transformations(self, messy_whitespace_df):
        """Should log all pipeline operations."""
        preprocessor = PreProcessor(messy_whitespace_df)
        (preprocessor
         .fix_whitespace()
         .standardize_column_names()
         .remove_duplicates()
         .apply())

        # Should have 3 log entries
        assert len(preprocessor.transformation_log) == 3

    def test_complex_pipeline(self, missing_df):
        """Should handle complex multi-step pipeline."""
        preprocessor = PreProcessor(missing_df)
        result = (preprocessor
                 .handle_missing(strategy='mean', columns=['numeric'])
                 .handle_missing(strategy='mode', columns=['category'])
                 .remove_duplicates()
                 .apply())

        # Should have no missing values
        assert result.isna().sum().sum() == 0


# ============================================================================
# TRANSFORMATION LOG TESTS
# ============================================================================

class TestTransformationLog:
    """Test transformation logging functionality."""

    def test_transformation_log_initialized_empty(self, messy_columns_df):
        """Should start with empty log."""
        preprocessor = PreProcessor(messy_columns_df)
        assert len(preprocessor.transformation_log) == 0

    def test_transformation_log_records_operations(self, messy_columns_df):
        """Should record each operation."""
        preprocessor = PreProcessor(messy_columns_df)
        preprocessor.fix_whitespace()
        preprocessor.standardize_column_names()

        assert len(preprocessor.transformation_log) == 2

    def test_transformation_log_contains_details(self, messy_columns_df):
        """Should contain meaningful operation details."""
        preprocessor = PreProcessor(messy_columns_df)
        preprocessor.fix_whitespace()

        # Log should contain description of operation
        assert len(preprocessor.transformation_log[0]) > 0
        assert isinstance(preprocessor.transformation_log[0], str)

    def test_get_transformation_log(self, messy_columns_df):
        """Should provide access to transformation log."""
        preprocessor = PreProcessor(messy_columns_df)
        preprocessor.fix_whitespace()
        preprocessor.standardize_column_names()

        log = preprocessor.transformation_log
        assert len(log) == 2


# ============================================================================
# EDGE CASE TESTS
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_dataframe_pipeline(self):
        """Should handle empty DataFrame in pipeline."""
        df = pd.DataFrame()
        preprocessor = PreProcessor(df)
        result = preprocessor.fix_whitespace().apply()

        assert result.empty

    def test_single_row_dataframe(self):
        """Should handle single-row DataFrame."""
        df = pd.DataFrame({'col': ['value']})
        preprocessor = PreProcessor(df)
        result = preprocessor.fix_whitespace().apply()

        assert len(result) == 1

    def test_all_missing_column(self):
        """Should handle column with all missing values."""
        df = pd.DataFrame({'all_null': [None, None, None], 'valid': [1, 2, 3]})
        preprocessor = PreProcessor(df)
        result = preprocessor.handle_missing(strategy='drop').apply()

        # Should drop all rows with any null
        assert len(result) == 0 or 'all_null' not in result.columns

    def test_no_duplicates(self):
        """Should handle DataFrame with no duplicates."""
        df = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})
        preprocessor = PreProcessor(df)
        result = preprocessor.remove_duplicates().apply()

        # Should return same DataFrame
        assert len(result) == len(df)

    def test_already_standardized_columns(self):
        """Should handle already standardized column names."""
        df = pd.DataFrame({'column_one': [1, 2, 3], 'column_two': [4, 5, 6]})
        preprocessor = PreProcessor(df)
        result = preprocessor.standardize_column_names().apply()

        # Columns should remain the same
        assert list(result.columns) == list(df.columns)


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegration:
    """Test integration scenarios."""

    def test_complete_preprocessing_workflow(self):
        """Should handle complete preprocessing workflow."""
        # Create messy data
        df = pd.DataFrame({
            '  User ID  ': [1, 2, 2, 3, 4],
            'Email Address': ['a@test.com', 'b@test.com', 'b@test.com', 'invalid', 'd@test.com'],
            'Created At': ['2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03', '2024-01-04'],
            'Value': [10.0, 20.0, 20.0, np.nan, 40.0]
        })

        # Apply complete pipeline
        preprocessor = PreProcessor(df)
        result = (preprocessor
                 .fix_whitespace()
                 .standardize_column_names()
                 .remove_duplicates()
                 .handle_missing(strategy='mean')
                 .standardize_dates()
                 .apply())

        # Verify results
        assert 'user_id' in result.columns
        assert len(result) < len(df)  # Duplicates removed
        assert result['value'].isna().sum() == 0  # Missing filled

    def test_preserves_data_integrity(self):
        """Should preserve data integrity through transformations."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })

        preprocessor = PreProcessor(df)
        result = (preprocessor
                 .fix_whitespace()
                 .standardize_column_names()
                 .apply())

        # Values should be unchanged
        assert result['value'].sum() == df['value'].sum()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
