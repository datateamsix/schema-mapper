"""Tests for utils module."""

import pytest
import pandas as pd
import numpy as np
from schema_mapper.utils import (
    standardize_column_name,
    detect_and_cast_types,
    infer_column_mode,
    get_column_description
)


class TestStandardizeColumnName:
    """Test standardize_column_name function."""
    
    def test_basic_standardization(self):
        """Test basic column name standardization."""
        assert standardize_column_name('User ID') == 'user_id'
        assert standardize_column_name('First Name') == 'first_name'
    
    def test_special_characters(self):
        """Test handling of special characters."""
        assert standardize_column_name('Email@Address') == 'email_address'
        assert standardize_column_name('Account Balance ($)') == 'account_balance'
        assert standardize_column_name('% Complete') == 'complete'
    
    def test_starting_with_number(self):
        """Test columns starting with numbers."""
        assert standardize_column_name('123InvalidStart') == '_123invalidstart'
    
    def test_empty_string(self):
        """Test empty string handling."""
        result = standardize_column_name('')
        assert result == 'unnamed_column'
    
    def test_consecutive_underscores(self):
        """Test removal of consecutive underscores."""
        assert standardize_column_name('User___ID') == 'user_id'


class TestDetectAndCastTypes:
    """Test detect_and_cast_types function."""
    
    def test_datetime_detection(self):
        """Test datetime type detection."""
        df = pd.DataFrame({'date': ['2024-01-01', '2024-01-02', '2024-01-03']})
        df_casted = detect_and_cast_types(df)
        
        assert pd.api.types.is_datetime64_any_dtype(df_casted['date'])
    
    def test_numeric_detection(self):
        """Test numeric type detection."""
        df = pd.DataFrame({'value': ['1', '2', '3']})
        df_casted = detect_and_cast_types(df)
        
        assert pd.api.types.is_integer_dtype(df_casted['value'])
    
    def test_float_detection(self):
        """Test float type detection."""
        df = pd.DataFrame({'value': ['1.5', '2.5', '3.5']})
        df_casted = detect_and_cast_types(df)
        
        assert pd.api.types.is_float_dtype(df_casted['value'])
    
    def test_boolean_detection(self):
        """Test boolean type detection."""
        df = pd.DataFrame({'active': ['yes', 'no', 'yes']})
        df_casted = detect_and_cast_types(df)
        
        assert df_casted['active'].dtype == bool
    
    def test_mixed_data_no_conversion(self):
        """Test that mixed data doesn't get converted."""
        df = pd.DataFrame({'mixed': ['text', '123', 'more text']})
        df_casted = detect_and_cast_types(df)
        
        # Should remain as object type
        assert df_casted['mixed'].dtype == object


class TestInferColumnMode:
    """Test infer_column_mode function."""
    
    def test_required_mode(self):
        """Test REQUIRED mode detection."""
        series = pd.Series([1, 2, 3, 4, 5])
        assert infer_column_mode(series) == 'REQUIRED'
    
    def test_nullable_mode(self):
        """Test NULLABLE mode detection."""
        series = pd.Series([1, 2, None, 4, 5])
        assert infer_column_mode(series) == 'NULLABLE'
    
    def test_all_nulls(self):
        """Test with all NULL values."""
        series = pd.Series([None, None, None])
        assert infer_column_mode(series) == 'NULLABLE'


class TestGetColumnDescription:
    """Test get_column_description function."""
    
    def test_basic_description(self):
        """Test basic description generation."""
        series = pd.Series([1, 2, 3, 4, 5])
        description = get_column_description(series)
        
        assert 'Type:' in description
        assert 'int64' in description
    
    def test_description_with_nulls(self):
        """Test description with NULL values."""
        series = pd.Series([1, 2, None, 4, 5])
        description = get_column_description(series)
        
        assert 'Nulls:' in description
    
    def test_description_with_low_cardinality(self):
        """Test description with low unique count."""
        series = pd.Series([1, 1, 2, 2, 3])
        description = get_column_description(series)
        
        assert 'Unique values:' in description
