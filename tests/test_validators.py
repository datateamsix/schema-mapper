"""Tests for validators module."""

import pytest
import pandas as pd
import numpy as np
from schema_mapper.validators import (
    DataFrameValidator,
    ValidationResult,
    validate_dataframe
)


class TestValidationResult:
    """Test ValidationResult class."""
    
    def test_initialization(self):
        """Test ValidationResult initialization."""
        result = ValidationResult()
        assert result.errors == []
        assert result.warnings == []
    
    def test_add_error(self):
        """Test adding errors."""
        result = ValidationResult()
        result.add_error("Test error")
        assert len(result.errors) == 1
        assert "Test error" in result.errors[0]
    
    def test_add_warning(self):
        """Test adding warnings."""
        result = ValidationResult()
        result.add_warning("Test warning")
        assert len(result.warnings) == 1
        assert "Test warning" in result.warnings[0]
    
    def test_has_errors(self):
        """Test has_errors method."""
        result = ValidationResult()
        assert not result.has_errors()
        result.add_error("Error")
        assert result.has_errors()
    
    def test_to_dict(self):
        """Test to_dict conversion."""
        result = ValidationResult()
        result.add_error("Error 1")
        result.add_warning("Warning 1")
        
        d = result.to_dict()
        assert 'errors' in d
        assert 'warnings' in d
        assert len(d['errors']) == 1
        assert len(d['warnings']) == 1


class TestDataFrameValidator:
    """Test DataFrameValidator class."""
    
    def test_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        validator = DataFrameValidator()
        df = pd.DataFrame()
        result = validator.validate(df)
        
        assert result.has_errors()
        assert any('empty' in e.lower() for e in result.errors)
    
    def test_valid_dataframe(self):
        """Test validation of valid DataFrame."""
        validator = DataFrameValidator()
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        result = validator.validate(df)
        
        assert not result.has_errors()
    
    def test_high_null_percentage(self):
        """Test detection of high NULL percentage."""
        validator = DataFrameValidator(high_null_threshold=95.0)
        df = pd.DataFrame({'col': [1] + [None] * 99})
        result = validator.validate(df)
        
        assert result.has_warnings()
        assert any('null' in w.lower() for w in result.warnings)
    
    def test_mixed_types(self):
        """Test detection of mixed types."""
        validator = DataFrameValidator()
        df = pd.DataFrame({'col': [1, 'string', 3.5]})
        result = validator.validate(df)
        
        assert result.has_warnings()
        assert any('mixed' in w.lower() for w in result.warnings)
    
    def test_long_column_name(self):
        """Test detection of long column names."""
        validator = DataFrameValidator(max_column_length=10)
        df = pd.DataFrame({'very_long_column_name': [1, 2, 3]})
        result = validator.validate(df)
        
        assert result.has_warnings()
        assert any('long' in w.lower() for w in result.warnings)


class TestValidateDataFrameFunction:
    """Test validate_dataframe convenience function."""
    
    def test_basic_validation(self):
        """Test basic validation."""
        df = pd.DataFrame({'col': [1, 2, 3]})
        issues = validate_dataframe(df)
        
        assert isinstance(issues, dict)
        assert 'errors' in issues
        assert 'warnings' in issues
    
    def test_custom_thresholds(self):
        """Test with custom thresholds."""
        df = pd.DataFrame({'col': [1] + [None] * 90})
        issues = validate_dataframe(df, high_null_threshold=80.0)
        
        assert len(issues['warnings']) > 0
