"""
Data validation utilities for schema mapping.

This module provides validation functions to check DataFrame quality
and identify potential issues before loading data.
"""

import pandas as pd
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation results."""
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"Validation error: {message}")
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Validation warning: {message}")
    
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0
    
    def to_dict(self) -> Dict[str, List[str]]:
        """Convert to dictionary format."""
        return {
            'errors': self.errors,
            'warnings': self.warnings
        }
    
    def __repr__(self) -> str:
        return f"ValidationResult(errors={len(self.errors)}, warnings={len(self.warnings)})"


class DataFrameValidator:
    """Validator for DataFrame quality checks."""
    
    def __init__(self, high_null_threshold: float = 95.0, max_column_length: int = 128):
        """
        Initialize validator.
        
        Args:
            high_null_threshold: Percentage threshold for high NULL warning (default: 95%)
            max_column_length: Maximum recommended column name length (default: 128)
        """
        self.high_null_threshold = high_null_threshold
        self.max_column_length = max_column_length
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Perform comprehensive validation on DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult containing errors and warnings
        """
        result = ValidationResult()
        
        # Run all validation checks
        self._check_empty(df, result)
        self._check_duplicate_columns(df, result)
        self._check_column_names(df, result)
        self._check_high_nulls(df, result)
        self._check_mixed_types(df, result)
        
        return result
    
    def _check_empty(self, df: pd.DataFrame, result: ValidationResult) -> None:
        """Check if DataFrame is empty."""
        if df.empty:
            result.add_error("DataFrame is empty")
    
    def _check_duplicate_columns(self, df: pd.DataFrame, result: ValidationResult) -> None:
        """Check for duplicate column names."""
        if len(df.columns) != len(set(df.columns)):
            duplicates = [col for col in df.columns if list(df.columns).count(col) > 1]
            result.add_error(f"Duplicate column names found: {set(duplicates)}")
    
    def _check_column_names(self, df: pd.DataFrame, result: ValidationResult) -> None:
        """Check for problematic column names."""
        for col in df.columns:
            if not col or (isinstance(col, str) and col.strip() == ''):
                result.add_error("Empty column name found")
            elif isinstance(col, str) and len(col) > self.max_column_length:
                result.add_warning(
                    f"Column name too long (>{self.max_column_length} chars): {col[:50]}..."
                )
    
    def _check_high_nulls(self, df: pd.DataFrame, result: ValidationResult) -> None:
        """Check for columns with high NULL percentages."""
        for col in df.columns:
            null_pct = (df[col].isna().sum() / len(df)) * 100
            if null_pct > self.high_null_threshold:
                result.add_warning(
                    f"Column '{col}' is {null_pct:.1f}% null - consider dropping"
                )
    
    def _check_mixed_types(self, df: pd.DataFrame, result: ValidationResult) -> None:
        """Check for mixed types in object columns."""
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].notna().any():
                types = df[col].dropna().apply(type).unique()
                if len(types) > 1:
                    type_names = [t.__name__ for t in types]
                    result.add_warning(
                        f"Column '{col}' has mixed types: {type_names}"
                    )


def validate_dataframe(
    df: pd.DataFrame,
    high_null_threshold: float = 95.0,
    max_column_length: int = 128
) -> Dict[str, List[str]]:
    """
    Convenience function to validate a DataFrame.
    
    Args:
        df: DataFrame to validate
        high_null_threshold: Percentage threshold for high NULL warning
        max_column_length: Maximum recommended column name length
        
    Returns:
        Dictionary with 'errors' and 'warnings' keys
        
    Example:
        >>> import pandas as pd
        >>> from schema_mapper.validators import validate_dataframe
        >>> df = pd.DataFrame({'col1': [1, 2, 3]})
        >>> issues = validate_dataframe(df)
        >>> if issues['errors']:
        ...     print("Errors found:", issues['errors'])
    """
    validator = DataFrameValidator(high_null_threshold, max_column_length)
    result = validator.validate(df)
    return result.to_dict()
