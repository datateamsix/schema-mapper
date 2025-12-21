"""
Data validation utilities for schema mapping.

This module provides validation functions to check DataFrame quality
and identify potential issues before loading data.
"""

import pandas as pd
from typing import Dict, List, Any
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
    """
    Lightweight validator that performs quick checks.

    For detailed analysis, use validate_detailed() which leverages the Profiler class.
    """

    def __init__(self, high_null_threshold: float = 95.0, max_column_length: int = 128):
        """
        Initialize validator.

        Args:
            high_null_threshold: Percentage threshold for high NULL warning (default: 95%)
            max_column_length: Maximum recommended column name length (default: 128)
        """
        self.high_null_threshold = high_null_threshold
        self.max_column_length = max_column_length
        self._profiler = None  # Lazy initialization for detailed validation

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Perform quick validation on DataFrame (backward compatible).

        This method performs fast, essential checks. For comprehensive
        analysis including anomaly detection, pattern recognition, and
        quality scoring, use validate_detailed().

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult containing errors and warnings

        Example:
            >>> validator = DataFrameValidator()
            >>> result = validator.validate(df)
            >>> if result.has_errors():
            ...     print("Errors:", result.errors)
        """
        result = ValidationResult()

        # Run all quick validation checks
        self._check_empty(df, result)
        self._check_duplicate_columns(df, result)
        self._check_column_names(df, result)
        self._check_high_nulls(df, result)
        self._check_mixed_types(df, result)

        return result

    def validate_detailed(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive validation using Profiler for deep analysis.

        This method provides extensive data quality metrics including:
        - Overall quality score
        - Anomaly detection
        - Pattern recognition
        - Missing value analysis
        - Cardinality analysis
        - Distribution analysis

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary with detailed validation results

        Example:
            >>> validator = DataFrameValidator()
            >>> detailed_results = validator.validate_detailed(df)
            >>> print(f"Quality Score: {detailed_results['quality_score']}/100")
            >>> print(f"Anomalies found: {len(detailed_results['anomalies'])}")
        """
        # Lazy import to avoid circular dependency
        from .profiler import Profiler

        logger.info("Performing detailed validation using Profiler...")

        # Initialize profiler if needed
        if self._profiler is None or self._profiler.df is not df:
            self._profiler = Profiler(df, name="validation_dataset")

        # Gather comprehensive metrics
        quality_assessment = self._profiler.assess_quality()
        anomalies = self._profiler.detect_anomalies(method='iqr', threshold=1.5)
        missing_values = self._profiler.analyze_missing_values()
        cardinality = self._profiler.analyze_cardinality()
        patterns = self._profiler.detect_patterns()
        duplicates = self._profiler.detect_duplicates()

        # Build detailed validation report
        detailed_report = {
            'quality_score': quality_assessment['overall_score'],
            'quality_breakdown': {
                'completeness': quality_assessment['completeness_score'],
                'uniqueness': quality_assessment['uniqueness_score'],
                'validity': quality_assessment['validity_score'],
                'consistency': quality_assessment['consistency_score'],
            },
            'interpretation': quality_assessment['interpretation'],
            'anomalies': anomalies,
            'missing_values': missing_values,
            'cardinality': cardinality,
            'patterns': patterns,
            'duplicates': duplicates,
            'recommendations': self._generate_recommendations(
                quality_assessment, anomalies, missing_values, duplicates
            )
        }

        return detailed_report

    def _generate_recommendations(
        self,
        quality: Dict,
        anomalies: Dict,
        missing: Dict,
        duplicates: Dict
    ) -> List[str]:
        """Generate actionable recommendations based on validation results."""
        recommendations = []

        # Quality-based recommendations
        if quality['overall_score'] < 60:
            recommendations.append(
                "⚠️ CRITICAL: Data quality score is below 60. Significant cleaning required."
            )

        # Anomaly recommendations
        if len(anomalies) > 0:
            for col, info in anomalies.items():
                if info['percentage'] > 5:
                    recommendations.append(
                        f"🔍 Column '{col}' has {info['count']} outliers ({info['percentage']:.1f}%). "
                        f"Consider investigating or winsorizing."
                    )

        # Missing value recommendations
        if missing['total_missing_percentage'] > 20:
            recommendations.append(
                f"🕳️ Dataset has {missing['total_missing_percentage']:.1f}% missing values. "
                "Consider imputation strategies."
            )

        for col in missing.get('high_missing_columns', []):
            pct = missing['missing_percentages'][col]
            recommendations.append(
                f"❌ Column '{col}' is {pct:.1f}% missing. Consider dropping this column."
            )

        # Duplicate recommendations
        if duplicates['count'] > 0:
            recommendations.append(
                f"👥 Found {duplicates['count']} duplicate rows ({duplicates['percentage']:.2f}%). "
                "Consider deduplication."
            )

        # If no issues, provide positive feedback
        if len(recommendations) == 0:
            recommendations.append(
                "✅ Data quality is excellent! Ready for production use."
            )

        return recommendations

    def validate_with_schema(self, df: pd.DataFrame, canonical_schema) -> ValidationResult:
        """
        Validate DataFrame against a canonical schema.

        This method checks:
        - Date/timestamp columns conform to specified date_format
        - Column presence (warns if schema columns missing from DataFrame)
        - Data type compatibility

        Args:
            df: DataFrame to validate
            canonical_schema: CanonicalSchema to validate against

        Returns:
            ValidationResult containing errors and warnings

        Example:
            >>> from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType
            >>> schema = CanonicalSchema(
            ...     table_name='events',
            ...     columns=[
            ...         ColumnDefinition('event_date', LogicalType.DATE, date_format='%Y-%m-%d')
            ...     ]
            ... )
            >>> validator = DataFrameValidator()
            >>> result = validator.validate_with_schema(df, schema)
        """
        from .canonical import LogicalType
        from .validation_rules import ValidationRules
        from datetime import datetime

        result = ValidationResult()

        # First validate schema itself
        schema_errors = canonical_schema.validate()
        if schema_errors:
            for error in schema_errors:
                result.add_error(f"Schema validation error: {error}")
            return result

        # Check for missing columns
        df_columns = set(df.columns)
        schema_columns = set(canonical_schema.column_names())
        missing_cols = schema_columns - df_columns
        if missing_cols:
            result.add_warning(f"Schema columns not found in DataFrame: {missing_cols}")

        # Validate date formats for temporal columns
        for col_def in canonical_schema.columns:
            if col_def.name not in df.columns:
                continue

            # Validate date/timestamp formats
            if col_def.logical_type in [LogicalType.DATE, LogicalType.TIMESTAMP, LogicalType.TIMESTAMPTZ]:
                if col_def.date_format:
                    # Sample validation - check first 100 non-null values
                    sample = df[col_def.name].dropna().head(100)
                    invalid_count = 0

                    for value in sample:
                        # Convert to string if needed
                        value_str = str(value) if not isinstance(value, str) else value

                        # Try to parse with specified format
                        try:
                            datetime.strptime(value_str, col_def.date_format)
                        except (ValueError, TypeError):
                            invalid_count += 1

                    if invalid_count > 0:
                        invalid_pct = (invalid_count / len(sample)) * 100
                        if invalid_pct > 10:  # More than 10% invalid
                            result.add_error(
                                f"Column '{col_def.name}': {invalid_count}/{len(sample)} values "
                                f"({invalid_pct:.1f}%) do not match expected format '{col_def.date_format}'"
                            )
                        else:
                            result.add_warning(
                                f"Column '{col_def.name}': {invalid_count}/{len(sample)} values "
                                f"({invalid_pct:.1f}%) do not match expected format '{col_def.date_format}'"
                            )

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
