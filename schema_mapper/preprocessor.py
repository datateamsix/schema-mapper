"""
Intelligent data cleaning and transformation pipeline.

This module provides the PreProcessor class for comprehensive data cleaning,
validation, transformation, and preparation with reproducible pipelines.
"""

import re
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Callable, Optional, Union
import logging
from datetime import datetime
from copy import deepcopy

logger = logging.getLogger(__name__)

# Optional dependencies with graceful fallbacks
try:
    import phonenumbers
    PHONENUMBERS_AVAILABLE = True
except ImportError:
    PHONENUMBERS_AVAILABLE = False
    logger.warning("phonenumbers not available - phone number standardization will be limited")

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    try:
        from fuzzywuzzy import fuzz, process
        RAPIDFUZZ_AVAILABLE = True
    except ImportError:
        RAPIDFUZZ_AVAILABLE = False
        logger.warning("rapidfuzz/fuzzywuzzy not available - fuzzy matching disabled")

try:
    from sklearn.impute import KNNImputer
    from sklearn.experimental import enable_iterative_imputer  # noqa
    from sklearn.impute import IterativeImputer
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not available - advanced imputation disabled")


class PreProcessor:
    """
    Intelligent data cleaning and transformation pipeline.

    Handles format standardization, validation, correction, missing values,
    and encoding with reproducible transformation pipelines.

    Example:
        >>> from schema_mapper import PreProcessor
        >>> df = pd.DataFrame({'Name': [' John', 'Jane '], 'Age': [25, None]})
        >>> preprocessor = PreProcessor(df)
        >>> df_clean = (preprocessor
        ...     .fix_whitespace()
        ...     .handle_missing_values(strategy='median')
        ...     .apply())
    """

    def __init__(self, df: pd.DataFrame, copy: bool = True, canonical_schema=None):
        """
        Initialize preprocessor with DataFrame.

        Args:
            df: Input DataFrame to preprocess
            copy: Whether to work on a copy (default True)
            canonical_schema: Optional CanonicalSchema to use for format specifications
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pd.DataFrame, got {type(df)}")

        if df.empty:
            logger.warning("Initializing PreProcessor with empty DataFrame")

        self.df = df.copy() if copy else df
        self._original_df = df.copy()  # Keep original for reference
        self._transformation_log: List[Dict[str, Any]] = []
        self.canonical_schema = canonical_schema  # Store schema for format specifications
        self._pending_transformations: List[Tuple[str, Dict]] = []

        logger.info(f"PreProcessor initialized with DataFrame shape {df.shape}")

    @property
    def transformation_log(self) -> List[Dict[str, Any]]:
        """
        Get transformation log for tracking all operations.

        Returns:
            List of transformation log entries

        Example:
            >>> preprocessor.fix_whitespace().standardize_column_names()
            >>> print(preprocessor.transformation_log)
        """
        # Return a simplified version for backward compatibility
        return [f"{entry['operation']}" for entry in self._transformation_log]

    def _log_transformation(self, operation: str, details: Dict[str, Any] = None):
        """Log a transformation for reproducibility."""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'details': details or {},
            'shape_before': self.df.shape
        }
        self._transformation_log.append(log_entry)

    def _update_shape_after(self):
        """Update the last log entry with shape after transformation."""
        if self._transformation_log:
            self._transformation_log[-1]['shape_after'] = self.df.shape

    # ========================================
    # FORMAT STANDARDIZATION
    # ========================================

    def apply_schema_formats(self) -> 'PreProcessor':
        """
        Apply date/time formats from canonical schema to DataFrame columns.

        This method reads date_format specifications from the canonical schema
        (if provided) and automatically applies them to the appropriate columns.

        Returns:
            Self for method chaining

        Raises:
            ValueError: If canonical_schema is not set

        Example:
            >>> from schema_mapper.canonical import CanonicalSchema, ColumnDefinition, LogicalType
            >>> schema = CanonicalSchema(
            ...     table_name='events',
            ...     columns=[
            ...         ColumnDefinition('event_date', LogicalType.DATE, date_format='%d/%m/%Y'),
            ...         ColumnDefinition('created_at', LogicalType.TIMESTAMP, date_format='%Y-%m-%d %H:%M:%S')
            ...     ]
            ... )
            >>> preprocessor = PreProcessor(df, canonical_schema=schema)
            >>> preprocessor.apply_schema_formats()
        """
        if self.canonical_schema is None:
            raise ValueError("Cannot apply schema formats: canonical_schema is not set")

        from .canonical import LogicalType

        # Validate schema first
        schema_errors = self.canonical_schema.validate()
        if schema_errors:
            logger.error(f"Schema validation errors: {schema_errors}")
            raise ValueError(f"Invalid canonical schema: {schema_errors}")

        # Apply date formats for temporal columns
        for col_def in self.canonical_schema.columns:
            # Only process temporal types with date_format specified
            if col_def.logical_type in [LogicalType.DATE, LogicalType.TIMESTAMP, LogicalType.TIMESTAMPTZ]:
                if col_def.date_format and col_def.name in self.df.columns:
                    logger.info(
                        f"Applying date format '{col_def.date_format}' to column '{col_def.name}' "
                        f"from canonical schema"
                    )
                    # Use standardize_dates with the schema's format
                    self.standardize_dates(
                        columns=[col_def.name],
                        format='auto',
                        target_format=col_def.date_format
                    )

        return self

    def standardize_dates(
        self,
        columns: Optional[Union[str, List[str]]] = None,
        format: str = 'auto',
        target_format: str = 'ISO8601'
    ) -> 'PreProcessor':
        """
        Standardize date formats across columns.

        Args:
            columns: Column name(s) to standardize (None = auto-detect date columns)
            format: Input format ('auto' for auto-detection or strptime format)
            target_format: Output format ('ISO8601', 'US', 'EU', or strptime format)

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.standardize_dates(['created_at', 'updated_at'])
            >>> preprocessor.standardize_dates()  # Auto-detect date columns
        """
        # Auto-detect date columns if not specified
        if columns is None:
            columns = []
            for col in self.df.columns:
                # Try to detect if column contains date-like strings
                if self.df[col].dtype == 'object':
                    sample = self.df[col].dropna().head(10)
                    if len(sample) > 0:
                        try:
                            pd.to_datetime(sample, errors='coerce')
                            # If at least 70% parse successfully, consider it a date column
                            parsed = pd.to_datetime(sample, errors='coerce')
                            if parsed.notna().sum() / len(sample) > 0.7:
                                columns.append(col)
                        except Exception as e:
                            logger.debug(f"Could not parse column {col} as datetime: {e}")
                            pass
            if not columns:
                logger.info("No date columns auto-detected")
                return self
        elif isinstance(columns, str):
            columns = [columns]
        else:
            columns = list(columns)

        self._log_transformation('standardize_dates', {
            'columns': columns,
            'format': format,
            'target_format': target_format
        })

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping")
                continue

            try:
                # Parse dates
                if format == 'auto':
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce', infer_datetime_format=True)
                else:
                    self.df[col] = pd.to_datetime(self.df[col], format=format, errors='coerce')

                # Format output
                if target_format == 'ISO8601':
                    # Keep as datetime (pandas default is ISO8601 for datetime)
                    pass
                elif target_format == 'US':
                    self.df[col] = self.df[col].dt.strftime('%m/%d/%Y')
                elif target_format == 'EU':
                    self.df[col] = self.df[col].dt.strftime('%d/%m/%Y')
                else:
                    # Custom format
                    self.df[col] = self.df[col].dt.strftime(target_format)

                logger.info(f"Standardized dates in column '{col}'")

            except Exception as e:
                logger.error(f"Failed to standardize dates in column '{col}': {e}")

        self._update_shape_after()
        return self

    def standardize_phone_numbers(
        self,
        column: str,
        country: str = 'US',
        format: str = 'E164'
    ) -> 'PreProcessor':
        """
        Standardize phone number formats.

        Args:
            column: Column name containing phone numbers
            country: Default country code (ISO 3166-1 alpha-2)
            format: Output format ('E164', 'INTERNATIONAL', 'NATIONAL', 'RFC3966')

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.standardize_phone_numbers('phone', country='US')
        """
        if not PHONENUMBERS_AVAILABLE:
            logger.warning("phonenumbers library not available - using basic standardization")
            return self._standardize_phone_numbers_basic(column)

        self._log_transformation('standardize_phone_numbers', {
            'column': column,
            'country': country,
            'format': format
        })

        if column not in self.df.columns:
            logger.warning(f"Column '{column}' not found")
            return self

        def format_phone(phone):
            if pd.isna(phone):
                return None
            try:
                parsed = phonenumbers.parse(str(phone), country)
                if not phonenumbers.is_valid_number(parsed):
                    return None

                format_map = {
                    'E164': phonenumbers.PhoneNumberFormat.E164,
                    'INTERNATIONAL': phonenumbers.PhoneNumberFormat.INTERNATIONAL,
                    'NATIONAL': phonenumbers.PhoneNumberFormat.NATIONAL,
                    'RFC3966': phonenumbers.PhoneNumberFormat.RFC3966
                }
                return phonenumbers.format_number(parsed, format_map.get(format, phonenumbers.PhoneNumberFormat.E164))
            except Exception:
                return None

        self.df[column] = self.df[column].apply(format_phone)
        logger.info(f"Standardized {self.df[column].notna().sum()} phone numbers in column '{column}'")

        self._update_shape_after()
        return self

    def _standardize_phone_numbers_basic(self, column: str) -> 'PreProcessor':
        """Basic phone number standardization without phonenumbers library."""
        def clean_phone(phone):
            if pd.isna(phone):
                return None
            # Remove all non-digit characters
            digits = re.sub(r'\D', '', str(phone))
            # Format as (XXX) XXX-XXXX for 10-digit US numbers
            if len(digits) == 10:
                return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11 and digits[0] == '1':
                return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
            return digits if digits else None

        self.df[column] = self.df[column].apply(clean_phone)
        return self

    def standardize_currency(
        self,
        columns: Union[str, List[str]],
        parse_symbols: bool = True
    ) -> 'PreProcessor':
        """
        Parse and standardize currency values.

        Args:
            columns: Column name(s) containing currency values
            parse_symbols: Whether to remove currency symbols and convert to numeric

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.standardize_currency(['price', 'cost'])
        """
        columns = [columns] if isinstance(columns, str) else columns

        self._log_transformation('standardize_currency', {
            'columns': columns,
            'parse_symbols': parse_symbols
        })

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping")
                continue

            if parse_symbols:
                # Remove currency symbols and commas, convert to numeric
                self.df[col] = (self.df[col]
                    .astype(str)
                    .str.replace(r'[$€£¥₹,\s]', '', regex=True)
                    .str.replace(r'[()]', '-', regex=True)  # Handle parentheses for negative
                )
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                logger.info(f"Parsed currency in column '{col}' to numeric")

        self._update_shape_after()
        return self

    # ========================================
    # NAMING CONVENTIONS (SQL Standards)
    # ========================================

    def standardize_column_names(self, convention: str = 'snake_case') -> 'PreProcessor':
        """
        Apply SQL naming conventions.

        Args:
            convention: Naming convention ('snake_case', 'camelCase', 'PascalCase')

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.standardize_column_names('snake_case')
        """
        self._log_transformation('standardize_column_names', {'convention': convention})

        old_columns = self.df.columns.tolist()

        if convention == 'snake_case':
            new_columns = [self._to_snake_case(col) for col in self.df.columns]
        elif convention == 'camelCase':
            new_columns = [self._to_camel_case(col) for col in self.df.columns]
        elif convention == 'PascalCase':
            new_columns = [self._to_pascal_case(col) for col in self.df.columns]
        else:
            raise ValueError(f"Unknown convention: {convention}")

        self.df.columns = new_columns
        logger.info(f"Standardized {len(new_columns)} column names to {convention}")

        self._update_shape_after()
        return self

    def apply_sql_naming_rules(self) -> 'PreProcessor':
        """
        Apply comprehensive SQL naming standards.

        Rules:
        - Lowercase
        - Replace spaces with underscores
        - Remove special characters
        - Prefix numbers with underscore
        - Maximum 63 characters (PostgreSQL limit)

        Returns:
            Self for method chaining
        """
        from .utils import standardize_column_name

        self._log_transformation('apply_sql_naming_rules', {})

        new_columns = [standardize_column_name(col) for col in self.df.columns]
        self.df.columns = new_columns

        logger.info("Applied SQL naming rules to all columns")
        self._update_shape_after()
        return self

    @staticmethod
    def _to_snake_case(s: str) -> str:
        """Convert string to snake_case."""
        # Handle acronyms and existing underscores
        s = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', s)
        s = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s)
        # Replace spaces and special chars
        s = re.sub(r'[^\w]+', '_', s)
        # Remove consecutive underscores
        s = re.sub(r'_+', '_', s)
        return s.lower().strip('_')

    @staticmethod
    def _to_camel_case(s: str) -> str:
        """Convert string to camelCase."""
        words = re.split(r'[^\w]+', s)
        return words[0].lower() + ''.join(word.capitalize() for word in words[1:])

    @staticmethod
    def _to_pascal_case(s: str) -> str:
        """Convert string to PascalCase."""
        words = re.split(r'[^\w]+', s)
        return ''.join(word.capitalize() for word in words)

    # ========================================
    # FORMAT ERROR FIXING
    # ========================================

    def fix_whitespace(
        self,
        columns: Optional[List[str]] = None,
        strategy: str = 'trim',
        fix_column_names: bool = True
    ) -> 'PreProcessor':
        """
        Remove leading/trailing whitespace or normalize internal spaces.

        Args:
            columns: Column names to fix (None = all object columns)
            strategy: 'trim' (remove leading/trailing) or 'normalize' (trim + collapse internal)
            fix_column_names: Whether to also strip whitespace from column names (default: True)

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.fix_whitespace(strategy='normalize')
        """
        # First, fix column names if requested
        if fix_column_names and len(self.df.columns) > 0:
            self.df.columns = self.df.columns.str.strip()

        if columns is None:
            columns = self.df.select_dtypes(include=['object']).columns.tolist()

        self._log_transformation('fix_whitespace', {
            'columns': columns,
            'strategy': strategy,
            'fix_column_names': fix_column_names
        })

        for col in columns:
            if col not in self.df.columns:
                continue

            if strategy == 'trim':
                self.df[col] = self.df[col].str.strip()
            elif strategy == 'normalize':
                # Trim and collapse multiple spaces to single space
                self.df[col] = self.df[col].str.strip().str.replace(r'\s+', ' ', regex=True)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")

        logger.info(f"Fixed whitespace in {len(columns)} columns ({strategy})")
        self._update_shape_after()
        return self

    def fix_case(
        self,
        columns: List[str],
        case: str = 'lower'
    ) -> 'PreProcessor':
        """
        Standardize text case.

        Args:
            columns: Column names to fix
            case: 'lower', 'upper', 'title', or 'sentence'

        Returns:
            Self for method chaining
        """
        self._log_transformation('fix_case', {'columns': columns, 'case': case})

        for col in columns:
            if col not in self.df.columns:
                continue

            if case == 'lower':
                self.df[col] = self.df[col].str.lower()
            elif case == 'upper':
                self.df[col] = self.df[col].str.upper()
            elif case == 'title':
                self.df[col] = self.df[col].str.title()
            elif case == 'sentence':
                self.df[col] = self.df[col].str.capitalize()
            else:
                raise ValueError(f"Unknown case: {case}")

        logger.info(f"Fixed case in {len(columns)} columns ({case})")
        self._update_shape_after()
        return self

    def remove_special_characters(
        self,
        columns: List[str],
        keep: str = '',
        replace_with: str = ''
    ) -> 'PreProcessor':
        """
        Remove or replace special characters.

        Args:
            columns: Column names to process
            keep: Characters to keep (e.g., '.-' to keep dots and hyphens)
            replace_with: Replacement string (default: remove)

        Returns:
            Self for method chaining
        """
        self._log_transformation('remove_special_characters', {
            'columns': columns,
            'keep': keep,
            'replace_with': replace_with
        })

        pattern = f"[^\\w\\s{re.escape(keep)}]"

        for col in columns:
            if col not in self.df.columns:
                continue

            self.df[col] = self.df[col].str.replace(pattern, replace_with, regex=True)

        logger.info(f"Removed special characters from {len(columns)} columns")
        self._update_shape_after()
        return self

    def normalize_text(self, columns: List[str]) -> 'PreProcessor':
        """
        Comprehensive text normalization (whitespace + case + special chars).

        Args:
            columns: Column names to normalize

        Returns:
            Self for method chaining
        """
        return (self
            .fix_whitespace(columns, strategy='normalize')
            .fix_case(columns, case='lower')
            .remove_special_characters(columns, keep='')
        )

    # To be continued in next part...
    def detect_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: str = 'first'
    ) -> Dict[str, Any]:
        """
        Detect duplicate rows with detailed report.

        Args:
            subset: Column names to consider for duplicates (None = all columns)
            keep: Which duplicates to mark ('first', 'last', False)

        Returns:
            Dictionary with duplicate information
        """
        duplicated = self.df.duplicated(subset=subset, keep=keep)
        dup_count = duplicated.sum()

        report = {
            'total_duplicates': int(dup_count),
            'duplicate_percentage': (dup_count / len(self.df)) * 100,
            'duplicate_indices': self.df[duplicated].index.tolist(),
            'unique_rows': len(self.df) - dup_count
        }

        logger.info(f"Found {dup_count} duplicate rows ({report['duplicate_percentage']:.2f}%)")
        return report

    def remove_duplicates(
        self,
        subset: Optional[List[str]] = None,
        keep: str = 'first'
    ) -> 'PreProcessor':
        """
        Remove duplicate rows.

        Args:
            subset: Column names to consider for duplicates (None = all columns)
            keep: Which duplicates to keep ('first', 'last', False)

        Returns:
            Self for method chaining
        """
        self._log_transformation('remove_duplicates', {'subset': subset, 'keep': keep})

        before_count = len(self.df)
        self.df = self.df.drop_duplicates(subset=subset, keep=keep)
        after_count = len(self.df)

        logger.info(f"Removed {before_count - after_count} duplicate rows")
        self._update_shape_after()
        return self

    def get_transformation_log(self) -> List[Dict[str, Any]]:
        """
        Get log of all transformations applied.

        Returns:
            List of transformation dictionaries
        """
        return deepcopy(self._transformation_log)

    def create_pipeline(self, operations: List[str]) -> 'PreProcessor':
        """
        Create a preprocessing pipeline from a list of operation names.

        Args:
            operations: List of method names to apply in sequence

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.create_pipeline([
            ...     'fix_whitespace',
            ...     'standardize_column_names',
            ...     'remove_duplicates'
            ... ])
        """
        logger.info(f"Creating pipeline with {len(operations)} operations")

        for operation in operations:
            if not hasattr(self, operation):
                logger.warning(f"Unknown operation: {operation}, skipping")
                continue

            method = getattr(self, operation)
            if callable(method):
                # Call the method without arguments (uses defaults)
                try:
                    result = method()
                    # If method doesn't return self, it might not be chainable
                    if result is not None and result != self:
                        logger.warning(f"Operation {operation} doesn't support chaining")
                except TypeError:
                    # Method requires arguments, skip with warning
                    logger.warning(f"Operation {operation} requires arguments, skipping")
            else:
                logger.warning(f"{operation} is not a callable method, skipping")

        return self

    def apply(self) -> pd.DataFrame:
        """
        Return the transformed DataFrame.

        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Applied {len(self._transformation_log)} transformations")
        return self.df.copy()

    def reset(self) -> 'PreProcessor':
        """
        Reset to original DataFrame and clear transformation log.

        Returns:
            Self for method chaining
        """
        self.df = self._original_df.copy()
        self._transformation_log = []
        logger.info("Reset to original DataFrame")
        return self

    # ========================================
    # VALIDATION
    # ========================================

    def validate_emails(self, column: str, fix: bool = False, add_column: bool = True) -> 'PreProcessor':
        """
        Validate email addresses, optionally fix common errors.

        Args:
            column: Column name containing emails
            fix: Whether to attempt fixing common errors
            add_column: Whether to add a validation column (default: True)

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.validate_emails('email_address')
            >>> # Adds 'email_valid' column with True/False
        """
        if column not in self.df.columns:
            logger.warning(f"Column '{column}' not found")
            return self

        self._log_transformation('validate_emails', {'column': column, 'fix': fix})

        # Email regex pattern (simplified)
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

        if fix:
            # Common fixes: trim whitespace, lowercase
            self.df[column] = self.df[column].str.strip().str.lower()

        valid_mask = self.df[column].str.match(email_pattern, na=False)
        valid_count = valid_mask.sum()
        total_count = self.df[column].notna().sum()

        if add_column:
            # Add validation result column
            self.df[f'{column}_valid'] = valid_mask

        logger.info(f"Email validation: {(valid_count / total_count * 100) if total_count > 0 else 0:.1f}% valid ({valid_count}/{total_count})")

        self._update_shape_after()
        return self

    def validate_phone_numbers(self, column: str, country: str = 'US', standardize: bool = False, add_column: bool = True) -> 'PreProcessor':
        """
        Validate phone numbers and optionally standardize them.

        Args:
            column: Column name containing phone numbers
            country: Default country code (default: 'US')
            standardize: Whether to standardize valid phone numbers (default: False)
            add_column: Whether to add a validation column (default: True)

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.validate_phone_numbers('phone', standardize=True)
            >>> # Adds 'phone_valid' column with True/False
        """
        if column not in self.df.columns:
            logger.warning(f"Column '{column}' not found")
            return self

        self._log_transformation('validate_phone_numbers', {
            'column': column,
            'country': country,
            'standardize': standardize
        })

        if not PHONENUMBERS_AVAILABLE:
            logger.warning("phonenumbers library not available - validation limited")
            # Basic validation: check if mostly digits
            valid_mask = self.df[column].astype(str).str.replace(r'\D', '', regex=True).str.len() >= 10
        else:
            def is_valid_phone(phone):
                if pd.isna(phone):
                    return False
                try:
                    parsed = phonenumbers.parse(str(phone), country)
                    return phonenumbers.is_valid_number(parsed)
                except Exception:
                    return False

            valid_mask = self.df[column].apply(is_valid_phone)

            if standardize:
                # Standardize valid phone numbers
                def format_phone(phone):
                    if pd.isna(phone):
                        return None
                    try:
                        parsed = phonenumbers.parse(str(phone), country)
                        if phonenumbers.is_valid_number(parsed):
                            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                        return phone
                    except Exception:
                        return phone

                self.df[column] = self.df[column].apply(format_phone)

        valid_count = valid_mask.sum()
        total_count = self.df[column].notna().sum()

        if add_column:
            # Add validation result column
            self.df[f'{column}_valid'] = valid_mask

        logger.info(f"Phone validation: {(valid_count / total_count * 100) if total_count > 0 else 0:.1f}% valid ({valid_count}/{total_count})")

        self._update_shape_after()
        return self

    def validate_ranges(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Validate numeric ranges.

        Args:
            column: Column name
            min_val: Minimum valid value (inclusive)
            max_val: Maximum valid value (inclusive)

        Returns:
            Dictionary with validation results
        """
        if column not in self.df.columns:
            raise KeyError(f"Column '{column}' not found")

        series = self.df[column]
        valid_mask = series.notna()

        if min_val is not None:
            valid_mask &= (series >= min_val)
        if max_val is not None:
            valid_mask &= (series <= max_val)

        valid_count = valid_mask.sum()
        total_count = series.notna().sum()

        report = {
            'valid_count': int(valid_count),
            'invalid_count': int(total_count - valid_count),
            'validity_percentage': (valid_count / total_count * 100) if total_count > 0 else 0,
            'min_val': min_val,
            'max_val': max_val,
            'invalid_indices': self.df[~valid_mask & series.notna()].index.tolist()
        }

        logger.info(f"Range validation: {report['validity_percentage']:.1f}% valid")
        return report

    def validate_custom(self, column: str, rule: Callable) -> Dict[str, Any]:
        """
        Validate using custom function.

        Args:
            column: Column name
            rule: Validation function that returns bool

        Returns:
            Dictionary with validation results
        """
        if column not in self.df.columns:
            raise KeyError(f"Column '{column}' not found")

        valid_mask = self.df[column].apply(lambda x: rule(x) if pd.notna(x) else False)
        valid_count = valid_mask.sum()
        total_count = self.df[column].notna().sum()

        report = {
            'valid_count': int(valid_count),
            'invalid_count': int(total_count - valid_count),
            'validity_percentage': (valid_count / total_count * 100) if total_count > 0 else 0,
            'invalid_indices': self.df[~valid_mask & self.df[column].notna()].index.tolist()
        }

        logger.info(f"Custom validation: {report['validity_percentage']:.1f}% valid")
        return report

    # ========================================
    # MISSING VALUE HANDLING
    # ========================================

    def handle_missing_values(
        self,
        strategy: str = 'auto',
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> 'PreProcessor':
        """
        Handle missing values with various strategies.

        Args:
            strategy: 'auto', 'drop', 'mean', 'median', 'mode', 'ffill', 'bfill', 'constant'
            columns: Columns to process (None = all columns with missing values)
            **kwargs: Additional arguments (e.g., fill_value for 'constant')

        Returns:
            Self for method chaining
        """
        self._log_transformation('handle_missing_values', {
            'strategy': strategy,
            'columns': columns,
            'kwargs': kwargs
        })

        if columns is None:
            columns = self.df.columns[self.df.isna().any()].tolist()

        for col in columns:
            if col not in self.df.columns:
                continue

            if strategy == 'auto':
                # Auto-select strategy based on column type
                if pd.api.types.is_numeric_dtype(self.df[col]):
                    self.df[col].fillna(self.df[col].median(), inplace=True)
                else:
                    self.df[col].fillna(self.df[col].mode()[0] if len(self.df[col].mode()) > 0 else None, inplace=True)
            elif strategy == 'drop':
                self.df.dropna(subset=[col], inplace=True)
            elif strategy == 'mean':
                # Only apply to numeric columns
                if pd.api.types.is_numeric_dtype(self.df[col]):
                    self.df[col].fillna(self.df[col].mean(), inplace=True)
                else:
                    logger.warning(f"Skipping mean imputation for non-numeric column: {col}")
            elif strategy == 'median':
                # Only apply to numeric columns
                if pd.api.types.is_numeric_dtype(self.df[col]):
                    self.df[col].fillna(self.df[col].median(), inplace=True)
                else:
                    logger.warning(f"Skipping median imputation for non-numeric column: {col}")
            elif strategy == 'mode':
                mode_val = self.df[col].mode()[0] if len(self.df[col].mode()) > 0 else None
                self.df[col].fillna(mode_val, inplace=True)
            elif strategy == 'ffill':
                self.df[col].fillna(method='ffill', inplace=True)
            elif strategy == 'bfill':
                self.df[col].fillna(method='bfill', inplace=True)
            elif strategy == 'constant':
                fill_value = kwargs.get('fill_value', 0)
                self.df[col].fillna(fill_value, inplace=True)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")

        logger.info(f"Handled missing values in {len(columns)} columns ({strategy})")
        self._update_shape_after()
        return self

    def handle_missing(
        self,
        strategy: str = 'auto',
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> 'PreProcessor':
        """
        Alias for handle_missing_values() for backward compatibility.

        Args:
            strategy: 'auto', 'drop', 'mean', 'median', 'mode', 'ffill', 'bfill', 'constant'
            columns: Columns to process (None = all columns with missing values)
            **kwargs: Additional arguments (e.g., fill_value for 'constant')

        Returns:
            Self for method chaining
        """
        return self.handle_missing_values(strategy=strategy, columns=columns, **kwargs)

    def impute_with_knn(
        self,
        columns: Optional[List[str]] = None,
        n_neighbors: int = 5
    ) -> 'PreProcessor':
        """
        Predictive imputation using KNN.

        Args:
            columns: Columns to impute (None = all numeric columns with missing values)
            n_neighbors: Number of neighbors for KNN

        Returns:
            Self for method chaining
        """
        if not SKLEARN_AVAILABLE:
            logger.error("scikit-learn not available - KNN imputation disabled")
            return self

        self._log_transformation('impute_with_knn', {
            'columns': columns,
            'n_neighbors': n_neighbors
        })

        if columns is None:
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            columns = [col for col in numeric_cols if self.df[col].isna().any()]

        if not columns:
            logger.info("No columns to impute")
            return self

        imputer = KNNImputer(n_neighbors=n_neighbors)
        self.df[columns] = imputer.fit_transform(self.df[columns])

        logger.info(f"Imputed {len(columns)} columns using KNN")
        self._update_shape_after()
        return self

    # ========================================
    # ENCODING
    # ========================================

    def one_hot_encode(
        self,
        columns: List[str],
        drop_first: bool = False,
        prefix: Optional[Union[str, List[str]]] = None,
        drop_original: bool = True
    ) -> 'PreProcessor':
        """
        One-hot encode categorical variables.

        Args:
            columns: Column names to encode
            drop_first: Drop first category to avoid multicollinearity
            prefix: Prefix for encoded column names
            drop_original: Whether to drop original columns after encoding (default: True)

        Returns:
            Self for method chaining

        Example:
            >>> preprocessor.one_hot_encode(['category'], drop_original=True)
        """
        self._log_transformation('one_hot_encode', {
            'columns': columns,
            'drop_first': drop_first,
            'prefix': prefix,
            'drop_original': drop_original
        })

        if drop_original:
            # Standard behavior: pd.get_dummies removes original columns
            self.df = pd.get_dummies(
                self.df,
                columns=columns,
                drop_first=drop_first,
                prefix=prefix
            )
        else:
            # Keep original columns: create encoded columns alongside originals
            for col in columns:
                if col not in self.df.columns:
                    continue

                # Get dummy columns
                dummies = pd.get_dummies(
                    self.df[col],
                    drop_first=drop_first,
                    prefix=prefix if prefix else col
                )

                # Add to DataFrame without removing original
                self.df = pd.concat([self.df, dummies], axis=1)

        logger.info(f"One-hot encoded {len(columns)} columns")
        self._update_shape_after()
        return self

    def label_encode(self, columns: List[str]) -> 'PreProcessor':
        """
        Label encode categorical variables.

        Args:
            columns: Column names to encode

        Returns:
            Self for method chaining
        """
        self._log_transformation('label_encode', {'columns': columns})

        for col in columns:
            if col not in self.df.columns:
                continue

            # Create mapping
            unique_values = self.df[col].dropna().unique()
            mapping = {val: idx for idx, val in enumerate(unique_values)}

            # Apply mapping
            self.df[col] = self.df[col].map(mapping)

            logger.info(f"Label encoded column '{col}' ({len(mapping)} categories)")

        self._update_shape_after()
        return self

    def ordinal_encode(self, column: str, ordering: List[Any]) -> 'PreProcessor':
        """
        Ordinal encoding with custom ordering.

        Args:
            column: Column name to encode
            ordering: Ordered list of categories (low to high)

        Returns:
            Self for method chaining
        """
        self._log_transformation('ordinal_encode', {
            'column': column,
            'ordering': ordering
        })

        if column not in self.df.columns:
            logger.warning(f"Column '{column}' not found")
            return self

        # Create mapping
        mapping = {val: idx for idx, val in enumerate(ordering)}

        # Apply mapping
        self.df[column] = self.df[column].map(mapping)

        logger.info(f"Ordinal encoded column '{column}' ({len(ordering)} categories)")
        self._update_shape_after()
        return self

    # ========================================
    # FUZZY MATCHING
    # ========================================

    def deduplicate_fuzzy(
        self,
        column: str,
        threshold: float = 0.8,
        keep: str = 'first'
    ) -> 'PreProcessor':
        """
        Fuzzy deduplication for text columns.

        Args:
            column: Column name to deduplicate
            threshold: Similarity threshold (0-1)
            keep: Which duplicates to keep ('first', 'last')

        Returns:
            Self for method chaining
        """
        if not RAPIDFUZZ_AVAILABLE:
            logger.warning("rapidfuzz/fuzzywuzzy not available - fuzzy deduplication disabled")
            return self

        self._log_transformation('deduplicate_fuzzy', {
            'column': column,
            'threshold': threshold,
            'keep': keep
        })

        if column not in self.df.columns:
            logger.warning(f"Column '{column}' not found")
            return self

        # Track which rows to keep
        to_drop = []
        seen = []

        for idx, value in enumerate(self.df[column]):
            if pd.isna(value):
                continue

            # Check against seen values
            is_duplicate = False
            for seen_val in seen:
                similarity = fuzz.ratio(str(value), str(seen_val)) / 100.0
                if similarity >= threshold:
                    is_duplicate = True
                    if keep != 'first':
                        # Remove the previously seen value
                        pass
                    break

            if is_duplicate and keep == 'first':
                to_drop.append(idx)
            else:
                seen.append(value)

        # Drop fuzzy duplicates
        self.df = self.df.drop(to_drop)

        logger.info(f"Removed {len(to_drop)} fuzzy duplicates from column '{column}'")
        self._update_shape_after()
        return self
