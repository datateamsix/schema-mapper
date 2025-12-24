"""
Utility functions for schema mapping.

This module provides helper functions for column name standardization,
type detection, and data cleaning.

Note: These functions maintain backward compatibility. For advanced preprocessing
capabilities, see the PreProcessor class (when available).
"""

import re
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


def standardize_column_name(col_name: str) -> str:
    """
    Standardize column names for database compatibility.
    
    Rules:
    - Convert to lowercase
    - Replace spaces and special chars with underscores
    - Remove consecutive underscores
    - Strip leading/trailing underscores
    - Ensure starts with letter or underscore (not number)
    
    Args:
        col_name: Original column name
        
    Returns:
        Standardized column name
        
    Example:
        >>> standardize_column_name("User ID#")
        'user_id'
        >>> standardize_column_name("First Name (Legal)")
        'first_name_legal'
    """
    if not isinstance(col_name, str):
        col_name = str(col_name)
    
    # Convert to lowercase and strip whitespace
    col = col_name.lower().strip()
    
    # Replace spaces and special characters with underscores
    col = re.sub(r'[^\w]+', '_', col)
    
    # Remove consecutive underscores
    col = re.sub(r'_+', '_', col)
    
    # Strip leading/trailing underscores
    col = col.strip('_')
    
    # Ensure starts with letter or underscore (not number)
    if col and col[0].isdigit():
        col = f"_{col}"
    
    # Handle empty result
    if not col:
        col = "unnamed_column"
        logger.warning(f"Column name '{col_name}' resulted in empty string, using 'unnamed_column'")
    
    return col


def detect_and_cast_types(
    df: pd.DataFrame,
    use_profiler: bool = False,
    profiler_insights: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Intelligently detect and cast DataFrame columns to optimal types.

    Attempts to convert object columns to more specific types:
    - Datetime (if >50% of values can be parsed)
    - Numeric (if >90% of values are numeric)
    - Boolean (if column has ≤2 unique values matching bool patterns)

    Args:
        df: Input DataFrame
        use_profiler: Whether to use Profiler for enhanced type detection (default: False)
        profiler_insights: Pre-computed profiler patterns (optional, for performance)

    Returns:
        DataFrame with optimized types

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'dates': ['2024-01-01', '2024-01-02']})
        >>> df_typed = detect_and_cast_types(df)
        >>> df_typed.dtypes['dates']
        dtype('<M8[ns]')

        >>> # Enhanced type detection with Profiler
        >>> df_typed = detect_and_cast_types(df, use_profiler=True)
    """
    df_casted = df.copy()

    # Get pattern insights from Profiler if requested
    patterns = profiler_insights
    if use_profiler and patterns is None:
        try:
            from .profiler import Profiler
            profiler = Profiler(df, name="type_detection")
            patterns = profiler.detect_patterns()
            logger.info("Using Profiler insights for enhanced type detection")
        except Exception as e:
            logger.warning(f"Could not use Profiler for type detection: {e}")
            patterns = None

    for col in df_casted.columns:
        # Skip if already datetime
        if pd.api.types.is_datetime64_any_dtype(df_casted[col]):
            continue

        # Try to infer better types for object columns
        if df_casted[col].dtype == 'object':
            # Use profiler patterns if available
            if patterns and col in patterns['details']:
                df_casted[col] = _cast_object_column_enhanced(
                    df_casted[col], col, patterns['details'][col]
                )
            else:
                df_casted[col] = _cast_object_column(df_casted[col], col)

    return df_casted


def _cast_object_column(series: pd.Series, col_name: str) -> pd.Series:
    """
    Attempt to cast an object column to a more specific type.
    
    Args:
        series: Series to cast
        col_name: Column name for logging
        
    Returns:
        Casted series
    """
    # Remove whitespace from strings
    if series.notna().any():
        series = series.apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    # Try datetime conversion
    series_datetime = _try_datetime_conversion(series, col_name)
    if series_datetime is not None:
        return series_datetime
    
    # Try numeric conversion
    series_numeric = _try_numeric_conversion(series, col_name)
    if series_numeric is not None:
        return series_numeric
    
    # Try boolean conversion
    series_bool = _try_boolean_conversion(series, col_name)
    if series_bool is not None:
        return series_bool
    
    return series


def _try_datetime_conversion(series: pd.Series, col_name: str) -> pd.Series:
    """Try to convert series to datetime."""
    try:
        converted = pd.to_datetime(series, errors='coerce', format='mixed')
        # If >50% converted successfully, use datetime
        success_rate = converted.notna().sum() / len(series)
        if success_rate > 0.5:
            logger.info(f"Column '{col_name}': Converted to datetime ({success_rate:.1%} success)")
            return converted
    except Exception as e:
        logger.debug(f"Column '{col_name}': Datetime conversion failed: {e}")
    return None


def _try_numeric_conversion(series: pd.Series, col_name: str) -> pd.Series:
    """Try to convert series to numeric."""
    try:
        converted = pd.to_numeric(series, errors='coerce')
        # If >90% converted successfully, use numeric
        success_rate = converted.notna().sum() / len(series)
        if success_rate > 0.9:
            # Check if integer
            if (converted.dropna() % 1 == 0).all():
                logger.info(f"Column '{col_name}': Converted to integer ({success_rate:.1%} success)")
                return converted.astype('Int64')
            else:
                logger.info(f"Column '{col_name}': Converted to float ({success_rate:.1%} success)")
                return converted
    except Exception as e:
        logger.debug(f"Column '{col_name}': Numeric conversion failed: {e}")
    return None


def _try_boolean_conversion(series: pd.Series, col_name: str) -> pd.Series:
    """Try to convert series to boolean."""
    unique_vals = series.dropna().unique()
    if len(unique_vals) > 2:
        return None
    
    bool_map = {
        'true': True, 'false': False,
        'yes': True, 'no': False,
        '1': True, '0': False,
        't': True, 'f': False,
        'y': True, 'n': False,
    }
    
    try:
        converted = series.str.lower().map(bool_map)
        success_rate = converted.notna().sum() / len(series)
        if success_rate > 0.8:
            logger.info(f"Column '{col_name}': Converted to boolean ({success_rate:.1%} success)")
            return converted
    except Exception as e:
        logger.debug(f"Column '{col_name}': Boolean conversion failed: {e}")
    
    return None


def _cast_object_column_enhanced(
    series: pd.Series,
    col_name: str,
    pattern_info: Dict
) -> pd.Series:
    """
    Enhanced object column casting using Profiler pattern insights.

    Args:
        series: Series to cast
        col_name: Column name for logging
        pattern_info: Pattern detection results from Profiler

    Returns:
        Casted series
    """
    # Check if column is primarily a date string
    if pattern_info.get('date_string', 0) > 70:
        logger.info(f"Column '{col_name}': Detected as date string ({pattern_info['date_string']:.1f}%)")
        try:
            return pd.to_datetime(series, errors='coerce', format='mixed')
        except Exception as e:
            logger.debug(f"Could not convert column '{col_name}' to datetime: {e}")
            pass

    # Check if column is primarily numeric (currency or percentage)
    if pattern_info.get('currency', 0) > 70:
        logger.info(f"Column '{col_name}': Detected as currency ({pattern_info['currency']:.1f}%)")
        # Remove currency symbols and convert
        cleaned = series.str.replace(r'[$€£¥,\s]', '', regex=True)
        return pd.to_numeric(cleaned, errors='coerce')

    if pattern_info.get('percentage', 0) > 70:
        logger.info(f"Column '{col_name}': Detected as percentage ({pattern_info['percentage']:.1f}%)")
        # Remove % and convert
        cleaned = series.str.replace(r'[%\s]', '', regex=True)
        return pd.to_numeric(cleaned, errors='coerce') / 100

    # Fall back to standard casting
    return _cast_object_column(series, col_name)


def infer_column_mode(series: pd.Series) -> str:
    """
    Determine if a column should be NULLABLE or REQUIRED.
    
    Args:
        series: Pandas Series
        
    Returns:
        'NULLABLE' if column has any NULL values, 'REQUIRED' otherwise
        
    Example:
        >>> import pandas as pd
        >>> s = pd.Series([1, 2, None])
        >>> infer_column_mode(s)
        'NULLABLE'
    """
    null_count = series.isna().sum()
    return 'NULLABLE' if null_count > 0 else 'REQUIRED'


def get_column_description(series: pd.Series) -> str:
    """
    Generate a helpful description for the column.
    
    Args:
        series: Pandas Series
        
    Returns:
        Column description with type info, null percentage, and unique count
        
    Example:
        >>> import pandas as pd
        >>> s = pd.Series([1, 2, 2, None, 3])
        >>> get_column_description(s)
        'Type: float64 | Nulls: 20.0% | Unique values: 3'
    """
    descriptions = []
    
    dtype_str = str(series.dtype)
    descriptions.append(f"Type: {dtype_str}")
    
    null_pct = (series.isna().sum() / len(series)) * 100
    if null_pct > 0:
        descriptions.append(f"Nulls: {null_pct:.1f}%")
    
    unique_count = series.nunique()
    if unique_count < 20:
        descriptions.append(f"Unique values: {unique_count}")
    
    return " | ".join(descriptions)


def prepare_dataframe_for_load(
    df: pd.DataFrame,
    standardize_columns: bool = True,
    auto_cast: bool = True,
    handle_nulls: bool = True
) -> pd.DataFrame:
    """
    Prepare DataFrame for loading into database.
    
    Args:
        df: Input DataFrame
        standardize_columns: Whether to standardize column names
        auto_cast: Whether to automatically detect and cast types
        handle_nulls: Whether to handle null values appropriately
        
    Returns:
        Prepared DataFrame
        
    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'User ID': [1, 2], 'Active': ['yes', 'no']})
        >>> df_prepared = prepare_dataframe_for_load(df)
        >>> list(df_prepared.columns)
        ['user_id', 'active']
    """
    df_prepared = df.copy()
    
    # Auto-cast types
    if auto_cast:
        logger.info("Auto-casting types...")
        df_prepared = detect_and_cast_types(df_prepared)
    
    # Standardize column names
    if standardize_columns:
        logger.info("Standardizing column names...")
        new_columns = {col: standardize_column_name(col) for col in df_prepared.columns}
        df_prepared.rename(columns=new_columns, inplace=True)
    
    # Handle nulls
    if handle_nulls:
        logger.info("Handling NULL values...")
        for col in df_prepared.columns:
            if df_prepared[col].dtype == 'object':
                df_prepared[col] = df_prepared[col].replace({np.nan: None})
                df_prepared[col] = df_prepared[col].replace({'': None})
    
    return df_prepared
