"""
Schema Mapper - Universal Database Schema Generator

A production-ready Python package for automated schema generation and data
standardization across BigQuery, Snowflake, Redshift, SQL Server, and PostgreSQL.

Example:
    >>> from schema_mapper import SchemaMapper, prepare_for_load
    >>> import pandas as pd
    >>> 
    >>> df = pd.read_csv('data.csv')
    >>> df_clean, schema, issues = prepare_for_load(df, target_type='bigquery')
    >>> 
    >>> if not issues['errors']:
    ...     print(f"Ready to load {len(schema)} columns!")
"""

import logging

from .__version__ import (
    __version__,
    __author__,
    __email__,
    __license__,
    __description__
)
from .core import SchemaMapper
from .type_mappings import SUPPORTED_PLATFORMS
from .validators import validate_dataframe
from .utils import standardize_column_name
from .profiler import Profiler
from .validation_rules import ValidationRules
from .preprocessor import PreProcessor
from .visualization import DataVisualizer

# Configure package-level logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    'SchemaMapper',
    'Profiler',
    'PreProcessor',
    'ValidationRules',
    'DataVisualizer',
    'prepare_for_load',
    'create_schema',
    'SUPPORTED_PLATFORMS',
    'validate_dataframe',
    'standardize_column_name',
    '__version__',
]


def create_schema(
    df,
    target_type='bigquery',
    standardize_columns=True,
    auto_cast=True,
    include_descriptions=False,
    return_mapping=False
):
    """
    Convenience function to generate schema from DataFrame.
    
    Args:
        df: Input DataFrame
        target_type: Target platform ('bigquery', 'snowflake', 'redshift', 
                    'sqlserver', 'postgresql')
        standardize_columns: Whether to standardize column names
        auto_cast: Whether to automatically detect and cast types
        include_descriptions: Whether to include column descriptions
        return_mapping: Whether to return column name mapping
        
    Returns:
        Schema list, or tuple of (schema list, column mapping dict) if return_mapping=True
        
    Example:
        >>> from schema_mapper import create_schema
        >>> import pandas as pd
        >>> 
        >>> df = pd.DataFrame({
        ...     'User ID': [1, 2, 3],
        ...     'Name': ['Alice', 'Bob', 'Charlie']
        ... })
        >>> schema = create_schema(df, target_type='bigquery')
        >>> print(f"Generated {len(schema)} fields")
    """
    mapper = SchemaMapper(target_type=target_type)
    schema, mapping = mapper.generate_schema(
        df,
        standardize_columns=standardize_columns,
        auto_cast=auto_cast,
        include_descriptions=include_descriptions
    )
    
    if return_mapping:
        return schema, mapping
    return schema


def prepare_for_load(
    df,
    target_type='bigquery',
    standardize_columns=True,
    auto_cast=True,
    validate=True,
    profile=False,
    preprocess_pipeline=None,
    canonical_schema=None
):
    """
    Prepare DataFrame for loading into target platform with schema.

    This is the recommended high-level function for most use cases. It validates,
    profiles (optional), preprocesses (optional), cleans, and prepares your data
    with an appropriate schema in one call.

    Args:
        df: Input DataFrame
        target_type: Target platform ('bigquery', 'snowflake', 'redshift',
                    'sqlserver', 'postgresql')
        standardize_columns: Whether to standardize column names
        auto_cast: Whether to automatically detect and cast types
        validate: Whether to validate the DataFrame
        profile: Whether to profile the DataFrame and include profiling report
                in the return value (default: False)
        preprocess_pipeline: List of preprocessing operations to apply before
                           schema generation. If None, no preprocessing is applied.
                           Available operations: 'fix_whitespace', 'standardize_columns',
                           'handle_missing', 'remove_duplicates', etc.
        canonical_schema: Optional CanonicalSchema that specifies date formats and
                         other column specifications. If provided, date formats will
                         be automatically applied during preprocessing and validated.

    Returns:
        If profile=False: Tuple of (prepared_df, schema, validation_issues)
        If profile=True: Tuple of (prepared_df, schema, validation_issues, profiling_report)

    Example:
        >>> from schema_mapper import prepare_for_load
        >>> import pandas as pd
        >>>
        >>> df = pd.read_csv('messy_data.csv')
        >>> df_clean, schema, issues = prepare_for_load(df, 'bigquery')
        >>>
        >>> if not issues['errors']:
        ...     # Ready to load!
        ...     df_clean.to_gbq('dataset.table', project_id='my-project')
        ... else:
        ...     print("Fix these errors:", issues['errors'])
        >>>
        >>> # With profiling and preprocessing
        >>> df_clean, schema, issues, report = prepare_for_load(
        ...     df, 'bigquery',
        ...     profile=True,
        ...     preprocess_pipeline=['fix_whitespace', 'remove_duplicates', 'handle_missing']
        ... )
        >>> print(f"Data Quality Score: {report['quality']['overall_score']}/100")
    """
    logger = logging.getLogger(__name__)
    mapper = SchemaMapper(target_type=target_type)

    # Optional: Profile first (on original data)
    profiling_report = None
    if profile:
        logger.info("Profiling original DataFrame...")
        profiling_report = mapper.profile_data(df, detailed=True)

    # Optional: Preprocess
    df_working = df.copy()
    if preprocess_pipeline:
        logger.info(f"Applying preprocessing pipeline: {preprocess_pipeline}")
        df_working = mapper.preprocess_data(
            df_working,
            pipeline=preprocess_pipeline,
            canonical_schema=canonical_schema
        )
    elif canonical_schema:
        # Apply schema formats even without explicit preprocessing pipeline
        logger.info("Applying date formats from canonical schema...")
        from .preprocessor import PreProcessor
        preprocessor = PreProcessor(df_working, canonical_schema=canonical_schema)
        df_working = preprocessor.apply_schema_formats().apply()

    # Validate if requested
    validation_issues = {'errors': [], 'warnings': []}
    if validate:
        if canonical_schema:
            # Use schema-aware validation
            logger.info("Validating DataFrame against canonical schema...")
            from .validators import DataFrameValidator
            validator = DataFrameValidator()
            schema_validation = validator.validate_with_schema(df_working, canonical_schema)
            validation_issues = schema_validation.to_dict()
            # Also run standard validation
            standard_validation = mapper.validate_dataframe(df_working)
            validation_issues['errors'].extend(standard_validation['errors'])
            validation_issues['warnings'].extend(standard_validation['warnings'])
        else:
            validation_issues = mapper.validate_dataframe(df_working)

        if validation_issues['errors']:
            logger.warning("Validation errors found:")
            for error in validation_issues['errors']:
                logger.warning(f"  - {error}")

    # Prepare DataFrame
    df_prepared = mapper.prepare_dataframe(
        df_working,
        standardize_columns=standardize_columns,
        auto_cast=auto_cast
    )

    # Generate schema
    schema, _ = mapper.generate_schema(
        df_working,
        standardize_columns=standardize_columns,
        auto_cast=auto_cast
    )

    logger.info(f"Prepared {len(df_prepared)} rows with {len(schema)} columns for {target_type}")

    if profile:
        return df_prepared, schema, validation_issues, profiling_report
    else:
        return df_prepared, schema, validation_issues


# Configure logging format
def configure_logging(level=logging.INFO):
    """
    Configure logging for the package.
    
    Args:
        level: Logging level (e.g., logging.INFO, logging.DEBUG)
        
    Example:
        >>> from schema_mapper import configure_logging
        >>> import logging
        >>> configure_logging(logging.DEBUG)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
