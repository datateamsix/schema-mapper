"""
Core SchemaMapper class for database schema generation.

This module provides the main SchemaMapper class that coordinates
all schema mapping operations.
"""

import pandas as pd
import json
from typing import Dict, List, Tuple, Optional
import logging

from .type_mappings import get_type_mapping, SUPPORTED_PLATFORMS
from .validators import DataFrameValidator
from .generators import get_ddl_generator
from .utils import (
    standardize_column_name,
    detect_and_cast_types,
    infer_column_mode,
    get_column_description,
    prepare_dataframe_for_load
)

logger = logging.getLogger(__name__)


class SchemaMapper:
    """
    Automated schema generation and data standardization for multiple platforms.
    
    Supports: BigQuery, Snowflake, Redshift, SQL Server, PostgreSQL
    
    Example:
        >>> from schema_mapper import SchemaMapper
        >>> import pandas as pd
        >>> 
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        >>> mapper = SchemaMapper('bigquery')
        >>> schema, _ = mapper.generate_schema(df)
        >>> print(schema)
    """
    
    def __init__(self, target_type: str = 'bigquery'):
        """
        Initialize the SchemaMapper.

        Args:
            target_type: Target platform ('bigquery', 'snowflake', 'redshift',
                        'sqlserver', 'postgresql')

        Raises:
            ValueError: If target_type is not supported
        """
        self.target_type = target_type.lower()
        self.type_map = get_type_mapping(self.target_type)
        self.ddl_generator = get_ddl_generator(self.target_type)
        self.profiler = None
        self.preprocessor = None
        self.incremental_generator = None  # Lazy-loaded when needed
        logger.info(f"Initialized SchemaMapper for {self.target_type}")
    
    def generate_schema(
        self,
        df: pd.DataFrame,
        standardize_columns: bool = True,
        auto_cast: bool = True,
        include_descriptions: bool = False
    ) -> Tuple[List[Dict], Dict[str, str]]:
        """
        Generate schema for target platform from DataFrame.
        
        Args:
            df: Input DataFrame
            standardize_columns: Whether to standardize column names
            auto_cast: Whether to automatically detect and cast types
            include_descriptions: Whether to include column descriptions
            
        Returns:
            Tuple of (schema list, column_mapping dict)
            
        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> schema, mapping = mapper.generate_schema(df)
            >>> print(mapping)  # {'User ID': 'user_id', ...}
        """
        logger.info(f"Generating schema for {len(df.columns)} columns")
        
        df_processed = df.copy()
        
        if auto_cast:
            logger.debug("Auto-casting types...")
            df_processed = detect_and_cast_types(df_processed)
        
        schema = []
        column_mapping = {}
        
        for original_col in df_processed.columns:
            # Standardize column name if requested
            if standardize_columns:
                new_col = standardize_column_name(original_col)
                column_mapping[original_col] = new_col
            else:
                new_col = original_col
            
            # Get pandas dtype and map to target type
            dtype_str = str(df_processed[original_col].dtype)
            target_type = self.type_map.get(dtype_str, self.type_map.get('object', 'STRING'))
            
            # Build schema field
            field = {
                'name': new_col,
                'type': target_type,
                'mode': infer_column_mode(df_processed[original_col])
            }
            
            if include_descriptions:
                field['description'] = get_column_description(df_processed[original_col])
            
            schema.append(field)
        
        logger.info(f"Generated schema with {len(schema)} fields")
        return schema, column_mapping

    def profile_data(
        self,
        df: pd.DataFrame,
        detailed: bool = True,
        show_progress: bool = True
    ) -> Dict:
        """
        Profile DataFrame before schema generation.

        Analyzes data quality, distributions, missing values, correlations,
        and patterns to help understand the dataset before loading.

        Args:
            df: DataFrame to profile
            detailed: Whether to generate detailed report (default: True)
            show_progress: Whether to show progress bars (default: True)

        Returns:
            Dictionary with profiling results:
            - If detailed=True: comprehensive report with all metrics
            - If detailed=False: basic summary statistics only

        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> report = mapper.profile_data(df)
            >>> print(f"Quality Score: {report['quality']['overall_score']}/100")
            >>>
            >>> # Quick summary only
            >>> summary = mapper.profile_data(df, detailed=False)
        """
        from .profiler import Profiler

        logger.info(f"Profiling DataFrame with {len(df)} rows, {len(df.columns)} columns")
        self.profiler = Profiler(df, name=self.target_type, show_progress=show_progress)

        if detailed:
            return self.profiler.generate_report(output_format='dict')
        else:
            return self.profiler.get_summary_stats().to_dict()

    def preprocess_data(
        self,
        df: pd.DataFrame,
        pipeline: Optional[List[str]] = None,
        canonical_schema=None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Preprocess DataFrame before schema generation.

        Applies data cleaning and transformation operations to prepare
        data for loading into the target platform.

        Args:
            df: DataFrame to preprocess
            pipeline: List of preprocessing operations to apply.
                     If None, applies default pipeline.
                     Available operations: 'fix_whitespace', 'standardize_columns',
                     'handle_missing', 'remove_duplicates', 'fix_types', etc.
            canonical_schema: Optional CanonicalSchema with date format specifications
            **kwargs: Additional arguments for preprocessing operations

        Returns:
            Preprocessed DataFrame

        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>>
            >>> # Default preprocessing
            >>> df_clean = mapper.preprocess_data(df)
            >>>
            >>> # Custom pipeline
            >>> df_clean = mapper.preprocess_data(df, pipeline=[
            ...     'fix_whitespace',
            ...     'standardize_columns',
            ...     'remove_duplicates',
            ...     'handle_missing'
            ... ])
            >>>
            >>> # With canonical schema for date formatting
            >>> df_clean = mapper.preprocess_data(df, canonical_schema=schema)
        """
        from .preprocessor import PreProcessor

        logger.info(f"Preprocessing DataFrame with {len(df)} rows, {len(df.columns)} columns")
        self.preprocessor = PreProcessor(df, canonical_schema=canonical_schema)

        if pipeline:
            # Apply custom pipeline
            for operation in pipeline:
                if not hasattr(self.preprocessor, operation):
                    logger.warning(f"Unknown preprocessing operation: {operation}")
                    continue
                # Call the operation method
                method = getattr(self.preprocessor, operation)
                self.preprocessor = method(**kwargs)

            # Apply schema formats if canonical_schema provided
            if canonical_schema:
                logger.info("Applying date formats from canonical schema...")
                self.preprocessor = self.preprocessor.apply_schema_formats()

            return self.preprocessor.apply()
        else:
            # Default preprocessing pipeline
            preprocessor = (self.preprocessor
                           .fix_whitespace()
                           .standardize_column_names())

            # Apply schema formats if canonical_schema provided
            if canonical_schema:
                logger.info("Applying date formats from canonical schema...")
                preprocessor = preprocessor.apply_schema_formats()

            return preprocessor.apply()

    def prepare_dataframe(
        self,
        df: pd.DataFrame,
        standardize_columns: bool = True,
        auto_cast: bool = True,
        handle_nulls: bool = True
    ) -> pd.DataFrame:
        """
        Prepare DataFrame for loading into target platform.
        
        Args:
            df: Input DataFrame
            standardize_columns: Whether to standardize column names
            auto_cast: Whether to automatically detect and cast types
            handle_nulls: Whether to handle null values appropriately
            
        Returns:
            Prepared DataFrame
            
        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> df_clean = mapper.prepare_dataframe(df)
        """
        logger.info("Preparing DataFrame for loading")
        return prepare_dataframe_for_load(df, standardize_columns, auto_cast, handle_nulls)
    
    def validate_dataframe(
        self,
        df: pd.DataFrame,
        high_null_threshold: float = 95.0
    ) -> Dict[str, List[str]]:
        """
        Validate DataFrame for common issues before loading.
        
        Args:
            df: Input DataFrame
            high_null_threshold: Percentage threshold for high NULL warning
            
        Returns:
            Dictionary with 'errors' and 'warnings' keys
            
        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> issues = mapper.validate_dataframe(df)
            >>> if issues['errors']:
            ...     print("Fix these:", issues['errors'])
        """
        logger.info("Validating DataFrame")
        validator = DataFrameValidator(high_null_threshold)
        result = validator.validate(df)
        return result.to_dict()
    
    def generate_ddl(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate CREATE TABLE DDL statement for target platform.
        
        Args:
            df: Input DataFrame
            table_name: Table name
            dataset_name: Dataset/schema/database name
            project_id: Project ID (BigQuery only)
            **kwargs: Additional arguments passed to generate_schema
            
        Returns:
            DDL statement
            
        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> ddl = mapper.generate_ddl(df, 'users', 'analytics', 'my-project')
            >>> print(ddl)
        """
        logger.info(f"Generating DDL for table {table_name}")
        schema, _ = self.generate_schema(df, **kwargs)
        return self.ddl_generator.generate(schema, table_name, dataset_name, project_id)
    
    def generate_bigquery_schema_json(self, df: pd.DataFrame, **kwargs) -> str:
        """
        Generate BigQuery schema in JSON format (for bq CLI or API).
        
        Args:
            df: Input DataFrame
            **kwargs: Additional arguments passed to generate_schema
            
        Returns:
            JSON string of schema
            
        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> schema_json = mapper.generate_bigquery_schema_json(df)
            >>> with open('schema.json', 'w') as f:
            ...     f.write(schema_json)
        """
        if self.target_type != 'bigquery':
            logger.warning(f"Generating BigQuery JSON schema for {self.target_type} mapper")
        
        schema, _ = self.generate_schema(df, **kwargs)
        bq_schema = []
        for field in schema:
            bq_field = {
                'name': field['name'],
                'type': field['type'],
                'mode': field.get('mode', 'NULLABLE')
            }
            if 'description' in field:
                bq_field['description'] = field['description']
            bq_schema.append(bq_field)
        
        return json.dumps(bq_schema, indent=2)
    
    @property
    def supported_platforms(self) -> List[str]:
        """Get list of supported platforms."""
        return SUPPORTED_PLATFORMS
    
    def generate_incremental_ddl(
        self,
        df: pd.DataFrame,
        table_name: str,
        config,  # IncrementalConfig
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Generate incremental load DDL.

        Generates platform-specific DDL for incremental load patterns like
        UPSERT, SCD Type 2, CDC, etc.

        Args:
            df: DataFrame to load
            table_name: Target table name
            config: IncrementalConfig object specifying load pattern and options
            dataset_name: Dataset/schema name
            project_id: Project ID (BigQuery only)
            **kwargs: Additional platform-specific arguments

        Returns:
            DDL statement(s) for incremental load

        Example:
            >>> from schema_mapper import SchemaMapper
            >>> from schema_mapper.incremental import IncrementalConfig, LoadPattern
            >>>
            >>> mapper = SchemaMapper('bigquery')
            >>> config = IncrementalConfig(
            ...     load_pattern=LoadPattern.UPSERT,
            ...     primary_keys=['user_id']
            ... )
            >>> ddl = mapper.generate_incremental_ddl(df, 'users', config)
        """
        # Generate schema first
        schema, _ = self.generate_schema(df)

        # Lazy-load incremental generator
        if self.incremental_generator is None:
            from .incremental import get_incremental_generator
            self.incremental_generator = get_incremental_generator(self.target_type)

        logger.info(f"Generating {config.load_pattern.value} DDL for {table_name}")

        # Generate DDL
        return self.incremental_generator.generate_incremental_ddl(
            schema=schema,
            table_name=table_name,
            config=config,
            dataset_name=dataset_name,
            project_id=project_id,
            **kwargs
        )

    def generate_merge_ddl(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_keys: List[str],
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Convenience method for generating MERGE (UPSERT) DDL.

        This is a shortcut for the common UPSERT pattern without needing
        to create a full IncrementalConfig object.

        Args:
            df: DataFrame to load
            table_name: Target table name
            primary_keys: List of primary key column names
            dataset_name: Dataset/schema name
            project_id: Project ID (BigQuery only)
            **kwargs: Additional arguments passed to IncrementalConfig

        Returns:
            MERGE/UPSERT DDL statement

        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> merge_ddl = mapper.generate_merge_ddl(
            ...     df,
            ...     'users',
            ...     primary_keys=['user_id']
            ... )
        """
        from .incremental import IncrementalConfig, LoadPattern

        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=primary_keys,
            **kwargs
        )

        return self.generate_incremental_ddl(
            df, table_name, config, dataset_name, project_id
        )

    def detect_primary_keys(
        self,
        df: pd.DataFrame,
        min_confidence: float = 0.7,
        return_all_candidates: bool = False
    ):
        """
        Automatically detect primary keys in DataFrame.

        Args:
            df: DataFrame to analyze
            min_confidence: Minimum confidence threshold (0.0 to 1.0)
            return_all_candidates: Return all candidates vs just best

        Returns:
            List of column names (if return_all_candidates=False)
            List of KeyCandidate objects (if return_all_candidates=True)

        Example:
            >>> mapper = SchemaMapper('bigquery')
            >>> keys = mapper.detect_primary_keys(df)
            >>> print(f"Detected keys: {keys}")
            Detected keys: ['user_id']

            >>> # Get all candidates with confidence scores
            >>> candidates = mapper.detect_primary_keys(df, return_all_candidates=True)
            >>> for candidate in candidates:
            ...     print(f"{candidate.columns}: {candidate.confidence:.2f} - {candidate.reasoning}")
        """
        from .incremental import PrimaryKeyDetector

        detector = PrimaryKeyDetector(min_confidence=min_confidence)

        if return_all_candidates:
            return detector.detect_keys(df, suggest_composite=True)
        else:
            best = detector.auto_detect_best_key(df, suggest_composite=True)
            return best.columns if best else []

    def __repr__(self) -> str:
        return f"SchemaMapper(target_type='{self.target_type}')"
