# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - 2025-01-XX

### Added
- **DataFrame Query Results**: All platform connectors now return pandas DataFrames from `execute_query()` instead of native database objects (RowIterator, cursors)
  - Automatic conversion for consistent API across all platforms
  - BigQuery: RowIterator → DataFrame via `.to_dataframe()`
  - SQL-based platforms: Cursor → DataFrame via `fetchall()` + column names
  - Improved error handling with ExecutionError exceptions
  - Debug logging for row counts

- **Enhanced Introspection Methods**:
  - `get_tables()`: Returns detailed table metadata as DataFrame (table_name, table_type, size_mb, num_rows, created, modified, description)
  - `get_schemas()` / `get_datasets()`: Lists all schemas/datasets with metadata (owner, created, location)
  - `get_database_tree()` / `get_project_tree()`: Returns hierarchical database structure in dict or DataFrame format
  - Support for both 'dict' (JSON-serializable) and 'dataframe' (flat table) output formats
  - Table count tracking and detailed metadata for data discovery workflows

- **Warehouse Structure Documentation**:
  - New `WAREHOUSE_STRUCTURE_COMPARISON.md` in docs/
  - Side-by-side comparison of all 5 platform hierarchies (Project/Database/Schema levels)
  - Terminology mapping for cross-platform migrations
  - Query syntax reference for each platform
  - Migration considerations and best practices
  - Complete API coverage matrix

### Enhanced
- **BigQuery Documentation**: Added prominent notes about `dataset.table` format requirement in class docstrings and query examples
- **Error Handling**: All new introspection methods include comprehensive error handling with platform-specific ExecutionError exceptions
- **Logging**: Enhanced logging for all introspection operations with row/table counts
- **Visualization Methods**: Enhanced profiler visualization capabilities with customizable styling
  - Updated `plot_histogram()` with color parameters (default: dark blue-grey #34495e, KDE: red #e74c3c)
  - New `plot_scatter_matrix()` method for pairwise relationship visualization with trend lines
  - Automatic axis and title labeling for all plots
  - Abstracted matplotlib complexity with sensible defaults
  - Added `Profiler.plot_distributions()` and `Profiler.plot_scatter_matrix()` wrapper methods

- **Machine Learning Features**: New methods for ML preprocessing and feature analysis
  - **Profiler.analyze_target_correlation()**: Analyze feature correlations against a target variable
    - Automatic binary encoding for categorical targets (classification tasks)
    - Label encoding for multi-class targets
    - Returns sorted DataFrame by correlation strength
    - Supports pearson, spearman, and kendall correlation methods
  - **Profiler.plot_target_correlation()**: Visualize feature importance with horizontal bar chart
    - Color-coded positive (green) and negative (red) correlations
    - Automatic target encoding for categorical variables
    - Configurable top-N features display
  - **PreProcessor.auto_encode_categorical()**: Intelligent categorical encoding
    - Auto-detects categorical columns based on data type and cardinality
    - Configurable thresholds for max categories and minimum frequency
    - Excludes specified columns (e.g., target, ID columns)
    - One-hot encoding with optional multicollinearity handling

### Use Cases Enabled
- Query databases and immediately get DataFrames for analysis
- Export complete warehouse inventories with metadata
- Discover database structure programmatically
- Plan cross-platform migrations with structure comparison
- Filter and analyze table metadata (find large tables, views, etc.)

## [1.1.0] - 2024-12-20

### Added
- **Data Profiling Module** (`profiler.py`)
  - Comprehensive statistical profiling with `Profiler` class
  - Quality assessment with overall quality score (0-100)
  - Anomaly detection using IQR, Z-score, and Isolation Forest methods
  - Pattern recognition (emails, phone numbers, URLs, IP addresses, dates, currency)
  - Cardinality analysis (high/medium/low cardinality classification)
  - Missing value analysis with detailed statistics
  - Duplicate detection and reporting
  - Distribution analysis (normal, skewed, bimodal classification)
  - Correlation analysis for numeric features
  - Data drift detection between datasets
  - Visualization support (distributions, correlations, missing values, outliers)
  - Report generation in multiple formats (dict, JSON, HTML)
  - Progress bars for long-running operations (via tqdm)
  - Column-level and dataset-level profiling capabilities

- **Data Preprocessing Module** (`preprocessor.py`)
  - Intelligent data cleaning with `PreProcessor` class
  - Format standardization:
    - Date standardization with auto-detection and custom formats
    - Phone number standardization (E164, INTERNATIONAL, NATIONAL formats)
    - Currency parsing and standardization
  - SQL naming conventions:
    - snake_case, camelCase, PascalCase conversion
    - Comprehensive SQL naming rules (lowercase, special char removal, etc.)
  - Text cleaning:
    - Whitespace fixing (trim, normalize strategies)
    - Case standardization (lower, upper, title, sentence)
    - Special character removal
  - Data validation:
    - Email validation with pattern matching
    - Phone number validation (with phonenumbers library support)
    - Numeric range validation
    - Custom validation rules
  - Missing value handling:
    - Auto-strategy selection based on data type
    - Multiple strategies (drop, mean, median, mode, ffill, bfill, constant)
    - KNN imputation support (with scikit-learn)
  - Duplicate handling:
    - Duplicate detection with detailed reporting
    - Exact duplicate removal
    - Fuzzy duplicate detection (with rapidfuzz/fuzzywuzzy)
  - Encoding:
    - One-hot encoding with prefix support
    - Label encoding
    - Ordinal encoding with custom ordering
  - Canonical schema integration:
    - Apply date formats from canonical schema
    - Schema-aware preprocessing
  - Method chaining for fluent API
  - Transformation logging for reproducibility
  - Pipeline creation from operation lists

- **Canonical Schema Enhancements**
  - Date format specifications in `ColumnDefinition`
  - Timezone support for timestamp columns
  - Automatic format application during preprocessing
  - Format validation against data

- **Integration Features**
  - `SchemaMapper.profile_data()` method for easy profiling
  - `SchemaMapper.preprocess_data()` method with pipeline support
  - `prepare_for_load()` enhanced with `profile` and `preprocess_pipeline` parameters
  - Seamless integration between profiling, preprocessing, and schema generation

- **Example Scripts**
  - `examples/profiler_demo.py` - Comprehensive profiling demonstration
  - `examples/preprocessor_demo.py` - Data cleaning and transformation examples
  - `examples/canonical_schema_date_formats_demo.py` - Date format usage
  - `examples/integration_demo.py` - End-to-end workflow
  - `examples/validation_rules_demo.py` - Validation examples
  - `examples/visualization_demo.py` - Visualization capabilities

### Enhanced
- `prepare_for_load()` function now supports:
  - `profile=True` parameter to generate data quality reports
  - `preprocess_pipeline` parameter for automated cleaning
  - `canonical_schema` parameter for format-aware processing
  - Returns 4 values when profiling: `(df_clean, schema, issues, report)`

- Data quality and cleaning capabilities now cover:
  - Statistical profiling and quality scoring
  - Automated anomaly and pattern detection
  - Intelligent missing value imputation
  - Format standardization across multiple data types
  - Reproducible transformation pipelines
  - Comprehensive validation frameworks

### Dependencies
- Optional dependencies for enhanced functionality:
  - `tqdm` - Progress bars for profiling operations
  - `phonenumbers` - Phone number validation and standardization
  - `rapidfuzz` or `fuzzywuzzy` - Fuzzy matching for deduplication
  - `scikit-learn` - Advanced imputation (KNN, Iterative)
  - `matplotlib` and `seaborn` - Data visualization

### Documentation
- Updated README.md with comprehensive profiler and preprocessor documentation
- Added API reference sections for `Profiler` and `PreProcessor` classes
- Documented new parameters for `prepare_for_load()` and `ColumnDefinition`
- Added feature capability tables and usage examples
- Linked to new example scripts

## [1.0.0] - 2024-12-18

### Added
- Initial release of schema-mapper
- Support for 5 database platforms:
  - BigQuery (Google Cloud Platform)
  - Snowflake (Multi-cloud)
  - Amazon Redshift (AWS)
  - Microsoft SQL Server (Azure/On-prem)
  - PostgreSQL (Open-source)
- Automatic type detection and casting
  - String to datetime conversion
  - String to numeric conversion
  - Boolean pattern detection
- Column name standardization
  - Lowercase conversion
  - Special character handling
  - Number prefix handling
- Data validation framework
  - Empty DataFrame detection
  - Duplicate column detection
  - High NULL percentage warnings
  - Mixed type detection
- Schema generation
  - Platform-specific type mappings
  - NULL/REQUIRED mode inference
  - Column descriptions
- DDL generation for all platforms
  - BigQuery with OPTIONS
  - Snowflake with inline COMMENT
  - Redshift with separate COMMENT statements
  - SQL Server with extended properties
  - PostgreSQL with COMMENT statements
- BigQuery schema JSON export (for bq CLI)
- Command-line interface
  - Schema generation
  - DDL generation
  - Data preparation
  - Multi-platform support
- Comprehensive test suite
  - Core functionality tests
  - Validator tests
  - Utility function tests
  - Multi-platform tests
- Complete documentation
  - README with examples
  - Package guide
  - API reference
  - Usage examples
- Python 3.8+ support

### Features
- Modular architecture with separation of concerns
- Factory pattern for DDL generators
- Extensive type mapping coverage (15+ types per platform)
- High-performance processing (1M+ rows tested)
- Production-ready error handling and logging
- pip-installable package
- Optional platform-specific dependencies

### Documentation
- Comprehensive README
- Package structure guide
- Development guide
- Contributing guidelines
- Usage examples
- API documentation

[1.0.0]: https://github.com/datateamsix/schema-mapper/releases/tag/v1.0.0
