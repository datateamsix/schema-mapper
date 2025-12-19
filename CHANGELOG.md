# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
