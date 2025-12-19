# Schema Mapper Package Guide

## 📦 Package Structure

```
schema-mapper-pkg/
├── schema_mapper/              # Main package directory
│   ├── __init__.py            # Package initialization & public API
│   ├── __version__.py         # Version information
│   ├── cli.py                 # Command-line interface
│   ├── core.py                # SchemaMapper main class
│   ├── generators.py          # DDL generation for each platform
│   ├── type_mappings.py       # Type conversion configurations
│   ├── utils.py               # Utility functions
│   └── validators.py          # Data validation logic
│
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── conftest.py            # Pytest fixtures
│   ├── test_core.py           # Core functionality tests
│   ├── test_utils.py          # Utility function tests
│   └── test_validators.py     # Validation tests
│
├── examples/                   # Usage examples
│   ├── basic_usage.py         # Basic usage example
│   └── multi_platform.py      # Multi-platform example
│
├── setup.py                    # Setup configuration (legacy)
├── pyproject.toml             # Modern Python project configuration
├── requirements.txt           # Core dependencies
├── README.md                  # Package documentation
├── LICENSE                    # MIT License
├── MANIFEST.in               # Additional files to include
├── Makefile                   # Common development tasks
└── .gitignore                # Git ignore rules
```

## 🏗️ Architecture Overview

### Modular Design

The package is designed with separation of concerns:

1. **type_mappings.py**: Configuration module
   - Contains all type mapping dictionaries
   - No business logic, pure configuration
   - Easy to extend with new platforms

2. **validators.py**: Data validation
   - `ValidationResult` class for structured results
   - `DataFrameValidator` class for validation logic
   - Reusable across different workflows

3. **utils.py**: Utility functions
   - Column name standardization
   - Type detection and casting
   - Helper functions
   - All functions are pure and testable

4. **generators.py**: DDL generation
   - Abstract base class `DDLGenerator`
   - Platform-specific implementations
   - Factory pattern for generator selection
   - Easy to add new platforms

5. **core.py**: Main orchestration
   - `SchemaMapper` class coordinates everything
   - Uses dependency injection
   - Clean public API

6. **cli.py**: Command-line interface
   - Argparse-based CLI
   - Integrates with core functionality
   - Registered as console script in setup.py

## 🚀 Installation & Development

### For Users

```bash
# Install from PyPI (once published)
pip install schema-mapper

# With platform-specific dependencies
pip install schema-mapper[bigquery]
pip install schema-mapper[all]
```

### For Developers

```bash
# Clone repository
git clone https://github.com/yourusername/schema-mapper.git
cd schema-mapper

# Install in development mode
pip install -e ".[dev]"

# Or use make
make install-dev
```

## 🧪 Running Tests

```bash
# Run all tests
pytest

# With coverage
pytest --cov=schema_mapper --cov-report=html

# Run specific test file
pytest tests/test_core.py

# Run specific test
pytest tests/test_core.py::TestSchemaMapper::test_generate_schema_basic

# Using make
make test
```

## 📝 Code Quality

### Linting

```bash
# Run flake8
flake8 schema_mapper tests

# Run mypy (type checking)
mypy schema_mapper

# Using make
make lint
```

### Formatting

```bash
# Format code with black
black schema_mapper tests

# Sort imports with isort
isort schema_mapper tests

# Using make
make format
```

## 📦 Building & Publishing

### Building the Package

```bash
# Clean previous builds
make clean

# Build distribution packages
make build

# This creates:
# - dist/schema-mapper-1.0.0.tar.gz (source distribution)
# - dist/schema_mapper-1.0.0-py3-none-any.whl (wheel)
```

### Publishing to PyPI

```bash
# Install twine (if not already installed)
pip install twine

# Upload to Test PyPI (for testing)
make upload-test

# Upload to production PyPI
make upload

# Manual upload
twine upload dist/*
```

### PyPI Setup

1. Create account on https://pypi.org/
2. Create account on https://test.pypi.org/ (for testing)
3. Create API tokens in account settings
4. Configure ~/.pypirc:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-...your-token...

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-...your-token...
```

## 🔧 Usage Examples

### As a Library

```python
from schema_mapper import prepare_for_load
import pandas as pd

df = pd.read_csv('data.csv')
df_clean, schema, issues = prepare_for_load(df, 'bigquery')

if not issues['errors']:
    print("Ready to load!")
```

### As a CLI Tool

```bash
# Generate schema
schema-mapper data.csv --platform bigquery --output schema.json

# Generate DDL
schema-mapper data.csv --platform snowflake --ddl --table-name users

# See all options
schema-mapper --help
```

## 🎯 Adding a New Platform

To add support for a new database platform:

### 1. Add Type Mappings

Edit `schema_mapper/type_mappings.py`:

```python
TYPE_MAPPINGS = {
    # ... existing platforms ...
    'newplatform': {
        'int64': 'BIGINT',
        'float64': 'DOUBLE',
        'object': 'VARCHAR(MAX)',
        # ... more mappings ...
    }
}
```

### 2. Create DDL Generator

Edit `schema_mapper/generators.py`:

```python
class NewPlatformDDLGenerator(DDLGenerator):
    """Generate DDL for New Platform."""
    
    def generate(self, schema, table_name, dataset_name=None, project_id=None):
        # Implementation here
        return ddl

# Add to factory function
def get_ddl_generator(platform):
    generators = {
        # ... existing ...
        'newplatform': NewPlatformDDLGenerator,
    }
    # ...
```

### 3. Add Tests

Create tests in `tests/test_core.py`:

```python
def test_newplatform_schema_generation():
    df = pd.DataFrame({'id': [1, 2, 3]})
    mapper = SchemaMapper('newplatform')
    schema, _ = mapper.generate_schema(df)
    assert len(schema) == 1
```

### 4. Update Documentation

- Add platform to README.md
- Update type mapping tables
- Add usage examples

## 📊 Version Management

Version is managed in `schema_mapper/__version__.py`:

```python
__version__ = "1.0.0"
```

To release a new version:

1. Update `__version__.py`
2. Update `CHANGELOG.md` (if you have one)
3. Commit changes
4. Tag release: `git tag v1.0.0`
5. Push tags: `git push --tags`
6. Build and publish: `make build && make upload`

## 🔒 Security Best Practices

1. **Never commit secrets**: Use environment variables or config files (in .gitignore)
2. **Validate inputs**: All user inputs are validated before processing
3. **SQL injection**: DDL is generated programmatically, not via string concatenation
4. **Dependencies**: Regularly update dependencies and check for vulnerabilities

```bash
# Check for security issues
pip install safety
safety check

# Update dependencies
pip install --upgrade -r requirements.txt
```

## 📈 Performance Optimization

### For Large DataFrames

```python
# Disable auto-casting for large datasets if types are known
df_clean, schema, issues = prepare_for_load(
    df,
    target_type='bigquery',
    auto_cast=False  # Skip type detection
)

# Process in chunks
for chunk in pd.read_csv('large_file.csv', chunksize=10000):
    df_clean, schema, issues = prepare_for_load(chunk, 'bigquery')
    # Process chunk...
```

### Memory Management

```python
# Use generators for large files
def process_large_file(filename):
    for chunk in pd.read_csv(filename, chunksize=10000):
        yield prepare_for_load(chunk, 'bigquery')

# Process without loading entire file
for df_clean, schema, issues in process_large_file('huge_data.csv'):
    # Load chunk to database
    pass
```

## 🐛 Debugging

### Enable Logging

```python
from schema_mapper import configure_logging
import logging

# Enable debug logging
configure_logging(logging.DEBUG)

# Now all operations will log detailed information
```

### Common Issues

**Issue**: Type detection not working
**Solution**: Check that >50% of values can be parsed for datetime, >90% for numeric

**Issue**: Column names not standardized
**Solution**: Ensure `standardize_columns=True` in function calls

**Issue**: Import errors
**Solution**: Reinstall package: `pip install -e .`

## 📚 Additional Resources

- **pandas Documentation**: https://pandas.pydata.org/docs/
- **Python Packaging Guide**: https://packaging.python.org/
- **pytest Documentation**: https://docs.pytest.org/
- **Type Hints (PEP 484)**: https://peps.python.org/pep-0484/

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Add tests
5. Run test suite: `make test`
6. Format code: `make format`
7. Submit pull request

## 📞 Support

For issues or questions:
- GitHub Issues: https://github.com/yourusername/schema-mapper/issues
- Email: dataeng@example.com

---

**Built with best practices for production-ready Python packages! 🚀**
