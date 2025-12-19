# schema-mapper v1.0.0

## 🎉 Initial Production Release

Universal database schema mapper for BigQuery, Snowflake, Redshift, SQL Server, and PostgreSQL. Automatically generate schemas, DDL statements, and prepare your data for loading into any major database platform.

## ✨ Features

### Platform Support
- ✅ **BigQuery** - Google Cloud Platform
- ✅ **Snowflake** - Multi-cloud data platform
- ✅ **Amazon Redshift** - AWS data warehouse
- ✅ **PostgreSQL** - Open-source database
- ✅ **SQL Server** - Microsoft database

### Core Capabilities
- 🔄 **Automatic Type Detection** - Intelligently converts strings to dates, numbers, booleans
- 📝 **Column Standardization** - Cleans messy column names for database compatibility
- ✅ **Data Validation** - Pre-load validation to catch errors early
- 🏗️ **DDL Generation** - Platform-specific CREATE TABLE statements
- ⚡ **Table Optimization** - Clustering, partitioning, and distribution strategies
- 📊 **BigQuery JSON Schema** - Export schema for bq CLI
- 💻 **CLI Interface** - Command-line tool for quick operations

### Performance & Optimization
- **Partitioning** - BigQuery (DATE/TIMESTAMP/RANGE), PostgreSQL (RANGE/LIST/HASH)
- **Clustering** - BigQuery (up to 4 columns), Snowflake (up to 4 columns)
- **Distribution** - Redshift (KEY/ALL/EVEN/AUTO)
- **Sort Keys** - Redshift (Compound/Interleaved)

## 📦 Installation

```bash
# Basic installation
pip install schema-mapper

# With platform-specific dependencies
pip install schema-mapper[bigquery]
pip install schema-mapper[snowflake]
pip install schema-mapper[all]
```

## 🚀 Quick Start

```python
from schema_mapper import prepare_for_load
import pandas as pd

# Load your data
df = pd.read_csv('data.csv')

# Prepare for ANY platform in one line
df_clean, schema, issues = prepare_for_load(
    df,
    target_type='bigquery'
)

# Ready to load!
print(f"✅ {len(schema)} columns prepared!")
```

## 📊 Quality Metrics

- ✅ **111/111 tests passing** (100% pass rate)
- ✅ **75% code coverage**
- ✅ **Production-ready** error handling
- ✅ **Type hints** throughout
- ✅ **Comprehensive documentation**

## 🔗 Links

- 📦 [PyPI Package](https://pypi.org/project/schema-mapper/1.0.0/)
- 📚 [Documentation](https://github.com/datateamsix/schema-mapper#readme)
- 🐛 [Report Issues](https://github.com/datateamsix/schema-mapper/issues)
- 💬 [Discussions](https://github.com/datateamsix/schema-mapper/discussions)

## 🙏 Credits

Built by **DataTeamSix** for data engineers working across multiple cloud platforms.

---

**Made for universal cloud data engineering! ☁️**
