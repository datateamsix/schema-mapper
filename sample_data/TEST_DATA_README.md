# Problematic Test Data - Online Clothing Retailer

## Overview
This dataset contains **1000 rows** of intentionally problematic data for an online clothing retailer. It's designed to test data quality, validation, and ETL pipelines.

**File**: `problematic_clothing_retailer_data.csv`
**Size**: ~200KB
**Rows**: 1000
**Columns**: 23 (includes SCD Type 2 and CDC columns)

## Data Quality Issues Included

### 1. Missing Values (492 total missing values across dataset)
- `NaN` / `None` values
- Empty strings `""`
- Placeholder values like `"N/A"`, `"TBD"`
- Mix of different null representations

**Missing value counts by column:**
```
Discount%           492 (49.2%)
Payment Method      279 (27.9%)
Customer ZIP        265 (26.5%)
product_category    263 (26.3%)
Customer Name       250 (25.0%)
Return Flag         218 (21.8%)
Order Status        157 (15.7%)
Price ($)            27 (2.7%)
Quantity             26 (2.6%)
ship_date            23 (2.3%)
customer_email       23 (2.3%)
Order ID             20 (2.0%)
```

### 2. Inconsistent Column Names
- **Whitespace issues**: `"Customer Name "` (trailing space), `" Order Date"` (leading space)
- **Mixed naming conventions**:
  - PascalCase: `"ProductName"`
  - Snake_case: `"customer_email"`, `"ship_date"`, `"product_category "`
  - Title Case with spaces: `"Order ID"`, `"Customer Name "`
- **Special characters**: `"Price ($)"`, `"Discount%"` (no space)

### 3. Date Format Inconsistencies
Dates appear in multiple formats:
- ISO 8601: `2025-04-15`
- US format: `11/18/2025`, `10-02-25`
- European format: `14.01.2025`, `25.01.2025`
- Alternative: `2025/05/01`
- Unix timestamps: `1754844871`
- Invalid dates: `2025-13-45`
- Non-date values: `"Not a date"`

### 4. Data Type Issues

#### Prices (`Price ($)`)
- Numbers with currency symbols: `$120.50`
- Negative prices: `-250.52`, `-200.63`
- Text values: `"FREE"`, `"N/A"`
- Inconsistent decimals: `19` (no decimals), `120.660` (3 decimals)
- Zero values: `0.00`
- Thousand separators: `1,234.56`

#### Quantities
- Negative quantities: `-1`
- Text values: `"one"`, `"N/A"`
- Unrealistic values: `999999`
- Zero quantities: `0`
- Decimal quantities: `2.0`, `4.0`

#### Total Amounts
- Calculation errors (doesn't match Price × Quantity)
- Scientific notation: `6.981400e+02`, `-1.633798e+08`
- Negative totals

### 5. Inconsistent Categorical Values

#### Order Status (15+ variations)
- Mixed case: `"completed"`, `"Completed"`, `"COMPLETED"`
- Different values for same status: `"cancelled"`, `"CANCELLED"`, `"canceled"`
- Inconsistent values: `"pending"`, `"Pending"`, `"PENDING"`
- Also: `"shipped"`, `"Shipped"`, `"processing"`, `"Processing"`
- Invalid: `"TBD"`, `"unknown"`, empty strings

#### Return Flag (10+ variations)
- `"Y"`, `"N"`
- `"Yes"`, `"No"`
- `"TRUE"`, `"FALSE"`
- `"1"`, `"0"`
- Empty strings, `None`

#### Product Category
- Mixed case: `"Men"`, `"Women"`, `"Kids"`, `"Unisex"`, `"UNISEX"`
- Empty strings and `None` values

### 6. Whitespace Issues
- Leading whitespace: `"  Purple  "` in colors
- Trailing whitespace: `" Cash "` in payment methods
- Product names with extra spaces
- Column names with spaces

### 7. Customer Data Issues

#### Email Addresses
- Missing domain: `customer3`
- Missing local part: `@hotmail.com`
- Double @: `customer7@@gmail.com`
- Not an email: `"NOT_AN_EMAIL"`
- Extra whitespace: `"   customer@email.com   "`

#### Customer Names
- Numbers instead of names: `"123456"`
- All lowercase: `"bob smith"`
- All uppercase: `"CHARLIE JOHNSON"`
- Name only (no first or last): `"Williams"`
- With suffixes: `"Eve Brown, Jr."`

#### ZIP Codes
- 4 digits: `2891`
- 5 digits: `19116`, `11504`
- 6 digits: `123456`
- ZIP+4 format: `12345-6789`
- Letters: `"ABCDE"`
- Whitespace

### 8. Order ID Issues
- Different formats: `"ORD-000001"`, `"ord-000001"`, `"ORD000001"`, `"000002"`
- Duplicates: Multiple `"DUPLICATE-001"` entries
- Missing prefixes
- Wrong padding: `"ORD-00006"` vs `"ORD-000001"`
- Whitespace: `"  ORD-000123  "`

### 9. Special Characters & Encoding
- Product names with™ symbol
- Product names with underscores instead of spaces: `"White_Scarf_-_XS"`
- Commas in values: `"Product,With,Commas"`
- Newlines in text: `"Product Name with\nNewline"`

### 10. Completely Empty Rows
- 10 rows are completely empty (all columns are `None`)

## Column Descriptions

### Base Transaction Columns

| Column Name | Expected Type | Issues |
|-------------|---------------|--------|
| `Order ID` | String | Duplicates, missing prefixes, whitespace, nulls |
| `customer_email` | String | Invalid formats, missing parts, special chars |
| `Customer Name ` | String | Mixed case, numbers, single names, trailing space in col name |
| ` Order Date` | Date | Multiple formats, invalid dates, leading space in col name |
| `ship_date` | Date | Multiple formats, invalid dates |
| `ProductName` | String | Whitespace, special chars, case inconsistency |
| `product_category ` | String | Case inconsistency, nulls, trailing space in col name |
| `Price ($)` | Decimal | Currency symbols, negatives, text, inconsistent decimals |
| `Quantity` | Integer | Negatives, text, unrealistic values, decimals |
| `Total Amount` | Decimal | Calculation errors, scientific notation |
| `Order Status` | String | 15+ variations of same values, case inconsistency |
| `Customer ZIP` | String | Various lengths, letters, invalid formats |
| `Payment Method` | String | Whitespace, nulls, mixed case |
| `Discount%` | Decimal | Mixed with/without % symbol, negatives, nulls |
| `Return Flag` | Boolean | 10+ representations of true/false |

### SCD Type 2 Columns

| Column Name | Expected Type | Issues | Purpose |
|-------------|---------------|--------|---------|
| `effective_from` | Date/Timestamp | Multiple formats, timestamps, nulls | When record became effective |
| `Effective To` | Date/Timestamp | Mixed case column name, various formats, "9999-12-31" | When record expires |
| `is_current` | Boolean | 20+ representations (Y/N, True/False, 1/0, yes/no, t/f) | Current record flag |
| `record_version` | Integer | String versions ("v1", "V2"), floats ("1.0"), nulls | Version tracking |
| `row_hash` | String | MD5 hashes (mixed case, truncated, invalid), nulls | Change detection |

**SCD Type 2 Issues:**
- **effective_from**: Mix of dates (2024-11-30), datetimes (2024-11-30 13:01:20), timestamps (1749747680), and strings ("2099-12-31")
- **Effective To**: Inconsistent column naming (different case from effective_from), 85% are far-future dates (2099-12-31, 9999-12-31), 15% are expired
- **is_current**: 20+ different representations of boolean values
- **record_version**: Mix of numbers (1, 2, 3), versioned strings (v1, V2), floats (1.0, 2.0)
- **row_hash**: MD5 hashes in various formats - lowercase, uppercase, truncated, some with "INVALID" suffix

### CDC (Change Data Capture) Columns

| Column Name | Expected Type | Issues | Purpose |
|-------------|---------------|--------|---------|
| `_operation` | String | 20+ variations of I/U/D codes | Operation type (Insert/Update/Delete) |
| `cdc_sequence` | Integer | Out of order, negative, zero-padded, nulls | Sequence/ordering |
| `CDC_Timestamp` | Timestamp | Mixed case column name, various formats, 30% nulls | When change occurred |

**CDC Issues:**
- **_operation**: Multiple representations:
  - Single character: `I`, `i`, `U`, `u`, `D`, `d`
  - Full words: `Insert`, `INSERT`, `insert`, `Update`, `UPDATE`, `update`, `Delete`, `DELETE`, `delete`
  - Numeric codes: `1` (Insert), `2` (Update), `3` (Delete)
  - Invalid: `X`, empty strings, nulls
  - Distribution: 60% Insert, 30% Update, 10% Delete

- **cdc_sequence**: Should be sequential but has:
  - Out-of-order sequences
  - Zero-padded formats ("0000000028")
  - Negative numbers (-1)
  - Unrealistic gaps

- **CDC_Timestamp**: Mix of:
  - ISO format: `2025-11-24 14:41:20.938138`
  - US format: `12/16/2025 15:16:20`
  - Unix timestamps: `1763981480`
  - High null rate (30%)

## Sample Problematic Rows

### Row 1 (Completely Empty)
All values are `NaN` - represents a completely empty row in the dataset.

### Row 2 (Multiple Issues)
```
Order ID: ord-000001 (lowercase prefix)
Email: customer1@hotmail.com (valid)
Name: 123456 (numbers instead of name)
Order Date: 1754844871 (Unix timestamp)
Price: -250.52 (negative price!)
Quantity: 0.0 (zero quantity)
Status: cancelled (lowercase)
```

### Row 3 (Case Inconsistencies)
```
Order ID: 000002 (missing prefix)
Email: customer2@@yahoo.com (double @)
Name: CHARLIE JOHNSON (all caps)
Status: COMPLETED (all caps)
Product: red hat - xs (all lowercase)
```

### Row 9 (Extreme Values)
```
Quantity: 999999.0 (unrealistic)
Total Amount: -163,379,800.00 (wildly incorrect calculation)
Price: -163.38 (negative)
Status: pending (lowercase)
```

## Usage Examples

### Load and Inspect
```python
import pandas as pd

df = pd.read_csv('problematic_clothing_retailer_data.csv')

# Check data types
print(df.dtypes)

# Find missing values
print(df.isnull().sum())

# See unique values in categorical columns
print(df['Order Status'].value_counts(dropna=False))
```

### Test Data Cleaning
```python
# Example: Clean column names
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

# Example: Standardize order status
df['order_status'] = df['order_status'].str.lower().str.strip()

# Example: Parse dates
from dateutil import parser
df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

# Example: Clean prices
df['price'] = df['price'].str.replace('$', '').str.replace(',', '')
df['price'] = pd.to_numeric(df['price'], errors='coerce')
```

### Test Schema Mapper - All Patterns
```python
from schema_mapper import SchemaMapper, IncrementalConfig, LoadPattern, DeleteStrategy

mapper = SchemaMapper('bigquery')

# Generate schema from messy data
schema, suggestions = mapper.generate_schema(df)
print(schema)

# Test basic UPSERT
config = IncrementalConfig(
    load_pattern=LoadPattern.UPSERT,
    primary_keys=['order_id']
)
ddl = mapper.generate_incremental_ddl(df, 'orders', config)
print(ddl)

# Test SCD Type 2
config_scd2 = IncrementalConfig(
    load_pattern=LoadPattern.SCD_TYPE2,
    primary_keys=['order_id'],
    effective_date_column='effective_from',
    expiration_date_column='effective_to',  # Note: actual column is 'Effective To' with space
    is_current_column='is_current',
    hash_columns=['customer_name', 'product_name', 'price']
)
ddl_scd2 = mapper.generate_incremental_ddl(df, 'orders_scd', config_scd2)
print(ddl_scd2)

# Test CDC
config_cdc = IncrementalConfig(
    load_pattern=LoadPattern.CDC_MERGE,
    primary_keys=['order_id'],
    operation_column='_operation',
    sequence_column='cdc_sequence',
    delete_strategy=DeleteStrategy.HARD_DELETE
)
ddl_cdc = mapper.generate_incremental_ddl(df, 'orders_cdc', config_cdc)
print(ddl_cdc)

# Test with different platforms
for platform in ['bigquery', 'snowflake', 'sqlserver']:
    mapper = SchemaMapper(platform)
    ddl = mapper.generate_incremental_ddl(df, 'orders', config_cdc)
    print(f"\n{platform.upper()} CDC DDL:")
    print(ddl[:500])
```

## Testing Scenarios

This dataset is perfect for testing:

### Core Data Quality
1. **Data Validation**: Detecting invalid dates, negative prices, missing required fields
2. **Type Inference**: Handling mixed types in columns
3. **Data Cleaning**: Standardizing categorical values, removing whitespace
4. **Null Handling**: Different representations of missing data
5. **Schema Detection**: Inferring correct data types from messy data
6. **Deduplication**: Finding and handling duplicate order IDs
7. **Date Parsing**: Handling multiple date formats (7+ formats for dates, timestamps)
8. **Error Handling**: Gracefully handling invalid values
9. **Column Name Standardization**: Cleaning inconsistent naming
10. **Data Quality Reporting**: Identifying and reporting data issues

### Incremental Load Patterns
11. **UPSERT/MERGE**: Testing insert + update logic with messy primary keys
12. **Incremental Timestamp**: Using order_date or ship_date as incremental column
13. **Append Only**: Simple insert testing with data quality issues
14. **Full Refresh**: Truncate and reload with validation

### SCD Type 2 Testing
15. **Effective Date Parsing**: Multiple date formats in effective_from and Effective To
16. **Current Flag Parsing**: 20+ boolean representations in is_current
17. **Version Management**: Handling version numbers as strings (v1, V2) and numbers
18. **Hash-based Change Detection**: Testing MD5 hash comparison with mixed case
19. **Expiration Logic**: Identifying current (2099-12-31) vs expired records
20. **Column Name Mapping**: Handling "effective_from" vs "Effective To" inconsistency

### CDC Pattern Testing
21. **Operation Code Parsing**: Normalizing 20+ variations of I/U/D codes
22. **Sequence Ordering**: Handling out-of-order, negative, and zero-padded sequences
23. **Timestamp Parsing**: Multiple timestamp formats with 30% nulls
24. **Delete Handling**: Processing DELETE operations (10% of records)
25. **Update vs Insert**: Distinguishing between UPDATE (30%) and INSERT (60%) operations
26. **Invalid Operations**: Handling invalid operation codes like "X"

### Platform-Specific Testing
27. **BigQuery**: MERGE syntax with all patterns
28. **Snowflake**: Transaction support, COPY INTO for staging
29. **SQL Server**: T-SQL MERGE, BULK INSERT, OUTPUT clause
30. **Redshift**: DELETE+INSERT pattern (no native MERGE)
31. **PostgreSQL**: INSERT ... ON CONFLICT DO UPDATE

### Advanced Scenarios
32. **Composite Key Detection**: Testing with order_id + date combinations
33. **Hash Collision Detection**: Identifying records with same hash but different data
34. **Temporal Consistency**: Ensuring effective_from < effective_to
35. **CDC Replay**: Replaying changes in sequence order
36. **Late-Arriving Data**: Handling out-of-sequence CDC events
37. **SCD Type 2 Versioning**: Creating new versions when data changes

## Data Generation

Generated using: `generate_test_data.py`

To regenerate:
```bash
python generate_test_data.py
```

## Notes

- This data is **intentionally problematic** for testing purposes
- Do NOT use this as a template for production data
- Real-world data issues are often more subtle but this covers common patterns
- Some values are mathematically impossible (negative quantities, wrong totals)
- Perfect for testing data validation and cleaning pipelines
