"""
PreProcessor Demo - Comprehensive data cleaning and transformation

This example demonstrates the PreProcessor class capabilities:
- Format standardization (dates, phone, currency)
- SQL naming conventions
- Whitespace and case fixing
- Duplicate removal
- Validation (emails, phone, ranges)
- Missing value handling
- Encoding (one-hot, label, ordinal)
- Transformation tracking
- Method chaining
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from schema_mapper import PreProcessor

# Create sample dataset with various data quality issues
np.random.seed(42)

print("=" * 80)
print("SCHEMA-MAPPER PREPROCESSOR DEMO")
print("=" * 80)

# ========================================
# SAMPLE DATA WITH QUALITY ISSUES
# ========================================

data = {
    # Column with bad naming (spaces, special chars)
    'User ID#': range(1, 21),

    # Names with whitespace issues
    'Full Name': [
        '  John Doe  ', 'Jane Smith', ' Bob Johnson ', 'ALICE WILSON',
        'Charlie Brown', '  Diana Prince  ', 'Eve Adams', 'Frank Miller',
        'Grace Lee', 'Henry Ford', 'Irene Chen', 'Jack Ryan',
        'Karen White', 'Leo King', 'Mary Queen', 'Nick Fury',
        'Olivia Pope', 'Peter Parker', 'Quinn Hart', 'Rita Skeeter'
    ],

    # Emails with issues
    'Email Address': [
        'JOHN@EXAMPLE.COM', 'jane@test.com', '  bob@demo.com  ', 'alice@test.com',
        'charlie@demo.com', 'invalid-email', 'eve@example.com', 'frank@test.com',
        'grace@demo.com', 'henry@test.com', 'irene@example.com', 'jack@test.com',
        'karen@demo.com', 'leo@test.com', 'mary@example.com', None,
        'olivia@demo.com', 'peter@test.com', 'quinn@example.com', 'rita@test.com'
    ],

    # Phone numbers in various formats
    'Phone Number': [
        '555-123-4567', '(555) 234-5678', '5553456789', '+1-555-456-7890',
        '555.567.8901', '555-678-9012', None, '555-890-1234',
        '(555) 901-2345', '5550123456', '555-123-4567', '555-234-5678',
        '555-345-6789', '555-456-7890', '555-567-8901', '555-678-9012',
        '555-789-0123', '555-890-1234', '555-901-2345', '555-012-3456'
    ],

    # Dates in mixed formats
    'Sign-up Date': [
        '2024-01-01', '01/15/2024', '2024-02-01', '02/20/2024',
        '2024-03-01', '03/15/2024', '2024-04-01', '04/20/2024',
        '2024-05-01', '05/15/2024', '2024-06-01', '06/20/2024',
        '2024-07-01', '07/15/2024', '2024-08-01', '08/20/2024',
        '2024-09-01', '09/15/2024', '2024-10-01', '10/20/2024'
    ],

    # Currency values
    'Account Balance': [
        '$1,234.56', '$2,345.67', '$3,456.78', '$4,567.89',
        '$5,678.90', '$6,789.01', '$7,890.12', '$8,901.23',
        '$9,012.34', '$10,123.45', '$11,234.56', '$12,345.67',
        '$13,456.78', '$14,567.89', '$15,678.90', '$16,789.01',
        '$17,890.12', '$18,901.23', '$19,012.34', '$20,123.45'
    ],

    # Age with missing values and outliers
    'Age': [
        25, 30, 35, None, 28, 32, 150, 29, 31, 27,
        33, 26, None, 34, 29, 31, 28, 30, 32, 26
    ],

    # Status with inconsistent case
    'Account Status': [
        'active', 'ACTIVE', 'inactive', 'Active', 'INACTIVE',
        'active', 'Active', 'inactive', 'ACTIVE', 'active',
        'inactive', 'Active', 'active', 'INACTIVE', 'active',
        'Active', 'inactive', 'active', 'ACTIVE', 'active'
    ],

    # Category for encoding
    'Membership': [
        'Bronze', 'Silver', 'Gold', 'Silver', 'Bronze',
        'Gold', 'Silver', 'Bronze', 'Gold', 'Silver',
        'Bronze', 'Gold', 'Silver', 'Bronze', 'Gold',
        'Silver', 'Bronze', 'Gold', 'Silver', 'Bronze'
    ],

    # Region for one-hot encoding
    'Region': [
        'North', 'South', 'East', 'West', 'North',
        'South', 'East', 'West', 'North', 'South',
        'East', 'West', 'North', 'South', 'East',
        'West', 'North', 'South', 'East', 'West'
    ]
}

df = pd.DataFrame(data)

# Add some duplicate rows
df = pd.concat([df, df.iloc[:2]], ignore_index=True)

print(f"\nOriginal DataFrame shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print("\nFirst 3 rows:")
print(df.head(3))

# ========================================
# 1. INITIALIZE PREPROCESSOR
# ========================================

print("\n" + "=" * 80)
print("1. INITIALIZING PREPROCESSOR")
print("=" * 80)

preprocessor = PreProcessor(df)
print(f"[OK] PreProcessor initialized with {df.shape[0]} rows, {df.shape[1]} columns")

# ========================================
# 2. FIX COLUMN NAMES (SQL STANDARDS)
# ========================================

print("\n" + "=" * 80)
print("2. STANDARDIZING COLUMN NAMES (SQL)")
print("=" * 80)

print(f"\nOriginal column names:")
for col in preprocessor.df.columns:
    print(f"  - '{col}'")

preprocessor.apply_sql_naming_rules()

print(f"\nStandardized column names:")
for col in preprocessor.df.columns:
    print(f"  - '{col}'")

# ========================================
# 3. FIX WHITESPACE ISSUES
# ========================================

print("\n" + "=" * 80)
print("3. FIXING WHITESPACE")
print("=" * 80)

print("\nBefore whitespace fix:")
print(preprocessor.df['full_name'].head(3).tolist())

preprocessor.fix_whitespace(strategy='normalize')

print("\nAfter whitespace fix:")
print(preprocessor.df['full_name'].head(3).tolist())

# ========================================
# 4. FIX CASE ISSUES
# ========================================

print("\n" + "=" * 80)
print("4. FIXING CASE")
print("=" * 80)

print("\nBefore case fix (account_status):")
print(preprocessor.df['account_status'].value_counts())

preprocessor.fix_case(['account_status'], case='lower')

print("\nAfter case fix (account_status):")
print(preprocessor.df['account_status'].value_counts())

# ========================================
# 5. STANDARDIZE DATES
# ========================================

print("\n" + "=" * 80)
print("5. STANDARDIZING DATES")
print("=" * 80)

print("\nBefore date standardization:")
print(preprocessor.df['sign_up_date'].head(5).tolist())

preprocessor.standardize_dates('sign_up_date')

print("\nAfter date standardization (ISO8601):")
print(preprocessor.df['sign_up_date'].head(5))

# ========================================
# 6. STANDARDIZE PHONE NUMBERS
# ========================================

print("\n" + "=" * 80)
print("6. STANDARDIZING PHONE NUMBERS")
print("=" * 80)

print("\nBefore phone standardization:")
print(preprocessor.df['phone_number'].head(5).tolist())

preprocessor.standardize_phone_numbers('phone_number', country='US')

print("\nAfter phone standardization:")
print(preprocessor.df['phone_number'].head(5).tolist())

# ========================================
# 7. STANDARDIZE CURRENCY
# ========================================

print("\n" + "=" * 80)
print("7. STANDARDIZING CURRENCY")
print("=" * 80)

print("\nBefore currency standardization:")
print(f"  Type: {preprocessor.df['account_balance'].dtype}")
print(f"  Sample: {preprocessor.df['account_balance'].head(3).tolist()}")

preprocessor.standardize_currency('account_balance')

print("\nAfter currency standardization:")
print(f"  Type: {preprocessor.df['account_balance'].dtype}")
print(f"  Sample: {preprocessor.df['account_balance'].head(3).tolist()}")

# ========================================
# 8. VALIDATE EMAILS
# ========================================

print("\n" + "=" * 80)
print("8. VALIDATING EMAILS")
print("=" * 80)

email_validation = preprocessor.validate_emails('email_address', fix=True)
print(f"\nEmail Validation Results:")
print(f"  Valid: {email_validation['valid_count']}")
print(f"  Invalid: {email_validation['invalid_count']}")
print(f"  Validity: {email_validation['validity_percentage']:.1f}%")

if email_validation['invalid_indices']:
    print(f"\nInvalid email indices: {email_validation['invalid_indices']}")

# ========================================
# 9. VALIDATE RANGES
# ========================================

print("\n" + "=" * 80)
print("9. VALIDATING AGE RANGES")
print("=" * 80)

age_validation = preprocessor.validate_ranges('age', min_val=18, max_val=120)
print(f"\nAge Range Validation (18-120):")
print(f"  Valid: {age_validation['valid_count']}")
print(f"  Invalid: {age_validation['invalid_count']}")
print(f"  Validity: {age_validation['validity_percentage']:.1f}%")

if age_validation['invalid_indices']:
    print(f"\nInvalid ages at indices: {age_validation['invalid_indices']}")
    print(f"Values: {preprocessor.df.loc[age_validation['invalid_indices'], 'age'].tolist()}")

# ========================================
# 10. HANDLE MISSING VALUES
# ========================================

print("\n" + "=" * 80)
print("10. HANDLING MISSING VALUES")
print("=" * 80)

print(f"\nMissing values before:")
missing_before = preprocessor.df.isna().sum()
print(missing_before[missing_before > 0])

# Handle missing ages with median
preprocessor.handle_missing_values(strategy='median', columns=['age'])

print(f"\nMissing values after (median imputation):")
missing_after = preprocessor.df.isna().sum()
print(missing_after[missing_after > 0])

# ========================================
# 11. DETECT AND REMOVE DUPLICATES
# ========================================

print("\n" + "=" * 80)
print("11. DETECTING AND REMOVING DUPLICATES")
print("=" * 80)

dup_report = preprocessor.detect_duplicates()
print(f"\nDuplicate Detection:")
print(f"  Total duplicates: {dup_report['total_duplicates']}")
print(f"  Duplicate percentage: {dup_report['duplicate_percentage']:.2f}%")
print(f"  Unique rows: {dup_report['unique_rows']}")

if dup_report['total_duplicates'] > 0:
    preprocessor.remove_duplicates(keep='first')
    print(f"\n[OK] Removed {dup_report['total_duplicates']} duplicate rows")
    print(f"  New shape: {preprocessor.df.shape}")

# ========================================
# 12. ENCODING - ORDINAL
# ========================================

print("\n" + "=" * 80)
print("12. ORDINAL ENCODING (Membership)")
print("=" * 80)

print("\nBefore ordinal encoding:")
print(preprocessor.df['membership'].value_counts().sort_index())

# Ordinal encode membership (Bronze < Silver < Gold)
preprocessor.ordinal_encode('membership', ordering=['Bronze', 'Silver', 'Gold'])

print("\nAfter ordinal encoding:")
print(preprocessor.df['membership'].value_counts().sort_index())
print(f"  0=Bronze, 1=Silver, 2=Gold")

# ========================================
# 13. ENCODING - ONE-HOT
# ========================================

print("\n" + "=" * 80)
print("13. ONE-HOT ENCODING (Region)")
print("=" * 80)

print(f"\nBefore one-hot encoding:")
print(f"  Shape: {preprocessor.df.shape}")
print(f"  Region values: {preprocessor.df['region'].unique()}")

# Store columns before encoding
cols_before = preprocessor.df.columns.tolist()

preprocessor.one_hot_encode(['region'], prefix='region')

print(f"\nAfter one-hot encoding:")
print(f"  Shape: {preprocessor.df.shape}")
new_cols = [col for col in preprocessor.df.columns if col not in cols_before and col.startswith('region_')]
print(f"  New columns: {new_cols}")

# ========================================
# 14. TRANSFORMATION LOG
# ========================================

print("\n" + "=" * 80)
print("14. TRANSFORMATION LOG")
print("=" * 80)

log = preprocessor.get_transformation_log()
print(f"\nTotal transformations applied: {len(log)}")
print("\nTransformation history:")
for i, entry in enumerate(log, 1):
    shape_info = f"{entry.get('shape_before', 'N/A')} -> {entry.get('shape_after', 'N/A')}"
    print(f"  {i:2d}. {entry['operation']:30s} | Shape: {shape_info}")

# ========================================
# 15. FINAL RESULT
# ========================================

print("\n" + "=" * 80)
print("15. FINAL CLEANED DATAFRAME")
print("=" * 80)

df_clean = preprocessor.apply()

print(f"\nFinal shape: {df_clean.shape}")
print(f"Columns ({len(df_clean.columns)}):")
for col in df_clean.columns:
    print(f"  - {col:30s} | {df_clean[col].dtype}")

print(f"\nData quality summary:")
print(f"  Total rows: {len(df_clean)}")
print(f"  Total columns: {len(df_clean.columns)}")
print(f"  Missing values: {df_clean.isna().sum().sum()}")
print(f"  Duplicate rows: {df_clean.duplicated().sum()}")
print(f"  Memory usage: {df_clean.memory_usage(deep=True).sum() / 1024:.2f} KB")

print("\n" + "=" * 80)
print("DEMO COMPLETE - ALL PREPROCESSING FEATURES DEMONSTRATED!")
print("=" * 80)

# ========================================
# 16. METHOD CHAINING EXAMPLE
# ========================================

print("\n" + "=" * 80)
print("16. BONUS: METHOD CHAINING EXAMPLE")
print("=" * 80)

# Create new preprocessor for chaining demo
df_chain = pd.DataFrame({
    'Name': ['  Alice  ', '  Bob  ', ' Charlie '],
    'Email': ['ALICE@TEST.COM', 'bob@TEST.com', 'charlie@test.com'],
    'Age': [25, None, 30],
    'Status': ['ACTIVE', 'active', 'INACTIVE']
})

print("\nOriginal data:")
print(df_chain)

# Chain multiple operations
df_result = (PreProcessor(df_chain)
    .fix_whitespace()
    .fix_case(['Email', 'Status'], case='lower')
    .handle_missing_values(strategy='median')
    .apply_sql_naming_rules()
    .apply()
)

print("\nAfter chained preprocessing:")
print(df_result)

print("\n[OK] Method chaining allows clean, readable preprocessing pipelines!")

# ========================================
# SUMMARY
# ========================================

print("\n" + "=" * 80)
print("SUMMARY OF CAPABILITIES DEMONSTRATED")
print("=" * 80)

capabilities = [
    "[OK] SQL naming conventions (apply_sql_naming_rules)",
    "[OK] Whitespace normalization (fix_whitespace)",
    "[OK] Case standardization (fix_case)",
    "[OK] Date standardization (standardize_dates)",
    "[OK] Phone number formatting (standardize_phone_numbers)",
    "[OK] Currency parsing (standardize_currency)",
    "[OK] Email validation (validate_emails)",
    "[OK] Range validation (validate_ranges)",
    "[OK] Missing value imputation (handle_missing_values)",
    "[OK] Duplicate detection & removal (detect/remove_duplicates)",
    "[OK] Ordinal encoding (ordinal_encode)",
    "[OK] One-hot encoding (one_hot_encode)",
    "[OK] Transformation logging (get_transformation_log)",
    "[OK] Method chaining support",
    "[OK] Copy vs in-place operations"
]

print("\nFeatures demonstrated:")
for cap in capabilities:
    print(f"  {cap}")

print(f"\nThe PreProcessor provides a complete data cleaning pipeline!")
print("Ready for production use with comprehensive transformation tracking.\n")
