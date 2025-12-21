"""
Profiler Demo - Comprehensive data profiling and quality analysis

This example demonstrates the Profiler class capabilities:
- Statistical profiling
- Quality assessment
- Anomaly detection
- Pattern recognition
- Visualization
- Report generation
"""

import pandas as pd
import numpy as np
from schema_mapper import Profiler

# Create sample dataset with various data quality issues
np.random.seed(42)

data = {
    # Clean numeric column
    'customer_id': range(1, 101),

    # Numeric with outliers
    'age': [25, 30, 35, 28, 32, 150, 29, 31, 27, 33] * 10,  # 150 is outlier

    # Numeric with skewed distribution
    'purchase_amount': np.random.exponential(scale=50, size=100),

    # Column with missing values
    'email': ['user{}@example.com'.format(i) if i % 4 != 0 else None for i in range(100)],

    # Phone numbers
    'phone': ['555-{:04d}'.format(i) for i in range(100)],

    # Mixed type column (data quality issue)
    'account_status': ['active'] * 80 + [1, 2, 3, 4, 5] * 4,

    # High cardinality
    'session_id': [f'sess_{i}_{np.random.randint(1000, 9999)}' for i in range(100)],

    # Low cardinality (categorical)
    'region': np.random.choice(['North', 'South', 'East', 'West'], 100),

    # Boolean-like strings
    'is_premium': np.random.choice(['yes', 'no'], 100),

    # Dates as strings
    'signup_date': pd.date_range('2024-01-01', periods=100).astype(str),

    # Currency values
    'balance': [f'${amt:.2f}' for amt in np.random.uniform(100, 5000, 100)],
}

df = pd.DataFrame(data)

# Add some duplicate rows
df = pd.concat([df, df.iloc[:5]], ignore_index=True)

print("=" * 80)
print("SCHEMA-MAPPER PROFILER DEMO")
print("=" * 80)

# Initialize Profiler
profiler = Profiler(df, name="customer_data")

print(f"\nDataset: {profiler.name}")
print(f"Shape: {df.shape}")
print()

# ========================================
# 1. DATASET OVERVIEW
# ========================================
print("=" * 80)
print("1. DATASET OVERVIEW")
print("=" * 80)

dataset_profile = profiler.profile_dataset()
print(f"\nRows: {dataset_profile['n_rows']:,}")
print(f"Columns: {dataset_profile['n_columns']}")
print(f"Memory Usage: {dataset_profile['memory_usage_mb']:.2f} MB")
print(f"Overall Quality Score: {dataset_profile['quality_score']}/100")

print("\nColumn Types:")
for dtype, count in dataset_profile['column_types'].items():
    print(f"  {dtype}: {count} columns")

# ========================================
# 2. SUMMARY STATISTICS
# ========================================
print("\n" + "=" * 80)
print("2. SUMMARY STATISTICS")
print("=" * 80)

summary = profiler.get_summary_stats()
print("\nExtended Summary Statistics:")
print(summary[['count', 'null_count', 'null_pct', 'unique_count', 'unique_pct']].head(10))

# ========================================
# 3. QUALITY ASSESSMENT
# ========================================
print("\n" + "=" * 80)
print("3. QUALITY ASSESSMENT")
print("=" * 80)

quality = profiler.assess_quality()
print(f"\nOverall Quality Score: {quality['overall_score']}/100")
print(f"Interpretation: {quality['interpretation']}")
print(f"\nBreakdown:")
print(f"  Completeness: {quality['completeness_score']:.2f}/100")
print(f"  Uniqueness: {quality['uniqueness_score']:.2f}/100")
print(f"  Validity: {quality['validity_score']:.2f}/100")
print(f"  Consistency: {quality['consistency_score']:.2f}/100")

# ========================================
# 4. MISSING VALUES ANALYSIS
# ========================================
print("\n" + "=" * 80)
print("4. MISSING VALUES ANALYSIS")
print("=" * 80)

missing = profiler.analyze_missing_values()
print(f"\nTotal Missing Values: {missing['total_missing']} ({missing['total_missing_percentage']:.2f}%)")

if missing['columns_with_missing']:
    print("\nColumns with Missing Values:")
    for col, count in missing['columns_with_missing'].items():
        pct = missing['missing_percentages'][col]
        print(f"  {col}: {count} missing ({pct:.1f}%)")

print(f"\nComplete Columns: {len(missing['complete_columns'])}")

# ========================================
# 5. ANOMALY DETECTION
# ========================================
print("\n" + "=" * 80)
print("5. ANOMALY DETECTION")
print("=" * 80)

# IQR method
print("\nIQR Method (threshold=1.5):")
anomalies_iqr = profiler.detect_anomalies(method='iqr', threshold=1.5)
for col, info in anomalies_iqr.items():
    print(f"\n  {col}:")
    print(f"    Outliers: {info['count']} ({info['percentage']:.2f}%)")
    print(f"    Sample values: {info['values'][:5]}")

# Z-score method
print("\nZ-Score Method (threshold=3.0):")
anomalies_zscore = profiler.detect_anomalies(method='zscore', threshold=3.0)
for col, info in anomalies_zscore.items():
    print(f"  {col}: {info['count']} outliers")

# ========================================
# 6. CARDINALITY ANALYSIS
# ========================================
print("\n" + "=" * 80)
print("6. CARDINALITY ANALYSIS")
print("=" * 80)

cardinality = profiler.analyze_cardinality()
print(f"\nHigh Cardinality Columns: {cardinality['high_cardinality_columns']}")
print(f"Medium Cardinality Columns: {cardinality['medium_cardinality_columns']}")
print(f"Low Cardinality Columns: {cardinality['low_cardinality_columns']}")

print("\nLow Cardinality Details (showing top values):")
for col in cardinality['low_cardinality_columns'][:3]:
    details = cardinality['details'][col]
    print(f"\n  {col}:")
    print(f"    Unique: {details['unique_count']} ({details['unique_percentage']:.1f}%)")
    if 'top_values' in details:
        print(f"    Top values: {dict(list(details['top_values'].items())[:3])}")

# ========================================
# 7. DUPLICATE DETECTION
# ========================================
print("\n" + "=" * 80)
print("7. DUPLICATE DETECTION")
print("=" * 80)

duplicates = profiler.detect_duplicates()
print(f"\nDuplicate Rows: {duplicates['count']} ({duplicates['percentage']:.2f}%)")
if duplicates['count'] > 0:
    print(f"Duplicate Indices: {duplicates['indices'][:10]}")

# ========================================
# 8. PATTERN DETECTION
# ========================================
print("\n" + "=" * 80)
print("8. PATTERN DETECTION")
print("=" * 80)

patterns = profiler.detect_patterns()
print("\nDetected Patterns:")
if patterns['email_columns']:
    print(f"  Email Columns: {patterns['email_columns']}")
if patterns['phone_columns']:
    print(f"  Phone Columns: {patterns['phone_columns']}")
if patterns['date_string_columns']:
    print(f"  Date String Columns: {patterns['date_string_columns']}")
if patterns['currency_columns']:
    print(f"  Currency Columns: {patterns['currency_columns']}")

print("\nPattern Match Percentages:")
for col in ['email', 'phone', 'balance']:
    if col in patterns['details']:
        detail = patterns['details'][col]
        print(f"  {col}:")
        for pattern_type, pct in detail.items():
            if pct > 50:
                print(f"    {pattern_type}: {pct:.1f}%")

# ========================================
# 9. DISTRIBUTION ANALYSIS
# ========================================
print("\n" + "=" * 80)
print("9. DISTRIBUTION ANALYSIS")
print("=" * 80)

distributions = profiler.analyze_distributions()
print("\nNumeric Column Distributions:")
for col, info in distributions.items():
    print(f"\n  {col}:")
    print(f"    Type: {info['distribution_type']}")
    print(f"    Skewness: {info['skewness']:.3f}")
    print(f"    Kurtosis: {info['kurtosis']:.3f}")
    print(f"    Mean: {info['mean']:.2f}, Median: {info['median']:.2f}")

# ========================================
# 10. CORRELATION ANALYSIS
# ========================================
print("\n" + "=" * 80)
print("10. CORRELATION ANALYSIS")
print("=" * 80)

correlations = profiler.find_correlations(threshold=0.5)
if len(correlations) > 0:
    print("\nHighly Correlated Pairs (|r| > 0.5):")
    print(correlations)
else:
    print("\nNo highly correlated pairs found (threshold=0.5)")

# ========================================
# 11. COLUMN-LEVEL PROFILING
# ========================================
print("\n" + "=" * 80)
print("11. DETAILED COLUMN PROFILING")
print("=" * 80)

print("\nAge Column (with outliers):")
age_profile = profiler.profile_column('age')
for key, value in age_profile.items():
    if key not in ['column']:
        print(f"  {key}: {value}")

print("\nRegion Column (categorical):")
region_profile = profiler.profile_column('region')
for key, value in region_profile.items():
    if key not in ['column'] and not isinstance(value, dict):
        print(f"  {key}: {value}")

# ========================================
# 12. COMPREHENSIVE REPORT
# ========================================
print("\n" + "=" * 80)
print("12. COMPREHENSIVE REPORT GENERATION")
print("=" * 80)

# Generate dict report
report = profiler.generate_report(output_format='dict')
print(f"\nReport generated with {len(report)} sections")
print(f"Sections: {list(report.keys())}")

# Export as JSON
json_report = profiler.generate_report(output_format='json')
print(f"\nJSON report size: {len(json_report)} characters")

# Optionally save reports
# with open('profiler_report.json', 'w') as f:
#     f.write(json_report)

# html_report = profiler.generate_report(output_format='html')
# with open('profiler_report.html', 'w') as f:
#     f.write(html_report)

# ========================================
# 13. VISUALIZATION (Optional - requires matplotlib/seaborn)
# ========================================
print("\n" + "=" * 80)
print("13. VISUALIZATION (Optional)")
print("=" * 80)

try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend

    print("\nGenerating visualizations...")

    # Distribution plots
    fig1 = profiler.plot_distributions(columns=['age', 'purchase_amount'])
    if fig1:
        fig1.savefig('distributions.png', dpi=100, bbox_inches='tight')
        print("  ✓ Saved distributions.png")

    # Correlation heatmap
    fig2 = profiler.plot_correlations()
    if fig2:
        fig2.savefig('correlations.png', dpi=100, bbox_inches='tight')
        print("  ✓ Saved correlations.png")

    # Missing values
    fig3 = profiler.plot_missing_values()
    if fig3:
        fig3.savefig('missing_values.png', dpi=100, bbox_inches='tight')
        print("  ✓ Saved missing_values.png")

    # Outliers
    fig4 = profiler.plot_outliers(columns=['age', 'purchase_amount'])
    if fig4:
        fig4.savefig('outliers.png', dpi=100, bbox_inches='tight')
        print("  ✓ Saved outliers.png")

    print("\n  Visualization files saved successfully!")

except ImportError:
    print("\n  Visualization skipped (requires: pip install matplotlib seaborn)")
except Exception as e:
    print(f"\n  Visualization error: {e}")

# ========================================
# SUMMARY
# ========================================
print("\n" + "=" * 80)
print("PROFILER DEMO COMPLETE")
print("=" * 80)

print(f"""
Summary of Capabilities Demonstrated:
  ✓ Dataset profiling ({dataset_profile['n_rows']} rows, {dataset_profile['n_columns']} columns)
  ✓ Quality assessment (Score: {quality['overall_score']}/100)
  ✓ Missing value analysis ({missing['total_missing']} missing values)
  ✓ Anomaly detection ({len(anomalies_iqr)} columns with outliers)
  ✓ Cardinality analysis
  ✓ Duplicate detection ({duplicates['count']} duplicates)
  ✓ Pattern recognition (emails, phones, dates, currency)
  ✓ Distribution analysis ({len(distributions)} numeric columns)
  ✓ Correlation analysis
  ✓ Column-level profiling
  ✓ Report generation (dict, json, html)
  ✓ Visualization (matplotlib/seaborn)

The Profiler provides comprehensive data intelligence in a simple, powerful API!
""")
