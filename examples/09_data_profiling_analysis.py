"""
Example 09: Data Profiling and Statistical Analysis

This example demonstrates comprehensive data profiling capabilities including:
- Statistical profiling and quality assessment
- Distribution analysis and visualization
- Correlation analysis
- Missing value and outlier detection
- Pattern recognition
- Report generation

Use Case:
- Exploratory Data Analysis (EDA)
- Data quality assessment before migration
- Understanding dataset characteristics
- Identifying data issues early
"""

import pandas as pd
import numpy as np
from schema_mapper.profiler import Profiler
from schema_mapper.visualization import DataVisualizer

# ============================================================================
# SETUP: Create Sample E-commerce Dataset
# ============================================================================

print("="*80)
print("DATA PROFILING AND STATISTICAL ANALYSIS EXAMPLE")
print("="*80)

# Generate realistic e-commerce data
np.random.seed(42)
n_customers = 5000

# Create customer data with intentional quality issues
data = {
    # Customer demographics
    'customer_id': range(1, n_customers + 1),
    'age': np.random.randint(18, 85, n_customers),
    'annual_income': np.random.normal(55000, 25000, n_customers),
    'credit_score': np.random.randint(300, 850, n_customers),

    # Purchase behavior
    'total_purchases': np.random.poisson(12, n_customers),
    'avg_order_value': np.random.gamma(2, 50, n_customers),
    'days_since_last_purchase': np.random.exponential(30, n_customers),
    'loyalty_points': np.random.randint(0, 10000, n_customers),

    # Account info
    'account_age_months': np.random.randint(1, 120, n_customers),
    'email_verified': np.random.choice([True, False], n_customers, p=[0.85, 0.15]),

    # Categorical
    'membership_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'],
                                        n_customers, p=[0.5, 0.3, 0.15, 0.05]),
    'preferred_category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Books', 'Sports'],
                                           n_customers),
}

df = pd.DataFrame(data)

# Add some data quality issues for profiling to catch
# 1. Missing values (5% in income)
missing_mask = np.random.random(n_customers) < 0.05
df.loc[missing_mask, 'annual_income'] = np.nan

# 2. Missing values (10% in days_since_last_purchase)
missing_mask = np.random.random(n_customers) < 0.10
df.loc[missing_mask, 'days_since_last_purchase'] = np.nan

# 3. Outliers (unrealistic ages)
outlier_indices = np.random.choice(n_customers, 50, replace=False)
df.loc[outlier_indices, 'age'] = np.random.randint(100, 150, 50)

# 4. Add some duplicates
duplicate_indices = np.random.choice(n_customers, 100, replace=False)
df = pd.concat([df, df.iloc[duplicate_indices]], ignore_index=True)

# 5. Add email pattern column
df['email'] = df['customer_id'].apply(lambda x: f'customer{x}@example.com' if x % 20 != 0 else f'invalid_email_{x}')

print(f"\nCreated dataset: {len(df)} rows, {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}\n")

# ============================================================================
# PART 1: BASIC PROFILING
# ============================================================================

print("\n" + "="*80)
print("PART 1: BASIC STATISTICAL PROFILING")
print("="*80)

# Initialize profiler
profiler = Profiler(df, name='ecommerce_customers', show_progress=True)

# Get summary statistics
print("\n1. Summary Statistics")
print("-" * 80)
stats = profiler.get_summary_stats()
print(f"Dataset: {stats['dataset_name']}")
print(f"Rows: {stats['n_rows']:,}")
print(f"Columns: {stats['n_columns']}")
print(f"Memory: {stats['memory_usage_mb']:.2f} MB")
print(f"Duplicate Rows: {stats['duplicate_rows']}")

# Profile individual column
print("\n2. Column-Level Profiling")
print("-" * 80)
age_profile = profiler.profile_column('age')
print(f"\nAge Statistics:")
print(f"  Mean: {age_profile['mean']:.1f}")
print(f"  Median: {age_profile['median']:.1f}")
print(f"  Std Dev: {age_profile['std']:.1f}")
print(f"  Min: {age_profile['min']:.1f}")
print(f"  Max: {age_profile['max']:.1f}")
print(f"  Skewness: {age_profile['skewness']:.2f}")
print(f"  Kurtosis: {age_profile['kurtosis']:.2f}")

# ============================================================================
# PART 2: DATA QUALITY ASSESSMENT
# ============================================================================

print("\n" + "="*80)
print("PART 2: DATA QUALITY ASSESSMENT")
print("="*80)

# Overall quality score
print("\n1. Overall Quality Score")
print("-" * 80)
quality = profiler.assess_quality()
print(f"Quality Score: {quality['overall_score']:.1f}/100")
print(f"Interpretation: {quality['interpretation']}")
print(f"\nComponent Scores:")
print(f"  Completeness: {quality['completeness_score']:.1f}")
print(f"  Uniqueness: {quality['uniqueness_score']:.1f}")
print(f"  Validity: {quality['validity_score']:.1f}")
print(f"  Consistency: {quality['consistency_score']:.1f}")

# Missing value analysis
print("\n2. Missing Value Analysis")
print("-" * 80)
missing = profiler.analyze_missing_values()
print(f"Total Missing: {missing['total_missing']:,} ({missing['total_missing_percentage']:.2f}%)")
print(f"Columns with Missing Values: {len(missing['columns_with_missing'])}")
if missing['high_missing_columns']:
    print(f"High Missing (>50%): {missing['high_missing_columns']}")

print("\nMissing by Column:")
for col, pct in list(missing['missing_percentages'].items())[:5]:
    print(f"  {col}: {pct:.2f}%")

# Duplicate detection
print("\n3. Duplicate Detection")
print("-" * 80)
duplicates = profiler.detect_duplicates()
print(f"Duplicate Rows: {duplicates['count']} ({duplicates['percentage']:.2f}%)")
print(f"Unique Rows: {duplicates['unique_count']:,}")

# ============================================================================
# PART 3: DISTRIBUTION AND CORRELATION ANALYSIS
# ============================================================================

print("\n" + "="*80)
print("PART 3: DISTRIBUTION AND CORRELATION ANALYSIS")
print("="*80)

# Distribution analysis
print("\n1. Distribution Analysis")
print("-" * 80)
distributions = profiler.analyze_distributions()
for col, dist_info in list(distributions.items())[:3]:
    print(f"\n{col}:")
    print(f"  Type: {dist_info['distribution_type']}")
    print(f"  Skewness: {dist_info['skewness']:.2f}")
    print(f"  Kurtosis: {dist_info['kurtosis']:.2f}")

# Cardinality analysis
print("\n2. Cardinality Analysis")
print("-" * 80)
cardinality = profiler.analyze_cardinality()
print(f"High Cardinality Columns: {cardinality['high_cardinality']}")
print(f"Medium Cardinality Columns: {cardinality['medium_cardinality']}")
print(f"Low Cardinality Columns: {cardinality['low_cardinality']}")

# Correlation analysis
print("\n3. Correlation Analysis")
print("-" * 80)
high_corr = profiler.find_correlations(threshold=0.5, method='pearson')
print(f"Found {len(high_corr)} highly correlated pairs (r >= 0.5):\n")
if len(high_corr) > 0:
    print(high_corr.head(10).to_string(index=False))

# ============================================================================
# PART 4: ANOMALY AND PATTERN DETECTION
# ============================================================================

print("\n" + "="*80)
print("PART 4: ANOMALY AND PATTERN DETECTION")
print("="*80)

# Detect outliers
print("\n1. Outlier Detection (IQR Method)")
print("-" * 80)
outliers = profiler.detect_anomalies(method='iqr', threshold=1.5)
for col, indices in list(outliers.items())[:3]:
    if len(indices) > 0:
        print(f"{col}: {len(indices)} outliers detected")

# Pattern recognition
print("\n2. Pattern Recognition")
print("-" * 80)
patterns = profiler.detect_patterns()
print(f"Email Columns: {patterns['email_columns']}")
print(f"Phone Columns: {patterns['phone_columns']}")
print(f"URL Columns: {patterns['url_columns']}")
print(f"Date String Columns: {patterns['date_string_columns']}")

# ============================================================================
# PART 5: VISUALIZATION
# ============================================================================

print("\n" + "="*80)
print("PART 5: VISUALIZATION EXAMPLES")
print("="*80)

print("\nGenerating visualizations...")

# Note: In a real environment, these would display plots
# For this example, we're just demonstrating the API

# 1. Distribution plots
print("  1. Creating distribution histograms...")
# fig1 = profiler.plot_distributions(
#     columns=['age', 'annual_income', 'avg_order_value'],
#     color='#34495e',
#     kde_color='#e74c3c'
# )
# fig1.savefig('examples/output/distributions.png', dpi=300, bbox_inches='tight')

# 2. Correlation heatmap
print("  2. Creating correlation matrix...")
# fig2 = profiler.plot_correlations(method='pearson')
# fig2.savefig('examples/output/correlations.png', dpi=300, bbox_inches='tight')

# 3. Missing values visualization
print("  3. Creating missing values plot...")
# fig3 = profiler.plot_missing_values()
# fig3.savefig('examples/output/missing_values.png', dpi=300, bbox_inches='tight')

# 4. Outlier detection (box plots)
print("  4. Creating outlier box plots...")
# fig4 = profiler.plot_outliers(columns=['age', 'annual_income', 'credit_score'])
# fig4.savefig('examples/output/outliers.png', dpi=300, bbox_inches='tight')

# 5. Scatter matrix
print("  5. Creating scatter plot matrix...")
# fig5 = profiler.plot_scatter_matrix(
#     columns=['age', 'annual_income', 'total_purchases', 'avg_order_value'],
#     color='#34495e',
#     diagonal='kde'
# )
# fig5.savefig('examples/output/scatter_matrix.png', dpi=300, bbox_inches='tight')

print("\n(Visualization code commented out - uncomment to generate plots)")

# ============================================================================
# PART 6: COMPREHENSIVE REPORTING
# ============================================================================

print("\n" + "="*80)
print("PART 6: REPORT GENERATION")
print("="*80)

# Generate comprehensive report
print("\n1. Generating Comprehensive Report")
print("-" * 80)
report = profiler.generate_report(output_format='dict')

print("\nReport Sections:")
print(f"  - Dataset: {len(report['dataset'])} metrics")
print(f"  - Quality: {len(report['quality'])} metrics")
print(f"  - Missing Values: {len(report['missing_values'])} metrics")
print(f"  - Cardinality: {len(report['cardinality'])} categories")
print(f"  - Duplicates: {len(report['duplicates'])} metrics")
print(f"  - Patterns: {len(report['patterns'])} pattern types")
print(f"  - Distributions: {len(report['distributions'])} columns analyzed")

# Export as JSON
print("\n2. Exporting Report")
print("-" * 80)
json_report = profiler.generate_report(output_format='json')
# with open('examples/output/profiling_report.json', 'w') as f:
#     f.write(json_report)
print("Report can be exported to JSON format")

# Export as HTML
html_report = profiler.generate_report(output_format='html')
# with open('examples/output/profiling_report.html', 'w') as f:
#     f.write(html_report)
print("Report can be exported to HTML format")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("PROFILING SUMMARY")
print("="*80)

print(f"""
Dataset: {profiler.name}
Rows: {len(df):,}
Columns: {len(df.columns)}

Quality Score: {quality['overall_score']:.1f}/100
Interpretation: {quality['interpretation']}

Key Findings:
  - Missing Values: {missing['total_missing']:,} cells ({missing['total_missing_percentage']:.2f}%)
  - Duplicate Rows: {duplicates['count']} ({duplicates['percentage']:.2f}%)
  - High Cardinality Columns: {len(cardinality['high_cardinality'])}
  - Detected Patterns: {len([p for p in patterns.values() if p])} types

Recommendations:
  1. Address {len(missing['high_missing_columns'])} columns with >50% missing values
  2. Remove or investigate {duplicates['count']} duplicate rows
  3. Review and handle outliers in {len([v for v in outliers.values() if len(v) > 0])} columns
  4. Validate email pattern in 'email' column
""")

print("\n" + "="*80)
print("Example completed! Use these profiling capabilities to:")
print("  - Assess data quality before migration")
print("  - Identify data issues early in the pipeline")
print("  - Generate reports for stakeholders")
print("  - Understand dataset characteristics for ML")
print("="*80)
