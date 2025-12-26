"""
Example 10: Machine Learning Feature Engineering and Preprocessing

This example demonstrates ML-focused capabilities including:
- Target correlation analysis for feature importance
- Feature importance visualization
- Automatic categorical encoding
- Data preprocessing for ML pipelines
- Complete ML preparation workflow

Use Case:
- Preparing data for machine learning models
- Feature selection and importance analysis
- Automated feature engineering
- Classification and regression tasks
"""

import pandas as pd
import numpy as np
from schema_mapper.profiler import Profiler
from schema_mapper.preprocessor import PreProcessor

# ============================================================================
# SETUP: Create Sample Customer Churn Dataset
# ============================================================================

print("="*80)
print("MACHINE LEARNING FEATURE ENGINEERING EXAMPLE")
print("="*80)

# Generate realistic customer churn data
np.random.seed(42)
n_customers = 10000

print("\nGenerating customer churn dataset...")

# Create features
data = {
    # Numeric features
    'customer_id': range(1, n_customers + 1),
    'age': np.random.randint(18, 80, n_customers),
    'tenure_months': np.random.randint(1, 120, n_customers),
    'monthly_charges': np.random.uniform(20, 200, n_customers),
    'total_charges': np.random.uniform(100, 20000, n_customers),
    'contract_length_months': np.random.choice([1, 12, 24, 36], n_customers),
    'num_support_tickets': np.random.poisson(3, n_customers),
    'avg_call_duration': np.random.gamma(2, 5, n_customers),
    'data_usage_gb': np.random.lognormal(3, 1, n_customers),

    # Categorical features
    'gender': np.random.choice(['Male', 'Female'], n_customers),
    'contract_type': np.random.choice(['Month-to-month', 'One year', 'Two year'], n_customers),
    'payment_method': np.random.choice(['Electronic check', 'Mailed check', 'Bank transfer', 'Credit card'], n_customers, p=[0.4, 0.2, 0.2, 0.2]),
    'internet_service': np.random.choice(['DSL', 'Fiber optic', 'No'], n_customers, p=[0.3, 0.5, 0.2]),
    'online_security': np.random.choice(['Yes', 'No', 'No internet service'], n_customers),
    'tech_support': np.random.choice(['Yes', 'No', 'No internet service'], n_customers),
    'device_protection': np.random.choice(['Yes', 'No', 'No internet service'], n_customers),

    # Low cardinality numeric (should be encoded)
    'dependents': np.random.choice([0, 1, 2, 3, 4], n_customers, p=[0.4, 0.3, 0.2, 0.08, 0.02]),
    'lines': np.random.choice([1, 2, 3], n_customers, p=[0.6, 0.3, 0.1]),
}

df = pd.DataFrame(data)

# Create target variable with realistic dependencies
churn_probability = np.zeros(n_customers)

# Factors that increase churn
churn_probability += (df['tenure_months'] < 12) * 0.3  # New customers churn more
churn_probability += (df['monthly_charges'] > 150) * 0.25  # High prices increase churn
churn_probability += (df['contract_type'] == 'Month-to-month') * 0.3  # No commitment
churn_probability += (df['num_support_tickets'] > 5) * 0.2  # Support issues
churn_probability += (df['payment_method'] == 'Electronic check') * 0.15  # Payment issues

# Factors that decrease churn
churn_probability -= (df['tenure_months'] > 60) * 0.4  # Loyal customers
churn_probability -= (df['tech_support'] == 'Yes') * 0.15  # Has support
churn_probability -= (df['online_security'] == 'Yes') * 0.1  # Has security

# Clip to valid probability range
churn_probability = np.clip(churn_probability, 0.05, 0.95)

# Generate binary target
df['churn'] = np.random.binomial(1, churn_probability)
df['churn'] = df['churn'].map({0: 'No', 1: 'Yes'})

# Create a numeric target for regression example
df['customer_lifetime_value'] = (
    df['tenure_months'] * df['monthly_charges'] *
    (1 - churn_probability) * np.random.uniform(0.8, 1.2, n_customers)
)

print(f"Created dataset: {len(df)} rows, {len(df.columns)} columns")
print(f"Churn rate: {(df['churn'] == 'Yes').sum() / len(df) * 100:.1f}%")
print(f"Average CLV: ${df['customer_lifetime_value'].mean():.2f}\n")

# ============================================================================
# PART 1: CLASSIFICATION - FEATURE IMPORTANCE FOR CHURN PREDICTION
# ============================================================================

print("\n" + "="*80)
print("PART 1: CLASSIFICATION - CHURN PREDICTION FEATURE IMPORTANCE")
print("="*80)

# Initialize profiler
profiler = Profiler(df, name='customer_churn_analysis')

# Analyze correlations with churn target
print("\n1. Target Correlation Analysis")
print("-" * 80)
print("Analyzing feature correlations with 'churn' target...")

churn_correlations = profiler.analyze_target_correlation(
    target_column='churn',
    method='pearson',
    top_n=15
)

print(f"\nTop 15 Features Correlated with Churn:\n")
print(churn_correlations.to_string(index=False))

# Check encoding info
if 'target_encoding' in churn_correlations.attrs:
    encoding = churn_correlations.attrs['target_encoding']
    print(f"\nTarget Encoding Applied:")
    print(f"  Type: {encoding['type']}")
    print(f"  Encoding: {encoding['encoding']}")
    print(f"  Classes: {encoding['n_classes']}")

# Visualize feature importance
print("\n2. Feature Importance Visualization")
print("-" * 80)
print("Creating feature importance plot...")
# fig1 = profiler.plot_target_correlation(
#     target_column='churn',
#     method='pearson',
#     top_n=15,
#     figsize=(10, 10)
# )
# fig1.savefig('examples/output/churn_feature_importance.png', dpi=300, bbox_inches='tight')
print("(Uncomment code to generate plot)")

# Insights
print("\n3. Key Insights")
print("-" * 80)
top_5 = churn_correlations.head(5)
print("\nMost Important Features for Churn Prediction:")
for idx, row in top_5.iterrows():
    direction = "increases" if row['correlation'] > 0 else "decreases"
    print(f"  {idx+1}. {row['feature']}: r={row['correlation']:.3f}")
    print(f"     → Higher values {direction} churn likelihood")

# ============================================================================
# PART 2: REGRESSION - FEATURE IMPORTANCE FOR CLV PREDICTION
# ============================================================================

print("\n" + "="*80)
print("PART 2: REGRESSION - CUSTOMER LIFETIME VALUE PREDICTION")
print("="*80)

print("\n1. Target Correlation Analysis")
print("-" * 80)
print("Analyzing feature correlations with 'customer_lifetime_value' target...")

clv_correlations = profiler.analyze_target_correlation(
    target_column='customer_lifetime_value',
    method='pearson',
    top_n=10
)

print(f"\nTop 10 Features Correlated with CLV:\n")
print(clv_correlations.to_string(index=False))

# Visualize
print("\n2. Feature Importance Visualization")
print("-" * 80)
print("Creating CLV feature importance plot...")
# fig2 = profiler.plot_target_correlation(
#     target_column='customer_lifetime_value',
#     method='spearman',
#     top_n=10,
#     figsize=(10, 8),
#     color_positive='#3498db',  # Blue for positive
#     color_negative='#95a5a6'   # Grey for negative
# )
# fig2.savefig('examples/output/clv_feature_importance.png', dpi=300, bbox_inches='tight')
print("(Uncomment code to generate plot)")

# ============================================================================
# PART 3: AUTOMATIC CATEGORICAL ENCODING
# ============================================================================

print("\n" + "="*80)
print("PART 3: AUTOMATIC CATEGORICAL ENCODING FOR ML")
print("="*80)

# Create a copy for preprocessing
df_ml = df.copy()

print("\n1. Before Encoding")
print("-" * 80)
print(f"Original shape: {df_ml.shape}")
print(f"Columns ({len(df_ml.columns)}): {list(df_ml.columns)}")

# Initialize preprocessor
preprocessor = PreProcessor(df_ml)

# Auto-detect and encode categorical columns
print("\n2. Auto-Detecting Categorical Columns")
print("-" * 80)
print("Detecting and encoding categorical features...")
print("Excluding: customer_id, churn, customer_lifetime_value")

preprocessor.auto_encode_categorical(
    exclude_columns=['customer_id', 'churn', 'customer_lifetime_value'],
    max_categories=10,
    min_frequency=0.01,
    drop_first=False,
    drop_original=True
)

print("\n3. After Encoding")
print("-" * 80)
print(f"New shape: {preprocessor.df.shape}")
print(f"Total columns: {len(preprocessor.df.columns)}")

# Show which columns were created
original_cols = set(df.columns)
new_cols = set(preprocessor.df.columns) - original_cols
encoded_cols = sorted(list(new_cols))

print(f"\n{len(encoded_cols)} new encoded columns created:")
for col in encoded_cols[:20]:  # Show first 20
    print(f"  - {col}")
if len(encoded_cols) > 20:
    print(f"  ... and {len(encoded_cols) - 20} more")

# ============================================================================
# PART 4: COMPLETE ML PREPROCESSING PIPELINE
# ============================================================================

print("\n" + "="*80)
print("PART 4: COMPLETE ML PREPROCESSING PIPELINE")
print("="*80)

# Start fresh
df_pipeline = df.copy()

print("\n1. Pipeline Steps")
print("-" * 80)
print("Building complete preprocessing pipeline for churn prediction...")

# Step 1: Initialize
preprocessor = PreProcessor(df_pipeline)
print("\n  Step 1: Initialize preprocessor")
print(f"    Initial shape: {preprocessor.df.shape}")

# Step 2: Handle missing values (if any)
print("\n  Step 2: Handle missing values")
missing_count = preprocessor.df.isna().sum().sum()
if missing_count > 0:
    preprocessor.handle_missing(strategy='auto')
    print(f"    Handled {missing_count} missing values")
else:
    print("    No missing values to handle")

# Step 3: Standardize column names for SQL compatibility
print("\n  Step 3: Standardize column names")
preprocessor.standardize_column_names(convention='snake_case')
print(f"    Standardized {len(preprocessor.df.columns)} column names")

# Step 4: Auto-encode categorical variables
print("\n  Step 4: Auto-encode categorical variables")
original_shape = preprocessor.df.shape
preprocessor.auto_encode_categorical(
    exclude_columns=['customer_id', 'churn', 'customer_lifetime_value'],
    max_categories=10,
    drop_first=True  # Drop first category to avoid multicollinearity
)
print(f"    Before: {original_shape[1]} columns")
print(f"    After: {preprocessor.df.shape[1]} columns")
print(f"    Created: {preprocessor.df.shape[1] - original_shape[1]} new features")

# Step 5: Feature selection based on correlation analysis
print("\n  Step 5: Feature selection (based on correlation analysis)")
# Get top features from earlier analysis
top_features = churn_correlations.head(10)['feature'].tolist()
print(f"    Selected top {len(top_features)} features based on correlation")
print(f"    Features: {', '.join(top_features[:5])}...")

print("\n2. Final ML-Ready Dataset")
print("-" * 80)
print(f"Shape: {preprocessor.df.shape}")
print(f"Columns: {preprocessor.df.shape[1]}")
print(f"Rows: {preprocessor.df.shape[0]}")

# Sample of the transformed data
print("\nSample of transformed data (first 5 rows, first 10 columns):")
print(preprocessor.df.iloc[:5, :10].to_string())

# ============================================================================
# PART 5: INTEGRATION WITH ML WORKFLOWS
# ============================================================================

print("\n" + "="*80)
print("PART 5: INTEGRATION WITH ML FRAMEWORKS")
print("="*80)

print("\n1. Preparing Data for scikit-learn")
print("-" * 80)

# Separate features and target
# Note: In real usage, you would do this after preprocessing
print("Separating features and target...")
print("""
# Example code for scikit-learn:
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

# Separate features and target
X = preprocessor.df.drop(['customer_id', 'churn', 'customer_lifetime_value'], axis=1)
y = preprocessor.df['churn'].map({'No': 0, 'Yes': 1})

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Train model
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

# Evaluate
accuracy = clf.score(X_test, y_test)
print(f"Accuracy: {accuracy:.2%}")
""")

print("\n2. Feature Importance from Model")
print("-" * 80)
print("""
# After training, compare with correlation-based importance:
feature_importance_df = pd.DataFrame({
    'feature': X.columns,
    'importance': clf.feature_importances_
}).sort_values('importance', ascending=False)

print("Top 10 Features from Model:")
print(feature_importance_df.head(10))

# Compare with correlation-based importance from profiler
print("\\nComparing with correlation-based importance...")
""")

# ============================================================================
# PART 6: ADVANCED USE CASES
# ============================================================================

print("\n" + "="*80)
print("PART 6: ADVANCED ML USE CASES")
print("="*80)

print("\n1. Multi-Target Analysis")
print("-" * 80)
print("Analyzing correlations for multiple targets...")

# Analyze for both classification and regression
targets = {
    'churn': 'Classification (Binary)',
    'customer_lifetime_value': 'Regression (Continuous)'
}

for target, task_type in targets.items():
    print(f"\n  Target: {target} ({task_type})")
    corr = profiler.analyze_target_correlation(target, top_n=5)
    print(f"  Top 3 Features:")
    for idx, row in corr.head(3).iterrows():
        print(f"    {idx+1}. {row['feature']}: {row['correlation']:.3f}")

print("\n2. Custom Encoding Strategies")
print("-" * 80)
print("""
# Different encoding strategies for different scenarios:

# Scenario A: High-cardinality categorical (keep top categories)
preprocessor.auto_encode_categorical(
    max_categories=20,      # Allow more categories
    min_frequency=0.05,     # Only frequent categories
    drop_first=True         # Avoid multicollinearity
)

# Scenario B: Conservative (only very low cardinality)
preprocessor.auto_encode_categorical(
    max_categories=5,       # Very restrictive
    min_frequency=0.10,     # Only very frequent
    drop_first=False        # Keep all for interpretation
)

# Scenario C: Keep original columns for analysis
preprocessor.auto_encode_categorical(
    exclude_columns=['target', 'id'],
    drop_original=False     # Keep both encoded and original
)
""")

print("\n3. Correlation Method Comparison")
print("-" * 80)
print("Comparing different correlation methods...")

methods = ['pearson', 'spearman', 'kendall']
print("\nTop 3 features by each method:")

for method in methods:
    corr = profiler.analyze_target_correlation('churn', method=method, top_n=3)
    print(f"\n  {method.capitalize()}:")
    for idx, row in corr.iterrows():
        print(f"    {idx+1}. {row['feature']}: {row['correlation']:.3f}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("ML FEATURE ENGINEERING SUMMARY")
print("="*80)

print(f"""
Dataset: Customer Churn Prediction
Original Shape: {df.shape}
ML-Ready Shape: {preprocessor.df.shape}

Feature Engineering:
  - Categorical columns detected and encoded
  - {preprocessor.df.shape[1] - df.shape[1]} new features created
  - Feature importance analyzed for both classification and regression

Top Features for Churn Prediction:
  1. {churn_correlations.iloc[0]['feature']}: {churn_correlations.iloc[0]['correlation']:.3f}
  2. {churn_correlations.iloc[1]['feature']}: {churn_correlations.iloc[1]['correlation']:.3f}
  3. {churn_correlations.iloc[2]['feature']}: {churn_correlations.iloc[2]['correlation']:.3f}

Next Steps:
  1. Use preprocessed data for model training
  2. Compare correlation-based importance with model importance
  3. Perform feature selection based on importance scores
  4. Iterate on feature engineering based on model performance
""")

print("\n" + "="*80)
print("Example completed! Use these ML capabilities to:")
print("  - Automatically identify important features")
print("  - Streamline feature engineering pipelines")
print("  - Prepare data for ML models efficiently")
print("  - Compare feature importance across methods")
print("="*80)
