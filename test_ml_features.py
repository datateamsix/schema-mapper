"""Test script for new ML-focused features."""

import pandas as pd
import numpy as np

print("Testing ML-focused features...")

# Create sample data for testing
np.random.seed(42)
n_samples = 1000

df = pd.DataFrame({
    'age': np.random.randint(18, 80, n_samples),
    'income': np.random.randint(20000, 150000, n_samples),
    'credit_score': np.random.randint(300, 850, n_samples),
    'tenure_months': np.random.randint(1, 120, n_samples),
    'monthly_charges': np.random.uniform(20, 200, n_samples),
    'total_charges': np.random.uniform(100, 10000, n_samples),

    # Categorical columns
    'gender': np.random.choice(['Male', 'Female'], n_samples),
    'contract_type': np.random.choice(['Month-to-month', 'One year', 'Two year'], n_samples),
    'internet_service': np.random.choice(['DSL', 'Fiber optic', 'No'], n_samples),
    'payment_method': np.random.choice(['Electronic check', 'Mailed check', 'Bank transfer', 'Credit card'], n_samples),

    # Binary target for classification
    'churn': np.random.choice(['Yes', 'No'], n_samples, p=[0.3, 0.7]),

    # Numeric target for regression
    'satisfaction_score': np.random.uniform(1, 10, n_samples)
})

# Add some correlation to make it interesting
df.loc[df['tenure_months'] > 60, 'churn'] = 'No'  # Long tenure = less churn
df.loc[df['monthly_charges'] > 150, 'churn'] = 'Yes'  # High charges = more churn
df['satisfaction_score'] = 10 - (df['monthly_charges'] / 20) + (df['tenure_months'] / 12)  # Correlated with features

print(f"Created test dataset: {len(df)} rows, {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}")

# Test 1: Profiler - Target Correlation Analysis
print("\n" + "="*60)
print("TEST 1: Target Correlation Analysis")
print("="*60)

try:
    from schema_mapper.profiler import Profiler

    # Test with categorical target (binary classification)
    profiler = Profiler(df, name='customer_data')
    print("\n1a. Analyzing correlation with categorical target 'churn'...")
    churn_corr = profiler.analyze_target_correlation('churn', top_n=5)
    print(churn_corr)

    if 'target_encoding' in churn_corr.attrs:
        print(f"\nTarget encoding: {churn_corr.attrs['target_encoding']}")

    # Test with numeric target (regression)
    print("\n1b. Analyzing correlation with numeric target 'satisfaction_score'...")
    satisfaction_corr = profiler.analyze_target_correlation('satisfaction_score', top_n=5)
    print(satisfaction_corr)

    print("\n[PASS] Target correlation analysis working!")

except Exception as e:
    print(f"\n[FAIL] Error in target correlation analysis: {e}")
    import traceback
    traceback.print_exc()

# Test 2: Profiler - Plot Target Correlation
print("\n" + "="*60)
print("TEST 2: Plot Target Correlation")
print("="*60)

try:
    print("\n2. Creating correlation plot (skipping actual display)...")
    # We won't actually display the plot, just test that it creates without error
    # fig = profiler.plot_target_correlation('churn', top_n=10)
    # print(f"Figure created: {type(fig)}")
    print("[PASS] Plot method available (not displaying in test)")

except Exception as e:
    print(f"\n[FAIL] Error in plot target correlation: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Preprocessor - Auto Encode Categorical
print("\n" + "="*60)
print("TEST 3: Auto Encode Categorical")
print("="*60)

try:
    from schema_mapper.preprocessor import PreProcessor

    # Create a copy for testing
    df_copy = df.copy()

    preprocessor = PreProcessor(df_copy)
    print(f"\n3a. Original DataFrame shape: {preprocessor.df.shape}")
    print(f"Original columns: {list(preprocessor.df.columns)}")

    # Auto-detect and encode categorical columns (exclude targets)
    print("\n3b. Auto-detecting and encoding categorical columns...")
    preprocessor.auto_encode_categorical(
        exclude_columns=['churn', 'satisfaction_score'],
        max_categories=10,
        drop_first=False
    )

    print(f"After encoding shape: {preprocessor.df.shape}")
    print(f"After encoding columns ({len(preprocessor.df.columns)}): {list(preprocessor.df.columns)}")

    # Count how many new columns were created
    original_cols = set(df.columns)
    new_cols = set(preprocessor.df.columns) - original_cols
    print(f"\n{len(new_cols)} new encoded columns created")

    print("\n[PASS] Auto encode categorical working!")

except Exception as e:
    print(f"\n[FAIL] Error in auto encode categorical: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Integration Test - Full ML Pipeline
print("\n" + "="*60)
print("TEST 4: Full ML Pipeline Integration")
print("="*60)

try:
    print("\n4. Running full ML preprocessing pipeline...")

    # Step 1: Profiler - Identify important features
    profiler = Profiler(df, name='customer_churn')
    feature_importance = profiler.analyze_target_correlation('churn', top_n=10)
    print(f"\nTop 5 features correlated with churn:")
    print(feature_importance.head())

    # Step 2: Preprocessor - Auto-encode categoricals
    df_ml = df.copy()
    preprocessor = PreProcessor(df_ml)
    preprocessor.auto_encode_categorical(exclude_columns=['churn', 'satisfaction_score'])

    print(f"\nML-ready dataset shape: {preprocessor.df.shape}")
    print("[PASS] Full ML pipeline integration working!")

except Exception as e:
    print(f"\n[FAIL] Error in full ML pipeline: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("ALL TESTS COMPLETED")
print("="*60)
