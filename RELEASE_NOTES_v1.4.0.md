# 🚀 schema-mapper v1.4.0: Machine Learning Feature Engineering

**Automate Feature Analysis. Intelligent Preprocessing. ML-Ready in Minutes.**

We're excited to announce v1.4.0—a major enhancement focused on **machine learning workflows**! This release adds intelligent feature engineering, automated categorical encoding, and feature importance analysis to help data scientists and ML engineers prepare data faster.

---

## 🎯 What's New

### 1. 🤖 Target Correlation Analysis
**Automatically identify the most important features for your ML models.**

```python
from schema_mapper.profiler import Profiler
import pandas as pd

# Load your data
df = pd.read_csv('customer_churn.csv')
profiler = Profiler(df, name='churn_analysis')

# Analyze feature importance (handles categorical targets!)
feature_importance = profiler.analyze_target_correlation(
    target_column='churn',  # Binary classification
    method='pearson',
    top_n=15
)

print(feature_importance)
#          feature  correlation  abs_correlation
# 0  tenure_months       -0.352            0.352
# 1 monthly_charges        0.298            0.298
# 2   support_tickets      0.245            0.245
```

**What's Special:**
- **Automatic target encoding**: Categorical targets → binary (0/1) for classification
- **Multi-class support**: Label encoding for 3+ classes
- **Sorted by strength**: Returns features ranked by absolute correlation
- **Works with regression too**: Numeric targets supported (pearson, spearman, kendall)

---

### 2. 📊 Feature Importance Visualization
**One-line code to create professional feature importance charts.**

```python
# Create color-coded bar chart
fig = profiler.plot_target_correlation(
    target_column='churn',
    top_n=15,
    figsize=(10, 8)
)
fig.savefig('feature_importance.png', dpi=300, bbox_inches='tight')
```

**Visual Features:**
- ✅ **Green bars** for positive correlations (increase target)
- ❌ **Red bars** for negative correlations (decrease target)
- 📏 **Automatic labeling** (correlation values on bars)
- 🎨 **Customizable colors** for brand consistency
- 📐 **Auto-sorted** by correlation strength (strongest → weakest)

**Perfect for:**
- Feature selection before modeling
- Stakeholder presentations
- Model documentation
- Identifying proxy variables

---

### 3. 🔧 Auto-Detect Categorical Encoding
**Smart detection and one-hot encoding with zero configuration.**

```python
from schema_mapper.preprocessor import PreProcessor

# Initialize with your DataFrame
preprocessor = PreProcessor(df)

# Auto-detect and encode ALL categorical columns
preprocessor.auto_encode_categorical(
    exclude_columns=['churn', 'customer_id'],  # Don't encode these
    max_categories=10,                          # Only low-cardinality
    min_frequency=0.01,                         # Ignore rare categories
    drop_first=True                             # Avoid multicollinearity
)

print(f"ML-ready: {preprocessor.df.shape}")
# Before: (10000, 15) → After: (10000, 45)
```

**Detection Criteria:**
- ✅ **Object/string columns** with ≤ max_categories unique values
- ✅ **Low-cardinality numeric** (e.g., 0/1/2 for # of dependents)
- ✅ **Frequency filtering** (ignore categories appearing < 1% of rows)
- ✅ **Smart exclusions** (automatically skip ID and target columns)

**What Gets Encoded:**
```python
# BEFORE auto-encoding:
# - gender: ['Male', 'Female']
# - contract_type: ['Month-to-month', 'One year', 'Two year']
# - internet_service: ['DSL', 'Fiber optic', 'No']

# AFTER auto-encoding:
# - gender_Male, gender_Female
# - contract_type_Month-to-month, contract_type_One_year, contract_type_Two_year
# - internet_service_DSL, internet_service_Fiber_optic, internet_service_No
```

**Configurable for Your Needs:**
```python
# Scenario A: Very restrictive (only obvious categoricals)
preprocessor.auto_encode_categorical(
    max_categories=5,      # Very low cardinality only
    min_frequency=0.10     # Category must appear in ≥10% of rows
)

# Scenario B: More permissive (higher cardinality OK)
preprocessor.auto_encode_categorical(
    max_categories=20,     # Allow more categories
    min_frequency=0.01,    # Include rarer categories
    drop_first=False       # Keep all for interpretability
)

# Scenario C: Keep original columns too
preprocessor.auto_encode_categorical(
    drop_original=False    # Keep both encoded and original
)
```

---

### 4. 🎨 Enhanced Visualizations
**Better defaults, more customization, same ease of use.**

```python
# Customizable histogram colors
profiler.plot_distributions(
    columns=['age', 'income', 'credit_score'],
    color='#34495e',      # Dark blue-grey (default)
    kde_color='#e74c3c'   # Red for KDE overlay
)

# Scatter plot matrix with trend lines
profiler.plot_scatter_matrix(
    columns=['age', 'income', 'purchases', 'balance'],
    color='#34495e',
    alpha=0.6,
    diagonal='hist'  # or 'kde'
)
```

**What's New:**
- 🎨 **Custom color parameters** with elegant defaults
- 📈 **Scatter matrix** with automatic trend lines
- 🏷️ **Auto-labeling** for axes and titles
- 📊 **Matplotlib abstraction** (no imports needed!)

---

## 💡 Why This Release Matters

### For Data Scientists

**Before v1.4.0:**
```python
# Manual feature importance analysis
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder

# 1. Encode target manually
le = LabelEncoder()
y_encoded = le.fit_transform(df['churn'])

# 2. Calculate correlations manually
correlations = []
for col in df.select_dtypes(include='number').columns:
    if col != 'churn':
        corr = df[col].corr(pd.Series(y_encoded))
        correlations.append({'feature': col, 'correlation': corr})

corr_df = pd.DataFrame(correlations).sort_values('correlation', key=abs, ascending=False)

# 3. Plot manually
plt.figure(figsize=(10, 8))
colors = ['g' if x > 0 else 'r' for x in corr_df['correlation']]
plt.barh(corr_df['feature'], corr_df['correlation'], color=colors)
plt.xlabel('Correlation')
plt.title('Feature Importance')
plt.tight_layout()
plt.savefig('importance.png')

# 4. Encode categoricals manually
categorical_cols = df.select_dtypes(include='object').columns
for col in categorical_cols:
    if col not in ['churn', 'id'] and df[col].nunique() < 10:
        dummies = pd.get_dummies(df[col], prefix=col)
        df = pd.concat([df, dummies], axis=1)
        df.drop(col, axis=1, inplace=True)
```

**After v1.4.0:**
```python
# 3 lines of code!
profiler = Profiler(df)
profiler.analyze_target_correlation('churn', top_n=15)
profiler.plot_target_correlation('churn', top_n=15)

# Auto-encoding in 1 line
PreProcessor(df).auto_encode_categorical(exclude_columns=['churn', 'id'])
```

**Time Saved:** 20+ minutes → 30 seconds ⚡

---

### For ML Engineers

**Complete ML Preprocessing Pipeline (scikit-learn):**

```python
from schema_mapper import Profiler, PreProcessor
import pandas as pd

# 1. Load data
df = pd.read_csv('customer_data.csv')

# 2. Identify important features
profiler = Profiler(df, name='churn_model')
importance = profiler.analyze_target_correlation('churn', top_n=20)

# 3. Visualize for stakeholders
fig = profiler.plot_target_correlation('churn', top_n=15)
fig.savefig('reports/feature_importance.png', dpi=300)

# 4. Preprocess for ML
preprocessor = PreProcessor(df)
preprocessor.auto_encode_categorical(
    exclude_columns=['churn', 'customer_id'],
    max_categories=10,
    drop_first=True
)

# 5. Train model
X = preprocessor.df.drop(['churn', 'customer_id'], axis=1)
y = preprocessor.df['churn'].map({'No': 0, 'Yes': 1})

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

print(f"Accuracy: {model.score(X_test, y_test):.2%}")
```

**Complete Pipeline with TensorFlow/Keras:**

```python
from schema_mapper import Profiler, PreProcessor
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# 1. Load sample data (clothing retailer orders)
df = pd.read_csv('sample_data/problematic_clothing_retailer_data.csv')

# 2. Profile data quality
profiler = Profiler(df, name='return_prediction')
quality = profiler.assess_quality()
print(f"Data Quality Score: {quality['overall_score']:.1f}/100")

# 3. Clean and preprocess
preprocessor = PreProcessor(df)
df_clean = (preprocessor
    .standardize_column_names()              # Fix column name issues
    .handle_missing_values(strategy='auto')   # Handle 492 missing values
    .remove_duplicates()                      # Remove duplicate orders
    .apply()
)

# 4. Analyze feature importance for Return Flag
profiler_clean = Profiler(df_clean, name='returns_analysis')
importance = profiler_clean.analyze_target_correlation(
    target_column='return_flag',  # Binary: Y/N
    method='pearson',
    top_n=15
)
print("\nTop Features Predicting Returns:")
print(importance.head(10))

# 5. Visualize feature importance
fig = profiler_clean.plot_target_correlation(
    'return_flag',
    top_n=12,
    figsize=(10, 8)
)
fig.savefig('return_feature_importance.png', dpi=300, bbox_inches='tight')

# 6. Auto-encode categorical columns
preprocessor_ml = PreProcessor(df_clean)
preprocessor_ml.auto_encode_categorical(
    exclude_columns=['return_flag', 'order_id', 'customer_email'],
    max_categories=15,  # Allow more categories for product data
    drop_first=False     # Keep all for neural network
)

# 7. Prepare features and target
# Convert return_flag to binary (Y=1, N=0)
y = preprocessor_ml.df['return_flag'].map({'Y': 1, 'N': 0, 'Yes': 1, 'No': 0})
y = y.fillna(0).astype(int)

# Select only numeric features for model
X = preprocessor_ml.df.select_dtypes(include=['number']).drop(['return_flag'], axis=1, errors='ignore')
X = X.fillna(0)  # Handle any remaining NaNs

print(f"\nML-Ready Dataset:")
print(f"  Features: {X.shape[1]}")
print(f"  Samples: {X.shape[0]}")
print(f"  Return Rate: {y.mean():.1%}")

# 8. Train/test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# 9. Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# 10. Build TensorFlow model
model = keras.Sequential([
    keras.layers.Dense(128, activation='relu', input_shape=(X_train_scaled.shape[1],)),
    keras.layers.Dropout(0.3),
    keras.layers.Dense(64, activation='relu'),
    keras.layers.Dropout(0.2),
    keras.layers.Dense(32, activation='relu'),
    keras.layers.Dense(1, activation='sigmoid')
])

model.compile(
    optimizer='adam',
    loss='binary_crossentropy',
    metrics=['accuracy', tf.keras.metrics.AUC(name='auc')]
)

# 11. Train model
history = model.fit(
    X_train_scaled, y_train,
    epochs=50,
    batch_size=32,
    validation_split=0.2,
    verbose=0
)

# 12. Evaluate
test_loss, test_acc, test_auc = model.evaluate(X_test_scaled, y_test, verbose=0)
print(f"\nModel Performance:")
print(f"  Accuracy: {test_acc:.2%}")
print(f"  AUC: {test_auc:.3f}")

# 13. Feature importance from model weights
# (First layer weights indicate importance)
first_layer_weights = model.layers[0].get_weights()[0]
feature_importance_tf = pd.DataFrame({
    'feature': X.columns,
    'importance': abs(first_layer_weights).mean(axis=1)
}).sort_values('importance', ascending=False)

print("\nTop 10 Features (TensorFlow Model):")
print(feature_importance_tf.head(10))
```

**Output:**
```
Data Quality Score: 68.5/100

Top Features Predicting Returns:
         feature  correlation  abs_correlation
0  discount_pct        0.342            0.342
1  total_amount       -0.298            0.298
2   price_usd          -0.287            0.287
3  quantity            0.156            0.156

ML-Ready Dataset:
  Features: 45
  Samples: 1000
  Return Rate: 21.8%

Model Performance:
  Accuracy: 84.50%
  AUC: 0.892

Top 10 Features (TensorFlow Model):
             feature  importance
0      discount_pct    0.0234
1      total_amount    0.0198
2         price_usd    0.0187
3  order_status_shipped  0.0145
...
```

---

## 📊 What's Included in v1.4.0

### **New Methods:**
- ✅ `Profiler.analyze_target_correlation()` - Feature importance analysis
- ✅ `Profiler.plot_target_correlation()` - Feature importance visualization
- ✅ `PreProcessor.auto_encode_categorical()` - Intelligent categorical encoding
- ✅ `DataVisualizer.plot_target_correlation()` - Low-level plotting function
- ✅ `Profiler.plot_scatter_matrix()` - Pairwise relationship visualization

### **Enhanced Methods:**
- ✅ `plot_histogram()` - Added color parameters
- ✅ `plot_distributions()` - Custom color support

### **New Examples:**
- ✅ `examples/09_data_profiling_analysis.py` - Comprehensive profiling workflow
- ✅ `examples/10_ml_feature_engineering.py` - ML preprocessing pipeline

### **Documentation:**
- ✅ Updated landing page with ML features
- ✅ Comprehensive CHANGELOG.md
- ✅ Updated examples README

---

## 🚀 Getting Started

### Installation

```bash
# Install with ML support
pip install schema-mapper[all]==1.4.0

# Or just the core + visualization
pip install schema-mapper==1.4.0
```

### Quick Example

```python
from schema_mapper import Profiler, PreProcessor
import pandas as pd

# Your data
df = pd.read_csv('data.csv')

# Feature importance
profiler = Profiler(df)
importance = profiler.analyze_target_correlation('target', top_n=10)
profiler.plot_target_correlation('target', top_n=10)

# Auto-encode
PreProcessor(df).auto_encode_categorical(exclude_columns=['target'])

# ML-ready! 🚀
```

---

## ⬆️ Upgrading from v1.3.0

**Fully backward compatible!** All v1.3.0 code continues to work.

New features are opt-in additions:

```bash
pip install --upgrade schema-mapper==1.4.0
```

**What's New:**
```python
# NEW in v1.4.0
profiler.analyze_target_correlation('target')
profiler.plot_target_correlation('target')
preprocessor.auto_encode_categorical()

# Still works (v1.3.0)
profiler.assess_quality()
profiler.detect_anomalies()
preprocessor.handle_missing_values()
```

---

## 🔬 Use Cases

### 1. **Feature Selection for Classification**
```python
# Identify top features for churn prediction
importance = profiler.analyze_target_correlation('churn', top_n=20)
top_features = importance.head(10)['feature'].tolist()

# Use only top features
X_selected = df[top_features]
```

### 2. **Quick EDA for Regression**
```python
# Find features correlated with house price
profiler.analyze_target_correlation('price', method='spearman')
profiler.plot_target_correlation('price', top_n=15)
```

### 3. **Automated Preprocessing Pipeline**
```python
# Complete pipeline
PreProcessor(df)
    .handle_missing_values(strategy='auto')
    .auto_encode_categorical(exclude_columns=['target', 'id'])
    .standardize_column_names()
    .apply()
```

### 4. **Multi-Class Classification**
```python
# Handles 3+ classes automatically
profiler.analyze_target_correlation('product_category')  # 5 classes
# Uses label encoding: category_A → 0, category_B → 1, etc.
```

---

## 📚 Resources

- **Documentation**: [README.md](https://github.com/datateamsix/schema-mapper#readme)
- **Examples**:
  - [09_data_profiling_analysis.py](https://github.com/datateamsix/schema-mapper/tree/main/examples/09_data_profiling_analysis.py)
  - [10_ml_feature_engineering.py](https://github.com/datateamsix/schema-mapper/tree/main/examples/10_ml_feature_engineering.py)
- **PyPI**: [pypi.org/project/schema-mapper](https://pypi.org/project/schema-mapper/)
- **Issues**: [Report bugs or request features](https://github.com/datateamsix/schema-mapper/issues)

---

## 🙏 Thank You

Thank you to everyone using schema-mapper for their data workflows! This release is designed to make ML preprocessing faster, smarter, and more automated.

**Built for lean data teams. Not lean capabilities.**

---

**Full Changelog**: https://github.com/datateamsix/schema-mapper/compare/v1.3.0...v1.4.0

**PyPI Release**: https://pypi.org/project/schema-mapper/1.4.0/
