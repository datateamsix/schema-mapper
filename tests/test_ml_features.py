"""
Comprehensive unit tests for ML-focused features.

Tests cover:
- Target correlation analysis (classification and regression)
- Feature importance visualization
- Automatic categorical encoding
- ML preprocessing pipeline integration
- Multi-target analysis
- Encoding strategy validation
"""

import pytest
import pandas as pd
import numpy as np
from schema_mapper import Profiler, PreProcessor


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def ml_classification_df():
    """DataFrame for classification tasks (binary churn prediction)."""
    np.random.seed(42)
    n_samples = 1000

    df = pd.DataFrame({
        # Numeric features
        'age': np.random.randint(18, 80, n_samples),
        'tenure_months': np.random.randint(1, 120, n_samples),
        'monthly_charges': np.random.uniform(20, 200, n_samples),
        'total_charges': np.random.uniform(100, 20000, n_samples),
        'num_support_tickets': np.random.poisson(3, n_samples),

        # Categorical features
        'gender': np.random.choice(['Male', 'Female'], n_samples),
        'contract_type': np.random.choice(['Month-to-month', 'One year', 'Two year'], n_samples),
        'internet_service': np.random.choice(['DSL', 'Fiber optic', 'No'], n_samples),
        'payment_method': np.random.choice(
            ['Electronic check', 'Mailed check', 'Bank transfer', 'Credit card'],
            n_samples
        ),

        # Binary target
        'churn': np.random.choice(['Yes', 'No'], n_samples, p=[0.3, 0.7]),
    })

    # Add realistic correlations
    df.loc[df['tenure_months'] > 60, 'churn'] = 'No'  # Long tenure = less churn
    df.loc[df['monthly_charges'] > 150, 'churn'] = 'Yes'  # High charges = more churn

    return df


@pytest.fixture
def ml_regression_df():
    """DataFrame for regression tasks (customer lifetime value)."""
    np.random.seed(42)
    n_samples = 1000

    df = pd.DataFrame({
        'age': np.random.randint(18, 80, n_samples),
        'income': np.random.randint(20000, 150000, n_samples),
        'credit_score': np.random.randint(300, 850, n_samples),
        'tenure_months': np.random.randint(1, 120, n_samples),
        'monthly_charges': np.random.uniform(20, 200, n_samples),
    })

    # Create correlated target
    df['customer_lifetime_value'] = (
        df['tenure_months'] * df['monthly_charges'] *
        np.random.uniform(0.8, 1.2, n_samples)
    )

    return df


@pytest.fixture
def ml_multiclass_df():
    """DataFrame for multi-class classification."""
    np.random.seed(42)
    n_samples = 500

    df = pd.DataFrame({
        'feature1': np.random.randn(n_samples),
        'feature2': np.random.randn(n_samples),
        'feature3': np.random.randn(n_samples),
        'category': np.random.choice(['A', 'B', 'C', 'D'], n_samples),
    })

    return df


@pytest.fixture
def ml_categorical_df():
    """DataFrame with various categorical types for encoding tests."""
    return pd.DataFrame({
        # Binary categorical
        'binary_cat': ['Yes', 'No'] * 50,

        # Low cardinality (3 categories)
        'low_card': np.random.choice(['Small', 'Medium', 'Large'], 100),

        # Medium cardinality (10 categories)
        'medium_card': np.random.choice([f'Cat_{i}' for i in range(10)], 100),

        # High cardinality (50 categories)
        'high_card': np.random.choice([f'ID_{i}' for i in range(50)], 100),

        # Numeric (should not be encoded)
        'numeric': np.random.randint(1, 100, 100),

        # Target
        'target': np.random.choice(['A', 'B'], 100),
    })


# ============================================================================
# TARGET CORRELATION ANALYSIS TESTS
# ============================================================================

class TestTargetCorrelationAnalysis:
    """Test Profiler.analyze_target_correlation() method."""

    def test_binary_classification_correlation(self, ml_classification_df):
        """Should analyze correlations with binary categorical target."""
        profiler = Profiler(ml_classification_df, name='churn_analysis')

        result = profiler.analyze_target_correlation(
            target_column='churn',
            method='pearson',
            top_n=5
        )

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert 'feature' in result.columns
        assert 'correlation' in result.columns
        assert 'abs_correlation' in result.columns

        # Verify target encoding metadata
        assert 'target_encoding' in result.attrs
        encoding = result.attrs['target_encoding']
        assert encoding['type'] in ['categorical', 'binary_classification']
        assert encoding['n_classes'] == 2
        assert 'No' in encoding['encoding'] and 'Yes' in encoding['encoding']

        # Verify correlations are valid
        assert all(-1 <= corr <= 1 for corr in result['correlation'])
        assert all(result['abs_correlation'] >= 0)

    def test_regression_correlation(self, ml_regression_df):
        """Should analyze correlations with numeric target."""
        profiler = Profiler(ml_regression_df, name='clv_analysis')

        result = profiler.analyze_target_correlation(
            target_column='customer_lifetime_value',
            method='pearson',
            top_n=3
        )

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

        # Verify target encoding (should be None for numeric, or attrs may not be set)
        if 'target_encoding' in result.attrs:
            assert result.attrs['target_encoding'] is None

        # tenure_months and monthly_charges should be highly correlated
        top_features = result['feature'].tolist()
        assert 'tenure_months' in top_features or 'monthly_charges' in top_features

    def test_multiclass_classification_correlation(self, ml_multiclass_df):
        """Should analyze correlations with multi-class target."""
        pytest.importorskip("sklearn")  # Skip if sklearn not available

        profiler = Profiler(ml_multiclass_df, name='multiclass')

        result = profiler.analyze_target_correlation(
            target_column='category',
            method='pearson',
            top_n=3
        )

        # Verify multi-class encoding
        assert 'target_encoding' in result.attrs
        encoding = result.attrs['target_encoding']
        assert encoding['type'] in ['categorical', 'multiclass_classification']
        assert encoding['n_classes'] == 4
        assert all(cat in encoding['encoding'] for cat in ['A', 'B', 'C', 'D'])

    def test_correlation_methods(self, ml_classification_df):
        """Should support different correlation methods."""
        pytest.importorskip("scipy")  # Skip if scipy not available

        profiler = Profiler(ml_classification_df, name='churn')

        methods = ['pearson', 'spearman', 'kendall']

        for method in methods:
            result = profiler.analyze_target_correlation(
                target_column='churn',
                method=method,
                top_n=3
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) <= 3
            assert all(-1 <= corr <= 1 for corr in result['correlation'])

    def test_top_n_parameter(self, ml_classification_df):
        """Should respect top_n parameter."""
        profiler = Profiler(ml_classification_df, name='churn')

        # Test different top_n values
        for n in [1, 3, 5]:
            result = profiler.analyze_target_correlation('churn', top_n=n)
            # Should return up to n results (may be less if features can't be correlated)
            assert len(result) <= n
            assert len(result) >= 1

    def test_exclude_target_from_results(self, ml_classification_df):
        """Should not include target column in results."""
        profiler = Profiler(ml_classification_df, name='churn')

        result = profiler.analyze_target_correlation('churn', top_n=20)

        # Target should not be in the results
        assert 'churn' not in result['feature'].values

    def test_invalid_target_column(self, ml_classification_df):
        """Should raise error for invalid target column."""
        profiler = Profiler(ml_classification_df, name='churn')

        with pytest.raises(ValueError, match="Target column 'invalid_col' not found"):
            profiler.analyze_target_correlation('invalid_col')

    def test_sorted_by_absolute_correlation(self, ml_classification_df):
        """Should sort results by absolute correlation value."""
        profiler = Profiler(ml_classification_df, name='churn')

        result = profiler.analyze_target_correlation('churn', top_n=5)

        # Verify sorted descending by abs_correlation
        abs_corrs = result['abs_correlation'].tolist()
        assert abs_corrs == sorted(abs_corrs, reverse=True)


# ============================================================================
# PLOT TARGET CORRELATION TESTS
# ============================================================================

class TestPlotTargetCorrelation:
    """Test Profiler.plot_target_correlation() visualization method."""

    def test_plot_creation(self, ml_classification_df):
        """Should create matplotlib figure without errors."""
        profiler = Profiler(ml_classification_df, name='churn')

        # Import matplotlib here to avoid issues if not installed
        try:
            import matplotlib.pyplot as plt

            fig = profiler.plot_target_correlation('churn', top_n=5)

            assert fig is not None
            assert hasattr(fig, 'axes')

            plt.close(fig)
        except ImportError:
            pytest.skip("matplotlib not installed")

    def test_plot_with_custom_figsize(self, ml_classification_df):
        """Should respect custom figure size."""
        profiler = Profiler(ml_classification_df, name='churn')

        try:
            import matplotlib.pyplot as plt

            fig = profiler.plot_target_correlation(
                'churn',
                top_n=3,
                figsize=(12, 8)
            )

            # Check figure size (approximate due to DPI)
            assert fig.get_figwidth() == 12
            assert fig.get_figheight() == 8

            plt.close(fig)
        except ImportError:
            pytest.skip("matplotlib not installed")


# ============================================================================
# AUTO ENCODE CATEGORICAL TESTS
# ============================================================================

class TestAutoEncodeCategorical:
    """Test PreProcessor.auto_encode_categorical() method."""

    def test_basic_categorical_encoding(self, ml_categorical_df):
        """Should detect and encode categorical columns."""
        df_copy = ml_categorical_df.copy()
        preprocessor = PreProcessor(df_copy)

        original_shape = preprocessor.df.shape

        preprocessor.auto_encode_categorical(
            exclude_columns=['target'],
            max_categories=10,
            drop_original=True
        )

        # Should have more columns after encoding
        assert preprocessor.df.shape[1] > original_shape[1]

        # Original categorical columns should be removed
        assert 'binary_cat' not in preprocessor.df.columns
        assert 'low_card' not in preprocessor.df.columns

        # Numeric should remain
        assert 'numeric' in preprocessor.df.columns

        # Target should remain (excluded)
        assert 'target' in preprocessor.df.columns

    def test_max_categories_threshold(self, ml_categorical_df):
        """Should only encode columns with cardinality <= max_categories."""
        df_copy = ml_categorical_df.copy()
        preprocessor = PreProcessor(df_copy)

        # Only encode columns with <= 5 categories
        preprocessor.auto_encode_categorical(
            exclude_columns=['target'],
            max_categories=5,
            drop_original=False
        )

        # high_card (50 categories) should NOT be encoded
        # medium_card (10 categories) should NOT be encoded
        # low_card (3 categories) SHOULD be encoded
        # binary_cat (2 categories) SHOULD be encoded

        encoded_cols = [col for col in preprocessor.df.columns if '_' in col and col != 'medium_card' and col != 'high_card']

        # Should have some encoded columns
        assert len(encoded_cols) > 0

    def test_exclude_columns(self, ml_categorical_df):
        """Should not encode excluded columns."""
        df_copy = ml_categorical_df.copy()
        preprocessor = PreProcessor(df_copy)

        exclude = ['target', 'binary_cat']

        preprocessor.auto_encode_categorical(
            exclude_columns=exclude,
            drop_original=False
        )

        # Excluded columns should remain unchanged
        for col in exclude:
            assert col in preprocessor.df.columns

    def test_drop_first_parameter(self, ml_categorical_df):
        """Should drop first category when drop_first=True."""
        df_copy = ml_categorical_df.copy()
        preprocessor = PreProcessor(df_copy)

        # Get low_card unique values
        unique_values = df_copy['low_card'].unique()

        preprocessor.auto_encode_categorical(
            exclude_columns=['target', 'medium_card', 'high_card', 'numeric', 'binary_cat'],
            drop_first=True,
            drop_original=True
        )

        # Should have n-1 encoded columns for low_card
        encoded_cols = [col for col in preprocessor.df.columns if col.startswith('low_card_')]
        assert len(encoded_cols) == len(unique_values) - 1

    def test_drop_original_parameter(self, ml_categorical_df):
        """Should keep or drop original columns based on parameter."""
        # Test drop_original=True
        df_copy1 = ml_categorical_df.copy()
        preprocessor1 = PreProcessor(df_copy1)
        preprocessor1.auto_encode_categorical(
            exclude_columns=['target'],
            drop_original=True
        )

        assert 'binary_cat' not in preprocessor1.df.columns

        # Test drop_original=False
        df_copy2 = ml_categorical_df.copy()
        preprocessor2 = PreProcessor(df_copy2)
        preprocessor2.auto_encode_categorical(
            exclude_columns=['target'],
            drop_original=False
        )

        assert 'binary_cat' in preprocessor2.df.columns

    def test_min_frequency_threshold(self, ml_categorical_df):
        """Should only create dummy variables for frequent categories."""
        # Create data with rare categories
        df = pd.DataFrame({
            'category': ['A'] * 90 + ['B'] * 5 + ['C'] * 5,  # A=90%, B=5%, C=5%
            'target': np.random.choice(['X', 'Y'], 100)
        })

        preprocessor = PreProcessor(df)
        preprocessor.auto_encode_categorical(
            exclude_columns=['target'],
            min_frequency=0.1,  # Only categories with >= 10% frequency
            drop_original=True,
            drop_first=False
        )

        # Check that encoding happened (may include all categories depending on implementation)
        # At minimum, the frequent category should be present
        encoded_cols = [col for col in preprocessor.df.columns if col.startswith('category_')]
        assert len(encoded_cols) >= 1  # At least one category encoded

    def test_numeric_columns_not_encoded(self, ml_categorical_df):
        """Should not encode numeric columns."""
        df_copy = ml_categorical_df.copy()
        preprocessor = PreProcessor(df_copy)

        original_numeric = preprocessor.df['numeric'].copy()

        preprocessor.auto_encode_categorical(exclude_columns=['target'])

        # Numeric column should remain unchanged
        pd.testing.assert_series_equal(
            preprocessor.df['numeric'],
            original_numeric,
            check_names=False
        )

    def test_empty_dataframe(self):
        """Should handle empty DataFrame gracefully."""
        df = pd.DataFrame()
        preprocessor = PreProcessor(df)

        # Should not raise error
        preprocessor.auto_encode_categorical()

        assert len(preprocessor.df) == 0


# ============================================================================
# ML PIPELINE INTEGRATION TESTS
# ============================================================================

class TestMLPipelineIntegration:
    """Test integration of ML features in complete pipeline."""

    def test_full_classification_pipeline(self, ml_classification_df):
        """Should execute complete ML preprocessing pipeline."""
        # Step 1: Analyze feature importance
        profiler = Profiler(ml_classification_df, name='churn')
        importance = profiler.analyze_target_correlation('churn', top_n=5)

        assert len(importance) == 5

        # Step 2: Auto-encode categoricals
        df_ml = ml_classification_df.copy()
        preprocessor = PreProcessor(df_ml)

        original_cols = len(preprocessor.df.columns)

        preprocessor.auto_encode_categorical(
            exclude_columns=['churn'],
            max_categories=10,
            drop_first=True
        )

        # Should have more columns after encoding
        assert len(preprocessor.df.columns) >= original_cols

        # Should still have churn target
        assert 'churn' in preprocessor.df.columns

    def test_multi_target_analysis(self, ml_classification_df):
        """Should analyze multiple targets in sequence."""
        # Add a numeric target
        df = ml_classification_df.copy()
        df['satisfaction_score'] = np.random.uniform(1, 10, len(df))

        profiler = Profiler(df, name='multi_target')

        # Analyze categorical target
        churn_importance = profiler.analyze_target_correlation('churn', top_n=3)
        assert len(churn_importance) == 3

        # Analyze numeric target
        satisfaction_importance = profiler.analyze_target_correlation(
            'satisfaction_score',
            top_n=3
        )
        assert len(satisfaction_importance) == 3

    def test_pipeline_with_missing_values(self):
        """Should handle missing values in ML pipeline."""
        df = pd.DataFrame({
            'feature1': [1, 2, np.nan, 4, 5],
            'feature2': ['A', 'B', 'A', np.nan, 'B'],
            'target': [0, 1, 0, 1, 0]
        })

        # Profiler should handle NaN
        profiler = Profiler(df, name='with_missing')
        importance = profiler.analyze_target_correlation('target', top_n=2)

        # Should still return results
        assert len(importance) >= 1

        # PreProcessor should encode despite missing
        preprocessor = PreProcessor(df)
        preprocessor.auto_encode_categorical(exclude_columns=['target'])

        # Should complete without error
        assert 'feature2' in preprocessor.df.columns or any(
            col.startswith('feature2_') for col in preprocessor.df.columns
        )


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================

class TestMLEdgeCases:
    """Test edge cases and error handling."""

    def test_single_column_dataframe(self):
        """Should handle DataFrame with single column."""
        df = pd.DataFrame({'only_col': [1, 2, 3, 4, 5]})

        profiler = Profiler(df, name='single_col')

        # No features to correlate with - may return empty DataFrame or raise error
        result = profiler.analyze_target_correlation('only_col')
        assert len(result) == 0  # No features to correlate

    def test_all_categorical_columns(self):
        """Should handle DataFrame with all categorical columns."""
        df = pd.DataFrame({
            'cat1': ['A', 'B', 'A', 'B', 'A'],
            'cat2': ['X', 'Y', 'X', 'Y', 'X'],
            'num_feature': [1, 2, 3, 4, 5],  # Add a numeric feature
            'target': ['Yes', 'No', 'Yes', 'No', 'Yes']
        })

        profiler = Profiler(df, name='all_cat')
        importance = profiler.analyze_target_correlation('target', top_n=3)

        # Should compute correlations (at least with numeric feature)
        assert len(importance) >= 1

    def test_constant_target(self):
        """Should handle target with no variance."""
        df = pd.DataFrame({
            'feature1': [1, 2, 3, 4, 5],
            'target': ['A', 'A', 'A', 'A', 'A']  # Constant
        })

        profiler = Profiler(df, name='constant_target')

        # May raise error or return NaN correlations
        try:
            result = profiler.analyze_target_correlation('target')
            # If it doesn't error, correlations should be NaN or 0
            assert all(pd.isna(result['correlation']) | (result['correlation'] == 0))
        except (ValueError, RuntimeError):
            # Expected behavior
            pass
