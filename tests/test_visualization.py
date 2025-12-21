"""
Tests for the visualization module.

These tests verify the DataVisualizer class and all visualization methods.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
import sys


# Test if matplotlib and seaborn are available
try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend for testing
    import matplotlib.pyplot as plt
    import seaborn as sns
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False


from schema_mapper.visualization import DataVisualizer, _ensure_viz_dependencies


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_numeric_df():
    """Sample DataFrame with numeric columns."""
    np.random.seed(42)
    return pd.DataFrame({
        'age': np.random.randint(18, 80, 100),
        'salary': np.random.randint(30000, 150000, 100),
        'experience': np.random.randint(0, 40, 100),
        'satisfaction': np.random.uniform(1, 5, 100)
    })


@pytest.fixture
def sample_mixed_df():
    """Sample DataFrame with mixed column types."""
    np.random.seed(42)
    return pd.DataFrame({
        'age': np.random.randint(18, 80, 100),
        'salary': np.random.randint(30000, 150000, 100),
        'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR'], 100),
        'gender': np.random.choice(['M', 'F'], 100),
        'active': np.random.choice([True, False], 100)
    })


@pytest.fixture
def sample_df_with_missing():
    """Sample DataFrame with missing values."""
    np.random.seed(42)
    df = pd.DataFrame({
        'col1': np.random.randn(100),
        'col2': np.random.randn(100),
        'col3': np.random.randn(100),
        'col4': np.random.randn(100)
    })
    # Introduce missing values
    df.loc[0:10, 'col1'] = np.nan
    df.loc[0:25, 'col2'] = np.nan
    df.loc[0:5, 'col3'] = np.nan
    return df


@pytest.fixture
def sample_time_series_df():
    """Sample DataFrame with time series data."""
    dates = pd.date_range('2023-01-01', periods=100, freq='D')
    np.random.seed(42)
    return pd.DataFrame({
        'date': dates,
        'sales': np.random.randint(1000, 5000, 100),
        'revenue': np.random.randint(10000, 50000, 100)
    })


# ============================================================================
# DEPENDENCY TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_ensure_viz_dependencies_success():
    """Test that _ensure_viz_dependencies returns plt and sns when available."""
    plt_module, sns_module = _ensure_viz_dependencies()
    assert plt_module is not None
    assert sns_module is not None
    assert hasattr(plt_module, 'subplots')
    assert hasattr(sns_module, 'set_style')


def test_ensure_viz_dependencies_failure():
    """Test that _ensure_viz_dependencies raises ImportError when dependencies unavailable."""
    with patch.dict(sys.modules, {'matplotlib.pyplot': None, 'seaborn': None}):
        with pytest.raises(ImportError, match="Visualization dependencies not installed"):
            # Force reimport to trigger the error
            import importlib
            import schema_mapper.visualization as viz_module
            importlib.reload(viz_module)
            viz_module._ensure_viz_dependencies()


# ============================================================================
# HISTOGRAM TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_histogram_basic(sample_numeric_df):
    """Test basic histogram plotting."""
    fig = DataVisualizer.plot_histogram(sample_numeric_df)

    assert fig is not None
    assert len(fig.axes) >= 4  # Should have 4 numeric columns

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_histogram_specific_columns(sample_numeric_df):
    """Test histogram plotting with specific columns."""
    fig = DataVisualizer.plot_histogram(sample_numeric_df, columns=['age', 'salary'])

    assert fig is not None
    assert len([ax for ax in fig.axes if ax.get_visible()]) == 2

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_histogram_custom_params(sample_numeric_df):
    """Test histogram with custom parameters."""
    fig = DataVisualizer.plot_histogram(
        sample_numeric_df,
        bins=20,
        figsize=(15, 10),
        title="Custom Title"
    )

    assert fig is not None
    assert fig._suptitle is not None
    assert "Custom Title" in fig._suptitle.get_text()

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_histogram_no_numeric_columns():
    """Test histogram with no numeric columns."""
    df = pd.DataFrame({'category': ['A', 'B', 'C']})
    fig = DataVisualizer.plot_histogram(df)

    assert fig is None


# ============================================================================
# BOXPLOT TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_boxplot_basic(sample_numeric_df):
    """Test basic boxplot plotting."""
    fig = DataVisualizer.plot_boxplot(sample_numeric_df)

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_boxplot_specific_columns(sample_numeric_df):
    """Test boxplot with specific columns."""
    fig = DataVisualizer.plot_boxplot(sample_numeric_df, columns=['age', 'salary'])

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_boxplot_custom_title(sample_numeric_df):
    """Test boxplot with custom title."""
    fig = DataVisualizer.plot_boxplot(sample_numeric_df, title="Custom Boxplot")

    assert fig is not None
    assert "Custom Boxplot" in fig.axes[0].get_title()

    plt.close(fig)


# ============================================================================
# SCATTER PLOT TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_scatter_basic(sample_numeric_df):
    """Test basic scatter plot."""
    fig = DataVisualizer.plot_scatter(sample_numeric_df, x='age', y='salary')

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_scatter_with_hue(sample_mixed_df):
    """Test scatter plot with hue parameter."""
    fig = DataVisualizer.plot_scatter(sample_mixed_df, x='age', y='salary', hue='department')

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_scatter_invalid_column(sample_numeric_df):
    """Test scatter plot with invalid column name."""
    with pytest.raises(KeyError, match="not found in DataFrame"):
        DataVisualizer.plot_scatter(sample_numeric_df, x='invalid', y='salary')


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_scatter_custom_params(sample_numeric_df):
    """Test scatter plot with custom parameters."""
    fig = DataVisualizer.plot_scatter(
        sample_numeric_df,
        x='age',
        y='salary',
        figsize=(12, 8),
        title="Age vs Salary"
    )

    assert fig is not None
    assert "Age vs Salary" in fig.axes[0].get_title()

    plt.close(fig)


# ============================================================================
# CORRELATION MATRIX TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_correlation_matrix_basic(sample_numeric_df):
    """Test basic correlation matrix."""
    fig = DataVisualizer.plot_correlation_matrix(sample_numeric_df)

    assert fig is not None
    assert len(fig.axes) == 2  # Plot + colorbar

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_correlation_matrix_methods(sample_numeric_df):
    """Test correlation matrix with different methods."""
    for method in ['pearson', 'spearman', 'kendall']:
        fig = DataVisualizer.plot_correlation_matrix(sample_numeric_df, method=method)
        assert fig is not None
        plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_correlation_matrix_no_annot(sample_numeric_df):
    """Test correlation matrix without annotations."""
    fig = DataVisualizer.plot_correlation_matrix(sample_numeric_df, annot=False)

    assert fig is not None

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_correlation_matrix_insufficient_columns():
    """Test correlation matrix with insufficient numeric columns."""
    df = pd.DataFrame({'col1': [1, 2, 3]})
    fig = DataVisualizer.plot_correlation_matrix(df)

    assert fig is None


# ============================================================================
# MISSING VALUES TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_missing_values_basic(sample_df_with_missing):
    """Test basic missing values plot."""
    fig = DataVisualizer.plot_missing_values(sample_df_with_missing)

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_missing_values_no_missing():
    """Test missing values plot with no missing data."""
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
    fig = DataVisualizer.plot_missing_values(df)

    assert fig is None


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_missing_values_custom_title(sample_df_with_missing):
    """Test missing values plot with custom title."""
    fig = DataVisualizer.plot_missing_values(sample_df_with_missing, title="Custom Missing")

    assert fig is not None
    assert "Custom Missing" in fig.axes[0].get_title()

    plt.close(fig)


# ============================================================================
# VALUE COUNTS TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_value_counts_basic(sample_mixed_df):
    """Test basic value counts plot."""
    fig = DataVisualizer.plot_value_counts(sample_mixed_df, column='department')

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_value_counts_custom_top_n(sample_mixed_df):
    """Test value counts with custom top_n."""
    fig = DataVisualizer.plot_value_counts(sample_mixed_df, column='department', top_n=2)

    assert fig is not None

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_value_counts_invalid_column(sample_mixed_df):
    """Test value counts with invalid column."""
    with pytest.raises(KeyError, match="not found in DataFrame"):
        DataVisualizer.plot_value_counts(sample_mixed_df, column='invalid')


# ============================================================================
# DISTRIBUTION COMPARISON TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_distribution_comparison_numeric(sample_numeric_df):
    """Test distribution comparison for numeric columns."""
    df1 = sample_numeric_df.copy()
    df2 = sample_numeric_df.copy()
    df2['age'] = df2['age'] + 10  # Shift distribution

    fig = DataVisualizer.plot_distribution_comparison(df1, df2, column='age')

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_distribution_comparison_categorical(sample_mixed_df):
    """Test distribution comparison for categorical columns."""
    df1 = sample_mixed_df.copy()
    df2 = sample_mixed_df.copy()

    fig = DataVisualizer.plot_distribution_comparison(df1, df2, column='department')

    assert fig is not None

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_distribution_comparison_custom_labels(sample_numeric_df):
    """Test distribution comparison with custom labels."""
    df1 = sample_numeric_df.copy()
    df2 = sample_numeric_df.copy()

    fig = DataVisualizer.plot_distribution_comparison(
        df1, df2, column='age',
        labels=('Before', 'After'),
        title='Age Distribution Change'
    )

    assert fig is not None
    assert 'Age Distribution Change' in fig.axes[0].get_title()

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_distribution_comparison_invalid_column(sample_numeric_df):
    """Test distribution comparison with invalid column."""
    df1 = sample_numeric_df.copy()
    df2 = sample_numeric_df.copy()

    with pytest.raises(KeyError, match="not found"):
        DataVisualizer.plot_distribution_comparison(df1, df2, column='invalid')


# ============================================================================
# TIME SERIES TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_time_series_basic(sample_time_series_df):
    """Test basic time series plot."""
    fig = DataVisualizer.plot_time_series(
        sample_time_series_df,
        date_column='date',
        value_columns=['sales']
    )

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_time_series_multiple_columns(sample_time_series_df):
    """Test time series plot with multiple value columns."""
    fig = DataVisualizer.plot_time_series(
        sample_time_series_df,
        date_column='date',
        value_columns=['sales', 'revenue']
    )

    assert fig is not None
    assert len(fig.axes) == 1

    plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_time_series_invalid_column(sample_time_series_df):
    """Test time series plot with invalid column."""
    with pytest.raises(KeyError, match="not found"):
        DataVisualizer.plot_time_series(
            sample_time_series_df,
            date_column='invalid',
            value_columns=['sales']
        )


# ============================================================================
# PAIRPLOT TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_pairplot_basic(sample_numeric_df):
    """Test basic pairplot."""
    pairplot = DataVisualizer.plot_pairplot(sample_numeric_df)

    assert pairplot is not None
    assert hasattr(pairplot, 'fig')

    plt.close(pairplot.fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_pairplot_with_hue(sample_mixed_df):
    """Test pairplot with hue parameter."""
    # Select only numeric columns for pairplot
    numeric_cols = ['age', 'salary']
    pairplot = DataVisualizer.plot_pairplot(
        sample_mixed_df,
        columns=numeric_cols,
        hue='department'
    )

    assert pairplot is not None

    plt.close(pairplot.fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_plot_pairplot_insufficient_columns():
    """Test pairplot with insufficient columns."""
    df = pd.DataFrame({'col1': [1, 2, 3]})
    pairplot = DataVisualizer.plot_pairplot(df)

    assert pairplot is None


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_all_plots_return_saveable_figures(sample_numeric_df, sample_mixed_df, tmp_path):
    """Test that all plot methods return figures that can be saved."""
    import os

    # Histogram
    fig = DataVisualizer.plot_histogram(sample_numeric_df)
    if fig:
        filepath = tmp_path / "histogram.png"
        fig.savefig(filepath)
        assert os.path.exists(filepath)
        plt.close(fig)

    # Boxplot
    fig = DataVisualizer.plot_boxplot(sample_numeric_df)
    if fig:
        filepath = tmp_path / "boxplot.png"
        fig.savefig(filepath)
        assert os.path.exists(filepath)
        plt.close(fig)

    # Scatter
    fig = DataVisualizer.plot_scatter(sample_numeric_df, x='age', y='salary')
    if fig:
        filepath = tmp_path / "scatter.png"
        fig.savefig(filepath)
        assert os.path.exists(filepath)
        plt.close(fig)

    # Correlation matrix
    fig = DataVisualizer.plot_correlation_matrix(sample_numeric_df)
    if fig:
        filepath = tmp_path / "correlation.png"
        fig.savefig(filepath)
        assert os.path.exists(filepath)
        plt.close(fig)


@pytest.mark.skipif(not VISUALIZATION_AVAILABLE, reason="Visualization dependencies not installed")
def test_visualization_with_profiler_integration(sample_numeric_df):
    """Test that DataVisualizer works with Profiler."""
    from schema_mapper import Profiler

    profiler = Profiler(sample_numeric_df, name="test_dataset")

    # Test that profiler methods work with new visualizer
    fig = profiler.plot_distributions()
    assert fig is not None
    plt.close(fig)

    fig = profiler.plot_correlations()
    assert fig is not None
    plt.close(fig)

    fig = profiler.plot_outliers()
    assert fig is not None
    plt.close(fig)
