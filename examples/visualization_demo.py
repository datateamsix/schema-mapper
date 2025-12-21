"""
Demo: Data Visualization with Schema Mapper

This example demonstrates the DataVisualizer module which provides easy-to-use
visualization functions without requiring users to import matplotlib or seaborn.
"""

import pandas as pd
import numpy as np
from schema_mapper import DataVisualizer, Profiler

# Suppress warnings for cleaner output
import warnings
warnings.filterwarnings('ignore')


def create_sample_data():
    """Create sample dataset for demonstration."""
    np.random.seed(42)

    # Generate synthetic employee data
    n_employees = 200

    data = {
        'employee_id': range(1, n_employees + 1),
        'age': np.random.randint(22, 65, n_employees),
        'salary': np.random.randint(40000, 150000, n_employees),
        'years_experience': np.random.randint(0, 40, n_employees),
        'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'], n_employees),
        'performance_score': np.random.uniform(1, 5, n_employees),
        'satisfaction': np.random.uniform(1, 10, n_employees),
    }

    df = pd.DataFrame(data)

    # Introduce some missing values
    df.loc[np.random.choice(df.index, 15, replace=False), 'performance_score'] = np.nan
    df.loc[np.random.choice(df.index, 10, replace=False), 'satisfaction'] = np.nan

    # Add some outliers
    df.loc[np.random.choice(df.index, 5, replace=False), 'salary'] = np.random.randint(200000, 500000, 5)

    return df


def demo_direct_visualizer():
    """Demonstrate using DataVisualizer directly."""
    print("=" * 80)
    print("DEMO 1: Using DataVisualizer Directly")
    print("=" * 80)

    df = create_sample_data()
    print(f"\nCreated sample dataset with {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {', '.join(df.columns)}")

    # 1. Histogram - Distribution of numeric columns
    print("\n1. Creating histograms for numeric distributions...")
    fig = DataVisualizer.plot_histogram(
        df,
        columns=['age', 'salary', 'years_experience', 'performance_score'],
        bins=25,
        title="Employee Metrics Distributions"
    )
    if fig:
        fig.savefig('output/histograms.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/histograms.png")

    # 2. Boxplot - Outlier detection
    print("\n2. Creating boxplots for outlier detection...")
    fig = DataVisualizer.plot_boxplot(
        df,
        columns=['age', 'salary', 'years_experience'],
        title="Outlier Detection - Employee Metrics"
    )
    if fig:
        fig.savefig('output/boxplots.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/boxplots.png")

    # 3. Scatter plot - Relationship between variables
    print("\n3. Creating scatter plot (Salary vs Experience)...")
    fig = DataVisualizer.plot_scatter(
        df,
        x='years_experience',
        y='salary',
        hue='department',
        title="Salary vs Years of Experience by Department"
    )
    if fig:
        fig.savefig('output/scatter.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/scatter.png")

    # 4. Correlation matrix
    print("\n4. Creating correlation heatmap...")
    fig = DataVisualizer.plot_correlation_matrix(
        df,
        method='pearson',
        title="Employee Metrics Correlation Matrix"
    )
    if fig:
        fig.savefig('output/correlations.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/correlations.png")

    # 5. Missing values visualization
    print("\n5. Visualizing missing values...")
    fig = DataVisualizer.plot_missing_values(
        df,
        title="Missing Values Analysis"
    )
    if fig:
        fig.savefig('output/missing_values.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/missing_values.png")

    # 6. Value counts - Categorical distribution
    print("\n6. Creating value counts plot for departments...")
    fig = DataVisualizer.plot_value_counts(
        df,
        column='department',
        top_n=5,
        title="Employee Distribution by Department"
    )
    if fig:
        fig.savefig('output/department_counts.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/department_counts.png")

    # 7. Pairplot - Explore relationships
    print("\n7. Creating pairplot for key metrics...")
    pairplot = DataVisualizer.plot_pairplot(
        df,
        columns=['age', 'salary', 'years_experience', 'satisfaction'],
        hue='department'
    )
    if pairplot:
        pairplot.savefig('output/pairplot.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/pairplot.png")


def demo_profiler_integration():
    """Demonstrate using Profiler with integrated visualizations."""
    print("\n" + "=" * 80)
    print("DEMO 2: Using Profiler with Integrated Visualization")
    print("=" * 80)

    df = create_sample_data()

    # Create profiler
    profiler = Profiler(df, name="employee_data", show_progress=False)

    print(f"\nInitialized Profiler for dataset with {len(df)} rows")

    # 1. Quick statistical summary
    print("\n1. Generating statistical summary...")
    summary = profiler.get_summary_stats()
    print(f"   ✓ Generated stats for {len(summary)} columns")

    # 2. Quality assessment
    print("\n2. Assessing data quality...")
    quality = profiler.assess_quality()
    print(f"   ✓ Overall Quality Score: {quality['overall_score']:.2f}/100")
    print(f"   ✓ Interpretation: {quality['interpretation']}")

    # 3. Visualizations through profiler
    print("\n3. Creating visualizations through Profiler...")

    # Distribution plots
    print("   - Generating distribution plots...")
    fig = profiler.plot_distributions()
    if fig:
        fig.savefig('output/profiler_distributions.png', dpi=300, bbox_inches='tight')
        print("     ✓ Saved to: output/profiler_distributions.png")

    # Correlation heatmap
    print("   - Generating correlation heatmap...")
    fig = profiler.plot_correlations(method='spearman')
    if fig:
        fig.savefig('output/profiler_correlations.png', dpi=300, bbox_inches='tight')
        print("     ✓ Saved to: output/profiler_correlations.png")

    # Missing values
    print("   - Visualizing missing values...")
    fig = profiler.plot_missing_values()
    if fig:
        fig.savefig('output/profiler_missing.png', dpi=300, bbox_inches='tight')
        print("     ✓ Saved to: output/profiler_missing.png")

    # Outliers
    print("   - Detecting outliers with boxplots...")
    fig = profiler.plot_outliers()
    if fig:
        fig.savefig('output/profiler_outliers.png', dpi=300, bbox_inches='tight')
        print("     ✓ Saved to: output/profiler_outliers.png")


def demo_comparison_visualization():
    """Demonstrate before/after comparison visualization."""
    print("\n" + "=" * 80)
    print("DEMO 3: Before/After Comparison Visualization")
    print("=" * 80)

    # Create original data
    df_before = create_sample_data()

    # Simulate data cleaning
    df_after = df_before.copy()
    df_after['salary'] = df_after['salary'].clip(upper=150000)  # Remove outliers
    df_after['performance_score'].fillna(df_after['performance_score'].mean(), inplace=True)  # Fill missing
    df_after['satisfaction'].fillna(df_after['satisfaction'].median(), inplace=True)  # Fill missing

    print("\nComparing distributions before and after data cleaning...")

    # Compare salary distribution
    print("\n1. Comparing salary distribution (outliers removed)...")
    fig = DataVisualizer.plot_distribution_comparison(
        df_before,
        df_after,
        column='salary',
        labels=('Before Cleaning', 'After Cleaning'),
        title="Salary Distribution: Before vs After Outlier Removal"
    )
    if fig:
        fig.savefig('output/comparison_salary.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/comparison_salary.png")

    # Compare performance score (missing values filled)
    print("\n2. Comparing performance score distribution (missing values filled)...")
    fig = DataVisualizer.plot_distribution_comparison(
        df_before,
        df_after,
        column='performance_score',
        labels=('With Missing', 'Missing Filled'),
        title="Performance Score: Before vs After Missing Value Imputation"
    )
    if fig:
        fig.savefig('output/comparison_performance.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/comparison_performance.png")


def demo_time_series_visualization():
    """Demonstrate time series visualization."""
    print("\n" + "=" * 80)
    print("DEMO 4: Time Series Visualization")
    print("=" * 80)

    # Create sample time series data
    dates = pd.date_range('2023-01-01', periods=365, freq='D')
    np.random.seed(42)

    # Simulate sales data with trend and seasonality
    trend = np.linspace(10000, 15000, 365)
    seasonality = 2000 * np.sin(np.arange(365) * 2 * np.pi / 365)
    noise = np.random.normal(0, 500, 365)

    df_time = pd.DataFrame({
        'date': dates,
        'sales': trend + seasonality + noise,
        'revenue': (trend + seasonality + noise) * 1.2 + np.random.normal(0, 1000, 365)
    })

    print(f"\nCreated time series data with {len(df_time)} days")

    print("\n1. Plotting sales and revenue over time...")
    fig = DataVisualizer.plot_time_series(
        df_time,
        date_column='date',
        value_columns=['sales', 'revenue'],
        title="Sales and Revenue Trends (2023)"
    )
    if fig:
        fig.savefig('output/time_series.png', dpi=300, bbox_inches='tight')
        print("   ✓ Saved to: output/time_series.png")


def main():
    """Run all demonstrations."""
    import os

    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)

    print("\n" + "=" * 80)
    print("Schema Mapper - Data Visualization Demo")
    print("=" * 80)
    print("\nThis demo showcases the DataVisualizer module capabilities.")
    print("All visualizations will be saved to the 'output/' directory.")

    try:
        # Run demos
        demo_direct_visualizer()
        demo_profiler_integration()
        demo_comparison_visualization()
        demo_time_series_visualization()

        print("\n" + "=" * 80)
        print("✓ ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print("\nCheck the 'output/' directory for all generated visualizations.")
        print("\nKey Takeaways:")
        print("  • DataVisualizer provides easy-to-use visualization methods")
        print("  • No need to import matplotlib or seaborn directly")
        print("  • Sensible defaults with customization options")
        print("  • Seamlessly integrates with Profiler class")
        print("  • Returns Figure objects for further customization")

    except ImportError as e:
        print(f"\n❌ Error: {e}")
        print("\nTo run this demo, install visualization dependencies:")
        print("  pip install matplotlib seaborn")
        print("  OR")
        print("  pip install schema-mapper[viz]")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
