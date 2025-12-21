"""
Data Visualization - Wrapper for matplotlib and seaborn visualization functions.

This module provides common data visualization patterns with sensible defaults,
hiding the complexity of matplotlib/seaborn from users.
"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Optional, Any
import logging

logger = logging.getLogger(__name__)


def _ensure_viz_dependencies():
    """
    Import matplotlib and seaborn only when needed.

    Returns:
        Tuple of (plt, sns) modules

    Raises:
        ImportError: If visualization dependencies are not installed
    """
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        # Set professional style
        sns.set_style("whitegrid")
        sns.set_palette("husl")
        return plt, sns
    except ImportError:
        raise ImportError(
            "Visualization dependencies not installed. "
            "Install with: pip install schema-mapper[viz] or pip install matplotlib seaborn"
        )


class DataVisualizer:
    """
    Wrapper for matplotlib and seaborn visualization functions.

    Provides common data visualization patterns with sensible defaults,
    hiding the complexity of matplotlib/seaborn from users.

    All methods are static and return matplotlib Figure objects for further
    customization and saving.

    Example:
        >>> from schema_mapper import DataVisualizer
        >>> import pandas as pd
        >>>
        >>> df = pd.read_csv('data.csv')
        >>>
        >>> # Generate visualizations without importing matplotlib/seaborn
        >>> fig = DataVisualizer.plot_histogram(df, columns=['age', 'salary'])
        >>> fig.savefig('distributions.png')
        >>>
        >>> fig = DataVisualizer.plot_correlation_matrix(df)
        >>> fig.savefig('correlations.png')
    """

    @staticmethod
    def plot_histogram(
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        bins: int = 30,
        figsize: Tuple[int, int] = (12, 8),
        title: Optional[str] = None
    ):
        """
        Generate histograms for numeric columns.

        Args:
            df: DataFrame to visualize
            columns: Specific columns to plot (default: all numeric)
            bins: Number of histogram bins (default: 30)
            figsize: Figure size tuple (width, height) (default: (12, 8))
            title: Overall figure title (default: None)

        Returns:
            Matplotlib Figure object

        Example:
            >>> fig = DataVisualizer.plot_histogram(df, columns=['age', 'income'])
            >>> fig.savefig('histograms.png', dpi=300, bbox_inches='tight')
        """
        plt, sns = _ensure_viz_dependencies()

        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()

        if len(columns) == 0:
            logger.warning("No numeric columns to plot")
            return None

        # Calculate grid dimensions
        n_cols = len(columns)
        n_plot_cols = min(3, n_cols)
        n_rows = (n_cols + n_plot_cols - 1) // n_plot_cols

        fig, axes = plt.subplots(n_rows, n_plot_cols, figsize=figsize)

        # Handle single subplot case
        if n_cols == 1:
            axes = [axes]
        else:
            axes = axes.flatten() if n_cols > 1 else [axes]

        # Plot histograms
        for idx, col in enumerate(columns):
            if col not in df.columns:
                logger.warning(f"Column '{col}' not found in DataFrame")
                continue

            series = df[col].dropna()

            if len(series) == 0:
                logger.warning(f"Column '{col}' has no data to plot")
                continue

            # Create histogram with KDE overlay
            axes[idx].hist(series, bins=bins, alpha=0.7, color='steelblue',
                          edgecolor='black', density=True, label='Histogram')

            # Add KDE overlay
            try:
                series.plot.kde(ax=axes[idx], color='red', linewidth=2, label='KDE')
                axes[idx].legend()
            except:
                pass  # KDE might fail for some distributions

            axes[idx].set_title(f'{col} Distribution', fontsize=10, fontweight='bold')
            axes[idx].set_xlabel(col, fontsize=9)
            axes[idx].set_ylabel('Density', fontsize=9)
            axes[idx].grid(True, alpha=0.3)

        # Hide unused subplots
        for idx in range(n_cols, len(axes)):
            axes[idx].set_visible(False)

        if title:
            fig.suptitle(title, fontsize=14, fontweight='bold', y=1.00)

        plt.tight_layout()
        return fig

    @staticmethod
    def plot_boxplot(
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        figsize: Tuple[int, int] = (12, 6),
        title: Optional[str] = None
    ):
        """
        Generate box plots for outlier detection.

        Args:
            df: DataFrame to visualize
            columns: Specific columns to plot (default: all numeric)
            figsize: Figure size tuple (default: (12, 6))
            title: Figure title (default: None)

        Returns:
            Matplotlib Figure object

        Example:
            >>> fig = DataVisualizer.plot_boxplot(df, columns=['age', 'salary'])
            >>> fig.savefig('outliers.png')
        """
        plt, sns = _ensure_viz_dependencies()

        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()

        if len(columns) == 0:
            logger.warning("No numeric columns to plot")
            return None

        fig, ax = plt.subplots(figsize=figsize)

        # Use seaborn for better styling
        df[columns].boxplot(ax=ax, patch_artist=True,
                           boxprops=dict(facecolor='lightblue', alpha=0.7),
                           medianprops=dict(color='red', linewidth=2),
                           flierprops=dict(marker='o', markerfacecolor='red',
                                          markersize=5, alpha=0.5))

        ax.set_title(title or 'Box Plots for Outlier Detection',
                    fontsize=12, fontweight='bold')
        ax.set_ylabel('Value', fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        return fig

    @staticmethod
    def plot_scatter(
        df: pd.DataFrame,
        x: str,
        y: str,
        hue: Optional[str] = None,
        figsize: Tuple[int, int] = (10, 6),
        title: Optional[str] = None
    ):
        """
        Generate scatter plot for two numeric columns.

        Args:
            df: DataFrame to visualize
            x: Column name for x-axis
            y: Column name for y-axis
            hue: Column name for color grouping (optional)
            figsize: Figure size tuple (default: (10, 6))
            title: Figure title (default: None)

        Returns:
            Matplotlib Figure object

        Example:
            >>> fig = DataVisualizer.plot_scatter(df, x='age', y='salary', hue='department')
            >>> fig.savefig('scatter.png')
        """
        plt, sns = _ensure_viz_dependencies()

        if x not in df.columns:
            raise KeyError(f"Column '{x}' not found in DataFrame")
        if y not in df.columns:
            raise KeyError(f"Column '{y}' not found in DataFrame")
        if hue and hue not in df.columns:
            raise KeyError(f"Column '{hue}' not found in DataFrame")

        fig, ax = plt.subplots(figsize=figsize)

        # Use seaborn for better styling and automatic hue handling
        sns.scatterplot(data=df, x=x, y=y, hue=hue, ax=ax,
                       alpha=0.6, s=50, edgecolor='black', linewidth=0.5)

        ax.set_title(title or f'{y} vs {x}', fontsize=12, fontweight='bold')
        ax.set_xlabel(x, fontsize=10)
        ax.set_ylabel(y, fontsize=10)
        ax.grid(True, alpha=0.3)

        # Add regression line if no hue
        if hue is None:
            try:
                z = np.polyfit(df[x].dropna(), df[y].dropna(), 1)
                p = np.poly1d(z)
                ax.plot(df[x], p(df[x]), "r--", alpha=0.8, linewidth=2, label='Trend')
                ax.legend()
            except:
                pass

        plt.tight_layout()
        return fig

    @staticmethod
    def plot_correlation_matrix(
        df: pd.DataFrame,
        method: str = 'pearson',
        figsize: Tuple[int, int] = (10, 8),
        annot: bool = True,
        title: Optional[str] = None
    ):
        """
        Generate correlation heatmap.

        Args:
            df: DataFrame to visualize
            method: Correlation method ('pearson', 'spearman', 'kendall')
            figsize: Figure size tuple (default: (10, 8))
            annot: Whether to annotate cells with values (default: True)
            title: Figure title (default: None)

        Returns:
            Matplotlib Figure object

        Example:
            >>> fig = DataVisualizer.plot_correlation_matrix(df, method='spearman')
            >>> fig.savefig('correlations.png')
        """
        plt, sns = _ensure_viz_dependencies()

        # Get numeric columns only
        numeric_df = df.select_dtypes(include=[np.number])

        if numeric_df.shape[1] < 2:
            logger.warning("Need at least 2 numeric columns for correlation matrix")
            return None

        # Calculate correlation
        corr = numeric_df.corr(method=method)

        fig, ax = plt.subplots(figsize=figsize)

        # Create heatmap
        sns.heatmap(corr, annot=annot, fmt='.2f', cmap='coolwarm', center=0,
                   square=True, linewidths=1, cbar_kws={"shrink": 0.8},
                   vmin=-1, vmax=1, ax=ax)

        ax.set_title(title or f'Correlation Matrix ({method.capitalize()})',
                    fontsize=12, fontweight='bold')

        plt.tight_layout()
        return fig

    @staticmethod
    def plot_missing_values(
        df: pd.DataFrame,
        figsize: Tuple[int, int] = (10, 6),
        title: Optional[str] = None
    ):
        """
        Visualize missing value patterns.

        Args:
            df: DataFrame to visualize
            figsize: Figure size tuple (default: (10, 6))
            title: Figure title (default: None)

        Returns:
            Matplotlib Figure object or None if no missing values

        Example:
            >>> fig = DataVisualizer.plot_missing_values(df)
            >>> if fig:
            ...     fig.savefig('missing_values.png')
        """
        plt, sns = _ensure_viz_dependencies()

        missing = df.isna().sum()
        missing = missing[missing > 0].sort_values(ascending=False)

        if len(missing) == 0:
            logger.info("No missing values to plot")
            return None

        fig, ax = plt.subplots(figsize=figsize)

        # Create bar plot
        missing.plot(kind='bar', ax=ax, color='coral', edgecolor='black', alpha=0.7)

        ax.set_title(title or 'Missing Values by Column',
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Column', fontsize=10)
        ax.set_ylabel('Missing Count', fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')

        # Add percentage labels on bars
        for i, (col, val) in enumerate(missing.items()):
            pct = (val / len(df)) * 100
            ax.text(i, val, f'{pct:.1f}%', ha='center', va='bottom', fontsize=8)

        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        return fig

    @staticmethod
    def plot_value_counts(
        df: pd.DataFrame,
        column: str,
        top_n: int = 10,
        figsize: Tuple[int, int] = (10, 6),
        title: Optional[str] = None
    ):
        """
        Bar plot of top N values in categorical column.

        Args:
            df: DataFrame to visualize
            column: Column name to analyze
            top_n: Number of top values to show (default: 10)
            figsize: Figure size tuple (default: (10, 6))
            title: Figure title (default: None)

        Returns:
            Matplotlib Figure object

        Example:
            >>> fig = DataVisualizer.plot_value_counts(df, 'category', top_n=15)
            >>> fig.savefig('top_categories.png')
        """
        plt, sns = _ensure_viz_dependencies()

        if column not in df.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame")

        value_counts = df[column].value_counts().head(top_n)

        if len(value_counts) == 0:
            logger.warning(f"No data in column '{column}'")
            return None

        fig, ax = plt.subplots(figsize=figsize)

        # Create bar plot
        value_counts.plot(kind='barh', ax=ax, color='steelblue',
                         edgecolor='black', alpha=0.7)

        ax.set_title(title or f'Top {top_n} Values in {column}',
                    fontsize=12, fontweight='bold')
        ax.set_xlabel('Count', fontsize=10)
        ax.set_ylabel(column, fontsize=10)
        ax.grid(True, alpha=0.3, axis='x')

        # Invert y-axis to show highest value on top
        ax.invert_yaxis()

        # Add count labels
        for i, (val, count) in enumerate(value_counts.items()):
            ax.text(count, i, f' {count:,}', va='center', fontsize=8)

        plt.tight_layout()
        return fig

    @staticmethod
    def plot_distribution_comparison(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        column: str,
        labels: Tuple[str, str] = ('Dataset 1', 'Dataset 2'),
        figsize: Tuple[int, int] = (10, 6),
        title: Optional[str] = None
    ):
        """
        Compare distributions between two datasets (e.g., before/after).

        Args:
            df1: First DataFrame
            df2: Second DataFrame
            column: Column name to compare
            labels: Tuple of (label1, label2) for the datasets
            figsize: Figure size tuple (default: (10, 6))
            title: Figure title (default: None)

        Returns:
            Matplotlib Figure object

        Example:
            >>> df_before = pd.read_csv('before.csv')
            >>> df_after = pd.read_csv('after.csv')
            >>> fig = DataVisualizer.plot_distribution_comparison(
            ...     df_before, df_after, 'age',
            ...     labels=('Before Cleaning', 'After Cleaning')
            ... )
            >>> fig.savefig('comparison.png')
        """
        plt, sns = _ensure_viz_dependencies()

        if column not in df1.columns:
            raise KeyError(f"Column '{column}' not found in first DataFrame")
        if column not in df2.columns:
            raise KeyError(f"Column '{column}' not found in second DataFrame")

        fig, ax = plt.subplots(figsize=figsize)

        series1 = df1[column].dropna()
        series2 = df2[column].dropna()

        # Check if numeric or categorical
        if pd.api.types.is_numeric_dtype(series1):
            # Numeric: use histograms with KDE
            ax.hist(series1, bins=30, alpha=0.5, label=labels[0],
                   color='blue', edgecolor='black', density=True)
            ax.hist(series2, bins=30, alpha=0.5, label=labels[1],
                   color='red', edgecolor='black', density=True)

            # Add KDE
            try:
                series1.plot.kde(ax=ax, color='darkblue', linewidth=2)
                series2.plot.kde(ax=ax, color='darkred', linewidth=2)
            except:
                pass

            ax.set_ylabel('Density', fontsize=10)
        else:
            # Categorical: use grouped bar chart
            value_counts1 = series1.value_counts().head(10)
            value_counts2 = series2.value_counts().head(10)

            comparison_df = pd.DataFrame({
                labels[0]: value_counts1,
                labels[1]: value_counts2
            }).fillna(0)

            comparison_df.plot(kind='bar', ax=ax, alpha=0.7,
                             color=['blue', 'red'], edgecolor='black')
            plt.xticks(rotation=45, ha='right')
            ax.set_ylabel('Count', fontsize=10)

        ax.set_title(title or f'{column} Distribution Comparison',
                    fontsize=12, fontweight='bold')
        ax.set_xlabel(column, fontsize=10)
        ax.legend()
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        return fig

    @staticmethod
    def plot_time_series(
        df: pd.DataFrame,
        date_column: str,
        value_columns: List[str],
        figsize: Tuple[int, int] = (12, 6),
        title: Optional[str] = None
    ):
        """
        Plot time series data.

        Args:
            df: DataFrame to visualize
            date_column: Column containing dates/timestamps
            value_columns: List of columns to plot over time
            figsize: Figure size tuple (default: (12, 6))
            title: Figure title (default: None)

        Returns:
            Matplotlib Figure object

        Example:
            >>> fig = DataVisualizer.plot_time_series(
            ...     df, date_column='date', value_columns=['sales', 'revenue']
            ... )
            >>> fig.savefig('time_series.png')
        """
        plt, sns = _ensure_viz_dependencies()

        if date_column not in df.columns:
            raise KeyError(f"Column '{date_column}' not found in DataFrame")

        for col in value_columns:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found in DataFrame")

        # Ensure date column is datetime
        df_plot = df.copy()
        df_plot[date_column] = pd.to_datetime(df_plot[date_column])
        df_plot = df_plot.sort_values(date_column)

        fig, ax = plt.subplots(figsize=figsize)

        for col in value_columns:
            ax.plot(df_plot[date_column], df_plot[col],
                   marker='o', markersize=4, linewidth=2, label=col, alpha=0.8)

        ax.set_title(title or 'Time Series', fontsize=12, fontweight='bold')
        ax.set_xlabel(date_column, fontsize=10)
        ax.set_ylabel('Value', fontsize=10)
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Rotate date labels
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        return fig

    @staticmethod
    def plot_pairplot(
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        hue: Optional[str] = None,
        figsize: Optional[Tuple[int, int]] = None
    ):
        """
        Generate pairwise scatter plots for exploring relationships.

        Args:
            df: DataFrame to visualize
            columns: Specific columns to include (default: all numeric)
            hue: Column name for color grouping (optional)
            figsize: Figure size tuple (default: auto-calculated)

        Returns:
            Seaborn PairGrid object (has .fig attribute for Figure)

        Example:
            >>> pairplot = DataVisualizer.plot_pairplot(df, hue='species')
            >>> pairplot.savefig('pairplot.png')
        """
        plt, sns = _ensure_viz_dependencies()

        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
            if hue and hue in columns:
                columns.remove(hue)

        if len(columns) < 2:
            logger.warning("Need at least 2 columns for pairplot")
            return None

        # Limit to 6 columns max for readability
        if len(columns) > 6:
            logger.warning(f"Too many columns ({len(columns)}), using first 6")
            columns = columns[:6]

        plot_df = df[columns + ([hue] if hue else [])]

        # Create pairplot
        pairplot = sns.pairplot(plot_df, hue=hue, diag_kind='kde',
                               corner=False, plot_kws={'alpha': 0.6})

        if figsize:
            pairplot.fig.set_size_inches(figsize)

        return pairplot
