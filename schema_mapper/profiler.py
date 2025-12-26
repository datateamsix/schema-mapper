"""
Data Profiler - Comprehensive data exploration and quality analysis.

This module provides statistical profiling, anomaly detection, pattern analysis,
and visualization capabilities for DataFrame exploration.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Union, Optional, Any
from collections import Counter
import logging
import re
from functools import lru_cache
import warnings

from .visualization import DataVisualizer

logger = logging.getLogger(__name__)

# Try to import tqdm for progress bars
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Fallback: no-op tqdm
    def tqdm(iterable, **kwargs):
        return iterable


class Profiler:
    """
    Comprehensive data profiling and quality analysis.

    Provides statistical summaries, anomaly detection, distribution analysis,
    and visualization capabilities for DataFrame exploration.

    Example:
        >>> from schema_mapper import Profiler
        >>> import pandas as pd
        >>>
        >>> df = pd.read_csv('data.csv')
        >>> profiler = Profiler(df, name="customer_data")
        >>>
        >>> # Quick statistical summary
        >>> stats = profiler.get_summary_stats()
        >>>
        >>> # Quality assessment
        >>> quality = profiler.assess_quality()
        >>> print(f"Data Quality Score: {quality['overall_score']}/100")
        >>>
        >>> # Detect anomalies
        >>> anomalies = profiler.detect_anomalies(method='iqr')
        >>>
        >>> # Visualize (matplotlib/seaborn lazy-loaded)
        >>> profiler.plot_distributions()
        >>> profiler.plot_correlations()
    """

    def __init__(self, df: pd.DataFrame, name: str = "dataset", show_progress: bool = True):
        """
        Initialize profiler with DataFrame.

        Args:
            df: DataFrame to profile
            name: Dataset name for reporting (default: "dataset")
            show_progress: Whether to show progress bars for long operations (default: True)

        Raises:
            ValueError: If DataFrame is empty or invalid
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pd.DataFrame, got {type(df)}")

        if df.empty:
            raise ValueError("Cannot profile empty DataFrame")

        self.df = df
        self.name = name
        self.show_progress = show_progress and TQDM_AVAILABLE
        self._profile_cache = {}

        logger.info(f"Initialized Profiler for '{name}': {len(df)} rows, {len(df.columns)} columns")
        if not TQDM_AVAILABLE and show_progress:
            logger.info("Install tqdm for progress bars: pip install tqdm")

    # ========================================
    # STATISTICAL PROFILING
    # ========================================

    def profile_dataset(self) -> Dict[str, Any]:
        """
        Generate comprehensive statistical profile of entire dataset.

        Returns:
            Dictionary with dataset-level statistics including:
            - Basic info (rows, columns, memory usage)
            - Column types distribution
            - Missing values summary
            - Duplicate rows count
            - Overall quality score

        Example:
            >>> profile = profiler.profile_dataset()
            >>> print(f"Rows: {profile['n_rows']}")
            >>> print(f"Quality Score: {profile['quality_score']}/100")
        """
        if 'dataset_profile' in self._profile_cache:
            return self._profile_cache['dataset_profile']

        logger.info(f"Profiling dataset '{self.name}'...")

        profile = {
            'name': self.name,
            'n_rows': len(self.df),
            'n_columns': len(self.df.columns),
            'memory_usage_mb': self.df.memory_usage(deep=True).sum() / 1024 / 1024,
            'column_types': self._get_column_type_distribution(),
            'missing_values': self.analyze_missing_values(),
            'duplicate_rows': self.detect_duplicates(),
            'quality_score': self.assess_quality()['overall_score'],
            'column_names': list(self.df.columns),
        }

        self._profile_cache['dataset_profile'] = profile
        return profile

    def profile_column(self, column: str) -> Dict[str, Any]:
        """
        Deep profile of a single column with type-specific metrics.

        Args:
            column: Column name to profile

        Returns:
            Dictionary with column-specific statistics

        Raises:
            KeyError: If column doesn't exist

        Example:
            >>> col_profile = profiler.profile_column('age')
            >>> print(f"Mean: {col_profile['mean']}")
            >>> print(f"Outliers: {len(col_profile['outliers'])}")
        """
        if column not in self.df.columns:
            raise KeyError(f"Column '{column}' not found in DataFrame")

        cache_key = f"column_profile_{column}"
        if cache_key in self._profile_cache:
            return self._profile_cache[cache_key]

        series = self.df[column]
        dtype = series.dtype

        profile = {
            'column': column,
            'dtype': str(dtype),
            'count': len(series),
            'null_count': series.isna().sum(),
            'null_percentage': (series.isna().sum() / len(series)) * 100,
            'unique_count': series.nunique(),
            'unique_percentage': (series.nunique() / len(series)) * 100,
        }

        # Type-specific statistics (check bool before numeric since bool is a subtype of numeric)
        if pd.api.types.is_bool_dtype(series):
            profile.update(self._profile_boolean_column(series))
        elif pd.api.types.is_numeric_dtype(series):
            profile.update(self._profile_numeric_column(series))
        elif pd.api.types.is_datetime64_any_dtype(series):
            profile.update(self._profile_datetime_column(series))
        else:  # Object/string columns
            profile.update(self._profile_text_column(series))

        self._profile_cache[cache_key] = profile
        return profile

    def get_summary_stats(self) -> pd.DataFrame:
        """
        Statistical summary extending pandas describe() with additional metrics.

        Returns:
            DataFrame with comprehensive statistics for all columns

        Example:
            >>> summary = profiler.get_summary_stats()
            >>> print(summary.T)  # Transpose for better viewing
        """
        logger.info("Generating summary statistics...")

        # Exclude boolean columns from describe to avoid numpy quantile errors
        bool_cols = self.df.select_dtypes(include=['bool']).columns
        df_for_describe = self.df.drop(columns=bool_cols) if len(bool_cols) > 0 else self.df

        # Start with pandas describe
        summary = df_for_describe.describe(include='all').T

        # Add boolean columns back with basic stats
        if len(bool_cols) > 0:
            for col in bool_cols:
                summary.loc[col] = {
                    'count': len(self.df[col]),
                    'unique': self.df[col].nunique(),
                    'top': self.df[col].mode().iloc[0] if len(self.df[col].mode()) > 0 else None,
                    'freq': self.df[col].value_counts().iloc[0] if len(self.df[col]) > 0 else 0
                }

        # Add custom metrics
        summary['dtype'] = self.df.dtypes
        summary['null_count'] = self.df.isna().sum()
        summary['null_pct'] = (self.df.isna().sum() / len(self.df)) * 100
        summary['unique_count'] = self.df.nunique()
        summary['unique_pct'] = (self.df.nunique() / len(self.df)) * 100

        # Add skewness and kurtosis for numeric columns
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            summary.loc[numeric_cols, 'skewness'] = self.df[numeric_cols].skew()
            summary.loc[numeric_cols, 'kurtosis'] = self.df[numeric_cols].kurtosis()

        return summary

    # ========================================
    # QUALITY METRICS
    # ========================================

    def assess_quality(self) -> Dict[str, Any]:
        """
        Overall data quality score and metrics.

        Calculates quality based on:
        - Completeness (% non-null)
        - Uniqueness (cardinality)
        - Validity (pattern matching for known types)
        - Consistency (type uniformity)

        Returns:
            Dictionary with quality scores (0-100 scale)

        Example:
            >>> quality = profiler.assess_quality()
            >>> if quality['overall_score'] < 70:
            ...     print("WARNING: Low data quality")
        """
        logger.info("Assessing data quality...")

        completeness = self._calculate_completeness()
        uniqueness = self._calculate_uniqueness()
        validity = self._calculate_validity()
        consistency = self._calculate_consistency()

        # Weighted average
        overall_score = (
            completeness * 0.35 +
            uniqueness * 0.15 +
            validity * 0.30 +
            consistency * 0.20
        )

        return {
            'overall_score': round(overall_score, 2),
            'completeness_score': round(completeness, 2),
            'uniqueness_score': round(uniqueness, 2),
            'validity_score': round(validity, 2),
            'consistency_score': round(consistency, 2),
            'interpretation': self._interpret_quality_score(overall_score)
        }

    def detect_anomalies(
        self,
        method: str = 'iqr',
        threshold: float = 1.5,
        columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Detect outliers and anomalies using various methods.

        Args:
            method: Detection method ('iqr', 'zscore', 'isolation_forest')
            threshold: Threshold value (1.5 for IQR, 3.0 for Z-score)
            columns: Specific columns to check (default: all numeric)

        Returns:
            Dictionary mapping column names to outlier information

        Example:
            >>> anomalies = profiler.detect_anomalies(method='iqr', threshold=1.5)
            >>> for col, info in anomalies.items():
            ...     print(f"{col}: {info['count']} outliers found")
        """
        logger.info(f"Detecting anomalies using {method} method...")

        if columns is None:
            columns = self.df.select_dtypes(include=[np.number]).columns.tolist()

        anomalies = {}

        # Add progress bar for column iteration
        col_iterator = tqdm(columns, desc="Detecting anomalies", disable=not self.show_progress)

        for col in col_iterator:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping")
                continue

            series = self.df[col].dropna()

            if method == 'iqr':
                outliers = self._detect_iqr_outliers(series, threshold)
            elif method == 'zscore':
                outliers = self._detect_zscore_outliers(series, threshold)
            elif method == 'isolation_forest':
                outliers = self._detect_isolation_forest_outliers(series)
            else:
                raise ValueError(f"Unknown method: {method}. Use 'iqr', 'zscore', or 'isolation_forest'")

            if len(outliers) > 0:
                anomalies[col] = {
                    'count': len(outliers),
                    'percentage': (len(outliers) / len(series)) * 100,
                    'indices': outliers.tolist(),
                    'values': series.iloc[outliers].tolist()[:10],  # First 10 outliers
                    'method': method
                }

        logger.info(f"Found anomalies in {len(anomalies)} column(s)")
        return anomalies

    def analyze_cardinality(self) -> Dict[str, Any]:
        """
        Cardinality analysis for each column.

        Classifies columns as:
        - High cardinality (>95% unique)
        - Medium cardinality (5-95% unique)
        - Low cardinality (<5% unique)

        Returns:
            Dictionary with cardinality information per column

        Example:
            >>> cardinality = profiler.analyze_cardinality()
            >>> print(cardinality['high_cardinality_columns'])
        """
        logger.info("Analyzing cardinality...")

        cardinality_info = {
            'high_cardinality_columns': [],
            'medium_cardinality_columns': [],
            'low_cardinality_columns': [],
            'details': {}
        }

        for col in self.df.columns:
            unique_pct = (self.df[col].nunique() / len(self.df)) * 100
            unique_count = self.df[col].nunique()

            details = {
                'unique_count': unique_count,
                'unique_percentage': round(unique_pct, 2),
                'total_count': len(self.df[col]),
            }

            if unique_pct > 95:
                cardinality_info['high_cardinality_columns'].append(col)
                details['category'] = 'high'
            elif unique_pct > 5:
                cardinality_info['medium_cardinality_columns'].append(col)
                details['category'] = 'medium'
            else:
                cardinality_info['low_cardinality_columns'].append(col)
                details['category'] = 'low'
                # For low cardinality, include top values
                details['top_values'] = self.df[col].value_counts().head(10).to_dict()

            cardinality_info['details'][col] = details

        return cardinality_info

    def detect_duplicates(self, subset: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Identify duplicate rows with statistics.

        Args:
            subset: Columns to consider for duplicates (default: all columns)

        Returns:
            Dictionary with duplicate information

        Example:
            >>> dupes = profiler.detect_duplicates()
            >>> if dupes['count'] > 0:
            ...     print(f"Found {dupes['count']} duplicate rows")
        """
        logger.info("Detecting duplicate rows...")

        duplicates = self.df.duplicated(subset=subset, keep=False)
        duplicate_count = duplicates.sum()

        result = {
            'count': int(duplicate_count),
            'percentage': (duplicate_count / len(self.df)) * 100,
            'indices': self.df[duplicates].index.tolist(),
        }

        if duplicate_count > 0:
            result['sample'] = self.df[duplicates].head(5).to_dict('records')

        return result

    def check_data_drift(self, reference_df: pd.DataFrame, threshold: float = 0.1) -> Dict[str, Any]:
        """
        Compare current dataset against reference for drift detection.

        Detects statistical drift in distributions between two datasets.

        Args:
            reference_df: Reference DataFrame to compare against
            threshold: Threshold for detecting significant drift (default: 0.1)

        Returns:
            Dictionary with drift metrics per column

        Example:
            >>> baseline_df = pd.read_csv('baseline.csv')
            >>> drift = profiler.check_data_drift(baseline_df)
            >>> for col, info in drift['drifted_columns'].items():
            ...     print(f"{col}: {info['drift_score']:.2%} drift")
        """
        logger.info("Checking for data drift...")

        drift_results = {
            'drifted_columns': {},
            'stable_columns': [],
            'new_columns': [],
            'missing_columns': []
        }

        current_cols = set(self.df.columns)
        reference_cols = set(reference_df.columns)

        drift_results['new_columns'] = list(current_cols - reference_cols)
        drift_results['missing_columns'] = list(reference_cols - current_cols)

        common_cols = current_cols & reference_cols

        for col in common_cols:
            current_series = self.df[col]
            reference_series = reference_df[col]

            drift_score = self._calculate_drift_score(current_series, reference_series)

            if drift_score > threshold:
                drift_results['drifted_columns'][col] = {
                    'drift_score': round(drift_score, 4),
                    'current_mean': float(current_series.mean()) if pd.api.types.is_numeric_dtype(current_series) else None,
                    'reference_mean': float(reference_series.mean()) if pd.api.types.is_numeric_dtype(reference_series) else None,
                }
            else:
                drift_results['stable_columns'].append(col)

        return drift_results

    # ========================================
    # PATTERN ANALYSIS
    # ========================================

    def detect_patterns(self) -> Dict[str, Any]:
        """
        Identify patterns in string columns (emails, phones, dates, URLs, etc.).

        Returns:
            Dictionary mapping columns to detected patterns

        Example:
            >>> patterns = profiler.detect_patterns()
            >>> if 'email' in patterns['email_columns']:
            ...     print("Email column detected!")
        """
        logger.info("Detecting patterns in string columns...")

        patterns = {
            'email_columns': [],
            'phone_columns': [],
            'url_columns': [],
            'ip_address_columns': [],
            'date_string_columns': [],
            'currency_columns': [],
            'percentage_columns': [],
            'details': {}
        }

        string_cols = self.df.select_dtypes(include=['object']).columns

        # Add progress bar for string column iteration
        col_iterator = tqdm(string_cols, desc="Detecting patterns", disable=not self.show_progress)

        for col in col_iterator:
            series = self.df[col].dropna().astype(str)

            if len(series) == 0:
                continue

            col_patterns = {
                'email': self._check_email_pattern(series),
                'phone': self._check_phone_pattern(series),
                'url': self._check_url_pattern(series),
                'ip_address': self._check_ip_pattern(series),
                'date_string': self._check_date_string_pattern(series),
                'currency': self._check_currency_pattern(series),
                'percentage': self._check_percentage_pattern(series),
            }

            # Add to category lists if >50% match
            for pattern_type, match_pct in col_patterns.items():
                if match_pct > 50:
                    patterns[f'{pattern_type}_columns'].append(col)

            patterns['details'][col] = col_patterns

        return patterns

    def analyze_distributions(self) -> Dict[str, Any]:
        """
        Distribution analysis for numeric columns.

        Identifies:
        - Normal distributions
        - Skewed distributions (left/right)
        - Bimodal distributions

        Returns:
            Dictionary with distribution characteristics per column

        Example:
            >>> dists = profiler.analyze_distributions()
            >>> for col, info in dists.items():
            ...     print(f"{col}: {info['distribution_type']}")
        """
        logger.info("Analyzing distributions...")

        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        distributions = {}

        # Add progress bar for numeric column iteration
        col_iterator = tqdm(numeric_cols, desc="Analyzing distributions", disable=not self.show_progress)

        for col in col_iterator:
            series = self.df[col].dropna()

            if len(series) < 3:
                continue

            skewness = series.skew()
            kurtosis = series.kurtosis()

            # Classify distribution
            dist_type = self._classify_distribution(skewness, kurtosis)

            distributions[col] = {
                'distribution_type': dist_type,
                'skewness': round(skewness, 3),
                'kurtosis': round(kurtosis, 3),
                'mean': float(series.mean()),
                'median': float(series.median()),
                'std': float(series.std()),
            }

        return distributions

    def find_correlations(self, threshold: float = 0.7, method: str = 'pearson') -> pd.DataFrame:
        """
        Detect highly correlated features.

        Args:
            threshold: Correlation threshold (default: 0.7)
            method: Correlation method ('pearson', 'spearman', 'kendall')

        Returns:
            DataFrame with correlated feature pairs

        Example:
            >>> corr_pairs = profiler.find_correlations(threshold=0.8)
            >>> print(f"Found {len(corr_pairs)} highly correlated pairs")
        """
        logger.info(f"Finding correlations (threshold={threshold}, method={method})...")

        numeric_df = self.df.select_dtypes(include=[np.number])

        if numeric_df.shape[1] < 2:
            logger.warning("Need at least 2 numeric columns for correlation analysis")
            return pd.DataFrame()

        corr_matrix = numeric_df.corr(method=method)

        # Extract upper triangle (avoid duplicates)
        correlations = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) >= threshold:
                    correlations.append({
                        'column_1': corr_matrix.columns[i],
                        'column_2': corr_matrix.columns[j],
                        'correlation': round(corr_value, 3)
                    })

        return pd.DataFrame(correlations)

    # ========================================
    # MISSING VALUE ANALYSIS
    # ========================================

    def analyze_missing_values(self) -> Dict[str, Any]:
        """
        Comprehensive missing value analysis with patterns.

        Returns:
            Dictionary with missing value statistics and patterns

        Example:
            >>> missing = profiler.analyze_missing_values()
            >>> print(f"Total missing: {missing['total_missing']}")
            >>> print(f"Columns with >50% missing: {missing['high_missing_columns']}")
        """
        logger.info("Analyzing missing values...")

        total_cells = self.df.size
        total_missing = self.df.isna().sum().sum()

        missing_per_column = self.df.isna().sum()
        missing_pct_per_column = (missing_per_column / len(self.df)) * 100

        result = {
            'total_missing': int(total_missing),
            'total_missing_percentage': round((total_missing / total_cells) * 100, 2),
            'columns_with_missing': missing_per_column[missing_per_column > 0].to_dict(),
            'missing_percentages': missing_pct_per_column[missing_pct_per_column > 0].to_dict(),
            'high_missing_columns': missing_pct_per_column[missing_pct_per_column > 50].index.tolist(),
            'complete_columns': missing_per_column[missing_per_column == 0].index.tolist(),
        }

        return result

    # ========================================
    # VISUALIZATION (Lazy-loaded wrappers)
    # ========================================

    def plot_distributions(
        self,
        columns: Optional[List[str]] = None,
        figsize: Tuple[int, int] = (12, 8),
        color: str = '#34495e',
        kde_color: str = '#e74c3c'
    ):
        """
        Generate histograms for numeric columns with customizable colors.

        Args:
            columns: Specific columns to plot (default: all numeric)
            figsize: Figure size tuple (width, height)
            color: Histogram bar color (default: '#34495e' dark blue-grey)
            kde_color: KDE line color (default: '#e74c3c' red)

        Returns:
            Matplotlib figure object

        Example:
            >>> # Default dark blue-grey histograms
            >>> fig = profiler.plot_distributions()
            >>> fig.savefig('distributions.png')
            >>>
            >>> # Custom grey color
            >>> fig = profiler.plot_distributions(
            ...     columns=['age', 'income'],
            ...     color='#7f8c8d',
            ...     kde_color='#27ae60'
            ... )
        """
        return DataVisualizer.plot_histogram(
            self.df,
            columns=columns,
            bins=30,
            figsize=figsize,
            title=f'{self.name} - Distributions',
            color=color,
            kde_color=kde_color
        )

    def plot_correlations(
        self,
        method: str = 'pearson',
        figsize: Tuple[int, int] = (10, 8)
    ):
        """
        Correlation heatmap for numeric columns.

        Args:
            method: Correlation method ('pearson', 'spearman', 'kendall')
            figsize: Figure size tuple

        Returns:
            Matplotlib figure object

        Example:
            >>> fig = profiler.plot_correlations(method='spearman')
            >>> fig.savefig('correlations.png')
        """
        return DataVisualizer.plot_correlation_matrix(
            self.df,
            method=method,
            figsize=figsize,
            annot=True,
            title=f'{self.name} - Correlation Matrix ({method.capitalize()})'
        )

    def plot_missing_values(self, figsize: Tuple[int, int] = (10, 6)):
        """
        Visualize missing value patterns.

        Args:
            figsize: Figure size tuple

        Returns:
            Matplotlib figure object

        Example:
            >>> fig = profiler.plot_missing_values()
            >>> fig.savefig('missing_values.png')
        """
        return DataVisualizer.plot_missing_values(
            self.df,
            figsize=figsize,
            title=f'{self.name} - Missing Values'
        )

    def plot_outliers(
        self,
        columns: Optional[List[str]] = None,
        figsize: Tuple[int, int] = (12, 6)
    ):
        """
        Box plots to visualize outliers in numeric columns.

        Args:
            columns: Specific columns to plot (default: all numeric)
            figsize: Figure size tuple

        Returns:
            Matplotlib figure object

        Example:
            >>> fig = profiler.plot_outliers(columns=['age', 'salary'])
            >>> fig.savefig('outliers.png')
        """
        return DataVisualizer.plot_boxplot(
            self.df,
            columns=columns,
            figsize=figsize,
            title=f'{self.name} - Outlier Detection'
        )

    def plot_scatter_matrix(
        self,
        columns: Optional[List[str]] = None,
        figsize: Tuple[int, int] = (12, 12),
        color: str = '#34495e',
        alpha: float = 0.6,
        diagonal: str = 'hist'
    ):
        """
        Generate scatter plot matrix showing pairwise relationships between numeric columns.

        Creates a grid of scatter plots for all combinations of numeric columns,
        with histograms or KDE plots on the diagonal. Automatically adds trend lines
        to scatter plots.

        Args:
            columns: Specific numeric columns to include (default: all numeric, max 6)
            figsize: Figure size tuple (default: (12, 12))
            color: Scatter plot and histogram color (default: '#34495e' dark blue-grey)
            alpha: Point transparency for scatter plots (default: 0.6)
            diagonal: Diagonal plot type - 'hist' or 'kde' (default: 'hist')

        Returns:
            Matplotlib figure object

        Example:
            >>> # Plot all numeric columns with default colors
            >>> fig = profiler.plot_scatter_matrix()
            >>> fig.savefig('scatter_matrix.png', dpi=300, bbox_inches='tight')
            >>>
            >>> # Plot specific columns with custom grey and KDE diagonal
            >>> fig = profiler.plot_scatter_matrix(
            ...     columns=['age', 'income', 'credit_score'],
            ...     color='#7f8c8d',
            ...     diagonal='kde'
            ... )
            >>> fig.savefig('custom_scatter_matrix.png')
        """
        return DataVisualizer.plot_scatter_matrix(
            self.df,
            columns=columns,
            figsize=figsize,
            title=f'{self.name} - Scatter Matrix',
            color=color,
            alpha=alpha,
            diagonal=diagonal
        )

    # ========================================
    # REPORTING
    # ========================================

    def generate_report(self, output_format: str = 'dict') -> Union[Dict, str]:
        """
        Generate comprehensive profiling report.

        Args:
            output_format: Format ('dict', 'json', or 'html')

        Returns:
            Report in requested format

        Example:
            >>> report = profiler.generate_report(output_format='dict')
            >>> print(report['quality']['overall_score'])
            >>>
            >>> # Export as JSON
            >>> json_report = profiler.generate_report(output_format='json')
            >>> with open('report.json', 'w') as f:
            ...     f.write(json_report)
        """
        logger.info(f"Generating report in {output_format} format...")

        report = {
            'dataset': self.profile_dataset(),
            'quality': self.assess_quality(),
            'missing_values': self.analyze_missing_values(),
            'cardinality': self.analyze_cardinality(),
            'duplicates': self.detect_duplicates(),
            'patterns': self.detect_patterns(),
            'distributions': self.analyze_distributions(),
        }

        if output_format == 'dict':
            return report
        elif output_format == 'json':
            import json
            return json.dumps(report, indent=2, default=str)
        elif output_format == 'html':
            return self._generate_html_report(report)
        else:
            raise ValueError(f"Unknown format: {output_format}. Use 'dict', 'json', or 'html'")

    # ========================================
    # PRIVATE HELPER METHODS
    # ========================================

    def _get_column_type_distribution(self) -> Dict[str, int]:
        """Get distribution of column types."""
        type_counts = Counter(str(dtype) for dtype in self.df.dtypes)
        return dict(type_counts)

    def _profile_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile numeric column."""
        return {
            'mean': float(series.mean()),
            'median': float(series.median()),
            'std': float(series.std()),
            'min': float(series.min()),
            'max': float(series.max()),
            'q25': float(series.quantile(0.25)),
            'q75': float(series.quantile(0.75)),
            'skewness': float(series.skew()),
            'kurtosis': float(series.kurtosis()),
        }

    def _profile_datetime_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile datetime column."""
        return {
            'min_date': str(series.min()),
            'max_date': str(series.max()),
            'date_range_days': (series.max() - series.min()).days,
        }

    def _profile_boolean_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile boolean column."""
        # Drop nulls to get clean counts
        series_clean = series.dropna()
        value_counts = series_clean.value_counts()

        true_count = int(value_counts.get(True, 0))
        false_count = int(value_counts.get(False, 0))
        total_count = len(series_clean)

        return {
            'true_count': true_count,
            'false_count': false_count,
            'true_percentage': (true_count / total_count * 100) if total_count > 0 else 0.0,
        }

    def _profile_text_column(self, series: pd.Series) -> Dict[str, Any]:
        """Profile text/object column."""
        string_series = series.astype(str)
        return {
            'mode': series.mode()[0] if len(series.mode()) > 0 else None,
            'mode_frequency': int(series.value_counts().iloc[0]) if len(series.value_counts()) > 0 else 0,
            'avg_length': float(string_series.str.len().mean()),
            'min_length': int(string_series.str.len().min()),
            'max_length': int(string_series.str.len().max()),
            'top_5_values': series.value_counts().head(5).to_dict(),
        }

    def _calculate_completeness(self) -> float:
        """Calculate completeness score (0-100)."""
        total_cells = self.df.size
        non_null_cells = self.df.count().sum()
        return (non_null_cells / total_cells) * 100

    def _calculate_uniqueness(self) -> float:
        """Calculate uniqueness score based on cardinality."""
        # Average cardinality across columns (weighted by data type)
        scores = []
        for col in self.df.columns:
            unique_pct = (self.df[col].nunique() / len(self.df)) * 100
            # High cardinality is good for IDs, bad for categories
            if pd.api.types.is_numeric_dtype(self.df[col]):
                scores.append(min(unique_pct, 100))
            else:
                # For categorical, medium cardinality is ideal
                scores.append(100 - abs(unique_pct - 50))
        return np.mean(scores)

    def _calculate_validity(self) -> float:
        """Calculate validity score based on pattern matching."""
        patterns = self.detect_patterns()
        # Count columns with detected patterns
        pattern_matches = sum(len(patterns[key]) for key in patterns if key.endswith('_columns'))
        string_cols = len(self.df.select_dtypes(include=['object']).columns)

        if string_cols == 0:
            return 100  # No string columns to validate

        return (pattern_matches / string_cols) * 100 if string_cols > 0 else 100

    def _calculate_consistency(self) -> float:
        """Calculate consistency score (type uniformity)."""
        # Check for mixed types in object columns
        inconsistent_cols = 0
        object_cols = self.df.select_dtypes(include=['object']).columns

        for col in object_cols:
            if self.df[col].notna().any():
                types = self.df[col].dropna().apply(type).unique()
                if len(types) > 1:
                    inconsistent_cols += 1

        if len(object_cols) == 0:
            return 100

        return ((len(object_cols) - inconsistent_cols) / len(object_cols)) * 100

    def _interpret_quality_score(self, score: float) -> str:
        """Interpret quality score with description."""
        if score >= 90:
            return "Excellent - Data is production-ready"
        elif score >= 75:
            return "Good - Minor issues to address"
        elif score >= 60:
            return "Fair - Requires cleaning"
        elif score >= 40:
            return "Poor - Significant issues"
        else:
            return "Critical - Major data quality problems"

    def _detect_iqr_outliers(self, series: pd.Series, threshold: float) -> np.ndarray:
        """Detect outliers using IQR method."""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR

        outliers = ((series < lower_bound) | (series > upper_bound))
        return np.where(outliers)[0]

    def _detect_zscore_outliers(self, series: pd.Series, threshold: float) -> np.ndarray:
        """Detect outliers using Z-score method."""
        z_scores = np.abs((series - series.mean()) / series.std())
        outliers = z_scores > threshold
        return np.where(outliers)[0]

    def _detect_isolation_forest_outliers(self, series: pd.Series) -> np.ndarray:
        """Detect outliers using Isolation Forest (requires sklearn)."""
        try:
            from sklearn.ensemble import IsolationForest

            X = series.values.reshape(-1, 1)
            clf = IsolationForest(contamination=0.1, random_state=42)
            predictions = clf.fit_predict(X)

            outliers = predictions == -1
            return np.where(outliers)[0]
        except ImportError:
            logger.warning("scikit-learn not installed. Install with: pip install scikit-learn")
            return np.array([])

    def _calculate_drift_score(self, current: pd.Series, reference: pd.Series) -> float:
        """Calculate drift score between two series."""
        if pd.api.types.is_numeric_dtype(current):
            # Use statistical distance for numeric
            mean_diff = abs(current.mean() - reference.mean())
            std_diff = abs(current.std() - reference.std())
            return (mean_diff + std_diff) / 2
        else:
            # Use distribution distance for categorical
            current_dist = current.value_counts(normalize=True)
            reference_dist = reference.value_counts(normalize=True)

            all_values = set(current_dist.index) | set(reference_dist.index)
            total_diff = 0

            for val in all_values:
                curr_freq = current_dist.get(val, 0)
                ref_freq = reference_dist.get(val, 0)
                total_diff += abs(curr_freq - ref_freq)

            return total_diff / 2

    def _check_email_pattern(self, series: pd.Series) -> float:
        """Check percentage of values matching email pattern."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        matches = series.str.match(email_pattern, na=False).sum()
        return (matches / len(series)) * 100

    def _check_phone_pattern(self, series: pd.Series) -> float:
        """Check percentage of values matching phone pattern."""
        phone_pattern = r'^[\d\s\(\)\-\+\.]{10,}$'
        matches = series.str.match(phone_pattern, na=False).sum()
        return (matches / len(series)) * 100

    def _check_url_pattern(self, series: pd.Series) -> float:
        """Check percentage of values matching URL pattern."""
        url_pattern = r'^https?://[^\s]+$'
        matches = series.str.match(url_pattern, na=False).sum()
        return (matches / len(series)) * 100

    def _check_ip_pattern(self, series: pd.Series) -> float:
        """Check percentage of values matching IP address pattern."""
        ip_pattern = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
        matches = series.str.match(ip_pattern, na=False).sum()
        return (matches / len(series)) * 100

    def _check_date_string_pattern(self, series: pd.Series) -> float:
        """Check percentage of values that can be parsed as dates."""
        try:
            parsed = pd.to_datetime(series, errors='coerce')
            success = parsed.notna().sum()
            return (success / len(series)) * 100
        except Exception as e:
            logger.debug(f"Error parsing dates in series: {e}")
            return 0.0

    def _check_currency_pattern(self, series: pd.Series) -> float:
        """Check percentage of values matching currency pattern."""
        currency_pattern = r'^[\$\€\£\¥]?\s?\d+[\d,\.]*$'
        matches = series.str.match(currency_pattern, na=False).sum()
        return (matches / len(series)) * 100

    def _check_percentage_pattern(self, series: pd.Series) -> float:
        """Check percentage of values matching percentage pattern."""
        pct_pattern = r'^\d+[\.\d]*\s?%$'
        matches = series.str.match(pct_pattern, na=False).sum()
        return (matches / len(series)) * 100

    def _classify_distribution(self, skewness: float, kurtosis: float) -> str:
        """Classify distribution based on skewness and kurtosis."""
        if abs(skewness) < 0.5 and abs(kurtosis) < 3:
            return "normal"
        elif skewness > 1:
            return "right_skewed"
        elif skewness < -1:
            return "left_skewed"
        elif kurtosis > 3:
            return "heavy_tailed"
        elif kurtosis < -1:
            return "light_tailed"
        else:
            return "other"


    def _generate_html_report(self, report: Dict) -> str:
        """Generate HTML report from profiling data."""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Profiling Report - {self.name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                h2 {{ color: #666; border-bottom: 2px solid #ddd; padding-bottom: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                .score {{ font-size: 24px; font-weight: bold; }}
                .excellent {{ color: green; }}
                .good {{ color: orange; }}
                .poor {{ color: red; }}
            </style>
        </head>
        <body>
            <h1>Data Profiling Report: {self.name}</h1>

            <h2>Dataset Overview</h2>
            <p>Rows: {report['dataset']['n_rows']:,}</p>
            <p>Columns: {report['dataset']['n_columns']}</p>
            <p>Memory Usage: {report['dataset']['memory_usage_mb']:.2f} MB</p>

            <h2>Quality Score</h2>
            <p class="score {self._get_score_class(report['quality']['overall_score'])}">
                {report['quality']['overall_score']}/100
            </p>
            <p>{report['quality']['interpretation']}</p>

            <h2>Missing Values</h2>
            <p>Total Missing: {report['missing_values']['total_missing']:,}
               ({report['missing_values']['total_missing_percentage']:.2f}%)</p>

            <h2>Duplicates</h2>
            <p>Duplicate Rows: {report['duplicates']['count']}
               ({report['duplicates']['percentage']:.2f}%)</p>
        </body>
        </html>
        """
        return html

    def _get_score_class(self, score: float) -> str:
        """Get CSS class for quality score."""
        if score >= 75:
            return "excellent"
        elif score >= 60:
            return "good"
        else:
            return "poor"
