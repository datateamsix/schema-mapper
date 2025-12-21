"""
Comprehensive unit tests for the Profiler class.

Tests cover:
- Core profiling functionality
- Statistical analysis
- Quality assessment
- Anomaly detection
- Pattern recognition
- Distribution analysis
- Visualization
- Edge cases
- Caching
- Progress bars
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from schema_mapper import Profiler
from schema_mapper.profiler import TQDM_AVAILABLE


# ========================================
# FIXTURES
# ========================================

@pytest.fixture
def sample_df():
    """Sample DataFrame with various data types."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'salary': [50000.0, 60000.0, 75000.0, 55000.0, 62000.0],
        'active': [True, True, False, True, False],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'diana@test.com', 'eve@test.com'],
    })


@pytest.fixture
def numeric_df():
    """DataFrame with only numeric columns."""
    return pd.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'col2': [10.5, 20.5, 30.5, 40.5, 50.5],
        'col3': [100, 200, 300, 400, 500],
    })


@pytest.fixture
def messy_df():
    """DataFrame with quality issues."""
    return pd.DataFrame({
        'mostly_null': [1, None, None, None, None, None, None, None, None, None],
        'has_outliers': [10, 20, 25, 30, 22, 28, 19, 21, 1000, 15],
        'duplicates': ['A', 'B', 'A', 'B', 'A', 'B', 'A', 'B', 'A', 'B'],
        'unique_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    })


@pytest.fixture
def pattern_df():
    """DataFrame with detectable patterns."""
    return pd.DataFrame({
        'email': ['user1@example.com', 'user2@test.com', 'user3@example.com'],
        'phone': ['555-123-4567', '555-987-6543', '555-555-5555'],
        'url': ['https://example.com', 'https://test.com', 'https://demo.com'],
        'ip': ['192.168.1.1', '10.0.0.1', '172.16.0.1'],
        'price': ['$19.99', '$29.99', '$39.99'],
        'percent': ['10%', '25%', '50%'],
    })


@pytest.fixture
def datetime_df():
    """DataFrame with datetime columns."""
    base_date = datetime(2024, 1, 1)
    return pd.DataFrame({
        'date': [base_date + timedelta(days=i) for i in range(5)],
        'timestamp': pd.date_range('2024-01-01', periods=5, freq='H'),
        'value': [10, 20, 30, 40, 50],
    })


# ========================================
# INITIALIZATION TESTS
# ========================================

class TestProfilerInitialization:
    """Test Profiler initialization and validation."""

    def test_init_with_valid_dataframe(self, sample_df):
        """Should initialize successfully with valid DataFrame."""
        profiler = Profiler(sample_df, name="test_data")
        assert profiler.df is sample_df
        assert profiler.name == "test_data"
        assert isinstance(profiler._profile_cache, dict)

    def test_init_with_empty_dataframe(self):
        """Should raise ValueError for empty DataFrame."""
        df = pd.DataFrame()
        with pytest.raises(ValueError, match="Cannot profile empty DataFrame"):
            Profiler(df)

    def test_init_with_invalid_type(self):
        """Should raise TypeError for non-DataFrame input."""
        with pytest.raises(TypeError, match="Expected pd.DataFrame"):
            Profiler([1, 2, 3])

    def test_init_default_name(self, sample_df):
        """Should use default name if not provided."""
        profiler = Profiler(sample_df)
        assert profiler.name == "dataset"

    def test_init_progress_bar_setting(self, sample_df):
        """Should respect show_progress parameter."""
        profiler_with = Profiler(sample_df, show_progress=True)
        profiler_without = Profiler(sample_df, show_progress=False)

        if TQDM_AVAILABLE:
            assert profiler_with.show_progress is True
            assert profiler_without.show_progress is False
        else:
            assert profiler_with.show_progress is False
            assert profiler_without.show_progress is False


# ========================================
# DATASET PROFILING TESTS
# ========================================

class TestDatasetProfiling:
    """Test dataset-level profiling methods."""

    def test_profile_dataset_basic_info(self, sample_df):
        """Should return basic dataset information."""
        profiler = Profiler(sample_df)
        profile = profiler.profile_dataset()

        assert profile['name'] == 'dataset'
        assert profile['n_rows'] == 5
        assert profile['n_columns'] == 6
        assert 'memory_usage_mb' in profile
        assert profile['memory_usage_mb'] > 0

    def test_profile_dataset_includes_quality_score(self, sample_df):
        """Should include quality score in profile."""
        profiler = Profiler(sample_df)
        profile = profiler.profile_dataset()

        assert 'quality_score' in profile
        assert 0 <= profile['quality_score'] <= 100

    def test_profile_dataset_caching(self, sample_df):
        """Should cache dataset profile."""
        profiler = Profiler(sample_df)

        # First call
        profile1 = profiler.profile_dataset()
        assert 'dataset_profile' in profiler._profile_cache

        # Second call should return cached result
        profile2 = profiler.profile_dataset()
        assert profile1 is profile2

    def test_get_summary_stats_structure(self, sample_df):
        """Should return DataFrame with expected columns."""
        profiler = Profiler(sample_df)
        summary = profiler.get_summary_stats()

        assert isinstance(summary, pd.DataFrame)
        assert 'dtype' in summary.columns
        assert 'null_count' in summary.columns
        assert 'null_pct' in summary.columns
        assert 'unique_count' in summary.columns

    def test_get_summary_stats_numeric_metrics(self, numeric_df):
        """Should include skewness and kurtosis for numeric columns."""
        profiler = Profiler(numeric_df)
        summary = profiler.get_summary_stats()

        assert 'skewness' in summary.columns
        assert 'kurtosis' in summary.columns
        assert not summary['skewness'].isna().all()


# ========================================
# COLUMN PROFILING TESTS
# ========================================

class TestColumnProfiling:
    """Test individual column profiling."""

    def test_profile_numeric_column(self, sample_df):
        """Should profile numeric column with statistical metrics."""
        profiler = Profiler(sample_df)
        profile = profiler.profile_column('age')

        assert profile['column'] == 'age'
        assert 'mean' in profile
        assert 'median' in profile
        assert 'std' in profile
        assert 'min' in profile
        assert 'max' in profile

    def test_profile_text_column(self, sample_df):
        """Should profile text column with string metrics."""
        profiler = Profiler(sample_df)
        profile = profiler.profile_column('name')

        assert profile['column'] == 'name'
        assert 'avg_length' in profile
        assert 'min_length' in profile
        assert 'max_length' in profile
        assert 'mode' in profile

    def test_profile_boolean_column(self, sample_df):
        """Should profile boolean column with counts."""
        profiler = Profiler(sample_df, show_progress=False)
        profile = profiler.profile_column('active')

        assert profile['column'] == 'active'
        assert 'true_count' in profile
        assert 'false_count' in profile
        assert 'true_percentage' in profile
        # Verify the counts are correct
        assert profile['true_count'] == 3
        assert profile['false_count'] == 2

    def test_profile_datetime_column(self, datetime_df):
        """Should profile datetime column with date metrics."""
        profiler = Profiler(datetime_df)
        profile = profiler.profile_column('date')

        assert profile['column'] == 'date'
        assert 'min_date' in profile
        assert 'max_date' in profile
        assert 'date_range_days' in profile

    def test_profile_nonexistent_column(self, sample_df):
        """Should raise KeyError for non-existent column."""
        profiler = Profiler(sample_df)
        with pytest.raises(KeyError, match="Column 'nonexistent' not found"):
            profiler.profile_column('nonexistent')

    def test_profile_column_caching(self, sample_df):
        """Should cache column profiles."""
        profiler = Profiler(sample_df)

        profile1 = profiler.profile_column('age')
        cache_key = 'column_profile_age'
        assert cache_key in profiler._profile_cache

        profile2 = profiler.profile_column('age')
        assert profile1 is profile2


# ========================================
# QUALITY ASSESSMENT TESTS
# ========================================

class TestQualityAssessment:
    """Test data quality assessment."""

    def test_assess_quality_returns_all_scores(self, sample_df):
        """Should return all quality score components."""
        profiler = Profiler(sample_df)
        quality = profiler.assess_quality()

        assert 'overall_score' in quality
        assert 'completeness_score' in quality
        assert 'uniqueness_score' in quality
        assert 'validity_score' in quality
        assert 'consistency_score' in quality

    def test_assess_quality_perfect_data(self, sample_df):
        """Should give high scores for clean data."""
        profiler = Profiler(sample_df)
        quality = profiler.assess_quality()

        assert quality['overall_score'] >= 80
        assert quality['completeness_score'] == 100  # No nulls

    def test_assess_quality_messy_data(self, messy_df):
        """Should give lower scores for problematic data."""
        profiler = Profiler(messy_df)
        quality = profiler.assess_quality()

        # Should detect low completeness due to mostly_null column
        assert quality['completeness_score'] < 100

    def test_analyze_missing_values(self, messy_df):
        """Should analyze missing values correctly."""
        profiler = Profiler(messy_df, show_progress=False)
        missing = profiler.analyze_missing_values()

        assert 'total_missing' in missing
        assert 'total_missing_percentage' in missing
        assert 'columns_with_missing' in missing

        # mostly_null column should be detected
        assert 'mostly_null' in missing['columns_with_missing']

    def test_detect_duplicates(self, messy_df):
        """Should detect duplicate rows."""
        profiler = Profiler(messy_df, show_progress=False)
        duplicates = profiler.detect_duplicates()

        assert 'count' in duplicates
        assert 'percentage' in duplicates

    def test_analyze_cardinality(self, messy_df):
        """Should classify column cardinality correctly."""
        profiler = Profiler(messy_df, show_progress=False)
        cardinality = profiler.analyze_cardinality()

        assert 'high_cardinality_columns' in cardinality
        assert 'medium_cardinality_columns' in cardinality
        assert 'low_cardinality_columns' in cardinality

        # unique_id should be high cardinality (100% unique)
        assert 'unique_id' in cardinality['high_cardinality_columns']

        # duplicates should be medium cardinality (20% unique = 2/10)
        assert 'duplicates' in cardinality['medium_cardinality_columns']


# ========================================
# ANOMALY DETECTION TESTS
# ========================================

class TestAnomalyDetection:
    """Test anomaly detection methods."""

    def test_detect_anomalies_iqr_method(self, messy_df):
        """Should detect outliers using IQR method."""
        profiler = Profiler(messy_df)
        anomalies = profiler.detect_anomalies(method='iqr', threshold=1.5)

        # has_outliers column should have anomalies detected
        assert 'has_outliers' in anomalies
        assert anomalies['has_outliers']['count'] > 0
        assert anomalies['has_outliers']['method'] == 'iqr'

    def test_detect_anomalies_zscore_method(self, messy_df):
        """Should detect outliers using Z-score method."""
        profiler = Profiler(messy_df)
        anomalies = profiler.detect_anomalies(method='zscore', threshold=2.0)

        assert 'has_outliers' in anomalies
        assert anomalies['has_outliers']['method'] == 'zscore'

    def test_detect_anomalies_isolation_forest(self, messy_df):
        """Should detect outliers using Isolation Forest."""
        profiler = Profiler(messy_df)
        anomalies = profiler.detect_anomalies(method='isolation_forest')

        # May or may not detect anomalies depending on sklearn behavior
        # Just verify it runs without error
        assert isinstance(anomalies, dict)

    def test_detect_anomalies_specific_columns(self, messy_df):
        """Should only check specified columns."""
        profiler = Profiler(messy_df)
        anomalies = profiler.detect_anomalies(method='iqr', columns=['has_outliers'])

        # Should only return results for specified column
        assert len(anomalies) <= 1
        if len(anomalies) > 0:
            assert 'has_outliers' in anomalies

    def test_detect_anomalies_invalid_method(self, sample_df):
        """Should raise ValueError for invalid method."""
        profiler = Profiler(sample_df)
        with pytest.raises(ValueError, match="Unknown method"):
            profiler.detect_anomalies(method='invalid_method')

    def test_detect_anomalies_no_numeric_columns(self):
        """Should handle DataFrame with no numeric columns."""
        df = pd.DataFrame({'text': ['a', 'b', 'c']})
        profiler = Profiler(df)
        anomalies = profiler.detect_anomalies()

        assert isinstance(anomalies, dict)
        assert len(anomalies) == 0


# ========================================
# PATTERN DETECTION TESTS
# ========================================

class TestPatternDetection:
    """Test pattern detection in string columns."""

    def test_detect_patterns_email(self, pattern_df):
        """Should detect email patterns."""
        profiler = Profiler(pattern_df)
        patterns = profiler.detect_patterns()

        assert 'email' in patterns['email_columns']
        assert patterns['details']['email']['email'] > 50

    def test_detect_patterns_phone(self, pattern_df):
        """Should detect phone number patterns."""
        profiler = Profiler(pattern_df)
        patterns = profiler.detect_patterns()

        assert 'phone' in patterns['phone_columns']
        assert patterns['details']['phone']['phone'] > 50

    def test_detect_patterns_url(self, pattern_df):
        """Should detect URL patterns."""
        profiler = Profiler(pattern_df)
        patterns = profiler.detect_patterns()

        assert 'url' in patterns['url_columns']
        assert patterns['details']['url']['url'] > 50

    def test_detect_patterns_ip(self, pattern_df):
        """Should detect IP address patterns."""
        profiler = Profiler(pattern_df)
        patterns = profiler.detect_patterns()

        assert 'ip' in patterns['ip_address_columns']
        assert patterns['details']['ip']['ip_address'] > 50

    def test_detect_patterns_currency(self, pattern_df):
        """Should detect currency patterns."""
        profiler = Profiler(pattern_df)
        patterns = profiler.detect_patterns()

        assert 'price' in patterns['currency_columns']
        assert patterns['details']['price']['currency'] > 50

    def test_detect_patterns_percentage(self, pattern_df):
        """Should detect percentage patterns."""
        profiler = Profiler(pattern_df)
        patterns = profiler.detect_patterns()

        assert 'percent' in patterns['percentage_columns']
        assert patterns['details']['percent']['percentage'] > 50

    def test_detect_patterns_no_string_columns(self, numeric_df):
        """Should handle DataFrame with no string columns."""
        profiler = Profiler(numeric_df)
        patterns = profiler.detect_patterns()

        assert len(patterns['email_columns']) == 0
        assert len(patterns['phone_columns']) == 0


# ========================================
# DISTRIBUTION ANALYSIS TESTS
# ========================================

class TestDistributionAnalysis:
    """Test distribution analysis."""

    def test_analyze_distributions_basic(self, sample_df):
        """Should analyze distributions for numeric columns."""
        profiler = Profiler(sample_df)
        dists = profiler.analyze_distributions()

        assert 'age' in dists
        assert 'salary' in dists
        assert dists['age']['distribution_type'] in ['normal', 'left_skewed', 'right_skewed']

    def test_analyze_distributions_skewness(self, sample_df):
        """Should calculate skewness and kurtosis."""
        profiler = Profiler(sample_df)
        dists = profiler.analyze_distributions()

        assert 'skewness' in dists['age']
        assert 'kurtosis' in dists['age']
        assert isinstance(dists['age']['skewness'], float)

    def test_analyze_distributions_no_numeric(self):
        """Should handle DataFrame with no numeric columns."""
        df = pd.DataFrame({'text': ['a', 'b', 'c']})
        profiler = Profiler(df)
        dists = profiler.analyze_distributions()

        assert len(dists) == 0

    def test_find_correlations(self, numeric_df):
        """Should find correlated numeric columns."""
        profiler = Profiler(numeric_df)
        corr = profiler.find_correlations(threshold=0.7)

        assert isinstance(corr, pd.DataFrame)

    def test_find_correlations_insufficient_columns(self):
        """Should handle DataFrame with < 2 numeric columns."""
        df = pd.DataFrame({'col1': [1, 2, 3], 'text': ['a', 'b', 'c']})
        profiler = Profiler(df)
        corr = profiler.find_correlations()

        assert len(corr) == 0


# ========================================
# VISUALIZATION TESTS
# ========================================

class TestVisualization:
    """Test visualization methods."""

    def test_plot_distributions_runs(self, sample_df):
        """Should run without error (requires matplotlib)."""
        profiler = Profiler(sample_df)
        try:
            profiler.plot_distributions()
            # If matplotlib is available, this should work
            assert True
        except ImportError:
            # If matplotlib not available, that's ok
            pytest.skip("matplotlib not installed")

    def test_plot_correlations_runs(self, numeric_df):
        """Should run without error (requires seaborn)."""
        profiler = Profiler(numeric_df)
        try:
            profiler.plot_correlations()
            assert True
        except ImportError:
            pytest.skip("seaborn not installed")

    def test_plot_missing_values_runs(self, messy_df):
        """Should run without error (requires matplotlib)."""
        profiler = Profiler(messy_df)
        try:
            profiler.plot_missing_values()
            assert True
        except ImportError:
            pytest.skip("matplotlib not installed")


# ========================================
# REPORT GENERATION TESTS
# ========================================

class TestReportGeneration:
    """Test comprehensive report generation."""

    def test_generate_report_dict_format(self, sample_df):
        """Should generate report in dict format."""
        profiler = Profiler(sample_df)
        report = profiler.generate_report(output_format='dict')

        assert isinstance(report, dict)
        assert 'dataset' in report
        assert 'quality' in report
        assert 'missing_values' in report
        assert 'cardinality' in report

    def test_generate_report_json_format(self, sample_df):
        """Should generate report in JSON format."""
        profiler = Profiler(sample_df)
        report = profiler.generate_report(output_format='json')

        assert isinstance(report, str)
        # Should be valid JSON
        import json
        parsed = json.loads(report)
        assert isinstance(parsed, dict)

    def test_generate_report_html_format(self, sample_df):
        """Should generate report in HTML format."""
        profiler = Profiler(sample_df)
        report = profiler.generate_report(output_format='html')

        assert isinstance(report, str)
        assert '<html>' in report or '<table>' in report

    def test_generate_report_invalid_format(self, sample_df):
        """Should raise ValueError for invalid format."""
        profiler = Profiler(sample_df)
        with pytest.raises(ValueError, match="Unknown format"):
            profiler.generate_report(output_format='invalid')


# ========================================
# EDGE CASE TESTS
# ========================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_row_dataframe(self):
        """Should handle DataFrame with single row."""
        df = pd.DataFrame({'col1': [1], 'col2': ['a']})
        profiler = Profiler(df)

        profile = profiler.profile_dataset()
        assert profile['n_rows'] == 1

    def test_single_column_dataframe(self):
        """Should handle DataFrame with single column."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        profiler = Profiler(df)

        profile = profiler.profile_dataset()
        assert profile['n_columns'] == 1

    def test_all_null_column(self):
        """Should handle column with all null values."""
        df = pd.DataFrame({'all_null': [None, None, None], 'valid': [1, 2, 3]})
        profiler = Profiler(df)

        col_profile = profiler.profile_column('all_null')
        assert col_profile['null_percentage'] == 100

    def test_all_duplicate_values(self):
        """Should handle column with all same values."""
        df = pd.DataFrame({'constant': [5, 5, 5, 5, 5]})
        profiler = Profiler(df)

        col_profile = profiler.profile_column('constant')
        assert col_profile['unique_count'] == 1

    def test_large_cardinality(self):
        """Should handle high cardinality columns efficiently."""
        df = pd.DataFrame({'unique_id': range(1000)})
        profiler = Profiler(df)

        cardinality = profiler.analyze_cardinality()
        assert 'unique_id' in cardinality['high_cardinality_columns']

    def test_mixed_types_in_column(self):
        """Should handle columns with mixed types."""
        df = pd.DataFrame({'mixed': [1, 'two', 3.0, None, True]})
        profiler = Profiler(df)

        # Should not crash
        profile = profiler.profile_column('mixed')
        assert profile['column'] == 'mixed'


# ========================================
# PROGRESS BAR TESTS
# ========================================

class TestProgressBars:
    """Test progress bar functionality."""

    def test_progress_bars_enabled_when_requested(self, sample_df):
        """Should show progress when requested and tqdm available."""
        profiler = Profiler(sample_df, show_progress=True)

        if TQDM_AVAILABLE:
            assert profiler.show_progress is True
        else:
            assert profiler.show_progress is False

    def test_progress_bars_disabled_when_requested(self, sample_df):
        """Should not show progress when disabled."""
        profiler = Profiler(sample_df, show_progress=False)
        assert profiler.show_progress is False

    def test_detect_anomalies_with_progress(self, messy_df):
        """Should run detect_anomalies with progress bar."""
        profiler = Profiler(messy_df, show_progress=True)
        # Should not crash whether or not tqdm is available
        anomalies = profiler.detect_anomalies()
        assert isinstance(anomalies, dict)

    def test_detect_patterns_with_progress(self, pattern_df):
        """Should run detect_patterns with progress bar."""
        profiler = Profiler(pattern_df, show_progress=True)
        patterns = profiler.detect_patterns()
        assert isinstance(patterns, dict)

    def test_analyze_distributions_with_progress(self, numeric_df):
        """Should run analyze_distributions with progress bar."""
        profiler = Profiler(numeric_df, show_progress=True)
        dists = profiler.analyze_distributions()
        assert isinstance(dists, dict)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
