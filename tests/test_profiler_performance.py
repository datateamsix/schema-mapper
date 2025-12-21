"""
Performance tests for the Profiler class.

Tests profiler methods with different dataset sizes:
- Small: 100 rows
- Medium: 10,000 rows
- Large: 100,000 rows
- Extra Large: 1,000,000 rows

Validates that methods complete within reasonable time limits.
"""

import pytest
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from schema_mapper import Profiler


# ========================================
# FIXTURES - DIFFERENT DATASET SIZES
# ========================================

@pytest.fixture
def small_df():
    """Small dataset - 100 rows."""
    np.random.seed(42)
    return pd.DataFrame({
        'id': range(100),
        'name': [f'user_{i}' for i in range(100)],
        'age': np.random.randint(18, 80, 100),
        'salary': np.random.uniform(30000, 150000, 100),
        'active': np.random.choice([True, False], 100),
        'email': [f'user{i}@example.com' for i in range(100)],
        'signup_date': pd.date_range('2024-01-01', periods=100),
        'score': np.random.normal(75, 15, 100),
    })


@pytest.fixture
def medium_df():
    """Medium dataset - 10,000 rows."""
    np.random.seed(42)
    return pd.DataFrame({
        'id': range(10000),
        'name': [f'user_{i}' for i in range(10000)],
        'age': np.random.randint(18, 80, 10000),
        'salary': np.random.uniform(30000, 150000, 10000),
        'active': np.random.choice([True, False], 10000),
        'email': [f'user{i}@example.com' for i in range(10000)],
        'signup_date': pd.date_range('2024-01-01', periods=10000),
        'score': np.random.normal(75, 15, 10000),
    })


@pytest.fixture
def large_df():
    """Large dataset - 100,000 rows."""
    np.random.seed(42)
    return pd.DataFrame({
        'id': range(100000),
        'name': [f'user_{i % 1000}' for i in range(100000)],  # Repeated names for cardinality
        'age': np.random.randint(18, 80, 100000),
        'salary': np.random.uniform(30000, 150000, 100000),
        'active': np.random.choice([True, False], 100000),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 100000),
        'score': np.random.normal(75, 15, 100000),
    })


@pytest.fixture
def extra_large_df():
    """Extra large dataset - 1,000,000 rows."""
    np.random.seed(42)
    return pd.DataFrame({
        'id': range(1000000),
        'age': np.random.randint(18, 80, 1000000),
        'salary': np.random.uniform(30000, 150000, 1000000),
        'score': np.random.normal(75, 15, 1000000),
    })


# ========================================
# PERFORMANCE TEST UTILITIES
# ========================================

def time_function(func, *args, **kwargs):
    """Time a function execution and return (result, duration)."""
    start = time.time()
    result = func(*args, **kwargs)
    duration = time.time() - start
    return result, duration


def assert_performance(duration, max_seconds, operation_name):
    """Assert that operation completed within time limit."""
    assert duration < max_seconds, (
        f"{operation_name} took {duration:.2f}s, expected < {max_seconds}s"
    )


# ========================================
# INITIALIZATION PERFORMANCE
# ========================================

class TestInitializationPerformance:
    """Test Profiler initialization with different dataset sizes."""

    def test_init_small_df(self, small_df):
        """Should initialize quickly with small dataset."""
        _, duration = time_function(Profiler, small_df, show_progress=False)
        assert_performance(duration, 1.0, "Small dataset initialization")

    def test_init_medium_df(self, medium_df):
        """Should initialize quickly with medium dataset."""
        _, duration = time_function(Profiler, medium_df, show_progress=False)
        assert_performance(duration, 2.0, "Medium dataset initialization")

    def test_init_large_df(self, large_df):
        """Should initialize quickly with large dataset."""
        _, duration = time_function(Profiler, large_df, show_progress=False)
        assert_performance(duration, 3.0, "Large dataset initialization")

    @pytest.mark.slow
    def test_init_extra_large_df(self, extra_large_df):
        """Should initialize with extra large dataset (marked slow)."""
        _, duration = time_function(Profiler, extra_large_df, show_progress=False)
        assert_performance(duration, 5.0, "Extra large dataset initialization")


# ========================================
# DATASET PROFILING PERFORMANCE
# ========================================

class TestDatasetProfilingPerformance:
    """Test dataset-level profiling performance."""

    def test_profile_dataset_small(self, small_df):
        """Should profile small dataset quickly."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.profile_dataset)
        assert_performance(duration, 2.0, "Small dataset profiling")

    def test_profile_dataset_medium(self, medium_df):
        """Should profile medium dataset in reasonable time."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.profile_dataset)
        assert_performance(duration, 5.0, "Medium dataset profiling")

    def test_profile_dataset_large(self, large_df):
        """Should profile large dataset in reasonable time."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.profile_dataset)
        assert_performance(duration, 10.0, "Large dataset profiling")

    def test_get_summary_stats_small(self, small_df):
        """Should get summary stats for small dataset quickly."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.get_summary_stats)
        assert_performance(duration, 1.0, "Small dataset summary stats")

    def test_get_summary_stats_medium(self, medium_df):
        """Should get summary stats for medium dataset quickly."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.get_summary_stats)
        assert_performance(duration, 3.0, "Medium dataset summary stats")

    def test_get_summary_stats_large(self, large_df):
        """Should get summary stats for large dataset in reasonable time."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.get_summary_stats)
        assert_performance(duration, 8.0, "Large dataset summary stats")


# ========================================
# COLUMN PROFILING PERFORMANCE
# ========================================

class TestColumnProfilingPerformance:
    """Test individual column profiling performance."""

    def test_profile_numeric_column_small(self, small_df):
        """Should profile numeric column quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.profile_column, 'age')
        assert_performance(duration, 0.5, "Small dataset numeric column profiling")

    def test_profile_numeric_column_medium(self, medium_df):
        """Should profile numeric column quickly on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.profile_column, 'age')
        assert_performance(duration, 1.0, "Medium dataset numeric column profiling")

    def test_profile_numeric_column_large(self, large_df):
        """Should profile numeric column in reasonable time on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.profile_column, 'age')
        assert_performance(duration, 2.0, "Large dataset numeric column profiling")

    @pytest.mark.slow
    def test_profile_numeric_column_extra_large(self, extra_large_df):
        """Should profile numeric column on extra large dataset."""
        profiler = Profiler(extra_large_df, show_progress=False)
        _, duration = time_function(profiler.profile_column, 'age')
        assert_performance(duration, 5.0, "Extra large dataset numeric column profiling")


# ========================================
# QUALITY ASSESSMENT PERFORMANCE
# ========================================

class TestQualityAssessmentPerformance:
    """Test quality assessment performance."""

    def test_assess_quality_small(self, small_df):
        """Should assess quality quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.assess_quality)
        assert_performance(duration, 2.0, "Small dataset quality assessment")

    def test_assess_quality_medium(self, medium_df):
        """Should assess quality in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.assess_quality)
        assert_performance(duration, 5.0, "Medium dataset quality assessment")

    def test_assess_quality_large(self, large_df):
        """Should assess quality in reasonable time on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.assess_quality)
        assert_performance(duration, 12.0, "Large dataset quality assessment")

    def test_analyze_missing_values_large(self, large_df):
        """Should analyze missing values efficiently on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.analyze_missing_values)
        assert_performance(duration, 2.0, "Large dataset missing values analysis")

    def test_detect_duplicates_large(self, large_df):
        """Should detect duplicates efficiently on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.detect_duplicates)
        assert_performance(duration, 3.0, "Large dataset duplicate detection")


# ========================================
# ANOMALY DETECTION PERFORMANCE
# ========================================

class TestAnomalyDetectionPerformance:
    """Test anomaly detection performance."""

    def test_detect_anomalies_iqr_small(self, small_df):
        """Should detect anomalies (IQR) quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.detect_anomalies, method='iqr')
        assert_performance(duration, 1.0, "Small dataset IQR anomaly detection")

    def test_detect_anomalies_iqr_medium(self, medium_df):
        """Should detect anomalies (IQR) in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.detect_anomalies, method='iqr')
        assert_performance(duration, 3.0, "Medium dataset IQR anomaly detection")

    def test_detect_anomalies_iqr_large(self, large_df):
        """Should detect anomalies (IQR) in reasonable time on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.detect_anomalies, method='iqr')
        assert_performance(duration, 5.0, "Large dataset IQR anomaly detection")

    def test_detect_anomalies_zscore_medium(self, medium_df):
        """Should detect anomalies (Z-score) in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.detect_anomalies, method='zscore')
        assert_performance(duration, 3.0, "Medium dataset Z-score anomaly detection")

    def test_detect_anomalies_isolation_forest_medium(self, medium_df):
        """Should detect anomalies (Isolation Forest) in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.detect_anomalies, method='isolation_forest')
        assert_performance(duration, 10.0, "Medium dataset Isolation Forest anomaly detection")


# ========================================
# PATTERN DETECTION PERFORMANCE
# ========================================

class TestPatternDetectionPerformance:
    """Test pattern detection performance."""

    def test_detect_patterns_small(self, small_df):
        """Should detect patterns quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.detect_patterns)
        assert_performance(duration, 2.0, "Small dataset pattern detection")

    def test_detect_patterns_medium(self, medium_df):
        """Should detect patterns in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.detect_patterns)
        assert_performance(duration, 10.0, "Medium dataset pattern detection")

    def test_detect_patterns_large(self, large_df):
        """Should detect patterns in reasonable time on large dataset."""
        # Note: large_df has 'name' column with string patterns to check
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.detect_patterns)
        assert_performance(duration, 10.0, "Large dataset pattern detection")


# ========================================
# DISTRIBUTION ANALYSIS PERFORMANCE
# ========================================

class TestDistributionPerformance:
    """Test distribution analysis performance."""

    def test_analyze_distributions_small(self, small_df):
        """Should analyze distributions quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.analyze_distributions)
        assert_performance(duration, 1.0, "Small dataset distribution analysis")

    def test_analyze_distributions_medium(self, medium_df):
        """Should analyze distributions in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.analyze_distributions)
        assert_performance(duration, 3.0, "Medium dataset distribution analysis")

    def test_analyze_distributions_large(self, large_df):
        """Should analyze distributions in reasonable time on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.analyze_distributions)
        assert_performance(duration, 5.0, "Large dataset distribution analysis")

    def test_find_correlations_medium(self, medium_df):
        """Should find correlations in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.find_correlations)
        assert_performance(duration, 3.0, "Medium dataset correlation analysis")

    def test_find_correlations_large(self, large_df):
        """Should find correlations in reasonable time on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.find_correlations)
        assert_performance(duration, 5.0, "Large dataset correlation analysis")


# ========================================
# CARDINALITY ANALYSIS PERFORMANCE
# ========================================

class TestCardinalityPerformance:
    """Test cardinality analysis performance."""

    def test_analyze_cardinality_small(self, small_df):
        """Should analyze cardinality quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.analyze_cardinality)
        assert_performance(duration, 1.0, "Small dataset cardinality analysis")

    def test_analyze_cardinality_medium(self, medium_df):
        """Should analyze cardinality in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.analyze_cardinality)
        assert_performance(duration, 3.0, "Medium dataset cardinality analysis")

    def test_analyze_cardinality_large(self, large_df):
        """Should analyze cardinality in reasonable time on large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        _, duration = time_function(profiler.analyze_cardinality)
        assert_performance(duration, 5.0, "Large dataset cardinality analysis")


# ========================================
# REPORT GENERATION PERFORMANCE
# ========================================

class TestReportGenerationPerformance:
    """Test report generation performance."""

    def test_generate_report_dict_small(self, small_df):
        """Should generate dict report quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.generate_report, output_format='dict')
        assert_performance(duration, 5.0, "Small dataset dict report generation")

    def test_generate_report_dict_medium(self, medium_df):
        """Should generate dict report in reasonable time on medium dataset."""
        profiler = Profiler(medium_df, show_progress=False)
        _, duration = time_function(profiler.generate_report, output_format='dict')
        assert_performance(duration, 30.0, "Medium dataset dict report generation")

    def test_generate_report_json_small(self, small_df):
        """Should generate JSON report quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.generate_report, output_format='json')
        assert_performance(duration, 5.0, "Small dataset JSON report generation")

    def test_generate_report_html_small(self, small_df):
        """Should generate HTML report quickly on small dataset."""
        profiler = Profiler(small_df, show_progress=False)
        _, duration = time_function(profiler.generate_report, output_format='html')
        assert_performance(duration, 5.0, "Small dataset HTML report generation")


# ========================================
# CACHING PERFORMANCE
# ========================================

class TestCachingPerformance:
    """Test that caching improves performance on repeated calls."""

    def test_profile_dataset_caching_speedup(self, medium_df):
        """Second call should be much faster due to caching."""
        profiler = Profiler(medium_df, show_progress=False)

        # First call
        _, duration1 = time_function(profiler.profile_dataset)

        # Second call (cached)
        _, duration2 = time_function(profiler.profile_dataset)

        # Cached call should be at least 10x faster
        assert duration2 < duration1 / 10, (
            f"Cached call took {duration2:.4f}s, should be << {duration1:.4f}s"
        )

    def test_profile_column_caching_speedup(self, medium_df):
        """Second call should be much faster due to caching."""
        profiler = Profiler(medium_df, show_progress=False)

        # First call
        _, duration1 = time_function(profiler.profile_column, 'age')

        # Second call (cached)
        _, duration2 = time_function(profiler.profile_column, 'age')

        # Cached call should be at least 10x faster
        assert duration2 < duration1 / 10, (
            f"Cached call took {duration2:.4f}s, should be << {duration1:.4f}s"
        )


# ========================================
# MEMORY EFFICIENCY
# ========================================

class TestMemoryEfficiency:
    """Test memory usage remains reasonable."""

    def test_large_dataset_memory_usage(self, large_df):
        """Should report reasonable memory usage for large dataset."""
        profiler = Profiler(large_df, show_progress=False)
        profile = profiler.profile_dataset()

        # Memory usage should be reported
        assert 'memory_usage_mb' in profile
        assert profile['memory_usage_mb'] > 0

        # Should be less than 100MB for this dataset
        assert profile['memory_usage_mb'] < 100, (
            f"Memory usage {profile['memory_usage_mb']:.2f}MB seems excessive"
        )


# ========================================
# SCALABILITY TESTS
# ========================================

class TestScalability:
    """Test that operations scale reasonably with dataset size."""

    def test_profile_dataset_scales_linearly(self, small_df, medium_df):
        """Profile time should scale roughly linearly with dataset size."""
        # Small dataset (100 rows)
        profiler_small = Profiler(small_df, show_progress=False)
        _, duration_small = time_function(profiler_small.profile_dataset)

        # Medium dataset (10,000 rows = 100x larger)
        profiler_medium = Profiler(medium_df, show_progress=False)
        _, duration_medium = time_function(profiler_medium.profile_dataset)

        # Should not be more than 200x slower (allows for overhead)
        ratio = duration_medium / duration_small if duration_small > 0 else float('inf')
        assert ratio < 200, (
            f"Performance ratio {ratio:.1f}x suggests poor scaling "
            f"({duration_small:.4f}s -> {duration_medium:.4f}s)"
        )


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-m', 'not slow'])
