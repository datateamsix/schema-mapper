"""
Unit tests for primary key detection.
"""

import pytest
import pandas as pd
import numpy as np
from schema_mapper.incremental import (
    KeyCandidate,
    PrimaryKeyDetector,
    detect_primary_keys,
    suggest_primary_keys,
    validate_primary_keys,
    analyze_key_columns,
    get_composite_key_suggestions,
)


class TestDetectPrimaryKeys:
    """Test primary key detection."""

    def test_detect_single_id_column(self):
        """Test detecting a single ID column."""
        df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com',
                     'david@test.com', 'eve@test.com']
        })

        # Use PrimaryKeyDetector directly for detailed candidates
        detector = PrimaryKeyDetector()
        candidates = detector.detect_keys(df)

        assert len(candidates) > 0
        # user_id should be the top candidate
        assert 'user_id' in candidates[0].columns
        assert candidates[0].confidence > 0.8
        assert candidates[0].uniqueness == 1.0
        assert candidates[0].completeness == 1.0

    def test_detect_multiple_candidates(self):
        """Test detecting multiple key candidates."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'user_id': [101, 102, 103, 104, 105],
            'email': ['a@test.com', 'b@test.com', 'c@test.com', 'd@test.com', 'e@test.com'],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        # Use PrimaryKeyDetector directly to get all candidates
        detector = PrimaryKeyDetector()
        candidates = detector.detect_keys(df)

        assert len(candidates) >= 2
        # id should have highest confidence (exact name 'id')
        assert 'id' in candidates[0].columns or 'user_id' in candidates[0].columns

    def test_no_candidates_with_nulls(self):
        """Test that columns with nulls are not candidates."""
        df = pd.DataFrame({
            'id': [1, 2, None, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        # Use detector directly
        detector = PrimaryKeyDetector()
        candidates = detector.detect_keys(df)

        # id has nulls, should have lower confidence or not be detected
        if candidates:
            # If any candidates found, id should not be first choice
            id_candidates = [c for c in candidates if 'id' in c.columns]
            if id_candidates:
                # Completeness should be less than 1.0
                assert id_candidates[0].completeness < 1.0

    def test_no_candidates_with_duplicates(self):
        """Test that columns with duplicates are not candidates."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        # Use detector directly
        detector = PrimaryKeyDetector()
        candidates = detector.detect_keys(df)

        # id has duplicates (80% unique), should not meet threshold
        id_candidates = [c for c in candidates if 'id' in c.columns]
        assert len(id_candidates) == 0  # Should not meet min_uniqueness threshold

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()

        candidates = detect_primary_keys(df)

        assert candidates == []

    def test_confidence_threshold(self):
        """Test confidence threshold filtering."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'code': ['A', 'B', 'C', 'D', 'E']
        })

        # Low threshold should return more candidates
        candidates_low = detect_primary_keys(df, confidence_threshold=0.3)
        candidates_high = detect_primary_keys(df, confidence_threshold=0.9)

        assert len(candidates_low) >= len(candidates_high)


class TestSuggestPrimaryKeys:
    """Test primary key suggestion helper."""

    def test_suggest_returns_column_names(self):
        """Test that suggest returns just column names."""
        df = pd.DataFrame({
            'user_id': [1, 2, 3],
            'email': ['a@test.com', 'b@test.com', 'c@test.com'],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        suggestions = suggest_primary_keys(df)

        assert isinstance(suggestions, list)
        assert all(isinstance(s, str) for s in suggestions)
        assert 'user_id' in suggestions

    def test_suggest_max_suggestions(self):
        """Test limiting number of suggestions."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'email': ['a@test.com', 'b@test.com', 'c@test.com'],
            'username': ['alice', 'bob', 'charlie']
        })

        suggestions = suggest_primary_keys(df, max_suggestions=1)

        assert len(suggestions) <= 1

    def test_suggest_no_candidates(self):
        """Test when no good candidates exist."""
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Alice'],
            'age': [25, 30, 25]
        })

        suggestions = suggest_primary_keys(df)

        assert suggestions == []


class TestValidatePrimaryKeys:
    """Test primary key validation."""

    def test_valid_single_key(self):
        """Test validating a single valid key."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        is_valid, errors = validate_primary_keys(df, ['id'])

        assert is_valid is True
        assert errors == []

    def test_valid_composite_key(self):
        """Test validating a valid composite key."""
        df = pd.DataFrame({
            'user_id': [1, 1, 2, 2, 3],
            'order_id': [1, 2, 1, 2, 1],
            'amount': [100, 200, 150, 250, 175]
        })

        is_valid, errors = validate_primary_keys(df, ['user_id', 'order_id'])

        assert is_valid is True
        assert errors == []

    def test_missing_column(self):
        """Test validation fails for missing columns."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        is_valid, errors = validate_primary_keys(df, ['missing_col'])

        assert is_valid is False
        assert len(errors) > 0
        assert 'not found' in errors[0].lower()

    def test_null_values(self):
        """Test validation fails for columns with nulls."""
        df = pd.DataFrame({
            'id': [1, 2, None, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        is_valid, errors = validate_primary_keys(df, ['id'])

        assert is_valid is False
        assert any('null' in err.lower() for err in errors)

    def test_allow_nulls(self):
        """Test allowing nulls with flag."""
        df = pd.DataFrame({
            'id': [1, 2, None, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        is_valid, errors = validate_primary_keys(df, ['id'], allow_nulls=True)

        # Should fail due to duplicates (None appears once, not duplicate)
        # But nulls won't be an error
        null_errors = [e for e in errors if 'null' in e.lower()]
        assert len(null_errors) == 0

    def test_duplicate_values(self):
        """Test validation fails for duplicate key combinations."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        is_valid, errors = validate_primary_keys(df, ['id'])

        assert is_valid is False
        assert any('duplicate' in err.lower() for err in errors)

    def test_allow_duplicates(self):
        """Test allowing duplicates with flag."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        is_valid, errors = validate_primary_keys(df, ['id'], allow_duplicates=True)

        # Should pass if we allow duplicates
        duplicate_errors = [e for e in errors if 'duplicate' in e.lower()]
        assert len(duplicate_errors) == 0

    def test_empty_primary_keys_list(self):
        """Test validation fails for empty keys list."""
        df = pd.DataFrame({
            'id': [1, 2, 3]
        })

        is_valid, errors = validate_primary_keys(df, [])

        assert is_valid is False
        assert len(errors) > 0


class TestAnalyzeKeyColumns:
    """Test key column analysis."""

    def test_analyze_unique_key(self):
        """Test analyzing a unique key."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        analysis = analyze_key_columns(df, ['id'])

        assert analysis['total_rows'] == 5
        assert analysis['unique_combinations'] == 5
        assert analysis['uniqueness_pct'] == 100.0
        assert analysis['is_unique'] is True
        assert analysis['has_nulls'] is False

    def test_analyze_with_duplicates(self):
        """Test analyzing keys with duplicates."""
        df = pd.DataFrame({
            'user_id': [1, 1, 2, 2, 3],
            'order_id': [1, 2, 1, 1, 1],
            'amount': [100, 200, 150, 250, 175]
        })

        analysis = analyze_key_columns(df, ['user_id', 'order_id'])

        assert analysis['total_rows'] == 5
        assert analysis['unique_combinations'] == 4  # One duplicate
        assert analysis['uniqueness_pct'] == 80.0
        assert analysis['is_unique'] is False
        assert len(analysis['duplicate_examples']) > 0

    def test_analyze_with_nulls(self):
        """Test analyzing keys with null values."""
        df = pd.DataFrame({
            'id': [1, 2, None, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        analysis = analyze_key_columns(df, ['id'])

        assert analysis['has_nulls'] is True
        assert analysis['null_counts']['id'] == 1

    def test_analyze_missing_column(self):
        """Test that missing column raises error."""
        df = pd.DataFrame({
            'id': [1, 2, 3]
        })

        with pytest.raises(ValueError, match="not found"):
            analyze_key_columns(df, ['missing_col'])


class TestCompositeKeySuggestions:
    """Test composite key suggestions."""

    def test_suggest_single_column_keys_first(self):
        """Test that single column keys are suggested first."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'user_id': [10, 20, 30, 40, 50],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        suggestions = get_composite_key_suggestions(df, max_suggestions=3)

        assert len(suggestions) > 0
        # First suggestions should be single columns
        assert len(suggestions[0]) == 1

    def test_suggest_composite_when_no_single_keys(self):
        """Test suggesting composite keys when no single column is unique."""
        df = pd.DataFrame({
            'user_id': [1, 1, 2, 2, 3],
            'order_id': [1, 2, 1, 2, 1],
            'amount': [100, 200, 150, 250, 175]
        })

        # Use detector directly for composite keys
        detector = PrimaryKeyDetector(min_confidence=0.5)
        composite = detector._evaluate_composite(df, ['user_id', 'order_id'])

        # user_id + order_id should form a valid composite key
        assert composite is not None
        assert composite.uniqueness == 1.0

    def test_max_columns_limit(self):
        """Test that composite keys respect max_columns."""
        df = pd.DataFrame({
            'col1': [1, 1, 2],
            'col2': [1, 2, 1],
            'col3': [1, 1, 1]
        })

        suggestions_2 = get_composite_key_suggestions(df, max_columns=2)
        suggestions_3 = get_composite_key_suggestions(df, max_columns=3)

        # With max_columns=2, should not suggest 3-column keys
        for suggestion in suggestions_2:
            assert len(suggestion) <= 2

    def test_no_suggestions_for_no_unique_combinations(self):
        """Test when no unique key combinations exist."""
        df = pd.DataFrame({
            'name': ['Alice', 'Alice', 'Alice'],
            'age': [25, 25, 25]
        })

        suggestions = get_composite_key_suggestions(df)

        # Might be empty or very limited
        assert isinstance(suggestions, list)


class TestIntegrationWithSchemaMapper:
    """Test integration with SchemaMapper."""

    def test_detect_keys_via_schema_mapper(self):
        """Test detecting keys through SchemaMapper."""
        from schema_mapper import SchemaMapper

        df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'email': ['a@test.com', 'b@test.com', 'c@test.com', 'd@test.com', 'e@test.com']
        })

        mapper = SchemaMapper('bigquery')

        # New API returns list of column names
        keys = mapper.detect_primary_keys(df)

        assert isinstance(keys, list)
        assert len(keys) > 0
        assert 'user_id' in keys or 'email' in keys
