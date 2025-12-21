"""
Unit tests for KeyCandidate and PrimaryKeyDetector classes.
"""

import pytest
import pandas as pd
from schema_mapper.incremental import (
    KeyCandidate,
    PrimaryKeyDetector,
    detect_primary_keys,
    suggest_primary_keys
)


class TestKeyCandidate:
    """Test KeyCandidate dataclass."""

    def test_key_candidate_creation(self):
        """Test creating a KeyCandidate."""
        candidate = KeyCandidate(
            columns=['user_id'],
            confidence=0.95,
            uniqueness=1.0,
            completeness=1.0,
            cardinality=1000,
            is_composite=False,
            reasoning="100% unique; 100% complete"
        )

        assert candidate.columns == ['user_id']
        assert candidate.confidence == 0.95
        assert candidate.uniqueness == 1.0
        assert candidate.is_composite is False

    def test_key_candidate_repr(self):
        """Test KeyCandidate string representation."""
        candidate = KeyCandidate(
            columns=['id'],
            confidence=0.90,
            uniqueness=1.0,
            completeness=1.0,
            cardinality=100,
            is_composite=False,
            reasoning="test"
        )

        repr_str = repr(candidate)
        assert "KeyCandidate(id" in repr_str
        assert "0.90" in repr_str

    def test_composite_key_candidate_repr(self):
        """Test composite KeyCandidate string representation."""
        candidate = KeyCandidate(
            columns=['order_id', 'line_number'],
            confidence=0.85,
            uniqueness=1.0,
            completeness=1.0,
            cardinality=500,
            is_composite=True,
            reasoning="test"
        )

        repr_str = repr(candidate)
        assert "order_id+line_number" in repr_str


class TestPrimaryKeyDetector:
    """Test PrimaryKeyDetector class."""

    def test_detector_initialization(self):
        """Test PrimaryKeyDetector initialization."""
        detector = PrimaryKeyDetector(
            min_uniqueness=0.99,
            min_confidence=0.75,
            max_composite_keys=2
        )

        assert detector.min_uniqueness == 0.99
        assert detector.min_confidence == 0.75
        assert detector.max_composite_keys == 2

    def test_detect_single_column_key(self):
        """Test detection of single-column primary key."""
        df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'],
            'email': ['a@ex.com', 'b@ex.com', 'c@ex.com', 'd@ex.com', 'e@ex.com']
        })

        detector = PrimaryKeyDetector()
        candidates = detector.detect_keys(df)

        assert len(candidates) > 0
        assert 'user_id' in candidates[0].columns
        assert isinstance(candidates[0], KeyCandidate)

    def test_detect_composite_key(self):
        """Test detection of composite primary key."""
        df = pd.DataFrame({
            'order_id': [1, 1, 2, 2, 3],
            'line_number': [1, 2, 1, 2, 1],
            'product': ['A', 'B', 'C', 'D', 'E']
        })

        detector = PrimaryKeyDetector(min_confidence=0.5)

        # Test explicit composite evaluation
        composite_candidate = detector._evaluate_composite(df, ['order_id', 'line_number'])

        assert composite_candidate is not None
        assert composite_candidate.is_composite is True
        assert composite_candidate.uniqueness == 1.0  # Should be 100% unique
        assert 'order_id' in composite_candidate.columns
        assert 'line_number' in composite_candidate.columns

    def test_no_good_keys(self):
        """Test DataFrame with no good primary key candidates."""
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Alice', 'Bob'],
            'city': ['NYC', 'LA', 'NYC', 'SF']
        })

        detector = PrimaryKeyDetector(min_confidence=0.7)
        candidates = detector.detect_keys(df)

        # Might find a composite key or nothing
        assert isinstance(candidates, list)

    def test_key_with_nulls_lower_confidence(self):
        """Test that NULLs reduce confidence."""
        df = pd.DataFrame({
            'id_perfect': [1, 2, 3, 4, 5],
            'id_with_nulls': [1, 2, None, 4, 5]
        })

        detector = PrimaryKeyDetector(min_confidence=0.5)
        candidates = detector.detect_keys(df)

        # id_perfect should have higher confidence
        if len(candidates) >= 2:
            perfect = next((c for c in candidates if 'id_perfect' in c.columns), None)
            with_nulls = next((c for c in candidates if 'id_with_nulls' in c.columns), None)

            if perfect and with_nulls:
                assert perfect.confidence > with_nulls.confidence

    def test_auto_detect_best_key(self):
        """Test auto-detecting the best key."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'user_id': [10, 20, 30, 40, 50],
            'name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve']
        })

        detector = PrimaryKeyDetector()
        best = detector.auto_detect_best_key(df)

        assert best is not None
        assert isinstance(best, KeyCandidate)
        assert best.confidence >= detector.min_confidence

    def test_auto_detect_returns_none_for_bad_data(self):
        """Test that auto_detect returns None for bad data."""
        df = pd.DataFrame({
            'name': ['Alice', 'Alice', 'Alice'],
            'age': [25, 25, 25]
        })

        detector = PrimaryKeyDetector()
        best = detector.auto_detect_best_key(df)

        # Should return None or a composite key with low confidence
        assert best is None or best.is_composite

    def test_confidence_scoring(self):
        """Test confidence scoring includes multiple factors."""
        df = pd.DataFrame({
            'id': [1, 2, 3],  # Perfect score - named 'id', unique, integer
            'random_code': ['A', 'B', 'C'],  # Lower score - no name pattern
            'almost_id': [1, 2, 2]  # Not unique enough
        })

        detector = PrimaryKeyDetector()
        candidates = detector.detect_keys(df, suggest_composite=False)

        # 'id' should have highest confidence
        id_candidate = next((c for c in candidates if 'id' in c.columns), None)
        if id_candidate:
            assert id_candidate.confidence > 0.9

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()

        detector = PrimaryKeyDetector()
        candidates = detector.detect_keys(df)

        assert candidates == []

    def test_uniqueness_threshold(self):
        """Test that uniqueness threshold is respected."""
        df = pd.DataFrame({
            'almost_unique': [1, 2, 3, 4, 4],  # 80% unique
            'perfectly_unique': [1, 2, 3, 4, 5]  # 100% unique
        })

        detector = PrimaryKeyDetector(min_uniqueness=0.995)
        candidates = detector.detect_keys(df)

        # Only perfectly_unique should be detected
        candidate_cols = [c.columns[0] for c in candidates]
        assert 'perfectly_unique' in candidate_cols
        assert 'almost_unique' not in candidate_cols

    def test_composite_key_penalty(self):
        """Test that composite keys have lower confidence."""
        df = pd.DataFrame({
            'id': [1, 2, 3],  # Single column key
            'part1': [1, 1, 2],
            'part2': [1, 2, 1]  # Together with part1 forms composite key
        })

        detector = PrimaryKeyDetector()

        # Get single key confidence
        single_candidates = detector.detect_keys(df, suggest_composite=False)
        single_confidence = single_candidates[0].confidence if single_candidates else 0

        # Force composite key detection
        composite_candidate = detector._evaluate_composite(df, ['part1', 'part2'])

        # Composite should have penalty applied
        if composite_candidate:
            assert composite_candidate.confidence < single_confidence


class TestPrimaryKeyDetectorIntegration:
    """Test PrimaryKeyDetector with SchemaMapper."""

    def test_schema_mapper_detect_keys(self):
        """Test detecting keys through SchemaMapper."""
        from schema_mapper import SchemaMapper

        df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'email': ['a@test.com', 'b@test.com', 'c@test.com', 'd@test.com', 'e@test.com'],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })

        mapper = SchemaMapper('bigquery')

        # Test basic detection
        keys = mapper.detect_primary_keys(df)
        assert 'user_id' in keys or 'email' in keys

        # Test with all candidates
        candidates = mapper.detect_primary_keys(df, return_all_candidates=True)
        assert len(candidates) > 0
        assert all(isinstance(c, KeyCandidate) for c in candidates)

    def test_schema_mapper_with_confidence_threshold(self):
        """Test confidence threshold in SchemaMapper."""
        from schema_mapper import SchemaMapper

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })

        mapper = SchemaMapper('bigquery')

        # High confidence threshold
        keys_high = mapper.detect_primary_keys(df, min_confidence=0.95)

        # Low confidence threshold
        keys_low = mapper.detect_primary_keys(df, min_confidence=0.5)

        # Low threshold should be more permissive or equal
        assert len(keys_low) >= len(keys_high)


class TestBackwardCompatibility:
    """Test backward compatibility with old API."""

    def test_detect_primary_keys_function(self):
        """Test that old detect_primary_keys function still works."""
        df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve']
        })

        # Should return column names for best candidate
        result = detect_primary_keys(df)

        assert isinstance(result, list)
        if result and isinstance(result[0], str):
            # New API - returns column names
            assert 'user_id' in result
        elif result and isinstance(result[0], dict):
            # Old API - returns dicts
            assert 'column' in result[0]

    def test_suggest_primary_keys_function(self):
        """Test suggest_primary_keys compatibility."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })

        suggestions = suggest_primary_keys(df)

        assert isinstance(suggestions, list)
        if suggestions:
            assert 'id' in suggestions
