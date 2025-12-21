"""
Automatic primary key detection from DataFrames.

This module provides intelligent primary key detection with confidence scoring,
supporting both single-column and composite keys.
"""

from typing import List, Dict, Optional, Tuple, Union
import pandas as pd
import numpy as np
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class KeyCandidate:
    """A candidate primary key with confidence metrics."""
    columns: List[str]
    confidence: float  # 0.0 to 1.0
    uniqueness: float  # % of unique combinations
    completeness: float  # % non-null
    cardinality: int  # Number of unique values
    is_composite: bool
    reasoning: str

    def __repr__(self):
        cols = "+".join(self.columns) if self.is_composite else self.columns[0]
        return f"KeyCandidate({cols}, confidence={self.confidence:.2f})"


class PrimaryKeyDetector:
    """
    Detects likely primary keys in a DataFrame.

    Uses heuristics based on:
    - Uniqueness (must be 100% or very close)
    - Completeness (no NULLs preferred)
    - Cardinality (higher is better)
    - Column name patterns (id, key, etc.)
    - Data type (integers/strings preferred)
    """

    def __init__(
        self,
        min_uniqueness: float = 0.995,  # 99.5% unique minimum
        min_confidence: float = 0.7,
        max_composite_keys: int = 3,  # Max columns in composite key
    ):
        """
        Initialize the primary key detector.

        Args:
            min_uniqueness: Minimum uniqueness threshold (0-1)
            min_confidence: Minimum confidence threshold (0-1)
            max_composite_keys: Maximum columns in composite key
        """
        self.min_uniqueness = min_uniqueness
        self.min_confidence = min_confidence
        self.max_composite_keys = max_composite_keys

    def detect_keys(
        self,
        df: pd.DataFrame,
        suggest_composite: bool = True
    ) -> List[KeyCandidate]:
        """
        Detect primary key candidates.

        Args:
            df: DataFrame to analyze
            suggest_composite: Whether to suggest composite keys

        Returns:
            List of KeyCandidate objects, sorted by confidence

        Example:
            >>> detector = PrimaryKeyDetector()
            >>> candidates = detector.detect_keys(df)
            >>> for c in candidates:
            ...     print(f"{c.columns}: {c.confidence:.2f} - {c.reasoning}")
        """
        if df.empty:
            logger.warning("Cannot detect primary keys in empty DataFrame")
            return []

        candidates = []

        # 1. Check single-column candidates
        for col in df.columns:
            candidate = self._evaluate_single_column(df, col)
            if candidate and candidate.confidence >= self.min_confidence:
                candidates.append(candidate)

        # 2. Check composite key candidates (if enabled and no single keys found)
        if suggest_composite and not candidates:
            composite_candidates = self._evaluate_composite_keys(df)
            candidates.extend(composite_candidates)

        # Sort by confidence (highest first)
        candidates.sort(key=lambda x: x.confidence, reverse=True)

        return candidates

    def _evaluate_single_column(
        self,
        df: pd.DataFrame,
        column: str
    ) -> Optional[KeyCandidate]:
        """Evaluate a single column as primary key candidate."""

        # Basic stats
        total_rows = len(df)
        if total_rows == 0:
            return None

        non_null_count = df[column].notna().sum()
        unique_count = df[column].nunique()

        # Calculate metrics
        completeness = non_null_count / total_rows
        uniqueness = unique_count / non_null_count if non_null_count > 0 else 0

        # Must be highly unique
        if uniqueness < self.min_uniqueness:
            return None

        # Calculate confidence
        confidence = self._calculate_confidence(
            column_name=column,
            uniqueness=uniqueness,
            completeness=completeness,
            cardinality=unique_count,
            dtype=df[column].dtype
        )

        # Generate reasoning
        reasoning = self._generate_reasoning(
            column,
            uniqueness,
            completeness,
            unique_count,
            total_rows
        )

        return KeyCandidate(
            columns=[column],
            confidence=confidence,
            uniqueness=uniqueness,
            completeness=completeness,
            cardinality=unique_count,
            is_composite=False,
            reasoning=reasoning
        )

    def _evaluate_composite_keys(
        self,
        df: pd.DataFrame,
        max_combinations: int = 20
    ) -> List[KeyCandidate]:
        """
        Evaluate composite key candidates.

        Tries combinations of 2-3 columns.
        """
        candidates = []

        # Get likely key columns (filter by name pattern and type)
        likely_columns = self._get_likely_key_columns(df)

        if not likely_columns:
            return []

        # Try 2-column combinations
        from itertools import combinations

        for combo in combinations(likely_columns, 2):
            candidate = self._evaluate_composite(df, list(combo))
            if candidate and candidate.confidence >= self.min_confidence:
                candidates.append(candidate)

            if len(candidates) >= max_combinations:
                break

        # Try 3-column combinations if no 2-column keys found
        if not candidates and self.max_composite_keys >= 3:
            for combo in combinations(likely_columns, min(3, len(likely_columns))):
                candidate = self._evaluate_composite(df, list(combo))
                if candidate and candidate.confidence >= self.min_confidence:
                    candidates.append(candidate)

                if len(candidates) >= max_combinations:
                    break

        return candidates

    def _evaluate_composite(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> Optional[KeyCandidate]:
        """Evaluate a composite key."""

        # Check uniqueness of combination
        total_rows = len(df)
        if total_rows == 0:
            return None

        # Check for non-null rows across all columns
        non_null_mask = df[columns].notna().all(axis=1)
        non_null_count = non_null_mask.sum()

        # Get unique combinations
        unique_count = df[columns].drop_duplicates().shape[0]

        completeness = non_null_count / total_rows
        uniqueness = unique_count / non_null_count if non_null_count > 0 else 0

        if uniqueness < self.min_uniqueness:
            return None

        # Lower confidence for composite keys
        base_confidence = (uniqueness + completeness) / 2
        composite_penalty = 0.1 * len(columns)  # Penalize more columns
        confidence = max(0, base_confidence - composite_penalty)

        reasoning = (
            f"Composite key ({'+'.join(columns)}) provides {uniqueness*100:.1f}% uniqueness "
            f"with {completeness*100:.1f}% completeness"
        )

        return KeyCandidate(
            columns=columns,
            confidence=confidence,
            uniqueness=uniqueness,
            completeness=completeness,
            cardinality=unique_count,
            is_composite=True,
            reasoning=reasoning
        )

    def _calculate_confidence(
        self,
        column_name: str,
        uniqueness: float,
        completeness: float,
        cardinality: int,
        dtype
    ) -> float:
        """Calculate confidence score for a key candidate."""

        # Base score from uniqueness and completeness
        base_score = (uniqueness * 0.6) + (completeness * 0.4)

        # Bonus for name patterns
        name_bonus = 0.0
        name_lower = column_name.lower()

        if name_lower in ['id', 'key', 'pk']:
            name_bonus = 0.15
        elif name_lower.endswith('_id') or name_lower.endswith('_key'):
            name_bonus = 0.10
        elif 'id' in name_lower or 'key' in name_lower:
            name_bonus = 0.05

        # Bonus for good data types
        type_bonus = 0.0
        if pd.api.types.is_integer_dtype(dtype):
            type_bonus = 0.05
        elif pd.api.types.is_string_dtype(dtype):
            type_bonus = 0.03

        # Bonus for high cardinality
        cardinality_bonus = min(0.05, cardinality / 1000000)  # Up to 0.05 for 1M+ rows

        # Final confidence (capped at 1.0)
        confidence = min(1.0, base_score + name_bonus + type_bonus + cardinality_bonus)

        return confidence

    def _generate_reasoning(
        self,
        column: str,
        uniqueness: float,
        completeness: float,
        unique_count: int,
        total_rows: int
    ) -> str:
        """Generate human-readable reasoning."""

        reasons = []

        if uniqueness >= 0.999:
            reasons.append(f"{uniqueness*100:.2f}% unique")
        else:
            dup_count = total_rows - unique_count
            reasons.append(f"{uniqueness*100:.2f}% unique ({dup_count} duplicates)")

        if completeness == 1.0:
            reasons.append("100% complete (no NULLs)")
        else:
            null_count = total_rows - int(completeness * total_rows)
            reasons.append(f"{completeness*100:.1f}% complete ({null_count} NULLs)")

        name_lower = column.lower()
        if any(pattern in name_lower for pattern in ['id', 'key', 'pk']):
            reasons.append("column name suggests primary key")

        return "; ".join(reasons)

    def _get_likely_key_columns(self, df: pd.DataFrame) -> List[str]:
        """Filter columns likely to be part of a key."""

        likely = []

        for col in df.columns:
            # Include if:
            # 1. Name suggests it's a key
            name_lower = col.lower()
            is_key_name = any(p in name_lower for p in ['id', 'key', 'code', 'number'])

            # 2. Good data type
            is_good_type = pd.api.types.is_integer_dtype(df[col]) or \
                          pd.api.types.is_string_dtype(df[col])

            # 3. High uniqueness
            if len(df) > 0:
                uniqueness = df[col].nunique() / len(df)
                is_unique_enough = uniqueness > 0.5  # At least 50% unique
            else:
                is_unique_enough = False

            if (is_key_name or is_good_type) and is_unique_enough:
                likely.append(col)

        return likely

    def auto_detect_best_key(
        self,
        df: pd.DataFrame,
        suggest_composite: bool = True
    ) -> Optional[KeyCandidate]:
        """
        Automatically detect the best primary key.

        Returns the highest confidence candidate, or None if no good candidates.

        Example:
            >>> detector = PrimaryKeyDetector()
            >>> best = detector.auto_detect_best_key(df)
            >>> if best:
            ...     print(f"Best key: {best.columns}")
        """
        candidates = self.detect_keys(df, suggest_composite)

        if candidates:
            best = candidates[0]
            if best.confidence >= self.min_confidence:
                return best

        return None


# Backward compatibility functions

def detect_primary_keys(
    df: pd.DataFrame,
    max_candidates: int = 5,
    confidence_threshold: float = 0.8,
    min_confidence: float = None,
    suggest_composite: bool = True
) -> Union[List[str], List[Dict[str, any]]]:
    """
    Convenience function to detect primary keys.

    This function provides backward compatibility with the old API
    while also supporting the new KeyCandidate-based system.

    Args:
        df: DataFrame to analyze
        max_candidates: Maximum number of candidates (legacy)
        confidence_threshold: Minimum confidence (legacy)
        min_confidence: Minimum confidence (new API, overrides confidence_threshold)
        suggest_composite: Whether to suggest composite keys

    Returns:
        List of column names for best candidate (if high confidence)
        OR List of candidate dicts (legacy format)

    Example:
        >>> keys = detect_primary_keys(df)
        >>> print(keys)
        ['user_id']

        >>> # Get detailed candidates
        >>> detector = PrimaryKeyDetector()
        >>> candidates = detector.detect_keys(df)
    """
    threshold = min_confidence if min_confidence is not None else confidence_threshold
    detector = PrimaryKeyDetector(min_confidence=threshold)

    # Try to get best candidate
    best_candidate = detector.auto_detect_best_key(df, suggest_composite)

    if best_candidate and best_candidate.confidence >= threshold:
        return best_candidate.columns
    else:
        # Return legacy format for compatibility
        candidates = detector.detect_keys(df, suggest_composite)
        return [
            {
                'column': c.columns[0] if not c.is_composite else '+'.join(c.columns),
                'confidence': c.confidence,
                'reason': c.reasoning,
                'uniqueness': c.uniqueness * 100,
                'null_pct': (1 - c.completeness) * 100,
                'dtype': 'composite' if c.is_composite else 'single'
            }
            for c in candidates[:max_candidates]
        ]


def suggest_primary_keys(
    df: pd.DataFrame,
    table_name: Optional[str] = None,
    max_suggestions: int = 3
) -> List[str]:
    """
    Get a simple list of suggested primary key column names.

    This is a convenience wrapper that returns just the column names.

    Args:
        df: DataFrame to analyze
        table_name: Optional table name for logging
        max_suggestions: Maximum number of suggestions

    Returns:
        List of column names that could be primary keys

    Example:
        >>> suggestions = suggest_primary_keys(df)
        >>> print(f"Suggested keys: {suggestions}")
        Suggested keys: ['user_id', 'email']
    """
    detector = PrimaryKeyDetector(min_confidence=0.7)
    best = detector.auto_detect_best_key(df, suggest_composite=True)

    if best:
        logger.info(
            f"Suggested primary keys for {table_name or 'table'}: {best.columns}"
        )
        return best.columns
    else:
        logger.warning(
            f"No primary key candidates found for {table_name or 'table'}"
        )
        return []


def validate_primary_keys(
    df: pd.DataFrame,
    primary_keys: List[str],
    allow_nulls: bool = False,
    allow_duplicates: bool = False
) -> Tuple[bool, List[str]]:
    """
    Validate that specified columns can serve as primary keys.

    Checks that the columns:
        - Exist in the DataFrame
        - Have no null values (unless allow_nulls=True)
        - Are unique when combined (unless allow_duplicates=True)

    Args:
        df: DataFrame to validate
        primary_keys: List of column names to validate
        allow_nulls: Whether to allow null values in keys
        allow_duplicates: Whether to allow duplicate key combinations

    Returns:
        Tuple of (is_valid, list_of_errors)
        - is_valid: True if keys are valid
        - list_of_errors: List of validation error messages (empty if valid)

    Example:
        >>> is_valid, errors = validate_primary_keys(df, ['user_id'])
        >>> if not is_valid:
        ...     print("Validation errors:", errors)
    """
    errors = []

    if not primary_keys:
        errors.append("primary_keys list is empty")
        return False, errors

    # Check that all columns exist
    missing_cols = [col for col in primary_keys if col not in df.columns]
    if missing_cols:
        errors.append(f"Columns not found in DataFrame: {missing_cols}")
        return False, errors

    # Check for nulls
    if not allow_nulls:
        for col in primary_keys:
            null_count = df[col].isna().sum()
            if null_count > 0:
                null_pct = (null_count / len(df)) * 100
                errors.append(
                    f"Column '{col}' has {null_count} null values ({null_pct:.1f}%)"
                )

    # Check for duplicates in key combination
    if not allow_duplicates:
        duplicate_count = df.duplicated(subset=primary_keys, keep=False).sum()
        if duplicate_count > 0:
            dup_pct = (duplicate_count / len(df)) * 100
            unique_count = df[primary_keys].drop_duplicates().shape[0]
            errors.append(
                f"Primary key combination has {duplicate_count} duplicate rows ({dup_pct:.1f}%). "
                f"Only {unique_count} unique combinations out of {len(df)} rows."
            )

    is_valid = len(errors) == 0

    if is_valid:
        logger.info(f"Primary keys {primary_keys} validated successfully")
    else:
        logger.warning(f"Primary key validation failed: {errors}")

    return is_valid, errors


def analyze_key_columns(
    df: pd.DataFrame,
    columns: List[str]
) -> Dict[str, any]:
    """
    Analyze specified columns for key suitability.

    Provides detailed statistics about the columns to help determine
    if they're good candidates for primary keys.

    Args:
        df: DataFrame to analyze
        columns: List of column names to analyze

    Returns:
        Dictionary with analysis results

    Example:
        >>> analysis = analyze_key_columns(df, ['user_id', 'order_id'])
        >>> print(f"Uniqueness: {analysis['uniqueness_pct']:.1f}%")
    """
    missing_cols = [col for col in columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

    total_rows = len(df)
    unique_combinations = df[columns].drop_duplicates().shape[0]
    uniqueness_pct = (unique_combinations / total_rows * 100) if total_rows > 0 else 0

    # Null counts
    null_counts = {col: int(df[col].isna().sum()) for col in columns}

    # Get duplicate examples (if any)
    duplicate_examples = []
    if unique_combinations < total_rows:
        duplicated_mask = df.duplicated(subset=columns, keep=False)
        if duplicated_mask.any():
            dup_df = df[duplicated_mask][columns].head(10)
            duplicate_examples = dup_df.to_dict('records')

    # Column data types
    column_dtypes = {col: str(df[col].dtype) for col in columns}

    # Sample values
    sample_values = {}
    for col in columns:
        samples = df[col].dropna().head(5).tolist()
        sample_values[col] = samples

    return {
        'total_rows': total_rows,
        'unique_combinations': unique_combinations,
        'uniqueness_pct': uniqueness_pct,
        'null_counts': null_counts,
        'duplicate_examples': duplicate_examples,
        'column_dtypes': column_dtypes,
        'sample_values': sample_values,
        'is_unique': uniqueness_pct == 100,
        'has_nulls': any(count > 0 for count in null_counts.values())
    }


def get_composite_key_suggestions(
    df: pd.DataFrame,
    max_columns: int = 3,
    max_suggestions: int = 3
) -> List[List[str]]:
    """
    Suggest composite (multi-column) primary keys.

    Tries to find combinations of columns that together form unique keys.

    Args:
        df: DataFrame to analyze
        max_columns: Maximum number of columns in a composite key
        max_suggestions: Maximum number of suggestions to return

    Returns:
        List of column name lists (each list is a composite key suggestion)

    Example:
        >>> suggestions = get_composite_key_suggestions(df)
        >>> for key in suggestions:
        ...     print(f"Composite key: {key}")
    """
    detector = PrimaryKeyDetector(max_composite_keys=max_columns)

    # First try single columns
    single_candidates = detector.detect_keys(df, suggest_composite=False)
    suggestions = []

    for candidate in single_candidates[:max_suggestions]:
        suggestions.append(candidate.columns)

    if len(suggestions) >= max_suggestions:
        return suggestions[:max_suggestions]

    # Try composite keys
    composite_candidates = detector._evaluate_composite_keys(df)
    for candidate in composite_candidates:
        if candidate.columns not in suggestions:
            suggestions.append(candidate.columns)
            if len(suggestions) >= max_suggestions:
                break

    return suggestions[:max_suggestions]
