"""Pytest configuration and fixtures."""

import pytest
import pandas as pd


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
        'active': [True, True, False, True, False]
    })


@pytest.fixture
def messy_dataframe():
    """Create a messy DataFrame for testing."""
    return pd.DataFrame({
        'User ID': [1, 2, 3],
        'First Name': ['Alice', 'Bob', 'Charlie'],
        'Created At': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Is Active?': ['yes', 'no', 'yes'],
        'Account Balance ($)': ['1000.50', '2000.75', '3000.00']
    })


@pytest.fixture
def dataframe_with_nulls():
    """Create a DataFrame with NULL values."""
    return pd.DataFrame({
        'required': [1, 2, 3, 4, 5],
        'nullable': [1, None, 3, None, 5],
        'sparse': [1] + [None] * 4
    })
