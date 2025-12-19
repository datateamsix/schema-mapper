"""Tests for canonical schema module."""

import pytest
import pandas as pd
from schema_mapper.canonical import (
    LogicalType,
    ColumnDefinition,
    OptimizationHints,
    CanonicalSchema,
    infer_canonical_schema,
    canonical_schema_to_dict
)


class TestLogicalType:
    """Test LogicalType enum."""

    def test_logical_types_defined(self):
        """Test that all expected logical types are defined."""
        expected_types = ['INTEGER', 'BIGINT', 'FLOAT', 'STRING', 'BOOLEAN', 'DATE', 'TIMESTAMP']
        for type_name in expected_types:
            assert hasattr(LogicalType, type_name)


class TestColumnDefinition:
    """Test ColumnDefinition dataclass."""

    def test_basic_column(self):
        """Test basic column definition."""
        col = ColumnDefinition(name='user_id', logical_type=LogicalType.BIGINT)
        assert col.name == 'user_id'
        assert col.logical_type == LogicalType.BIGINT
        assert col.nullable == True  # Default

    def test_not_null_column(self):
        """Test NOT NULL column."""
        col = ColumnDefinition(name='id', logical_type=LogicalType.INTEGER, nullable=False)
        assert col.nullable == False

    def test_column_with_description(self):
        """Test column with description."""
        col = ColumnDefinition(
            name='email',
            logical_type=LogicalType.STRING,
            description='User email address'
        )
        assert col.description == 'User email address'

    def test_string_column_with_max_length(self):
        """Test string column with max_length."""
        col = ColumnDefinition(
            name='code',
            logical_type=LogicalType.STRING,
            max_length=10
        )
        assert col.max_length == 10


class TestOptimizationHints:
    """Test OptimizationHints dataclass."""

    def test_empty_hints(self):
        """Test empty optimization hints."""
        hints = OptimizationHints()
        assert not hints.has_optimizations()

    def test_partition_columns(self):
        """Test partition columns hint."""
        hints = OptimizationHints(partition_columns=['event_date'])
        assert hints.has_optimizations()
        assert 'event_date' in hints.partition_columns

    def test_cluster_columns(self):
        """Test cluster columns hint."""
        hints = OptimizationHints(cluster_columns=['user_id', 'event_type'])
        assert hints.has_optimizations()
        assert len(hints.cluster_columns) == 2


class TestCanonicalSchema:
    """Test CanonicalSchema dataclass."""

    def test_basic_schema(self):
        """Test basic schema creation."""
        schema = CanonicalSchema(
            table_name='events',
            columns=[
                ColumnDefinition('id', LogicalType.BIGINT, nullable=False),
                ColumnDefinition('name', LogicalType.STRING)
            ]
        )
        assert schema.table_name == 'events'
        assert len(schema.columns) == 2

    def test_schema_with_dataset(self):
        """Test schema with dataset name."""
        schema = CanonicalSchema(
            table_name='events',
            dataset_name='analytics',
            columns=[]
        )
        assert schema.dataset_name == 'analytics'

    def test_get_column(self):
        """Test get_column method."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition('id', LogicalType.INTEGER),
                ColumnDefinition('name', LogicalType.STRING)
            ]
        )
        col = schema.get_column('id')
        assert col is not None
        assert col.name == 'id'

        missing = schema.get_column('nonexistent')
        assert missing is None

    def test_column_names(self):
        """Test column_names method."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition('id', LogicalType.INTEGER),
                ColumnDefinition('name', LogicalType.STRING)
            ]
        )
        names = schema.column_names()
        assert names == ['id', 'name']

    def test_validate_success(self):
        """Test successful validation."""
        schema = CanonicalSchema(
            table_name='events',
            columns=[ColumnDefinition('id', LogicalType.INTEGER)]
        )
        errors = schema.validate()
        assert len(errors) == 0

    def test_validate_missing_table_name(self):
        """Test validation fails without table name."""
        schema = CanonicalSchema(
            table_name='',
            columns=[ColumnDefinition('id', LogicalType.INTEGER)]
        )
        errors = schema.validate()
        assert len(errors) > 0
        assert any('table name' in e.lower() for e in errors)

    def test_validate_no_columns(self):
        """Test validation fails without columns."""
        schema = CanonicalSchema(table_name='test', columns=[])
        errors = schema.validate()
        assert len(errors) > 0
        assert any('column' in e.lower() for e in errors)

    def test_validate_duplicate_columns(self):
        """Test validation fails with duplicate column names."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[
                ColumnDefinition('id', LogicalType.INTEGER),
                ColumnDefinition('id', LogicalType.STRING)  # Duplicate
            ]
        )
        errors = schema.validate()
        assert len(errors) > 0
        assert any('duplicate' in e.lower() for e in errors)

    def test_validate_optimization_column_missing(self):
        """Test validation fails when optimization column doesn't exist."""
        schema = CanonicalSchema(
            table_name='test',
            columns=[ColumnDefinition('id', LogicalType.INTEGER)],
            optimization=OptimizationHints(partition_columns=['nonexistent'])
        )
        errors = schema.validate()
        assert len(errors) > 0
        assert any('partition' in e.lower() and 'not found' in e.lower() for e in errors)


class TestInferCanonicalSchema:
    """Test infer_canonical_schema function."""

    def test_basic_inference(self):
        """Test basic schema inference."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        schema = infer_canonical_schema(df, table_name='users')

        assert schema.table_name == 'users'
        assert len(schema.columns) == 2
        assert schema.get_column('id') is not None
        assert schema.get_column('name') is not None

    def test_type_inference(self):
        """Test logical type inference."""
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True],
            'date_col': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
        })

        schema = infer_canonical_schema(df, table_name='test', auto_cast=False)

        assert schema.get_column('int_col').logical_type == LogicalType.BIGINT
        assert schema.get_column('float_col').logical_type == LogicalType.FLOAT
        assert schema.get_column('str_col').logical_type == LogicalType.STRING
        assert schema.get_column('bool_col').logical_type == LogicalType.BOOLEAN
        assert schema.get_column('date_col').logical_type == LogicalType.TIMESTAMP

    def test_nullable_inference(self):
        """Test nullable/required inference."""
        df = pd.DataFrame({
            'required': [1, 2, 3],
            'nullable': [1, None, 3]
        })

        schema = infer_canonical_schema(df, table_name='test')

        assert schema.get_column('required').nullable == False
        assert schema.get_column('nullable').nullable == True

    def test_column_standardization(self):
        """Test column name standardization."""
        df = pd.DataFrame({
            'User ID': [1, 2, 3],
            'First Name': ['Alice', 'Bob', 'Charlie']
        })

        schema = infer_canonical_schema(df, table_name='test', standardize_columns=True)

        assert schema.get_column('user_id') is not None
        assert schema.get_column('first_name') is not None
        assert schema.get_column('User ID') is None  # Original name not present

    def test_optimization_hints(self):
        """Test optimization hints in inference."""
        df = pd.DataFrame({
            'event_id': [1, 2, 3],
            'user_id': [100, 101, 102],
            'event_date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })

        schema = infer_canonical_schema(
            df,
            table_name='events',
            partition_columns=['event_date'],
            cluster_columns=['user_id']
        )

        assert 'event_date' in schema.optimization.partition_columns
        assert 'user_id' in schema.optimization.cluster_columns

    def test_with_dataset_and_project(self):
        """Test inference with dataset and project ID."""
        df = pd.DataFrame({'id': [1, 2, 3]})

        schema = infer_canonical_schema(
            df,
            table_name='events',
            dataset_name='analytics',
            project_id='my-project'
        )

        assert schema.dataset_name == 'analytics'
        assert schema.project_id == 'my-project'


class TestCanonicalSchemaToDict:
    """Test canonical_schema_to_dict function."""

    def test_basic_serialization(self):
        """Test basic schema to dict conversion."""
        schema = CanonicalSchema(
            table_name='events',
            columns=[
                ColumnDefinition('id', LogicalType.INTEGER, nullable=False),
                ColumnDefinition('name', LogicalType.STRING)
            ]
        )

        schema_dict = canonical_schema_to_dict(schema)

        assert schema_dict['table_name'] == 'events'
        assert len(schema_dict['columns']) == 2
        assert schema_dict['columns'][0]['name'] == 'id'
        assert schema_dict['columns'][0]['logical_type'] == 'integer'
        assert schema_dict['columns'][0]['nullable'] == False

    def test_serialization_with_optimization(self):
        """Test serialization with optimization hints."""
        schema = CanonicalSchema(
            table_name='events',
            columns=[ColumnDefinition('id', LogicalType.INTEGER)],
            optimization=OptimizationHints(
                partition_columns=['event_date'],
                cluster_columns=['user_id']
            )
        )

        schema_dict = canonical_schema_to_dict(schema)

        assert schema_dict['optimization'] is not None
        assert 'event_date' in schema_dict['optimization']['partition_columns']
        assert 'user_id' in schema_dict['optimization']['cluster_columns']
