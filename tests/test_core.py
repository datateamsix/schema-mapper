"""Tests for core SchemaMapper functionality."""

import pytest
import pandas as pd
import numpy as np
from schema_mapper import SchemaMapper, create_schema, prepare_for_load


class TestSchemaMapper:
    """Test SchemaMapper class."""
    
    def test_initialization(self):
        """Test SchemaMapper initialization."""
        mapper = SchemaMapper('bigquery')
        assert mapper.target_type == 'bigquery'
        assert mapper.type_map is not None
        
    def test_invalid_platform(self):
        """Test initialization with invalid platform."""
        with pytest.raises(ValueError):
            SchemaMapper('invalid_platform')
    
    def test_generate_schema_basic(self):
        """Test basic schema generation."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        mapper = SchemaMapper('bigquery')
        schema, mapping = mapper.generate_schema(df, standardize_columns=False, auto_cast=False)
        
        assert len(schema) == 2
        assert schema[0]['name'] == 'id'
        assert schema[0]['type'] == 'INTEGER'
        assert schema[1]['name'] == 'name'
        assert schema[1]['type'] == 'STRING'
    
    def test_column_standardization(self):
        """Test column name standardization."""
        df = pd.DataFrame({
            'User ID': [1, 2, 3],
            'First Name': ['Alice', 'Bob', 'Charlie']
        })
        
        mapper = SchemaMapper('bigquery')
        schema, mapping = mapper.generate_schema(df, standardize_columns=True)
        
        assert mapping['User ID'] == 'user_id'
        assert mapping['First Name'] == 'first_name'
        assert schema[0]['name'] == 'user_id'
        assert schema[1]['name'] == 'first_name'
    
    def test_null_detection(self):
        """Test NULL mode detection."""
        df = pd.DataFrame({
            'required': [1, 2, 3],
            'nullable': [1, None, 3]
        })
        
        mapper = SchemaMapper('bigquery')
        schema, _ = mapper.generate_schema(df, standardize_columns=False, auto_cast=False)
        
        assert schema[0]['mode'] == 'REQUIRED'
        assert schema[1]['mode'] == 'NULLABLE'
    
    def test_prepare_dataframe(self):
        """Test DataFrame preparation."""
        df = pd.DataFrame({
            'User ID': [1, 2, 3],
            'Is Active': ['yes', 'no', 'yes']
        })
        
        mapper = SchemaMapper('bigquery')
        df_prepared = mapper.prepare_dataframe(df)
        
        assert 'user_id' in df_prepared.columns
        assert 'is_active' in df_prepared.columns
        assert df_prepared['is_active'].dtype == bool
    
    def test_validate_dataframe(self):
        """Test DataFrame validation."""
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        mapper = SchemaMapper('bigquery')
        issues = mapper.validate_dataframe(df)
        
        assert 'errors' in issues
        assert 'warnings' in issues
        assert len(issues['errors']) == 0
    
    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        df = pd.DataFrame()
        
        mapper = SchemaMapper('bigquery')
        issues = mapper.validate_dataframe(df)
        
        assert len(issues['errors']) > 0
        assert 'empty' in issues['errors'][0].lower()
    
    def test_generate_ddl_bigquery(self):
        """Test BigQuery DDL generation."""
        df = pd.DataFrame({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})
        
        mapper = SchemaMapper('bigquery')
        ddl = mapper.generate_ddl(df, 'test_table', 'test_dataset', 'test_project')
        
        assert 'CREATE TABLE' in ddl
        assert '`test_project.test_dataset.test_table`' in ddl
        assert 'id INTEGER' in ddl
        assert 'name STRING' in ddl
    
    def test_generate_bigquery_schema_json(self):
        """Test BigQuery schema JSON generation."""
        df = pd.DataFrame({'id': [1, 2, 3]})
        
        mapper = SchemaMapper('bigquery')
        schema_json = mapper.generate_bigquery_schema_json(df)
        
        import json
        schema = json.loads(schema_json)
        assert len(schema) == 1
        assert schema[0]['name'] == 'id'
        assert schema[0]['type'] == 'INTEGER'
        assert 'mode' in schema[0]


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_create_schema(self):
        """Test create_schema function."""
        df = pd.DataFrame({'id': [1, 2, 3]})
        schema = create_schema(df, 'bigquery')
        
        assert len(schema) == 1
        assert schema[0]['name'] == 'id'
    
    def test_create_schema_with_mapping(self):
        """Test create_schema with mapping return."""
        df = pd.DataFrame({'User ID': [1, 2, 3]})
        schema, mapping = create_schema(df, 'bigquery', return_mapping=True)
        
        assert len(schema) == 1
        assert 'User ID' in mapping
        assert mapping['User ID'] == 'user_id'
    
    def test_prepare_for_load(self):
        """Test prepare_for_load function."""
        df = pd.DataFrame({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})
        df_clean, schema, issues = prepare_for_load(df, 'bigquery')
        
        assert len(df_clean) == 3
        assert len(schema) == 2
        assert 'errors' in issues
        assert 'warnings' in issues


class TestMultiplePlatforms:
    """Test schema generation for all platforms."""
    
    @pytest.mark.parametrize('platform', [
        'bigquery', 'snowflake', 'redshift', 'sqlserver', 'postgresql'
    ])
    def test_platform_schema_generation(self, platform):
        """Test schema generation for each platform."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [1.5, 2.5, 3.5],
            'text': ['a', 'b', 'c']
        })
        
        mapper = SchemaMapper(platform)
        schema, _ = mapper.generate_schema(df, standardize_columns=False, auto_cast=False)
        
        assert len(schema) == 3
        assert all('name' in field for field in schema)
        assert all('type' in field for field in schema)
        assert all('mode' in field for field in schema)
    
    @pytest.mark.parametrize('platform', [
        'bigquery', 'snowflake', 'redshift', 'sqlserver', 'postgresql'
    ])
    def test_platform_ddl_generation(self, platform):
        """Test DDL generation for each platform."""
        df = pd.DataFrame({'id': [1, 2, 3]})
        
        mapper = SchemaMapper(platform)
        ddl = mapper.generate_ddl(df, 'test_table', 'test_schema')
        
        assert 'CREATE TABLE' in ddl
        assert 'test_table' in ddl
