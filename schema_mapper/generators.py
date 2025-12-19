"""
DDL generation for different database platforms.

This module provides platform-specific DDL generators for creating
CREATE TABLE statements.
"""

from typing import List, Dict, Optional
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class DDLGenerator(ABC):
    """Abstract base class for DDL generators."""
    
    @abstractmethod
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """Generate DDL statement."""
        pass


class BigQueryDDLGenerator(DDLGenerator):
    """Generate DDL for BigQuery."""
    
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """
        Generate BigQuery CREATE TABLE statement.
        
        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Dataset name
            project_id: GCP project ID
            
        Returns:
            DDL statement
        """
        # Build full table name
        if project_id and dataset_name:
            full_table = f"`{project_id}.{dataset_name}.{table_name}`"
        elif dataset_name:
            full_table = f"`{dataset_name}.{table_name}`"
        else:
            full_table = f"`{table_name}`"
        
        # Build column definitions
        col_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            if field.get('description'):
                # Escape quotes in description
                desc = field['description'].replace('"', '\\"')
                col_def += f" OPTIONS(description=\"{desc}\")"
            col_defs.append(col_def)
        
        ddl = f"CREATE TABLE {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"
        
        logger.info(f"Generated BigQuery DDL for {full_table}")
        return ddl


class SnowflakeDDLGenerator(DDLGenerator):
    """Generate DDL for Snowflake."""
    
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """
        Generate Snowflake CREATE TABLE statement.
        
        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name
            project_id: Not used for Snowflake
            
        Returns:
            DDL statement
        """
        full_table = f"{dataset_name}.{table_name}" if dataset_name else table_name
        
        col_defs = []
        for field in schema:
            col_def = f"  {field['name']} {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            if field.get('description'):
                # Escape single quotes in description
                desc = field['description'].replace("'", "''")
                col_def += f" COMMENT '{desc}'"
            col_defs.append(col_def)
        
        ddl = f"CREATE TABLE {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"
        
        logger.info(f"Generated Snowflake DDL for {full_table}")
        return ddl


class RedshiftDDLGenerator(DDLGenerator):
    """Generate DDL for Redshift."""
    
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """
        Generate Redshift CREATE TABLE statement.
        
        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name
            project_id: Not used for Redshift
            
        Returns:
            DDL statement with separate COMMENT statements
        """
        full_table = f"{dataset_name}.{table_name}" if dataset_name else table_name
        
        col_defs = []
        for field in schema:
            col_def = f'  "{field["name"]}" {field["type"]}'
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            col_defs.append(col_def)
        
        ddl = f"CREATE TABLE {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"
        
        # Add column comments separately (Redshift requirement)
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"COMMENT ON COLUMN {full_table}.{field['name']} IS '{desc}';"
                comments.append(comment)
        
        if comments:
            ddl += "\n\n-- Column comments\n" + "\n".join(comments)
        
        logger.info(f"Generated Redshift DDL for {full_table}")
        return ddl


class SQLServerDDLGenerator(DDLGenerator):
    """Generate DDL for SQL Server."""
    
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """
        Generate SQL Server CREATE TABLE statement.
        
        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name (defaults to 'dbo')
            project_id: Not used for SQL Server
            
        Returns:
            DDL statement with extended properties for descriptions
        """
        schema_name = dataset_name or 'dbo'
        full_table = f"[{schema_name}].[{table_name}]"
        
        col_defs = []
        for field in schema:
            col_def = f"  [{field['name']}] {field['type']}"
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            else:
                col_def += " NULL"
            col_defs.append(col_def)
        
        ddl = f"CREATE TABLE {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"
        
        # Add extended properties for descriptions
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"""EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'{desc}',
    @level0type = N'SCHEMA', @level0name = N'{schema_name}',
    @level1type = N'TABLE', @level1name = N'{table_name}',
    @level2type = N'COLUMN', @level2name = N'{field['name']}';"""
                comments.append(comment)
        
        if comments:
            ddl += "\n\n-- Column descriptions\n" + "\n".join(comments)
        
        logger.info(f"Generated SQL Server DDL for {full_table}")
        return ddl


class PostgreSQLDDLGenerator(DDLGenerator):
    """Generate DDL for PostgreSQL."""
    
    def generate(
        self,
        schema: List[Dict],
        table_name: str,
        dataset_name: Optional[str] = None,
        project_id: Optional[str] = None
    ) -> str:
        """
        Generate PostgreSQL CREATE TABLE statement.
        
        Args:
            schema: List of field dictionaries
            table_name: Table name
            dataset_name: Schema name
            project_id: Not used for PostgreSQL
            
        Returns:
            DDL statement with COMMENT statements
        """
        full_table = f'"{dataset_name}"."{table_name}"' if dataset_name else f'"{table_name}"'
        
        col_defs = []
        for field in schema:
            col_def = f'  "{field["name"]}" {field["type"]}'
            if field.get('mode') == 'REQUIRED':
                col_def += " NOT NULL"
            col_defs.append(col_def)
        
        ddl = f"CREATE TABLE {full_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"
        
        # Add column comments
        comments = []
        for field in schema:
            if field.get('description'):
                desc = field['description'].replace("'", "''")
                comment = f"COMMENT ON COLUMN {full_table}.{field['name']} IS '{desc}';"
                comments.append(comment)
        
        if comments:
            ddl += "\n\n-- Column comments\n" + "\n".join(comments)
        
        logger.info(f"Generated PostgreSQL DDL for {full_table}")
        return ddl


# Factory function
def get_ddl_generator(platform: str) -> DDLGenerator:
    """
    Get the appropriate DDL generator for a platform.
    
    Args:
        platform: Target database platform
        
    Returns:
        DDLGenerator instance
        
    Raises:
        ValueError: If platform is not supported
    """
    generators = {
        'bigquery': BigQueryDDLGenerator,
        'snowflake': SnowflakeDDLGenerator,
        'redshift': RedshiftDDLGenerator,
        'sqlserver': SQLServerDDLGenerator,
        'postgresql': PostgreSQLDDLGenerator,
    }
    
    platform_lower = platform.lower()
    if platform_lower not in generators:
        raise ValueError(
            f"No DDL generator for platform: {platform}. "
            f"Supported: {', '.join(generators.keys())}"
        )
    
    return generators[platform_lower]()
