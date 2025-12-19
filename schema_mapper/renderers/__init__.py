"""
Schema renderers - convert canonical schemas to platform-specific formats.

This package implements the Renderer Pattern for generating platform-specific
DDL, JSON schemas, and CLI commands from canonical schemas.
"""

from .base import SchemaRenderer
from .factory import RendererFactory
from .bigquery import BigQueryRenderer
from .snowflake import SnowflakeRenderer
from .redshift import RedshiftRenderer
from .postgresql import PostgreSQLRenderer

__all__ = [
    'SchemaRenderer',
    'RendererFactory',
    'BigQueryRenderer',
    'SnowflakeRenderer',
    'RedshiftRenderer',
    'PostgreSQLRenderer',
]
