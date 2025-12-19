"""
Factory for creating platform-specific renderers.

Provides a simple interface to get the right renderer for each platform
without needing to know implementation details.
"""

from typing import Dict, Type
from .base import SchemaRenderer
from .bigquery import BigQueryRenderer
from .snowflake import SnowflakeRenderer
from .redshift import RedshiftRenderer
from .postgresql import PostgreSQLRenderer
from ..canonical import CanonicalSchema


class RendererFactory:
    """
    Factory for creating platform-specific schema renderers.

    Usage:
        renderer = RendererFactory.get_renderer('bigquery', canonical_schema)
        ddl = renderer.to_ddl()
        cli_cmd = renderer.to_cli_create()
    """

    _renderers: Dict[str, Type[SchemaRenderer]] = {
        'bigquery': BigQueryRenderer,
        'snowflake': SnowflakeRenderer,
        'redshift': RedshiftRenderer,
        'postgresql': PostgreSQLRenderer,
    }

    @classmethod
    def get_renderer(cls, platform: str, schema: CanonicalSchema) -> SchemaRenderer:
        """
        Get platform-specific renderer.

        Args:
            platform: Platform name ('bigquery', 'snowflake', 'redshift', 'postgresql')
            schema: Canonical schema to render

        Returns:
            Platform-specific renderer instance

        Raises:
            ValueError: If platform is not supported or schema is invalid
        """
        platform_lower = platform.lower()

        if platform_lower not in cls._renderers:
            raise ValueError(
                f"Unsupported platform: {platform}. "
                f"Supported platforms: {', '.join(cls.supported_platforms())}"
            )

        renderer_class = cls._renderers[platform_lower]
        return renderer_class(schema)

    @classmethod
    def supported_platforms(cls) -> list:
        """Get list of supported platform names."""
        return list(cls._renderers.keys())

    @classmethod
    def supports_json_schema(cls, platform: str) -> bool:
        """
        Check if platform supports JSON schemas.

        Args:
            platform: Platform name

        Returns:
            True if platform supports JSON schemas (currently only BigQuery)
        """
        platform_lower = platform.lower()

        if platform_lower not in cls._renderers:
            return False

        # BigQuery is the only platform with native JSON schema support
        return platform_lower == 'bigquery'

    @classmethod
    def register_renderer(cls, platform: str, renderer_class: Type[SchemaRenderer]):
        """
        Register a custom renderer for a platform.

        This allows extending the factory with new platforms without
        modifying the core code.

        Args:
            platform: Platform name
            renderer_class: Renderer class (must inherit from SchemaRenderer)

        Example:
            class DatabricksRenderer(SchemaRenderer):
                ...

            RendererFactory.register_renderer('databricks', DatabricksRenderer)
        """
        if not issubclass(renderer_class, SchemaRenderer):
            raise TypeError(f"{renderer_class} must inherit from SchemaRenderer")

        cls._renderers[platform.lower()] = renderer_class

    @classmethod
    def get_all_renderers(cls, schema: CanonicalSchema) -> Dict[str, SchemaRenderer]:
        """
        Get renderers for all supported platforms.

        Args:
            schema: Canonical schema to render

        Returns:
            Dictionary mapping platform name to renderer instance
            Only includes platforms where the schema is valid
        """
        renderers = {}

        for platform in cls.supported_platforms():
            try:
                renderer = cls.get_renderer(platform, schema)
                renderers[platform] = renderer
            except ValueError as e:
                # Schema not compatible with this platform, skip it
                pass

        return renderers


__all__ = ['RendererFactory']
