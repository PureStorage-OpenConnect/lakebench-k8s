"""Module system for Lakebench component types.

Each component type (catalog, query engine, pipeline engine, table format)
is represented by a Protocol interface. Concrete implementations register
with the ModuleRegistry, which replaces hardcoded branching in the core.
"""

from lakebench.modules.base import (
    CatalogModule,
    PipelineEngineModule,
    QueryEngineModule,
    TableFormatModule,
)
from lakebench.modules.registry import ModuleRegistry

__all__ = [
    "CatalogModule",
    "ModuleRegistry",
    "PipelineEngineModule",
    "QueryEngineModule",
    "TableFormatModule",
]
