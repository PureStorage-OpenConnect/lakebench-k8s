"""Module registry for Lakebench component types.

Infrastructure for registry-driven deployment (v1.4). Currently the
deployment engine uses hardcoded deployer imports. This registry will
replace those conditionals when adapter classes are created for each
module type.

Used by: test_characterization.py (validates registry API).
Not yet used by: deploy/engine.py, deploy/destroy.py.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lakebench.modules.base import (
        CatalogModule,
        PipelineEngineModule,
        QueryEngineModule,
        TableFormatModule,
    )

logger = logging.getLogger(__name__)


class ModuleRegistry:
    """Singleton registry for all component modules."""

    def __init__(self) -> None:
        self._catalogs: dict[str, CatalogModule] = {}
        self._query_engines: dict[str, QueryEngineModule] = {}
        self._pipeline_engines: dict[str, PipelineEngineModule] = {}
        self._table_formats: dict[str, TableFormatModule] = {}
        self._combinations: list[tuple[str, str, str, str]] = []

    # -- Registration --------------------------------------------------------

    def register_catalog(self, module: CatalogModule) -> None:
        self._catalogs[module.name] = module

    def register_query_engine(self, module: QueryEngineModule) -> None:
        self._query_engines[module.name] = module

    def register_pipeline_engine(self, module: PipelineEngineModule) -> None:
        self._pipeline_engines[module.name] = module

    def register_table_format(self, module: TableFormatModule) -> None:
        self._table_formats[module.name] = module

    def register_combination(
        self,
        catalog: str,
        table_format: str,
        pipeline_engine: str,
        query_engine: str,
    ) -> None:
        self._combinations.append((catalog, table_format, pipeline_engine, query_engine))

    # -- Lookup --------------------------------------------------------------

    def get_catalog(self, name: str) -> CatalogModule:
        if name not in self._catalogs:
            raise KeyError(
                f"Unknown catalog module: {name!r}. Registered: {sorted(self._catalogs)}"
            )
        return self._catalogs[name]

    def get_query_engine(self, name: str) -> QueryEngineModule:
        if name not in self._query_engines:
            raise KeyError(
                f"Unknown query engine module: {name!r}. Registered: {sorted(self._query_engines)}"
            )
        return self._query_engines[name]

    def get_pipeline_engine(self, name: str) -> PipelineEngineModule:
        if name not in self._pipeline_engines:
            raise KeyError(
                f"Unknown pipeline engine module: {name!r}. "
                f"Registered: {sorted(self._pipeline_engines)}"
            )
        return self._pipeline_engines[name]

    def get_table_format(self, name: str) -> TableFormatModule:
        if name not in self._table_formats:
            raise KeyError(
                f"Unknown table format module: {name!r}. Registered: {sorted(self._table_formats)}"
            )
        return self._table_formats[name]

    # -- Validation ----------------------------------------------------------

    def validate_combination(
        self,
        catalog: str,
        table_format: str,
        pipeline_engine: str,
        query_engine: str,
    ) -> bool:
        return (catalog, table_format, pipeline_engine, query_engine) in self._combinations

    @property
    def supported_combinations(self) -> list[tuple[str, str, str, str]]:
        return list(self._combinations)

    # -- Introspection -------------------------------------------------------

    @property
    def catalog_names(self) -> list[str]:
        return sorted(self._catalogs)

    @property
    def query_engine_names(self) -> list[str]:
        return sorted(self._query_engines)

    @property
    def pipeline_engine_names(self) -> list[str]:
        return sorted(self._pipeline_engines)

    @property
    def table_format_names(self) -> list[str]:
        return sorted(self._table_formats)
