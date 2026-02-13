"""Lakebench configuration module."""

from .autosizer import resolve_auto_sizing
from .loader import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigParseError,
    ConfigValidationError,
    generate_default_config,
    generate_example_config_yaml,
    load_config,
    save_config,
)
from .scale import (
    ComputeGuidance,
    DatagenGuidance,
    FullComputeGuidance,
    ScaleDimensions,
    TrinoGuidance,
    compute_guidance,
    customer360_dimensions,
    full_compute_guidance,
    get_dimensions,
)
from .schema import (
    CatalogType,
    DatagenMode,
    FileFormatType,
    ImagePullPolicy,
    LakebenchConfig,
    ProcessingPattern,
    QueryEngineType,
    TableFormatType,
    WorkloadSchema,
    parse_size_to_bytes,
    parse_spark_memory,
)

__all__ = [
    # Config classes
    "LakebenchConfig",
    # Scale
    "ScaleDimensions",
    "ComputeGuidance",
    "TrinoGuidance",
    "DatagenGuidance",
    "FullComputeGuidance",
    "get_dimensions",
    "compute_guidance",
    "full_compute_guidance",
    "customer360_dimensions",
    # Auto-sizing
    "resolve_auto_sizing",
    # Enums
    "DatagenMode",
    "CatalogType",
    "FileFormatType",
    "ImagePullPolicy",
    "ProcessingPattern",
    "QueryEngineType",
    "TableFormatType",
    "WorkloadSchema",
    # Loader functions
    "load_config",
    "save_config",
    "generate_default_config",
    "generate_example_config_yaml",
    # Helpers
    "parse_size_to_bytes",
    "parse_spark_memory",
    # Exceptions
    "ConfigError",
    "ConfigFileNotFoundError",
    "ConfigParseError",
    "ConfigValidationError",
]
