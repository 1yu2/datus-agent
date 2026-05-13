# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from datus_db_core import AdapterMetadata, BaseSqlConnector, ConnectorRegistry, connector_registry

from .sqlite_connector import SQLiteConnector

__all__ = [
    "BaseSqlConnector",
    "SQLiteConnector",
    "DuckdbConnector",
    "DMConnector",
    "connector_registry",
    "ConnectorRegistry",
    "AdapterMetadata",
]


def _register_builtin_connectors():
    """Register built-in connectors (SQLite, DuckDB, DM)"""
    # SQLite (0 dependencies, no schema support)
    try:
        from .builtin_configs import SQLiteConfig
        from .sqlite_connector import SQLiteConnector

        connector_registry.register(
            "sqlite",
            SQLiteConnector,
            config_class=SQLiteConfig,
            display_name="SQLite",
            capabilities=set(),
        )
    except ImportError:
        pass

    # DuckDB (small dependency, database + schema)
    try:
        from .builtin_configs import DuckDBConfig
        from .duckdb_connector import DuckdbConnector

        connector_registry.register(
            "duckdb",
            DuckdbConnector,
            config_class=DuckDBConfig,
            display_name="DuckDB",
            capabilities={"database", "schema"},
        )
        # Add to __all__ dynamically
        if "DuckdbConnector" not in __all__:
            __all__.append("DuckdbConnector")
        globals()["DuckdbConnector"] = DuckdbConnector
    except ImportError:
        pass

    # DM (Dameng) — schema support, dmPython driver loaded lazily at connect()
    try:
        from .builtin_configs import DMConfig
        from .dm_connector import DMConnector

        connector_registry.register(
            "dm",
            DMConnector,
            config_class=DMConfig,
            display_name="DM (Dameng)",
            capabilities={"schema"},
        )
        globals()["DMConnector"] = DMConnector
    except ImportError:
        pass


# Initialize built-in connectors and discover adapter plugins
_register_builtin_connectors()
connector_registry.discover_adapters()
