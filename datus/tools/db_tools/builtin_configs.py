# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Configuration classes for built-in database adapters."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class SQLiteConfig(BaseModel):
    """SQLite-specific configuration."""

    model_config = ConfigDict(extra="forbid")

    uri: str = Field(
        ...,
        description="SQLite database URI (e.g., sqlite:////path/to/db.sqlite)",
        json_schema_extra={"input_type": "file_path"},
    )


class DuckDBConfig(BaseModel):
    """DuckDB-specific configuration."""

    model_config = ConfigDict(extra="forbid")

    uri: str = Field(
        ...,
        description="DuckDB database URI (e.g., duckdb:////path/to/db.duckdb)",
        json_schema_extra={
            "input_type": "file_path",
            "default_sample": "duckdb-demo.duckdb",
        },
    )
    read_only: bool = Field(default=False, description="Whether to open the DuckDB database in read-only mode")
    enable_external_access: bool = Field(default=True, description="Enable DuckDB external file access")
    memory_limit: Optional[str] = Field(default=None, description="DuckDB memory limit, e.g. '2GB'")
    iceberg: Optional[Dict[str, Any]] = Field(default=None, description="DuckDB Iceberg REST catalog configuration")


class DMConfig(BaseModel):
    """DM (Dameng) database configuration — server connection parameters."""

    model_config = ConfigDict(extra="forbid")

    host: str = Field(..., description="DM server host")
    port: int = Field(default=5236, description="DM server port")
    username: str = Field(..., description="DM login username")
    password: str = Field(..., description="DM login password")
    database: Optional[str] = Field(default=None, description="Optional database/instance name")
    schema_name: Optional[str] = Field(
        default=None,
        alias="schema",
        description="Default schema (DM upper-cases unquoted identifiers)",
    )
    autocommit: bool = Field(default=True, description="Whether to autocommit DML")
