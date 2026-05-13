# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Any, Dict, List, Literal, Optional, override

from datus_db_core import BaseSqlConnector, SchemaNamespaceMixin
from pandas import DataFrame
from pyarrow import Table

from datus.schemas.base import TABLE_TYPE
from datus.schemas.node_models import ExecuteSQLResult
from datus.tools.db_tools._migration_compat import MigrationTargetMixin
from datus.tools.db_tools.config import DMConfig
from datus.utils.constants import DBType
from datus.utils.exceptions import DatusException, ErrorCode
from datus.utils.loggings import get_logger

logger = get_logger(__name__)


def _quote(identifier: str) -> str:
    """Quote a DM identifier so casing is preserved (DM upper-cases unquoted names)."""
    return f'"{identifier}"'


class DMConnector(BaseSqlConnector, SchemaNamespaceMixin, MigrationTargetMixin):
    """
    Connector for DM (Dameng) databases using the dmPython driver.

    DM is an Oracle-like dialect: identifiers are upper-cased unless quoted,
    pagination uses ROWNUM, and metadata is exposed through SYS.ALL_TABLES /
    ALL_TAB_COLUMNS / ALL_VIEWS keyed by OWNER (= schema).
    """

    def __init__(self, config: DMConfig):
        super().__init__(config, dialect=DBType.DM)
        self.host = config.host
        self.port = int(config.port)
        self.username = config.username
        self.password = config.password
        self.database_name = config.database or self.username
        self.default_schema = config.default_schema or self.username
        self.autocommit = bool(config.autocommit)
        self.connection = None

    @override
    def connect(self):
        if self.connection:
            return
        try:
            import dmPython  # noqa: WPS433 — lazy import keeps dmPython optional
        except ImportError as e:
            raise DatusException(
                ErrorCode.COMMON_MISSING_DEPENDENCY,
                message="dmPython is not installed. Install it via `pip install datus-agent[dm]`.",
            ) from e

        try:
            self.connection = dmPython.connect(
                user=self.username,
                password=self.password,
                server=self.host,
                port=self.port,
                autoCommit=self.autocommit,
            )
            if self.default_schema:
                cursor = self.connection.cursor()
                try:
                    cursor.execute(f"SET SCHEMA {_quote(self.default_schema)}")
                finally:
                    cursor.close()
        except Exception as e:
            raise DatusException(
                ErrorCode.DB_CONNECTION_FAILED,
                message_args={"error_message": str(e)},
            ) from e

    @override
    def close(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error closing DM connection: {e}")
            finally:
                self.connection = None

    @override
    def test_connection(self) -> bool:
        opened_here = self.connection is None
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            return True
        except Exception as e:
            raise DatusException(
                ErrorCode.DB_CONNECTION_FAILED,
                message_args={"error_message": str(e)},
            ) from e
        finally:
            if opened_here:
                self.close()

    def _handle_exception(self, e: Exception, sql: str = "") -> DatusException:
        if isinstance(e, DatusException):
            return e

        msg = str(e)
        lower = msg.lower()

        try:
            import dmPython
        except ImportError:
            dmPython = None  # type: ignore[assignment]

        if dmPython is not None:
            if isinstance(e, getattr(dmPython, "IntegrityError", ())):
                return DatusException(
                    ErrorCode.DB_CONSTRAINT_VIOLATION,
                    message_args={"sql": sql, "error_message": msg},
                )
            if isinstance(e, getattr(dmPython, "ProgrammingError", ())):
                return DatusException(
                    ErrorCode.DB_EXECUTION_SYNTAX_ERROR,
                    message_args={"sql": sql, "error_message": msg},
                )

        if "syntax" in lower or "near" in lower:
            return DatusException(
                ErrorCode.DB_EXECUTION_SYNTAX_ERROR,
                message_args={"sql": sql, "error_message": msg},
            )
        if "does not exist" in lower or "not found" in lower or "invalid table" in lower:
            return DatusException(
                ErrorCode.DB_TABLE_NOT_EXISTS,
                message_args={"table_name": sql, "error_message": msg},
            )
        if "timeout" in lower or "timed out" in lower:
            return DatusException(
                ErrorCode.DB_CONNECTION_TIMEOUT,
                message_args={"error_message": msg},
            )
        return DatusException(
            ErrorCode.DB_EXECUTION_ERROR,
            message_args={"sql": sql, "error_message": msg},
        )

    def _execute_write(self, sql: str, return_lastrowid: bool = False) -> ExecuteSQLResult:
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(sql)
            if not self.autocommit:
                self.connection.commit()
            lastrowid = getattr(cursor, "lastrowid", None) if return_lastrowid else None
            return ExecuteSQLResult(
                success=True,
                sql_query=sql,
                sql_return=str(lastrowid) if lastrowid is not None else str(cursor.rowcount),
                row_count=cursor.rowcount,
            )
        except Exception as e:
            ex = self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql)

    @override
    def execute_insert(self, sql: str) -> ExecuteSQLResult:
        return self._execute_write(sql, return_lastrowid=True)

    @override
    def execute_update(self, sql: str) -> ExecuteSQLResult:
        return self._execute_write(sql)

    @override
    def execute_delete(self, sql: str) -> ExecuteSQLResult:
        return self._execute_write(sql)

    @override
    def execute_ddl(self, sql: str) -> ExecuteSQLResult:
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(sql)
            if not self.autocommit:
                self.connection.commit()
            return ExecuteSQLResult(success=True, sql_query=sql, sql_return="Success", row_count=0)
        except Exception as e:
            ex = self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql)

    @override
    def execute_query(
        self, sql: str, result_format: Literal["csv", "arrow", "pandas", "list"] = "csv"
    ) -> ExecuteSQLResult:
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            result_list = [dict(zip(columns, row)) for row in rows]
            df = DataFrame(result_list, columns=columns)
            if result_format == "csv":
                result: Any = df.to_csv(index=False)
            elif result_format == "arrow":
                result = Table.from_pandas(df)
            elif result_format == "pandas":
                result = df
            else:
                result = result_list
            return ExecuteSQLResult(
                success=True,
                sql_query=sql,
                sql_return=result,
                row_count=len(rows),
                result_format=result_format,
            )
        except Exception as e:
            ex = self._handle_exception(e, sql)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql)

    @override
    def execute_pandas(self, sql: str) -> ExecuteSQLResult:
        return self.execute_query(sql, result_format="pandas")

    @override
    def execute_csv(self, sql: str) -> ExecuteSQLResult:
        return self.execute_query(sql, result_format="csv")

    @override
    def execute_queries(self, queries: List[str]) -> List[Any]:
        results: List[Any] = []
        self.connect()
        try:
            for query in queries:
                cursor = self.connection.cursor()
                cursor.execute(query)
                if cursor.description:
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    results.append([dict(zip(columns, row)) for row in rows])
                else:
                    results.append(cursor.rowcount)
            if not self.autocommit:
                self.connection.commit()
        except Exception as e:
            if not self.autocommit and self.connection is not None:
                try:
                    self.connection.rollback()
                except Exception:
                    pass
            raise self._handle_exception(e, "\n".join(queries))
        return results

    @override
    def execute_content_set(self, sql_query: str) -> ExecuteSQLResult:
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(sql_query)
            return ExecuteSQLResult(success=True, sql_query=sql_query, sql_return="Success", row_count=0)
        except Exception as e:
            ex = self._handle_exception(e, sql_query)
            return ExecuteSQLResult(success=False, error=str(ex), sql_query=sql_query)

    def _resolve_schema(self, schema_name: str = "") -> str:
        return schema_name or self.default_schema

    @override
    def get_databases(self, catalog_name: str = "", include_sys: bool = False) -> List[str]:
        return [self.database_name]

    @override
    def get_schemas(self, catalog_name: str = "", database_name: str = "") -> List[str]:
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute("SELECT USERNAME FROM SYS.ALL_USERS ORDER BY USERNAME")
        return [row[0] for row in cursor.fetchall()]

    @override
    def get_tables(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        self.connect()
        owner = self._resolve_schema(schema_name)
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM SYS.ALL_TABLES WHERE OWNER = :owner ORDER BY TABLE_NAME",
            {"owner": owner},
        )
        return [row[0] for row in cursor.fetchall()]

    @override
    def get_views(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        self.connect()
        owner = self._resolve_schema(schema_name)
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT VIEW_NAME FROM SYS.ALL_VIEWS WHERE OWNER = :owner ORDER BY VIEW_NAME",
            {"owner": owner},
        )
        return [row[0] for row in cursor.fetchall()]

    @override
    def full_name(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        owner = self._resolve_schema(schema_name)
        if owner and table_name:
            return f"{_quote(owner)}.{_quote(table_name)}"
        return _quote(table_name)

    @override
    def do_switch_context(self, catalog_name: str = "", database_name: str = "", schema_name: str = ""):
        if not schema_name:
            return
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SET SCHEMA {_quote(schema_name)}")
            self.default_schema = schema_name
        finally:
            cursor.close()

    def _build_create_table_ddl(self, owner: str, table_name: str) -> str:
        """Reconstruct a CREATE TABLE statement from ALL_TAB_COLUMNS metadata."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT "
            "FROM SYS.ALL_TAB_COLUMNS WHERE OWNER = :owner AND TABLE_NAME = :tbl ORDER BY COLUMN_ID",
            {"owner": owner, "tbl": table_name},
        )
        rows = cursor.fetchall()
        if not rows:
            return ""

        column_defs: List[str] = []
        for col in rows:
            name, dtype, length, precision, scale, nullable, default = col
            type_str = dtype
            if dtype in ("VARCHAR", "VARCHAR2", "CHAR") and length:
                type_str = f"{dtype}({length})"
            elif dtype in ("NUMBER", "DECIMAL") and precision is not None:
                type_str = f"{dtype}({precision},{scale or 0})"
            piece = f"{_quote(name)} {type_str}"
            if default is not None and str(default).strip():
                piece += f" DEFAULT {default}"
            if nullable == "N":
                piece += " NOT NULL"
            column_defs.append(piece)

        return f"CREATE TABLE {_quote(owner)}.{_quote(table_name)} (\n  " + ",\n  ".join(column_defs) + "\n)"

    def _build_create_view_ddl(self, owner: str, view_name: str) -> str:
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT TEXT FROM SYS.ALL_VIEWS WHERE OWNER = :owner AND VIEW_NAME = :v",
            {"owner": owner, "v": view_name},
        )
        row = cursor.fetchone()
        if not row or row[0] is None:
            return ""
        return f"CREATE VIEW {_quote(owner)}.{_quote(view_name)} AS {row[0]}"

    @override
    def get_tables_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", tables: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        self.connect()
        owner = self._resolve_schema(schema_name)
        names = tables if tables is not None else self.get_tables(schema_name=owner)
        out: List[Dict[str, str]] = []
        for tbl in names:
            try:
                definition = self._build_create_table_ddl(owner, tbl)
            except Exception as e:
                logger.warning(f"Failed to build DDL for {owner}.{tbl}: {e}")
                continue
            if not definition:
                continue
            out.append(
                {
                    "identifier": self.identifier(
                        database_name=database_name or self.database_name,
                        schema_name=owner,
                        table_name=tbl,
                    ),
                    "catalog_name": "",
                    "database_name": database_name or self.database_name,
                    "schema_name": owner,
                    "table_name": tbl,
                    "definition": definition,
                    "table_type": "table",
                }
            )
        return out

    @override
    def get_views_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[Dict[str, str]]:
        self.connect()
        owner = self._resolve_schema(schema_name)
        out: List[Dict[str, str]] = []
        for v in self.get_views(schema_name=owner):
            try:
                definition = self._build_create_view_ddl(owner, v)
            except Exception as e:
                logger.warning(f"Failed to build DDL for view {owner}.{v}: {e}")
                continue
            if not definition:
                continue
            out.append(
                {
                    "identifier": self.identifier(
                        database_name=database_name or self.database_name,
                        schema_name=owner,
                        table_name=v,
                    ),
                    "catalog_name": "",
                    "database_name": database_name or self.database_name,
                    "schema_name": owner,
                    "table_name": v,
                    "definition": definition,
                    "table_type": "view",
                }
            )
        return out

    @override
    def get_schema(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> List[Dict[str, Any]]:
        if not table_name:
            return []
        self.connect()
        owner = self._resolve_schema(schema_name)
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "SELECT COLUMN_ID, COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT "
                "FROM SYS.ALL_TAB_COLUMNS WHERE OWNER = :owner AND TABLE_NAME = :tbl ORDER BY COLUMN_ID",
                {"owner": owner, "tbl": table_name},
            )
            return [
                {
                    "cid": row[0],
                    "name": row[1],
                    "type": row[2],
                    "nullable": row[3] != "N",
                    "default_value": row[4],
                    "pk": 0,
                }
                for row in cursor.fetchall()
            ]
        except Exception as e:
            raise self._handle_exception(e, f"DESCRIBE {self.full_name(schema_name=owner, table_name=table_name)}")

    @override
    def get_sample_rows(
        self,
        tables: Optional[List[str]] = None,
        top_n: int = 5,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_type: TABLE_TYPE = "table",
    ) -> List[Dict[str, Any]]:
        self.connect()
        owner = self._resolve_schema(schema_name)
        samples: List[Dict[str, Any]] = []

        targets: List[str]
        if tables:
            targets = list(tables)
        else:
            targets = []
            if table_type in ("full", "table"):
                targets.extend(self.get_tables(schema_name=owner))
            if table_type in ("full", "view"):
                targets.extend(self.get_views(schema_name=owner))

        for tbl in targets:
            try:
                cursor = self.connection.cursor()
                cursor.execute(
                    f"SELECT * FROM {self.full_name(schema_name=owner, table_name=tbl)} WHERE ROWNUM <= {int(top_n)}"
                )
                rows = cursor.fetchall()
                if not rows:
                    continue
                columns = [desc[0] for desc in cursor.description]
                df = DataFrame([dict(zip(columns, row)) for row in rows])
                samples.append(
                    {
                        "catalog_name": "",
                        "database_name": database_name or self.database_name,
                        "schema_name": owner,
                        "table_name": tbl,
                        "sample_rows": df.to_csv(index=False),
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to get sample rows for {owner}.{tbl}: {e}")
        return samples

    def to_dict(self) -> Dict[str, Any]:
        return {
            "db_type": DBType.DM,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "database": self.database_name,
            "schema": self.default_schema,
        }

    def get_type(self) -> str:
        return DBType.DM

    # ==================== MigrationTargetMixin ====================

    def describe_migration_capabilities(self) -> Dict[str, Any]:
        return {
            "supported": True,
            "dialect_family": "oracle",
            "requires": [],
            "forbids": [
                "DUPLICATE KEY (StarRocks-only)",
                "DISTRIBUTED BY HASH ... BUCKETS (StarRocks-only)",
                "ENGINE = ... (MySQL/ClickHouse syntax)",
                "LIMIT N (use WHERE ROWNUM <= N)",
            ],
            "type_hints": {
                "VARCHAR": "VARCHAR(n)",
                "TEXT": "CLOB",
                "JSON": "CLOB",
                "DECIMAL(p,s)": "NUMBER(p,s)",
                "DOUBLE": "DOUBLE",
                "FLOAT": "FLOAT",
                "BOOLEAN": "BIT",
                "DATE": "DATE",
                "TIMESTAMP": "TIMESTAMP",
                "BLOB": "BLOB",
            },
            "example_ddl": (
                'CREATE TABLE "SCHEMA"."T" (\n'
                '  "ID" NUMBER(19) NOT NULL,\n'
                '  "NAME" VARCHAR(255),\n'
                '  "CREATED_AT" TIMESTAMP\n'
                ")"
            ),
        }

    def suggest_table_layout(self, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {}

    def validate_ddl(self, ddl: str) -> List[str]:
        import re as _re

        errors: List[str] = []
        upper = ddl.upper()
        if _re.search(r"DUPLICATE\s+KEY", upper):
            errors.append("DUPLICATE KEY is StarRocks-only syntax; DM does not support it")
        if _re.search(r"DISTRIBUTED\s+BY", upper) and "BUCKETS" in upper:
            errors.append("DISTRIBUTED BY ... BUCKETS is StarRocks syntax; DM does not support it")
        if _re.search(r"\bENGINE\s*=", upper):
            errors.append("ENGINE clause is MySQL/ClickHouse syntax; DM does not support it")
        if _re.search(r"\bLIMIT\s+\d", upper):
            errors.append("LIMIT N is not supported; use WHERE ROWNUM <= N instead")
        return errors

    def map_source_type(self, source_dialect: str, source_type: str) -> Optional[str]:
        import re as _re

        base = _re.sub(r"\(.*\)", "", source_type.strip().upper()).strip()
        overrides = {
            "TEXT": "CLOB",
            "STRING": "VARCHAR",
            "JSON": "CLOB",
            "JSONB": "CLOB",
            "UUID": "VARCHAR(36)",
            "BOOLEAN": "BIT",
            "BOOL": "BIT",
            "DOUBLE": "DOUBLE",
            "FLOAT4": "FLOAT",
            "FLOAT8": "DOUBLE",
            "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
            "DATETIME": "TIMESTAMP",
            "HUGEINT": "NUMBER(38)",
            "LARGEINT": "NUMBER(38)",
            "DECIMAL": "NUMBER",
        }
        return overrides.get(base)
