# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Unit tests for the DM (Dameng) connector.

All tests mock the dmPython driver via sys.modules so CI does not need DM
installed. Tests focus on: identifier quoting, SQL emission shape, ROWNUM
sampling, error mapping, and the MigrationTargetMixin contract.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from datus.tools.db_tools._migration_compat import MigrationTargetMixin
from datus.tools.db_tools.config import DMConfig
from datus.tools.db_tools.dm_connector import DMConnector, _quote
from datus.utils.constants import DBType
from datus.utils.exceptions import DatusException, ErrorCode


@pytest.fixture
def fake_dm_module():
    """Inject a stub `dmPython` module so DMConnector.connect() succeeds without the real driver."""
    module = MagicMock()
    module.Error = type("Error", (Exception,), {})
    module.DatabaseError = type("DatabaseError", (module.Error,), {})
    module.OperationalError = type("OperationalError", (module.DatabaseError,), {})
    module.IntegrityError = type("IntegrityError", (module.DatabaseError,), {})
    module.ProgrammingError = type("ProgrammingError", (module.DatabaseError,), {})
    sys.modules["dmPython"] = module
    yield module
    sys.modules.pop("dmPython", None)


def _config(**overrides):
    defaults = dict(
        host="127.0.0.1",
        port=5236,
        username="SYSDBA",
        password="SYSDBA001",
        database="DAMENG",
        default_schema="SYSDBA",
        autocommit=True,
        timeout_seconds=30,
    )
    defaults.update(overrides)
    return DMConfig(**defaults)


def _make_connector(fake_dm_module, fetchall=None, fetchone=None, lastrowid=None, rowcount=0, description=None):
    """Build a DMConnector pre-wired with a mock connection/cursor for assertions."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall if fetchall is not None else []
    cursor.fetchone.return_value = fetchone
    cursor.lastrowid = lastrowid
    cursor.rowcount = rowcount
    cursor.description = description

    connection = MagicMock()
    connection.cursor.return_value = cursor
    fake_dm_module.connect.return_value = connection

    connector = DMConnector(_config())
    connector.connect()
    return connector, connection, cursor


# ---------------------------------------------------------------------------
# Enum + identifier quoting
# ---------------------------------------------------------------------------


class TestDBTypeEnum:
    def test_dm_enum_present(self):
        assert DBType.DM == "dm"


class TestQuoteHelper:
    def test_quote_wraps_in_double_quotes(self):
        assert _quote("USERS") == '"USERS"'

    def test_quote_preserves_case(self):
        assert _quote("MixedCase") == '"MixedCase"'


# ---------------------------------------------------------------------------
# connect() lifecycle
# ---------------------------------------------------------------------------


class TestConnectLifecycle:
    def test_connect_invokes_dmpython_with_params(self, fake_dm_module):
        fake_dm_module.connect.return_value = MagicMock()
        connector = DMConnector(_config())
        connector.connect()

        fake_dm_module.connect.assert_called_once_with(
            user="SYSDBA",
            password="SYSDBA001",
            server="127.0.0.1",
            port=5236,
            autoCommit=True,
        )

    def test_connect_is_idempotent(self, fake_dm_module):
        fake_dm_module.connect.return_value = MagicMock()
        connector = DMConnector(_config())
        connector.connect()
        connector.connect()
        assert fake_dm_module.connect.call_count == 1

    def test_connect_issues_set_schema(self, fake_dm_module):
        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value = cursor
        fake_dm_module.connect.return_value = connection

        connector = DMConnector(_config(default_schema="MYAPP"))
        connector.connect()

        executed = [call.args[0] for call in cursor.execute.call_args_list]
        assert 'SET SCHEMA "MYAPP"' in executed

    def test_missing_dm_python_raises_dependency_missing(self, monkeypatch):
        """When dmPython is unavailable, connect() raises COMMON_DEPENDENCY_MISSING."""
        # Python treats sys.modules[name] = None as "known missing": `import name` raises ImportError.
        monkeypatch.setitem(sys.modules, "dmPython", None)

        connector = DMConnector(_config())
        with pytest.raises(DatusException) as exc_info:
            connector.connect()
        assert exc_info.value.code == ErrorCode.COMMON_MISSING_DEPENDENCY

    def test_close_resets_connection(self, fake_dm_module):
        fake_dm_module.connect.return_value = MagicMock()
        connector = DMConnector(_config())
        connector.connect()
        assert connector.connection is not None
        connector.close()
        assert connector.connection is None


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


class TestTestConnection:
    def test_runs_dual_probe(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module, fetchone=(1,))
        assert connector.test_connection() is True
        executed = [call.args[0] for call in cursor.execute.call_args_list]
        assert "SELECT 1 FROM DUAL" in executed


# ---------------------------------------------------------------------------
# Identifier handling
# ---------------------------------------------------------------------------


class TestFullName:
    def test_with_schema(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        assert connector.full_name(schema_name="APP", table_name="USERS") == '"APP"."USERS"'

    def test_falls_back_to_default_schema(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        # default_schema=SYSDBA from fixture
        assert connector.full_name(table_name="USERS") == '"SYSDBA"."USERS"'

    def test_preserves_case(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        assert connector.full_name(schema_name="MyApp", table_name="MixedCase") == '"MyApp"."MixedCase"'


# ---------------------------------------------------------------------------
# Metadata queries — assert exact SQL strings + bind parameters
# ---------------------------------------------------------------------------


class TestMetadataQueries:
    def test_get_tables_uses_all_tables_with_owner_bind(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module, fetchall=[("USERS",), ("ORDERS",)])

        result = connector.get_tables(schema_name="APP")

        assert result == ["USERS", "ORDERS"]
        sql, params = cursor.execute.call_args.args
        assert "SYS.ALL_TABLES" in sql
        assert "OWNER = :owner" in sql
        assert params == {"owner": "APP"}

    def test_get_tables_defaults_to_connection_schema(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module, fetchall=[])
        connector.get_tables()
        _sql, params = cursor.execute.call_args.args
        assert params == {"owner": "SYSDBA"}

    def test_get_views_uses_all_views(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module, fetchall=[("V_REPORT",)])
        result = connector.get_views(schema_name="APP")
        assert result == ["V_REPORT"]
        sql, _ = cursor.execute.call_args.args
        assert "SYS.ALL_VIEWS" in sql

    def test_get_databases_returns_configured_name(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        assert connector.get_databases() == ["DAMENG"]

    def test_get_schemas_queries_all_users(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module, fetchall=[("SYSDBA",), ("APP",)])
        assert connector.get_schemas() == ["SYSDBA", "APP"]
        sql = cursor.execute.call_args.args[0]
        assert "SYS.ALL_USERS" in sql

    def test_get_schema_parses_columns(self, fake_dm_module):
        rows = [
            (1, "ID", "NUMBER", "N", None),
            (2, "NAME", "VARCHAR", "Y", "'unknown'"),
        ]
        connector, _conn, _cur = _make_connector(fake_dm_module, fetchall=rows)
        cols = connector.get_schema(schema_name="APP", table_name="USERS")
        assert cols[0] == {
            "cid": 1,
            "name": "ID",
            "type": "NUMBER",
            "nullable": False,
            "default_value": None,
            "pk": 0,
        }
        assert cols[1]["nullable"] is True
        assert cols[1]["default_value"] == "'unknown'"

    def test_get_schema_empty_table_name_returns_empty(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        assert connector.get_schema(table_name="") == []


# ---------------------------------------------------------------------------
# Sample rows — must use ROWNUM not LIMIT
# ---------------------------------------------------------------------------


class TestSampleRows:
    def test_uses_rownum_pagination(self, fake_dm_module):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(1, "alice")]
        cursor.description = [("ID", None), ("NAME", None)]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        fake_dm_module.connect.return_value = connection

        connector = DMConnector(_config())
        connector.connect()
        samples = connector.get_sample_rows(tables=["USERS"], top_n=5, schema_name="APP")

        sql = cursor.execute.call_args.args[0]
        assert "ROWNUM <= 5" in sql
        assert "LIMIT" not in sql.upper().split()  # ensure no bare LIMIT keyword
        assert '"APP"."USERS"' in sql
        assert samples[0]["table_name"] == "USERS"
        assert samples[0]["schema_name"] == "APP"


# ---------------------------------------------------------------------------
# do_switch_context
# ---------------------------------------------------------------------------


class TestSwitchContext:
    def test_set_schema_emitted(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module)
        connector.do_switch_context(schema_name="APP")
        executed = [call.args[0] for call in cursor.execute.call_args_list]
        assert 'SET SCHEMA "APP"' in executed
        assert connector.default_schema == "APP"

    def test_empty_schema_is_noop(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module)
        prior_count = cursor.execute.call_count
        connector.do_switch_context(schema_name="")
        assert cursor.execute.call_count == prior_count


# ---------------------------------------------------------------------------
# _handle_exception mapping
# ---------------------------------------------------------------------------


class TestHandleException:
    def test_passthrough_datus_exception(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        original = DatusException(ErrorCode.DB_TABLE_NOT_EXISTS, message_args={"table_name": "T"})
        assert connector._handle_exception(original) is original

    def test_integrity_error_maps_to_constraint_violation(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        err = fake_dm_module.IntegrityError("UNIQUE violated")
        ex = connector._handle_exception(err, sql="INSERT ...")
        assert ex.code == ErrorCode.DB_CONSTRAINT_VIOLATION

    def test_programming_error_maps_to_syntax(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        err = fake_dm_module.ProgrammingError("bad SQL")
        ex = connector._handle_exception(err, sql="SELECT ...")
        assert ex.code == ErrorCode.DB_EXECUTION_SYNTAX_ERROR

    def test_syntax_substring_detection(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        ex = connector._handle_exception(RuntimeError("Syntax error near 'FROM'"), sql="SELECT")
        assert ex.code == ErrorCode.DB_EXECUTION_SYNTAX_ERROR

    def test_table_not_found_substring_detection(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        ex = connector._handle_exception(RuntimeError("table USERS does not exist"))
        assert ex.code == ErrorCode.DB_TABLE_NOT_EXISTS

    def test_timeout_substring_detection(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        ex = connector._handle_exception(RuntimeError("operation timeout"))
        assert ex.code == ErrorCode.DB_CONNECTION_TIMEOUT

    def test_fallback_to_execution_error(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        ex = connector._handle_exception(RuntimeError("something else"))
        assert ex.code == ErrorCode.DB_EXECUTION_ERROR


# ---------------------------------------------------------------------------
# execute_* — happy path + error path via _handle_exception
# ---------------------------------------------------------------------------


class TestExecutePaths:
    def test_execute_query_returns_csv(self, fake_dm_module):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(1, "a"), (2, "b")]
        cursor.description = [("ID", None), ("NAME", None)]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        fake_dm_module.connect.return_value = connection

        connector = DMConnector(_config())
        connector.connect()
        result = connector.execute_query('SELECT * FROM "T"')

        assert result.success is True
        assert result.row_count == 2
        assert "ID" in result.sql_return
        assert "NAME" in result.sql_return

    def test_execute_query_failure_returns_unsuccessful_result(self, fake_dm_module):
        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value = cursor
        fake_dm_module.connect.return_value = connection

        # Skip the SET SCHEMA emitted by connect() — install the failure side-effect afterwards.
        connector = DMConnector(_config(default_schema=None))
        connector.connect()
        cursor.execute.side_effect = fake_dm_module.ProgrammingError("bad SQL")

        result = connector.execute_query("BROKEN SQL")
        assert result.success is False
        assert result.error

    def test_execute_ddl_success(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        result = connector.execute_ddl('CREATE TABLE "T" (ID NUMBER)')
        assert result.success is True
        assert result.sql_return == "Success"

    def test_execute_insert_returns_rowcount(self, fake_dm_module):
        connector, _conn, cursor = _make_connector(fake_dm_module, rowcount=3)
        result = connector.execute_insert("INSERT INTO T VALUES (1)")
        assert result.success is True
        assert result.row_count == 3


# ---------------------------------------------------------------------------
# to_dict / get_type
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_to_dict_shape(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        d = connector.to_dict()
        assert d["db_type"] == DBType.DM
        assert d["host"] == "127.0.0.1"
        assert d["port"] == 5236
        assert d["schema"] == "SYSDBA"

    def test_get_type_returns_dm(self, fake_dm_module):
        connector, _conn, _cur = _make_connector(fake_dm_module)
        assert connector.get_type() == DBType.DM


# ---------------------------------------------------------------------------
# Migration mixin (pure-logic — no driver involvement)
# ---------------------------------------------------------------------------


@pytest.fixture
def bare_connector():
    """Build a connector without calling __init__ — sufficient for testing the mixin."""
    return DMConnector.__new__(DMConnector)


class TestMigrationMixin:
    def test_is_migration_target(self, bare_connector):
        assert isinstance(bare_connector, MigrationTargetMixin)

    def test_dialect_family_oracle(self, bare_connector):
        assert bare_connector.describe_migration_capabilities()["dialect_family"] == "oracle"

    def test_validate_rejects_starrocks_syntax(self, bare_connector):
        ddl = "CREATE TABLE t (id INTEGER) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 10"
        errors = bare_connector.validate_ddl(ddl)
        assert any("DUPLICATE KEY" in e for e in errors)

    def test_validate_rejects_engine_clause(self, bare_connector):
        errors = bare_connector.validate_ddl("CREATE TABLE t (id INT) ENGINE = MergeTree()")
        assert any("ENGINE" in e for e in errors)

    def test_validate_rejects_limit(self, bare_connector):
        """DM uses ROWNUM, not LIMIT — must surface the dialect mismatch."""
        errors = bare_connector.validate_ddl("SELECT * FROM t LIMIT 10")
        assert any("LIMIT" in e for e in errors)

    def test_validate_accepts_dm_ddl(self, bare_connector):
        ddl = 'CREATE TABLE "T" ("ID" NUMBER(19) NOT NULL, "NAME" VARCHAR(255))'
        assert bare_connector.validate_ddl(ddl) == []

    def test_suggest_layout_empty(self, bare_connector):
        assert bare_connector.suggest_table_layout([]) == {}

    def test_map_boolean_to_bit(self, bare_connector):
        """DM has no native BOOLEAN — use BIT."""
        assert bare_connector.map_source_type("mysql", "BOOLEAN") == "BIT"

    def test_map_text_to_clob(self, bare_connector):
        assert bare_connector.map_source_type("postgres", "TEXT") == "CLOB"

    def test_map_decimal_to_number(self, bare_connector):
        assert bare_connector.map_source_type("mysql", "DECIMAL(10,2)") == "NUMBER"

    def test_unmapped_type_returns_none(self, bare_connector):
        assert bare_connector.map_source_type("mysql", "INTEGER") is None


# ---------------------------------------------------------------------------
# _db_config_to_connection_config DM branch (db_manager wiring)
# ---------------------------------------------------------------------------


class TestDbManagerDmBranch:
    def _db_config(self, **overrides):
        defaults = dict(
            type="dm",
            host="dmhost",
            port=5236,
            username="SYSDBA",
            password="pwd",
            database="DAMENG",
            schema="APP",
            catalog=None,
            uri=None,
            logic_name="",
            extra=None,
            path_pattern=None,
        )
        defaults.update(overrides)
        ns = SimpleNamespace(**defaults)
        ns.to_dict = lambda: {k: v for k, v in defaults.items()}
        return ns

    def test_returns_dmconfig_with_expected_fields(self):
        from datus.tools.db_tools.db_manager import DBManager

        mgr = DBManager({})
        result = mgr._db_config_to_connection_config(self._db_config())

        assert isinstance(result, DMConfig)
        assert result.host == "dmhost"
        assert result.port == 5236
        assert result.username == "SYSDBA"
        assert result.default_schema == "APP"
        assert result.autocommit is True

    def test_missing_host_raises(self):
        from datus.tools.db_tools.db_manager import DBManager

        mgr = DBManager({})
        with pytest.raises(DatusException, match="host"):
            mgr._db_config_to_connection_config(self._db_config(host=""))

    def test_missing_username_raises(self):
        from datus.tools.db_tools.db_manager import DBManager

        mgr = DBManager({})
        with pytest.raises(DatusException, match="username"):
            mgr._db_config_to_connection_config(self._db_config(username=""))

    def test_autocommit_override_via_extra(self):
        from datus.tools.db_tools.db_manager import DBManager

        mgr = DBManager({})
        result = mgr._db_config_to_connection_config(self._db_config(extra={"autocommit": False}))
        assert result.autocommit is False

    def test_default_port_when_missing(self):
        from datus.tools.db_tools.db_manager import DBManager

        mgr = DBManager({})
        result = mgr._db_config_to_connection_config(self._db_config(port=None))
        assert result.port == 5236
