from __future__ import annotations

import datetime
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from importlib import import_module, util
from types import ModuleType
from typing import Any, Iterable, Optional, Sequence, Tuple, Union, List, Dict


class ExecuteQueryType(Enum):
    """
    実行するSQLの種類を表す列挙型。

    内部的にクエリ構築・実行処理の分岐に利用される。
    """    
    SELECT = 1
    INSERT = 2
    UPDATE = 3
    DELETE = 4
    COUNT = 5


@dataclass(frozen=True)
class SqlExpr:
    """
    信頼できる生SQL式（RAW SQL）を表すラッパークラス。

    通常、select / group_by / order_by では
    カラム名を安全にクオートするが、
    COUNT(*), SUM(x) などのSQL関数は識別子ではないため
    本クラスで明示的に包んで指定する。

    例:
    select(SqlExpr("COUNT(*)"), "cnt")
    """
    sql: str


@dataclass
class _WhereClause:
    """
    WHERE句の1条件を表す内部用データクラス。

    ユーザーから直接参照されることは想定していない。
    """    
    column: str
    op: str
    value: Any = None


@dataclass
class QueryState:
    """
    1クエリ分の状態を保持する内部ステートクラス。

    fluent interface（from_table().where().select()...）で
    構築された情報を一時的に保持し、
    SQL生成後に必ず reset() される。
    """    
    table: Optional[str] = None
    selects: List[Union[str, SqlExpr]] = field(default_factory=list)
    wheres: List[_WhereClause] = field(default_factory=list)
    group_by: List[Union[str, SqlExpr]] = field(default_factory=list)
    order_by: List[Tuple[Union[str, SqlExpr], str]] = field(default_factory=list)
    # insert/update payload
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def reset(self) -> None:
        """内部状態を初期化する（次のクエリ用）。"""
        self.table = None
        self.selects.clear()
        self.wheres.clear()
        self.group_by.clear()
        self.order_by.clear()
        self.rows.clear()


@dataclass(frozen=True)
class ConnectionSettings:
    """
    データベース接続設定をまとめた不変データクラス。

    SqlManager 初期化時に dict から生成され、
    以降は変更されない。
    """
    user: str
    passwd: str
    host: str
    db: str
    driver: Optional[str] = None
    charset: str = "utf8mb4"
    autocommit: bool = True


class SqlManager:
    """
    MySQL向けの軽量SQLビルダー兼実行クラス。

    特徴:
    - fluent interface による直感的なSQL構築
    - プレースホルダによる安全な値バインド
    - 識別子（テーブル名・カラム名）の検証とクオート
    - トランザクション管理対応

    対応ドライバ:
    - pymysql
    - MySQLdb (mysqlclient)

    複雑なSQL（JOIN, サブクエリ等）は raw_execute() を使用する。
    """

    _DRIVER_MODULES = {
        "pymysql": "pymysql",
        "mysqldb": "MySQLdb",
    }

    _IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self, settings: dict) -> None:
        """
        SqlManager を初期化する。

        Args:
            settings (dict): DB接続設定。
                必須キー: user, passwd, host, db

                任意キー: driver, charset, autocommit

            Raises:
                ValueError: 必須キーが不足している場合
                ImportError: 利用可能なDBドライバが存在しない場合
        """
        required_keys = ("user", "passwd", "host", "db")
        missing = [k for k in required_keys if k not in settings]
        if missing:
            raise ValueError(f"Missing required connection settings: {', '.join(missing)}")

        self._settings = ConnectionSettings(
            user=settings["user"],
            passwd=settings["passwd"],
            host=settings["host"],
            db=settings["db"],
            driver=settings.get("driver"),
            charset=settings.get("charset", "utf8mb4"),
            autocommit=settings.get("autocommit", True),
        )

        # 指定または自動選択されたDBドライバモジュール
        self._driver_module: ModuleType = self._load_driver(self._settings.driver)

        self._state = QueryState()

        # トランザクション管理用
        self._in_tx = False
        self._tx_conn = None

        # 最後に実行したクエリ情報（デバッグ・テスト用）
        self._last_query: str = ""
        self._last_params: Optional[Tuple[Any, ...]] = None

    # ----------------------------
    # Transaction API (compatible)
    # ----------------------------
    def begin_transaction(self) -> None:
        self._in_tx = True

    def end_transaction(self, is_succeed: bool) -> None:
        if not self._tx_conn:
            # Nothing to close/commit
            self._in_tx = False
            return

        try:
            if is_succeed:
                self._tx_conn.commit()
            else:
                self._tx_conn.rollback()
        finally:
            try:
                self._tx_conn.close()
            finally:
                self._tx_conn = None
                self._in_tx = False

    @contextmanager
    def transaction(self):
        """
        Usage:
            with manager.transaction():
                manager.from_table(...).set(...).create()
        """
        self.begin_transaction()
        ok = False
        try:
            yield self
            ok = True
        finally:
            self.end_transaction(ok)

    # ----------------------------
    # Fluent builder
    # ----------------------------
    def from_table(self, table: str) -> "SqlManager":
        self._state.table = table
        return self

    # Where variants
    def where(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> "SqlManager":
        return self._add_where(column, "=", value)

    def where_gt(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> "SqlManager":
        return self._add_where(column, ">", value)

    def where_gte(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> "SqlManager":
        return self._add_where(column, ">=", value)

    def where_lt(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> "SqlManager":
        return self._add_where(column, "<", value)

    def where_lte(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> "SqlManager":
        return self._add_where(column, "<=", value)

    def where_like(self, column: str, value: Union[str, int]) -> "SqlManager":
        return self._add_where(column, "LIKE", value)

    def where_in(self, column: str, values: list) -> "SqlManager":
        return self._add_where(column, "IN", values)

    def where_not_in(self, column: str, values: list) -> "SqlManager":
        return self._add_where(column, "NOT IN", values)

    def where_is_null(self, column: str) -> "SqlManager":
        return self._add_where(column, "IS NULL", None)

    def where_is_not_null(self, column: str) -> "SqlManager":
        return self._add_where(column, "IS NOT NULL", None)

    def select(self, column: Union[str, SqlExpr], as_column: Optional[str] = None) -> "SqlManager":
        if isinstance(column, SqlExpr):
            sel = column.sql
        else:
            sel = self._quote_identifier(column)
        if as_column:
            sel += f" AS {self._quote_identifier(as_column)}"
        self._state.selects.append(SqlExpr(sel) if isinstance(column, SqlExpr) else sel)
        return self

    # Insert/Update payload
    def set(self, column: Union[str, dict], value: Any = None) -> "SqlManager":
        """
        Compatible with:
          - set("name", "x")
          - set({"name": "x", "score": 1})
        """
        if isinstance(column, dict):
            data = column
        else:
            data = {column: value}

        if self._state.rows:
            self._state.rows[0].update(data)
        else:
            self._state.rows.append(dict(data))
        return self

    def sets(self, data: Union[dict, List[dict]]) -> "SqlManager":
        if isinstance(data, dict):
            self._state.rows.append(dict(data))
            return self
        if not data:
            raise ValueError("sets() received an empty list.")
        if not isinstance(data[0], dict):
            raise ValueError("sets() expects dict or list[dict].")
        self._state.rows.extend([dict(x) for x in data])
        return self

    def group_by(self, columns: Union[str, list, SqlExpr]) -> "SqlManager":
        if isinstance(columns, SqlExpr):
            self._state.group_by = [columns]
            return self
        if isinstance(columns, str):
            cols = [c.strip() for c in columns.split(",") if c.strip()]
        else:
            cols = [str(c).strip() for c in columns if str(c).strip()]
        self._state.group_by = cols
        return self

    def order_by_asc(self, columns: list) -> "SqlManager":
        for c in columns:
            self._state.order_by.append((c if isinstance(c, SqlExpr) else str(c), "ASC"))
        return self

    def order_by_desc(self, columns: list) -> "SqlManager":
        for c in columns:
            self._state.order_by.append((c if isinstance(c, SqlExpr) else str(c), "DESC"))
        return self

    # ----------------------------
    # Execution API
    # ----------------------------
    def create(self) -> None:
        self._execute(ExecuteQueryType.INSERT)

    def update(self) -> None:
        self._execute(ExecuteQueryType.UPDATE)

    def delete(self) -> None:
        self._execute(ExecuteQueryType.DELETE)

    def count(self) -> int:
        return int(self._execute(ExecuteQueryType.COUNT))

    def find_records(self, is_dict_cursor: bool = False) -> Any:
        return self._execute(ExecuteQueryType.SELECT, is_dict_cursor=is_dict_cursor)

    def raw_execute(
        self,
        query: str,
        params: Optional[Sequence[Any]] = None,
        is_dict_cursor: bool = False,
    ) -> Any:
        conn = self._get_connection()
        cur = self._get_cursor(conn, is_dict_cursor)

        try:
            if params is None:
                cur.execute(query)
                self._last_params = None
            else:
                cur.execute(query, tuple(params))
                self._last_params = tuple(params)

            self._last_query = query

            if query.strip().lower().startswith("select"):
                return cur.fetchall()
            return None
        finally:
            cur.close()
            self._close_if_not_in_tx(conn)

    def get_last_query(self) -> str:
        return self._last_query

    def get_last_parameters(self) -> Union[tuple, None]:
        return self._last_params

    def get_last_query_info(self) -> Tuple[str, Union[tuple, None]]:
        return self._last_query, self._last_params

    # ----------------------------
    # Internal: build + execute
    # ----------------------------
    def _execute(self, kind: ExecuteQueryType, is_dict_cursor: Optional[bool] = None):
        query, params = self._build_query(kind)

        conn = self._get_connection()
        cur = self._get_cursor(conn, bool(is_dict_cursor))

        self._last_query = query
        self._last_params = tuple(params) if params else None

        try:
            if params:
                cur.execute(query, tuple(params))
            else:
                cur.execute(query)

            if kind == ExecuteQueryType.SELECT:
                return cur.fetchall()

            if kind == ExecuteQueryType.COUNT:
                rows = cur.fetchall()
                return int(rows[0][0])

            return None
        finally:
            cur.close()
            self._close_if_not_in_tx(conn)

    def _build_query(self, kind: ExecuteQueryType) -> Tuple[str, List[Any]]:
        if not self._state.table:
            raise ValueError("Table is not set. Call from_table('...') first.")

        params: List[Any] = []

        table_sql = self._quote_identifier(self._state.table)

        if kind in (ExecuteQueryType.SELECT, ExecuteQueryType.COUNT):
            if kind == ExecuteQueryType.COUNT:
                select_sql = "COUNT(*)"
            else:
                if not self._state.selects:
                    select_sql = "*"
                else:
                    select_sql = ", ".join(self._render_selects(self._state.selects))

            query = f"SELECT {select_sql} FROM {table_sql}"
            query += self._render_where(params)
            query += self._render_group_by()
            query += self._render_order_by()
            self._state.reset()
            return query, params

        if kind == ExecuteQueryType.INSERT:
            if not self._state.rows:
                raise ValueError("No data to insert. Use set()/sets() first.")
            query = f"INSERT INTO {table_sql} " + self._render_insert(params)
            self._state.reset()
            return query, params

        if kind == ExecuteQueryType.UPDATE:
            if not self._state.rows:
                raise ValueError("No data to update. Use set() first.")
            if not self._state.wheres:
                raise ValueError("UPDATE without WHERE is blocked for safety.")
            query = f"UPDATE {table_sql} SET " + self._render_update(params)
            query += self._render_where(params)
            self._state.reset()
            return query, params

        if kind == ExecuteQueryType.DELETE:
            if not self._state.wheres:
                raise ValueError("DELETE without WHERE is blocked for safety.")
            query = f"DELETE FROM {table_sql}"
            query += self._render_where(params)
            self._state.reset()
            return query, params

        raise ValueError(f"Unsupported query type: {kind}")

    def _render_selects(self, selects: List[Union[str, SqlExpr]]) -> List[str]:
        out: List[str] = []
        for s in selects:
            if isinstance(s, SqlExpr):
                out.append(s.sql)
            else:
                # already quoted in select()
                out.append(str(s))
        return out

    def _render_where(self, params: List[Any]) -> str:
        if not self._state.wheres:
            return ""

        pieces: List[str] = []
        for clause in self._state.wheres:
            col = self._quote_identifier(clause.column)
            op = clause.op.upper()

            if op in ("IS NULL", "IS NOT NULL"):
                pieces.append(f"{col} {op}")
                continue

            if op in ("IN", "NOT IN"):
                values = list(clause.value or [])
                if not values:
                    # IN () is invalid; treat as false condition
                    pieces.append("1=0" if op == "IN" else "1=1")
                    continue
                placeholders = ", ".join(["%s"] * len(values))
                pieces.append(f"{col} {op} ({placeholders})")
                params.extend(values)
                continue

            # normal binary ops (=, >, >=, <, <=, LIKE, etc.)
            pieces.append(f"{col} {op} %s")
            params.append(clause.value)

        return " WHERE " + " AND ".join(pieces)

    def _render_group_by(self) -> str:
        if not self._state.group_by:
            return ""
        cols: List[str] = []
        for c in self._state.group_by:
            if isinstance(c, SqlExpr):
                cols.append(c.sql)
            else:
                cols.append(self._quote_identifier(str(c)))
        return " GROUP BY " + ", ".join(cols)

    def _render_order_by(self) -> str:
        if not self._state.order_by:
            return ""
        cols: List[str] = []
        for c, direction in self._state.order_by:
            if isinstance(c, SqlExpr):
                cols.append(f"{c.sql} {direction}")
            else:
                cols.append(f"{self._quote_identifier(str(c))} {direction}")
        return " ORDER BY " + ", ".join(cols)

    def _render_insert(self, params: List[Any]) -> str:
        # Use keys of first row as column order; require all rows have same keys
        rows = self._state.rows
        cols = list(rows[0].keys())
        if not cols:
            raise ValueError("Insert row has no columns.")

        for r in rows:
            if list(r.keys()) != cols:
                raise ValueError("All rows in sets() must have the same columns and order.")

        col_sql = ", ".join(self._quote_identifier(c) for c in cols)

        value_groups: List[str] = []
        for r in rows:
            values = [r[c] for c in cols]
            params.extend(values)
            value_groups.append("(" + ", ".join(["%s"] * len(values)) + ")")

        return f"({col_sql}) VALUES " + ", ".join(value_groups)

    def _render_update(self, params: List[Any]) -> str:
        row = self._state.rows[0]
        if not row:
            raise ValueError("Update payload is empty.")
        assigns: List[str] = []
        for col, val in row.items():
            assigns.append(f"{self._quote_identifier(col)} = %s")
            params.append(val)
        return ", ".join(assigns)

    def _add_where(self, column: str, op: str, value: Any) -> "SqlManager":
        self._state.wheres.append(_WhereClause(column=column, op=op, value=value))
        return self

    # ----------------------------
    # Connection / cursor
    # ----------------------------
    def _get_connection(self):
        if self._in_tx:
            if self._tx_conn is None:
                self._tx_conn = self._connect(autocommit=False)
            return self._tx_conn
        return self._connect(autocommit=True)

    def _close_if_not_in_tx(self, conn) -> None:
        if not self._in_tx:
            conn.close()

    def _connect(self, autocommit: bool):
        options = {
            "user": self._settings.user,
            "passwd": self._settings.passwd,
            "host": self._settings.host,
            "db": self._settings.db,
            "charset": self._settings.charset,
            "autocommit": autocommit,
        }
        return self._driver_module.connect(**options)

    def _get_cursor(self, conn, is_dict_cursor: bool):
        if is_dict_cursor:
            # MySQLdb and pymysql both expose cursors.DictCursor in similar shape
            return conn.cursor(self._driver_module.cursors.DictCursor)
        return conn.cursor()

    # ----------------------------
    # Driver loading
    # ----------------------------
    def _load_driver(self, driver: Optional[str]) -> ModuleType:
        if driver is not None:
            normalized = driver.lower()
            module_name = self._DRIVER_MODULES.get(normalized)
            if module_name is None:
                raise ValueError(f"Unsupported driver: {driver}")
            if util.find_spec(module_name) is None:
                raise ImportError(f"Driver '{driver}' could not be loaded.")
            return import_module(module_name)

        # Auto-pick in priority order
        for candidate in ("pymysql", "mysqldb"):
            module_name = self._DRIVER_MODULES[candidate]
            if util.find_spec(module_name) is None:
                continue
            return import_module(module_name)

        raise ImportError("No supported MySQL driver is available in the current environment.")

    # ----------------------------
    # Identifier safety
    # ----------------------------
    def _quote_identifier(self, ident: str) -> str:
        """
        Quote a MySQL identifier with backticks after validation.
        Allows dotted identifiers like "schema.table" or "table.column".
        """
        parts = ident.split(".")
        for p in parts:
            if not self._IDENT_RE.match(p):
                raise ValueError(f"Invalid identifier: {ident!r}")
        return ".".join(f"`{p}`" for p in parts)
