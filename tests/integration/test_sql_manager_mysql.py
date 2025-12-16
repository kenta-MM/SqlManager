# tests/integration/test_sql_manager_mysql_all_public.py
import os
import unittest
from dotenv import load_dotenv

from sql_manager import SqlManager

try:
    import MySQLdb  # mysqlclient
except ImportError as e:
    raise ImportError(
        "MySQLdb(mysqlclient) が必要です。例: pip install mysqlclient"
    ) from e



def _db_settings() -> dict:
    load_dotenv()

    return {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "passwd": os.getenv("DB_PASSWORD"),
        "db": os.getenv("DB_NAME"),
    }


def _connect_raw():
    s = _db_settings()
    conn = MySQLdb.connect(
        host=s["host"],
        user=s["user"],
        passwd=s["passwd"],
        db=s["db"],
        charset="utf8mb4",
    )
    conn.autocommit(True)
    return conn


def _exec_raw(sql: str, params=None):
    conn = _connect_raw()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
        finally:
            cur.close()
    finally:
        conn.close()


class TestSqlManagerMySQL_AllPublic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manager = SqlManager(settings=_db_settings())

        # テスト用テーブル作成（SqlManager に raw/execute が無い想定なので MySQLdb で用意）
        _exec_raw(
            """
            CREATE TABLE IF NOT EXISTS test_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                score INT NULL,
                note VARCHAR(255) NULL,
                created_at DATETIME NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )

    def setUp(self):
        _exec_raw("TRUNCATE TABLE test_items")

    # ---- 基本: from_table / set / create / find_records ----
    def test_from_table_set_create_find_records(self):
        self.manager.from_table("test_items")
        self.manager.set("name", "insert test")
        self.manager.set("score", 10)
        self.manager.create()

        rows = (
            self.manager
            .from_table("test_items")
            .where("name", "insert test")
            .find_records()
        )
        self.assertEqual(len(rows), 1)

    def test_set_accepts_dict(self):
        self.manager.from_table("test_items")
        self.manager.set({"name": "dict insert", "score": 99})
        self.manager.create()

        rows = (
            self.manager
            .from_table("test_items")
            .where("name", "dict insert")
            .find_records()
        )
        self.assertEqual(len(rows), 1)

    def test_sets_insert_multiple(self):
        # fluent が return self の設計ならこれでOK。もしダメなら戻り値を受けて書き換え。
        self.manager.from_table("test_items")
        self.manager.sets([
            {"name": "a", "score": 1},
            {"name": "b", "score": 2},
        ])
        self.manager.create()

        rows = self.manager.from_table("test_items").find_records()
        self.assertEqual(len(rows), 2)

    # ---- where 系（=, >, >=, <, <=, LIKE, IS NULL, IS NOT NULL）----
    def test_where_variants(self):
        self.manager.from_table("test_items").sets([
            {"name": "x", "score": 10, "note": None},
            {"name": "y", "score": 20, "note": "memo"},
            {"name": "z", "score": 30, "note": "memo"},
        ]).create()

        rows = self.manager.from_table("test_items").where_gt("score", 10).find_records()
        self.assertEqual(len(rows), 2)

        rows = self.manager.from_table("test_items").where_gte("score", 20).find_records()
        self.assertEqual(len(rows), 2)

        rows = self.manager.from_table("test_items").where_lt("score", 30).find_records()
        self.assertEqual(len(rows), 2)

        rows = self.manager.from_table("test_items").where_lte("score", 20).find_records()
        self.assertEqual(len(rows), 2)

        rows = self.manager.from_table("test_items").where_like("name", "y").find_records()
        self.assertEqual(len(rows), 1)

        rows = self.manager.from_table("test_items").where_is_null("note").find_records()
        self.assertEqual(len(rows), 1)

        rows = self.manager.from_table("test_items").where_is_not_null("note").find_records()
        self.assertEqual(len(rows), 2)

        rows = self.manager.from_table("test_items").where_in("score", [10, 20]).find_records()
        self.assertEqual(len(rows), 2)

        rows = self.manager.from_table("test_items").where_not_in("score", [10, 20]).find_records()
        self.assertEqual(len(rows), 1)


    # ---- select / order_by / group_by ----
    def test_select_and_alias(self):
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 1},
            {"name": "b", "score": 2},
        ]).create()

        rows = (
            self.manager
            .from_table("test_items")
            .select("name", "n")
            .select("score", "s")
            .order_by_asc(["score"])
            .find_records(is_dict_cursor=True)
        )
        self.assertEqual(rows[0]["n"], "a")
        self.assertEqual(rows[0]["s"], 1)

    def test_group_by_functionality(self):
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 10},
            {"name": "a", "score": 20},
            {"name": "b", "score": 30},
        ]).create()

        rows = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("COUNT(*)", "cnt")
            .group_by("name")
            .find_records(is_dict_cursor=True)
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["name"], "a")
        self.assertEqual(rows[0]["cnt"], 2)
        self.assertEqual(rows[1]["name"], "b")

    def test_order_by_desc(self):
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 1},
            {"name": "b", "score": 2},
        ]).create()

        rows = (
            self.manager
            .from_table("test_items")
            .order_by_desc(["score"])
            .find_records(is_dict_cursor=True)
        )
        self.assertEqual(rows[0]["score"], 2)

    # ---- update / delete / count ----
    def test_update(self):
        self.manager.from_table("test_items").set("name", "u").set("score", 1).create()
        self.manager.from_table("test_items").set("score", 99).where("name", "u").update()

        rows = self.manager.from_table("test_items").where("score", 99).find_records()
        self.assertEqual(len(rows), 1)

    def test_delete(self):
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 1},
            {"name": "b", "score": 2},
        ]).create()

        self.manager.from_table("test_items").where("name", "a").delete()
        rows = self.manager.from_table("test_items").find_records()
        self.assertEqual(len(rows), 1)

    def test_count(self):
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 1},
            {"name": "b", "score": 2},
        ]).create()

        cnt = self.manager.from_table("test_items").count()
        self.assertEqual(cnt, 2)

    # ---- transaction: begin_transaction / end_transaction ----
    def test_transaction_commit(self):
        self.manager.begin_transaction()
        try:
            self.manager.from_table("test_items").set("name", "tx").set("score", 1).create()
            self.manager.end_transaction(is_succeed=True)
        except Exception:
            self.manager.end_transaction(is_succeed=False)
            raise

        rows = self.manager.from_table("test_items").where("name", "tx").find_records()
        self.assertEqual(len(rows), 1)

    def test_transaction_rollback(self):
        self.manager.begin_transaction()
        try:
            self.manager.from_table("test_items").set("name", "tx").set("score", 1).create()
            self.manager.end_transaction(is_succeed=False)
        except Exception:
            raise

        rows = self.manager.from_table("test_items").where("name", "tx").find_records()
        self.assertEqual(len(rows), 0)

    # ---- last query APIs ----
    def test_get_last_query_and_parameters_and_info(self):
        self.manager.from_table("test_items").set("name", "last").set("score", 1).create()

        q = self.manager.get_last_query()
        p = self.manager.get_last_parameters()
        qi = self.manager.get_last_query_info()

        self.assertIsInstance(q, str)
        self.assertTrue(q.upper().startswith("INSERT INTO"))
        self.assertIsInstance(qi, tuple)
        self.assertEqual(qi[0], q)
        self.assertEqual(qi[1], p)

        _ = self.manager.from_table("test_items").where("name", "last").find_records()
        q2 = self.manager.get_last_query()
        self.assertTrue(q2.upper().startswith("SELECT"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
