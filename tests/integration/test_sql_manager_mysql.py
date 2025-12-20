# tests/integration/test_sql_manager_mysql_all_public.py
import os
import unittest
from dotenv import load_dotenv
from sql_manager import SqlManager, SqlExpr


class TestSqlManagerMySQL_AllPublic(unittest.TestCase):
    """
    SqlManager の公開 API が MySQL 上で正しく動作することを検証する
    Integration Test。

    - fluent interface（from_table / set / where など）が期待通り動くこと
    - SELECT / INSERT / UPDATE / DELETE / COUNT / TRANSACTION を網羅
    - SQL の結果そのものではなく「振る舞い」を保証する
    """

    DRIVER = None

    @classmethod
    def setUpClass(cls):
        """
        テスト全体で共通して使用する SqlManager と
        テスト用テーブルを初期化する。
        """
        load_dotenv()

        settings = {
            "host": os.getenv("DB_HOST"),
            "user": os.getenv("DB_USER"),
            "passwd": os.getenv("DB_PASSWORD"),
            "db": os.getenv("DB_NAME"),
        }

        if cls.DRIVER:
            settings["driver"] = cls.DRIVER

        try:
            cls.manager = SqlManager(settings=settings)
        except ImportError as exc:
            raise unittest.SkipTest(f"{cls.__name__} はドライバ {cls.DRIVER!r} を利用できないためスキップします: {exc}") from exc

        cls.manager.raw_execute(
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

        cls.manager.raw_execute(
            """
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )

        cls.manager.raw_execute(
            """
            CREATE TABLE IF NOT EXISTS test_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NULL,
                item VARCHAR(255) NOT NULL,
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )

    def setUp(self):
        """
        各テストは常にクリーンな状態で開始する。
        """
        self.manager.raw_execute("TRUNCATE TABLE test_items")
        self.manager.raw_execute("TRUNCATE TABLE test_orders")
        self.manager.raw_execute("TRUNCATE TABLE test_users")

    # ---- 基本: from_table / set / create / find_records ----
    def test_from_table_set_create_find_records(self):
        """
        from_table → set → create → find_records の基本的な INSERT / SELECT
        フローが正しく動作し、INSERTした内容が保存されていることを確認する。
        """
        self.manager.from_table("test_items")
        self.manager.set("name", "insert test")
        self.manager.set("score", 10)
        self.manager.create()

        rows = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .select("note")
            .select("created_at")
            .where("name", "insert test")
            .find_records(is_dict_cursor=True)
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "insert test")
        self.assertEqual(rows[0]["score"], 10)
        self.assertIsNone(rows[0]["note"])
        self.assertIsNone(rows[0]["created_at"])

    def test_set_accepts_dict(self):
        """
        set() が dict を受け取り、複数カラムを一度に
        INSERT でき、内容が正しく保存されることを確認する。
        """
        self.manager.from_table("test_items")
        self.manager.set({"name": "dict insert", "score": 99})
        self.manager.create()

        rows = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .select("note")
            .select("created_at")
            .where("name", "dict insert")
            .find_records(is_dict_cursor=True)
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "dict insert")
        self.assertEqual(rows[0]["score"], 99)
        self.assertIsNone(rows[0]["note"])
        self.assertIsNone(rows[0]["created_at"])

    def test_sets_insert_multiple(self):
        """
        sets() に list[dict] を渡した場合、
        複数レコードが一度に INSERT され、内容が正しいことを確認する。
        """
        self.manager.from_table("test_items")
        self.manager.sets([
            {"name": "a", "score": 1},
            {"name": "b", "score": 2},
        ])
        self.manager.create()

        rows = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .order_by_asc(["name"])
            .find_records(is_dict_cursor=True)
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["name"], "a")
        self.assertEqual(rows[0]["score"], 1)
        self.assertEqual(rows[1]["name"], "b")
        self.assertEqual(rows[1]["score"], 2)

    # ---- where 系（=, >, >=, <, <=, LIKE, IS NULL, IS NOT NULL）----
    def test_where_variants(self):
        """
        where 系 API がすべて正しく機能することを確認する。

        対象:
        - where (=)
        - where_gt / gte / lt / lte
        - where_like
        - where_is_null / where_is_not_null
        - where_in / where_not_in
        """
        self.manager.from_table("test_items").sets([
            {"name": "x", "score": 10, "note": None},
            {"name": "y", "score": 20, "note": "memo"},
            {"name": "z", "score": 30, "note": "memo"},
        ]).create()

        rows = self.manager.from_table("test_items").where_gt("score", 10).find_records(is_dict_cursor=True)
        self.assertEqual({r["name"] for r in rows}, {"y", "z"})

        rows = self.manager.from_table("test_items").where_gte("score", 20).find_records(is_dict_cursor=True)
        self.assertEqual({r["name"] for r in rows}, {"y", "z"})

        rows = self.manager.from_table("test_items").where_lt("score", 30).find_records(is_dict_cursor=True)
        self.assertEqual({r["name"] for r in rows}, {"x", "y"})

        rows = self.manager.from_table("test_items").where_lte("score", 20).find_records(is_dict_cursor=True)
        self.assertEqual({r["name"] for r in rows}, {"x", "y"})

        rows = self.manager.from_table("test_items").where_like("name", "y").find_records(is_dict_cursor=True)
        self.assertEqual([r["name"] for r in rows], ["y"])

        rows = self.manager.from_table("test_items").where_is_null("note").find_records(is_dict_cursor=True)
        self.assertEqual([r["name"] for r in rows], ["x"])

        rows = self.manager.from_table("test_items").where_is_not_null("note").find_records(is_dict_cursor=True)
        self.assertEqual({r["name"] for r in rows}, {"y", "z"})

        rows = self.manager.from_table("test_items").where_in("score", [10, 20]).find_records(is_dict_cursor=True)
        self.assertEqual({r["name"] for r in rows}, {"x", "y"})

        rows = self.manager.from_table("test_items").where_not_in("score", [10, 20]).find_records(is_dict_cursor=True)
        self.assertEqual({r["name"] for r in rows}, {"z"})

    # ---- select / order_by / group_by ----
    def test_select_and_alias(self):
        """
        select() でカラム指定とエイリアス指定ができ、
        order_by_asc() により並び順が制御できることを確認する。
        """
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
        """
        group_by() により GROUP BY 集計が正しく行われることを確認する。

        - COUNT(*) の集計結果が正しいこと
        - ORDER BY 未指定時は結果順に依存せず検証する
        """
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 10},
            {"name": "a", "score": 20},
            {"name": "b", "score": 30},
        ]).create()

        rows = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select(SqlExpr("COUNT(*)"), "cnt")
            .group_by("name")
            .find_records(is_dict_cursor=True)
        )

        by_name = {r["name"]: r["cnt"] for r in rows}
        self.assertEqual(by_name, {"a": 2, "b": 1})

    def test_group_by_accepts_list_and_comma_separated(self):
        """
        group_by() が以下の指定形式を受け付けることを確認する。

        - list 形式: group_by(["name"])
        - カンマ区切り文字列: group_by("name, score")
        """
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 10},
            {"name": "a", "score": 20},
            {"name": "b", "score": 30},
            {"name": "b", "score": 40},
        ]).create()

        rows1 = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select(SqlExpr("COUNT(*)"), "cnt")
            .group_by(["name"])
            .find_records(is_dict_cursor=True)
        )
        by_name1 = {r["name"]: r["cnt"] for r in rows1}
        self.assertEqual(by_name1, {"a": 2, "b": 2})

        rows2 = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .select(SqlExpr("COUNT(*)"), "cnt")
            .group_by("name, score")
            .find_records(is_dict_cursor=True)
        )
        self.assertTrue(all(r["cnt"] == 1 for r in rows2))

    def test_order_by_desc(self):
        """
        order_by_desc() により降順ソートが正しく行われることを確認する。
        """
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
        """
        where 条件付き update() により
        対象レコードのみが更新され、更新していない列が変わらないことも確認する。
        """
        # 対象行と対象外行を用意し、更新対象の列以外が変わらないことを確認する
        self.manager.from_table("test_items").sets([
            {"name": "u1", "score": 1, "note": "keep"},
            {"name": "u2", "score": 2, "note": "keep2"},
        ]).create()

        # u1 の score のみ更新
        self.manager.from_table("test_items").set("score", 99).where("name", "u1").update()

        u1 = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .select("note")
            .where("name", "u1")
            .find_records(is_dict_cursor=True)
        )[0]
        u2 = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .select("note")
            .where("name", "u2")
            .find_records(is_dict_cursor=True)
        )[0]

        # 対象行: score が更新され、note は不変
        self.assertEqual(u1["name"], "u1")
        self.assertEqual(u1["score"], 99)
        self.assertEqual(u1["note"], "keep")

        # 対象外行: 全て不変
        self.assertEqual(u2["name"], "u2")
        self.assertEqual(u2["score"], 2)
        self.assertEqual(u2["note"], "keep2")

    def test_delete(self):
        """
        where 条件付き delete() により
        対象データだけ削除できており、他データが消えていないことを確認する。
        """
        self.manager.from_table("test_items").sets([
            {"name": "del_a", "score": 1, "note": "A"},
            {"name": "del_b", "score": 2, "note": "B"},
        ]).create()

        # del_a だけ削除
        self.manager.from_table("test_items").where("name", "del_a").delete()

        # del_a は無い
        rows_a = self.manager.from_table("test_items").where("name", "del_a").find_records()
        self.assertEqual(len(rows_a), 0)

        # del_b は残っていて内容も変わらない
        rows_b = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .select("note")
            .where("name", "del_b")
            .find_records(is_dict_cursor=True)
        )
        self.assertEqual(len(rows_b), 1)
        self.assertEqual(rows_b[0]["name"], "del_b")
        self.assertEqual(rows_b[0]["score"], 2)
        self.assertEqual(rows_b[0]["note"], "B")

    def test_count(self):
        """
        count() がテーブル内のレコード件数を
        正しく返すことを確認する。
        """
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 1},
            {"name": "b", "score": 2},
        ]).create()

        cnt = self.manager.from_table("test_items").count()
        self.assertEqual(cnt, 2)

    # ---- transaction: begin_transaction / end_transaction ----
    def test_transaction_commit(self):
        """
        begin_transaction → end_transaction(True) により
        INSERT が commit され、内容が残っていることを確認する。
        """
        self.manager.begin_transaction()
        try:
            self.manager.from_table("test_items").set("name", "tx").set("score", 1).create()
            self.manager.end_transaction(is_succeed=True)
        except Exception:
            self.manager.end_transaction(is_succeed=False)
            raise

        rows = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select("score")
            .where("name", "tx")
            .find_records(is_dict_cursor=True)
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "tx")
        self.assertEqual(rows[0]["score"], 1)

    def test_transaction_rollback(self):
        """
        begin_transaction → end_transaction(False) により
        INSERT が rollback され、データが残っていないことを確認する。
        """
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
        """
        get_last_query / get_last_parameters / get_last_query_info が
        直近で実行された SQL 情報を正しく保持していることを確認する。
        """
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

    # ---- join ----
    def test_inner_join(self):
        """
        inner_join() により一致する行のみ取得できることを確認する。
        """
        self.manager.from_table("test_users").sets([
            {"name": "u1"},
            {"name": "u2"},
        ]).create()

        # u1 のみ注文がある
        u1_id = self.manager.from_table("test_users").select("id").where("name", "u1").find_records()[0][0]
        self.manager.from_table("test_orders").sets([
            {"user_id": u1_id, "item": "apple"},
        ]).create()

        rows = (
            self.manager
            .from_table("test_users")
            .inner_join("test_orders", "test_users.id = test_orders.user_id")
            .select("test_users.name", "user_name")
            .select("test_orders.item", "order_item")
            .order_by_asc(["test_users.id"])
            .find_records(is_dict_cursor=True)
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["user_name"], "u1")
        self.assertEqual(rows[0]["order_item"], "apple")

    def test_left_join_includes_unmatched(self):
        """
        left_join() により左側テーブルの全行が取得でき、
        右側が無い場合は NULL になることを確認する。
        """
        self.manager.from_table("test_users").sets([
            {"name": "u1"},
            {"name": "u2"},
        ]).create()

        u1_id = self.manager.from_table("test_users").select("id").where("name", "u1").find_records()[0][0]
        self.manager.from_table("test_orders").sets([
            {"user_id": u1_id, "item": "apple"},
        ]).create()

        rows = (
            self.manager
            .from_table("test_users")
            .left_join("test_orders", "test_users.id = test_orders.user_id")
            .select("test_users.name", "user_name")
            .select("test_orders.item", "order_item")
            .order_by_asc(["test_users.id"])
            .find_records(is_dict_cursor=True)
        )

        # u1 は item が入り、u2 は NULL
        self.assertEqual([r["user_name"] for r in rows], ["u1", "u2"])
        self.assertEqual(rows[0]["order_item"], "apple")
        self.assertIsNone(rows[1]["order_item"])

    def test_cross_join_cartesian_product(self):
        """
        cross_join() により直積（行数 = 左件数 * 右件数）になり、
        すべての組み合わせが取得できることを確認する。
        """
        self.manager.from_table("test_users").sets([
            {"name": "u1"},
            {"name": "u2"},
        ]).create()

        self.manager.from_table("test_orders").sets([
            {"user_id": None, "item": "a"},
            {"user_id": None, "item": "b"},
            {"user_id": None, "item": "c"},
        ]).create()

        rows = (
            self.manager
            .from_table("test_users")
            .cross_join("test_orders")
            .select("test_users.id")
            .select("test_orders.id")
            .find_records()
        )

        # 件数チェック（2 × 3 = 6）
        self.assertEqual(len(rows), 6)

        # ---- 中身のチェック ----
        # users.id と orders.id をそれぞれ取得
        user_ids = {
            r[0] for r in self.manager
                .from_table("test_users")
                .select("id")
                .find_records()
        }
        order_ids = {
            r[0] for r in self.manager
                .from_table("test_orders")
                .select("id")
                .find_records()
        }

        # 期待される直積
        expected_pairs = {
            (u_id, o_id)
            for u_id in user_ids
            for o_id in order_ids
        }

        actual_pairs = set(rows)

        self.assertEqual(actual_pairs, expected_pairs)

    def test_having_filters_grouped_rows(self):
        """
        GROUP BY + HAVING により、集計結果で絞り込めることを確認する。
        （AS など別機能への依存を避け、tuple で検証する）
        """
        self.manager.from_table("test_items").sets([
            {"name": "a", "score": 10},
            {"name": "a", "score": 20},
            {"name": "b", "score": 30},
        ]).create()

        rows = (
            self.manager
            .from_table("test_items")
            .select("name")
            .select(SqlExpr("COUNT(*)"))
            .group_by("name")
            .having_gt(SqlExpr("COUNT(*)"), 1)
            .find_records()
        )

        # HAVING COUNT(*) > 1 なので "a" だけが残り、件数は2
        self.assertEqual(set(rows), {("a", 2)})

    def test_having_without_group_by_is_blocked(self):
        self.manager.from_table("test_items").set("name", "x").set("score", 1).create()

        with self.assertRaises(ValueError):
            self.manager.from_table("test_items") \
                .select("name") \
                .having_gt(SqlExpr("COUNT(*)"), 1) \
                .find_records()

class TestSqlManagerMySQL_PyMySQL(TestSqlManagerMySQL_AllPublic):
    DRIVER = "pymysql"


class TestSqlManagerMySQL_MySQLDB(TestSqlManagerMySQL_AllPublic):
    DRIVER = "mysqldb"


if __name__ == "__main__":
    unittest.main(verbosity=2)
