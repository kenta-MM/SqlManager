import unittest
from unittest.mock import MagicMock, patch

from sql_manager import SqlManager, SqlExpr


class TestSqlManagerQuery(unittest.TestCase):
    """
    SqlManager クラスの単体テストクラス。
    実際のデータベースには接続せず、unittest.mock.MagicMock を利用して
    クエリ生成処理や実行処理が正しく呼び出されているかを検証する。
    """

    def setUp(self):
        self.sql_manager, self.connection_mock, self.cursor_mock = self._create_sql_manager()

    def _create_sql_manager(self):
        fake_driver = self._fake_driver_module()

        # リファクタ後は _load_driver() が driver module を返す
        with patch.object(SqlManager, "_load_driver", return_value=fake_driver):
            manager = SqlManager({
                "user": "user",
                "passwd": "passwd",
                "host": "localhost",
                "db": "test_db",
                "driver": "pymysql",
            })

        cursor_mock = MagicMock()
        cursor_mock.execute.return_value = None
        cursor_mock.fetchall.return_value = ()

        connection_mock = MagicMock()
        # cursor(factory) でも cursor() でも同じモックを返す
        connection_mock.cursor = MagicMock(return_value=cursor_mock)

        # _connect(autocommit: bool) をモック化してDB接続しない
        manager._connect = MagicMock(return_value=connection_mock)

        return manager, connection_mock, cursor_mock

    def _fake_driver_module(self):
        """テスト用の疑似ドライバーを生成する。"""

        class DummyCursor:
            ...

        class DummyCursors:
            DictCursor = DummyCursor

        class DummyDriver:
            cursors = DummyCursors()

            @staticmethod
            def connect(**_kwargs):
                return MagicMock()

        return DummyDriver()

    def assert_last_query(self, expected_query, expected_params, manager=None, cursor=None):
        manager = manager or self.sql_manager
        cursor = cursor or self.cursor_mock

        self.assertEqual(expected_query, manager.get_last_query())
        self.assertEqual(expected_params, manager.get_last_parameters())

        if expected_params is None:
            cursor.execute.assert_called_once_with(expected_query)
        else:
            cursor.execute.assert_called_once_with(expected_query, expected_params)

    # ----------------------------
    # INSERT
    # ----------------------------
    def test_create_builds_insert_query_with_single_record(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users").set("name", "Alice").set("age", 30).create()

        expected_query = "INSERT INTO `users` (`name`, `age`) VALUES (%s, %s)"
        expected_params = ("Alice", 30)

        self.assert_last_query(expected_query, expected_params)

    def test_create_builds_insert_query_with_multiple_records(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users").sets([
            {"name": "Alice", "type": 1},
            {"name": "Bob", "type": 2},
            {"name": "Charlie", "type": 3},
        ]).create()

        expected_query = (
            "INSERT INTO `users` (`name`, `type`) VALUES "
            "(%s, %s), (%s, %s), (%s, %s)"
        )
        expected_params = ("Alice", 1, "Bob", 2, "Charlie", 3)

        self.assert_last_query(expected_query, expected_params)

    # ----------------------------
    # UPDATE / DELETE
    # ----------------------------
    def test_update_builds_update_query_with_where_clause(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users").set("name", "Alice").set("status", "active").where("id", 10).update()

        expected_query = "UPDATE `users` SET `name` = %s, `status` = %s WHERE `id` = %s"
        expected_params = ("Alice", "active", 10)

        self.assert_last_query(expected_query, expected_params)

    def test_update_without_where_is_blocked(self):
        """リファクタ後は UPDATE without WHERE を安全のため禁止。"""
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users").set("status", "active")
        with self.assertRaises(ValueError):
            self.sql_manager.update()

        self.cursor_mock.execute.assert_not_called()

    def test_delete_builds_delete_query_with_in_condition(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users").where_in("status", ["active", "pending"]).delete()

        expected_query = "DELETE FROM `users` WHERE `status` IN (%s, %s)"
        expected_params = ("active", "pending")

        self.assert_last_query(expected_query, expected_params)

    def test_delete_without_where_is_blocked(self):
        """リファクタ後は DELETE without WHERE を安全のため禁止。"""
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users")
        with self.assertRaises(ValueError):
            self.sql_manager.delete()

        self.cursor_mock.execute.assert_not_called()

    def test_where_in_empty_list_turns_into_false_condition_for_select(self):
        """
        IN [] は SQL 的に無効なので、実装では 1=0 にする（常に0件）。
        """
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users").select("id").where_in("status", []).find_records()

        expected_query = "SELECT `id` FROM `users` WHERE 1=0"
        self.assert_last_query(expected_query, None)

    # ----------------------------
    # SELECT / COUNT
    # ----------------------------
    def test_select_builds_query_with_conditions_and_grouping(self):
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users") \
            .select("name") \
            .select(SqlExpr("COUNT(*)"), "name_count") \
            .where_like("name", "a%") \
            .group_by("type") \
            .order_by_asc(["name"]) \
            .find_records()

        expected_query = (
            "SELECT `name`, COUNT(*) AS `name_count` FROM `users` "
            "WHERE `name` LIKE %s GROUP BY `type` ORDER BY `name` ASC"
        )
        expected_params = ("a%",)

        self.assert_last_query(expected_query, expected_params)

    def test_count_builds_query_with_comparison_conditions(self):
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ((5,),)

        count = self.sql_manager.from_table("users").where_gte("score", 80).order_by_desc(["score"]).count()

        expected_query = "SELECT COUNT(*) FROM `users` WHERE `score` >= %s ORDER BY `score` DESC"
        expected_params = (80,)

        self.assertEqual(5, count)
        self.assert_last_query(expected_query, expected_params)

    def test_select_builds_query_with_limit_and_offset(self):
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users") \
            .select("name") \
            .where("status", "active") \
            .order_by_asc(["id"]) \
            .limit(10, offset=5) \
            .find_records()

        expected_query = (
            "SELECT `name` FROM `users` "
            "WHERE `status` = %s ORDER BY `id` ASC LIMIT %s OFFSET %s"
        )
        expected_params = ("active", 10, 5)
        self.assert_last_query(expected_query, expected_params)

    def test_limit_without_offset(self):
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users").select("id").limit(3).find_records()

        expected_query = "SELECT `id` FROM `users` LIMIT %s"
        expected_params = (3,)
        self.assert_last_query(expected_query, expected_params)

    def test_select_handles_null_checks(self):
        # --- IS NULL ---
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users").where_is_null("deleted_at").select("id").find_records()

        expected_query_null = "SELECT `id` FROM `users` WHERE `deleted_at` IS NULL"
        self.assert_last_query(expected_query_null, None)

        # --- IS NOT NULL ---
        manager, _, cursor = self._create_sql_manager()
        cursor.execute.reset_mock()
        cursor.fetchall.return_value = ()

        manager.from_table("users").where_is_not_null("deleted_at").select("id").find_records()

        expected_query_not_null = "SELECT `id` FROM `users` WHERE `deleted_at` IS NOT NULL"
        self.assert_last_query(expected_query_not_null, None, manager=manager, cursor=cursor)

    def test_where_not_in_and_comparison_queries(self):
        # NOT IN DELETE
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users").where_not_in("status", ["inactive", "deleted"]).delete()

        expected_delete_query = "DELETE FROM `users` WHERE `status` NOT IN (%s, %s)"
        expected_delete_params = ("inactive", "deleted")
        self.assert_last_query(expected_delete_query, expected_delete_params)

        # < SELECT
        manager_lt, _, cursor_lt = self._create_sql_manager()
        cursor_lt.execute.reset_mock()
        cursor_lt.fetchall.return_value = ()

        manager_lt.from_table("users").where_lt("score", 50).select("score").find_records()

        expected_select_query = "SELECT `score` FROM `users` WHERE `score` < %s"
        expected_select_params = (50,)
        self.assert_last_query(expected_select_query, expected_select_params, manager=manager_lt, cursor=cursor_lt)

        # <= SELECT
        manager_lte, _, cursor_lte = self._create_sql_manager()
        cursor_lte.execute.reset_mock()
        cursor_lte.fetchall.return_value = ()

        manager_lte.from_table("users").where_lte("score", 60).select("score").find_records()

        expected_select_lte_query = "SELECT `score` FROM `users` WHERE `score` <= %s"
        expected_select_lte_params = (60,)
        self.assert_last_query(expected_select_lte_query, expected_select_lte_params, manager=manager_lte, cursor=cursor_lte)

    # ----------------------------
    # Identifier safety
    # ----------------------------
    def test_invalid_identifier_raises(self):
        """識別子は検証されるので、不正な識別子は ValueError になる。"""
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users")
        with self.assertRaises(ValueError):
            self.sql_manager.select("COUNT(*)")  # 関数は SqlExpr で渡すべき

        self.cursor_mock.execute.assert_not_called()

    def test_limit_rejects_negative_values(self):
        self.cursor_mock.execute.reset_mock()

        with self.assertRaises(ValueError):
            self.sql_manager.from_table("users").limit(-1)

        self.cursor_mock.execute.assert_not_called()

    # ----------------------------
    # JOIN
    # ----------------------------
    def test_select_builds_query_with_inner_join(self):
        """
        INNER JOIN が FROM の直後に入り、ON 句が付くことを確認する。
        JOINテストなので、AS / DictCursor / ORDER BY などは使わない。
        """
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users") \
            .inner_join("orders", "users.id = orders.user_id") \
            .select("users.id") \
            .select("orders.id") \
            .find_records()

        expected_query = (
            "SELECT `users`.`id`, `orders`.`id` FROM `users` "
            "INNER JOIN `orders` ON users.id = orders.user_id"
        )
        self.assert_last_query(expected_query, None)

    def test_select_builds_query_with_left_join(self):
        """
        LEFT JOIN が生成されることを確認する。
        JOINテストなので、selectの別名や並び替えなどの仕様には依存しない。
        """
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users") \
            .left_join("orders", "users.id = orders.user_id") \
            .select("users.id") \
            .select("orders.id") \
            .find_records()

        expected_query = (
            "SELECT `users`.`id`, `orders`.`id` FROM `users` "
            "LEFT JOIN `orders` ON users.id = orders.user_id"
        )
        self.assert_last_query(expected_query, None)

    def test_select_builds_query_with_cross_join(self):
        """
        CROSS JOIN が生成され、ON が付かないことを確認する。
        直積の行数などはDBが無いので検証しない（クエリ文字列のみ）。
        """
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users") \
            .cross_join("orders") \
            .select("users.id") \
            .select("orders.id") \
            .find_records()

        expected_query = (
            "SELECT `users`.`id`, `orders`.`id` FROM `users` "
            "CROSS JOIN `orders`"
        )
        self.assert_last_query(expected_query, None)

    def test_join_identifier_validation_invalid_table_raises(self):
        """
        JOIN でも識別子検証が効いていることを確認する。
        ただし検証はクエリ生成（実行）時に行われるため、find_records() で ValueError を確認する。
        """
        self.cursor_mock.execute.reset_mock()

        with self.assertRaises(ValueError):
            self.sql_manager.from_table("users") \
                .inner_join("orders;DROP", "users.id = orders.user_id") \
                .select("users.id") \
                .find_records()

        self.cursor_mock.execute.assert_not_called()

    def test_select_builds_query_with_group_by_and_having(self):
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table("users") \
            .select("name") \
            .select(SqlExpr("COUNT(*)")) \
            .group_by("name") \
            .having_gt(SqlExpr("COUNT(*)"), 1) \
            .find_records()

        expected_query = (
            "SELECT `name`, COUNT(*) FROM `users` "
            "GROUP BY `name` HAVING COUNT(*) > %s"
        )
        expected_params = (1,)

        self.assert_last_query(expected_query, expected_params)

    def test_having_without_group_by_is_blocked(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table("users").select("name")
        with self.assertRaises(ValueError):
            self.sql_manager.having_gt(SqlExpr("COUNT(*)"), 1).find_records()

        self.cursor_mock.execute.assert_not_called()



if __name__ == "__main__":
    unittest.main()
