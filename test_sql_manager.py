import unittest
from unittest.mock import MagicMock

from sql_manager import SqlManager


class TestSqlManager(unittest.TestCase):

    def setUp(self):
        self.sql_manager, self.connection_mock, self.cursor_mock = self._create_sql_manager()

    def _create_sql_manager(self):
        manager = SqlManager({
            'user': 'user',
            'passwd': 'passwd',
            'host': 'localhost',
            'db': 'test_db',
            'driver': 'pymysql'
        })

        cursor_mock = MagicMock()
        cursor_mock.execute.return_value = None
        cursor_mock.fetchall.return_value = ()

        connection_mock = MagicMock()
        connection_mock.cursor.return_value = cursor_mock

        manager._connect = MagicMock(return_value=connection_mock)

        return manager, connection_mock, cursor_mock

    def assert_last_query(self, expected_query, expected_params, manager=None, cursor=None):
        manager = manager or self.sql_manager
        cursor = cursor or self.cursor_mock

        self.assertEqual(expected_query, manager.get_last_query())
        self.assertEqual(expected_params, manager.get_last_parameters())

        if expected_params is None:
            cursor.execute.assert_called_once_with(expected_query)
        else:
            cursor.execute.assert_called_once_with(expected_query, expected_params)

    def test_create_builds_insert_query_with_single_record(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table('users')
        self.sql_manager.set('name', 'Alice')
        self.sql_manager.set('age', 30)
        self.sql_manager.create()

        expected_query = "INSERT INTO users(`name`, `age`) VALUES(%s, %s)"
        expected_params = ('Alice', 30)

        self.assert_last_query(expected_query, expected_params)

    def test_create_builds_insert_query_with_multiple_records(self):
        manager, _, cursor = self._create_sql_manager()
        cursor.execute.reset_mock()

        manager.from_table('users')
        manager.sets([
            {'name': 'Alice', 'type': 1},
            {'name': 'Bob', 'type': 2},
            {'name': 'Charlie', 'type': 3},
        ])
        manager.create()

        expected_query = "INSERT INTO users(`name`, `type`) VALUES(%s, %s),(%s, %s),(%s, %s)"
        expected_params = ('Alice', 1, 'Bob', 2, 'Charlie', 3)

        self.assert_last_query(expected_query, expected_params, manager=manager, cursor=cursor)

    def test_update_builds_update_query_with_where_clause(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table('users')
        self.sql_manager.set('name', 'Alice')
        self.sql_manager.set('status', 'active')
        self.sql_manager.where('id', 10)
        self.sql_manager.update()

        expected_query = "UPDATE users SET `name` = %s, `status` = %s WHERE `id` = %s"
        expected_params = ('Alice', 'active', 10)

        self.assert_last_query(expected_query, expected_params)

    def test_delete_builds_delete_query_with_in_condition(self):
        self.cursor_mock.execute.reset_mock()

        self.sql_manager.from_table('users')
        self.sql_manager.where_in('status', ['active', 'pending'])
        self.sql_manager.delete()

        expected_query = "DELETE FROM users WHERE `status` IN (%s, %s)"
        expected_params = ('active', 'pending')

        self.assert_last_query(expected_query, expected_params)

    def test_select_builds_query_with_conditions_and_grouping(self):
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ()

        self.sql_manager.from_table('users')
        self.sql_manager.select('name')
        self.sql_manager.select('COUNT(*)', 'name_count')
        self.sql_manager.where_like('name', 'a%')
        self.sql_manager.order_by_asc(['name'])
        self.sql_manager.group_by('type')
        self.sql_manager.find_records()

        expected_query = "SELECT `name`,COUNT(*) AS name_count FROM users WHERE `name` LIKE %s ORDER BY name ASC  GROUP BY type"
        expected_params = ('a%',)

        self.assert_last_query(expected_query, expected_params)

    def test_count_builds_query_with_comparison_conditions(self):
        self.cursor_mock.execute.reset_mock()
        self.cursor_mock.fetchall.return_value = ((5,),)

        self.sql_manager.from_table('users')
        self.sql_manager.where_gte('score', 80)
        self.sql_manager.order_by_desc(['score'])
        count = self.sql_manager.count()

        expected_query = "SELECT COUNT(*) FROM users  WHERE `score` >= %s ORDER BY score DESC "
        expected_params = (80,)

        self.assertEqual(5, count)
        self.assert_last_query(expected_query, expected_params)

    def test_select_handles_null_checks(self):
        cursor = self.cursor_mock
        cursor.execute.reset_mock()
        cursor.fetchall.return_value = ()

        self.sql_manager.from_table('users')
        self.sql_manager.where_is_null('deleted_at')
        self.sql_manager.select('id')
        self.sql_manager.find_records()

        expected_query_null = "SELECT `id` FROM users WHERE `deleted_at` IS NULL"
        self.assert_last_query(expected_query_null, None)

        manager, _, cursor = self._create_sql_manager()
        cursor.execute.reset_mock()
        cursor.fetchall.return_value = ()

        manager.from_table('users')
        manager.where_is_not_null('deleted_at')
        manager.select('id')
        manager.find_records()

        expected_query_not_null = "SELECT `id` FROM users WHERE `deleted_at` IS NOT NULL"
        self.assert_last_query(expected_query_not_null, None, manager=manager, cursor=cursor)

    def test_where_not_in_and_comparison_queries(self):
        manager, _, cursor = self._create_sql_manager()
        cursor.execute.reset_mock()

        manager.from_table('users')
        manager.where_not_in('status', ['inactive', 'deleted'])
        manager.delete()

        expected_delete_query = "DELETE FROM users WHERE `status` NOT IN (%s, %s)"
        expected_delete_params = ('inactive', 'deleted')
        self.assert_last_query(expected_delete_query, expected_delete_params, manager=manager, cursor=cursor)

        manager_lt, _, cursor_lt = self._create_sql_manager()
        cursor_lt.execute.reset_mock()
        cursor_lt.fetchall.return_value = ()

        manager_lt.from_table('users')
        manager_lt.where_lt('score', 50)
        manager_lt.select('score')
        manager_lt.find_records()

        expected_select_query = "SELECT `score` FROM users WHERE `score` < %s"
        expected_select_params = (50,)
        self.assert_last_query(expected_select_query, expected_select_params, manager=manager_lt, cursor=cursor_lt)

        manager_lte, _, cursor_lte = self._create_sql_manager()
        cursor_lte.execute.reset_mock()
        cursor_lte.fetchall.return_value = ()

        manager_lte.from_table('users')
        manager_lte.where_lte('score', 60)
        manager_lte.select('score')
        manager_lte.find_records()

        expected_select_lte_query = "SELECT `score` FROM users WHERE `score` <= %s"
        expected_select_lte_params = (60,)
        self.assert_last_query(expected_select_lte_query, expected_select_lte_params, manager=manager_lte, cursor=cursor_lte)


if __name__ == "__main__":
    unittest.main()
