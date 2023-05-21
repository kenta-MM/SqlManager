import unittest
from unittest.mock import MagicMock
from SqlManager import SqlManager
import json

class TestSqlManager(unittest.TestCase):
    
    def setUp(self):
        print(f"begin test: {self._testMethodName}")

    def tearDown(self):
        print(f"end test: {self._testMethodName}")

        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.where_gte('id', 1)
        sqlManager.delete()

    def test_create(self) -> None:

        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.set('name', 'test_create')
        sqlManager.create()

        sqlManager.where('name', 'test_create')
        sqlManager.select('name')
        self.assertEqual((('test_create',),), sqlManager.find_records(), '想定した結果と一致しません。')


    def test_update(self) -> None:

        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.set('name', 'test_create')
        sqlManager.create()

        sqlManager.where('name', 'test_create')
        sqlManager.set('name', 'test_update')
        sqlManager.update()

        sqlManager.where('name', 'test_update')
        sqlManager.select('name')
        self.assertEqual((('test_update',),), sqlManager.find_records(), '想定した結果と一致しません。')


    def test_delete(self) -> None:

        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.set('name', 'test_create')
        sqlManager.create()

        sqlManager.where('name', 'test_create')
        sqlManager.delete()

        sqlManager.where('name', 'test_create')
        self.assertEqual((), sqlManager.find_records(), '想定した結果と一致しません。')


    def test_count(self) -> None:
        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.set('name', 'test_count')
        sqlManager.create()

        self.assertEqual(1, sqlManager.count(), '想定した結果と一致しません。')


        sqlManager.from_table('test')
        sqlManager.set('name', 'test_count2')
        sqlManager.create()

        sqlManager.where('name', 'test_count2')
        self.assertEqual(1, sqlManager.count(), '想定した結果と一致しません。')

    def test_find_records(self):
        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.set('name', 'test_find_records')
        sqlManager.create()

        sqlManager.select('name')
        sqlManager.select('type')
        self.assertEqual((('test_find_records',None),), sqlManager.find_records())

        sqlManager.select('name')
        sqlManager.select('type')
        self.assertEqual(({'name': 'test_find_records', 'type': None},), sqlManager.find_records(True))

    def test_sets(self):
        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.sets([
            {'name' : 'test_find_records', 'type' : 1},
            {'name' : 'test_find_records', 'type' : 2},
            {'name' : 'test_find_records', 'type' : 3},
            {'name' : 'test_find_records', 'type' : 4},
        ])
        sqlManager.create()

        self.assertEqual(4, sqlManager.count())

    def test_wheres(self):
        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.sets([
            {'name' : 'a_test_find_records_a', 'type' : 1},
            {'name' : 'b_test_find_records_a', 'type' : 2},
            {'name' : 'a_test_find_records_b', 'type' : 3},
            {'name' : 'b_test_find_records_b', 'type' : 4},
            {'name' : 'c_test_find_records_c', 'type' : None},
        ])
        sqlManager.create()

        sqlManager.where('type', 1)
        sqlManager.select('COUNT(*)')
        self.assertEqual(((1,),), sqlManager.find_records())

        sqlManager.where_in('type', [1, 2])
        sqlManager.select('COUNT(*)')
        self.assertEqual(((2,),), sqlManager.find_records())

        sqlManager.where_not_in('type', [1, 2, 3])
        sqlManager.select('COUNT(*)')
        self.assertEqual(((1,),), sqlManager.find_records())

        sqlManager.where_gt('type', 1)
        sqlManager.select('COUNT(*)')
        self.assertEqual(((3,),), sqlManager.find_records())

        sqlManager.where_gte('type', 1)
        sqlManager.select('COUNT(*)')
        self.assertEqual(((4,),), sqlManager.find_records())

        sqlManager.where_lt('type', 4)
        sqlManager.select('COUNT(*)')
        self.assertEqual(((3,),), sqlManager.find_records())

        sqlManager.where_lte('type', 4)
        sqlManager.select('COUNT(*)')
        self.assertEqual(((4,),), sqlManager.find_records())

        sqlManager.where_is_null('type')
        sqlManager.select('COUNT(*)')
        self.assertEqual(((1,),), sqlManager.find_records())
        
        sqlManager.where_is_not_null('type')
        sqlManager.select('COUNT(*)')
        self.assertEqual(((4,),), sqlManager.find_records())

        sqlManager.where_like('name', '%_a')
        sqlManager.select('COUNT(*)')
        self.assertEqual(((2,),), sqlManager.find_records())

        sqlManager.where_like('name', 'a_%')
        sqlManager.select('COUNT(*)')
        self.assertEqual(((2,),), sqlManager.find_records())

        sqlManager.where_like('name', 'a_%_a')
        sqlManager.select('COUNT(*)')
        self.assertEqual(((1,),), sqlManager.find_records())        

    def test_order_by(self):
    
        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.set('name', 'test_order_by_desc_a')
        sqlManager.create()

        sqlManager.set('name', 'test_order_by_desc_b')
        sqlManager.create()

        sqlManager.order_by_desc(['name'])
        sqlManager.select('name')
        self.assertEqual((('test_order_by_desc_b',), ('test_order_by_desc_a',)),sqlManager.find_records())
        
        sqlManager.order_by_asc(['name'])
        sqlManager.select('name')
        self.assertEqual((('test_order_by_desc_a',), ('test_order_by_desc_b',)),sqlManager.find_records())

    def test_group_by(self):
        sqlManager = self.__get_sql_manager()
        sqlManager.from_table('test')
        sqlManager.set('name', 'test_group_by_a')
        sqlManager.set('type', 1)
        sqlManager.create()

        sqlManager.set('name', 'test_group_by_b')
        sqlManager.set('type', 1)
        sqlManager.create()

        sqlManager.group_by('type')
        sqlManager.select('count(type)')
        self.assertEqual(((2,),), sqlManager.find_records())

        sqlManager.set('name', 'test_group_by_a')
        sqlManager.set('type', 1)
        sqlManager.create()
        
        sqlManager.group_by(['name', 'type'])
        sqlManager.select('name')
        sqlManager.select('type')
        sqlManager.select('count(*)')
        self.assertEqual((('test_group_by_a', 1, 2),('test_group_by_b', 1, 1)), sqlManager.find_records())

    def test_transaction(self):
        sqlManager = self.__get_sql_manager()

        sqlManager.from_table('test')
        sqlManager.set('name', 'test_transaction')
        sqlManager.set('type', 1)
        sqlManager.create()

        sqlManager.begin_transaction()
        sqlManager.where('name', 'test_transaction')
        sqlManager.set('name', 'test_transaction_commit')
        sqlManager.update()
        sqlManager.end_transaction(True)

        sqlManager.where('name', 'test_transaction_commit')
        sqlManager.select('name')
        self.assertEqual((('test_transaction_commit',),), sqlManager.find_records())

        sqlManager.begin_transaction()
        sqlManager.where('name', 'test_transaction')
        sqlManager.set('name', 'test_transaction_rollback')
        sqlManager.update()
        sqlManager.end_transaction(False)

        sqlManager.where('name', 'test_transaction_rollback')
        sqlManager.select('name')
        self.assertEqual((), sqlManager.find_records())


    def __get_connect_mock(self) -> MagicMock:
        """
        Sqlテスト用モックを取得

        Returns
        -------
            conn_mock : MagicMock
                Sqlテスト用モック

        """
        result_mock = MagicMock()

        cur_mock = MagicMock()
        cur_mock.execute.return_value = result_mock

        conn_mock = MagicMock()
        conn_mock.cursor.return_value = cur_mock

        return conn_mock
    
    def __get_sql_manager(self) -> SqlManager:
        """
        テスト用SqlManagerを取得

        Returns
        -------
            SqlMaanger
        """

        sql_setting = {}
        with open('test_setting.json', 'r') as f:
            sql_setting = json.load(f)

        return SqlManager({
                'user' : sql_setting['user'],
                'passwd' : sql_setting['passwd'],
                'host' : sql_setting['host'],
                'db' : sql_setting['db']
            })

if __name__ == "__main__":
    unittest.main()