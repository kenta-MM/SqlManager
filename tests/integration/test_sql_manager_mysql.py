import unittest
from sql_manager import SqlManager

class TestSqlManagerMySQL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.manager = SqlManager({
            'user': 'root',
            'passwd': 'root',
            'host': '127.0.0.1',
            'db': 'test_db',
            'driver': 'pymysql'
        })

        cls.manager.raw("""
        CREATE TABLE users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50),
            age INT
        )
        """)

    def tearDown(self):
        self.manager.raw("DELETE FROM users")

    def test_insert_and_select(self):
        self.manager.from_table('users') \
            .set('name', 'Alice') \
            .set('age', 30) \
            .create()

        rows = self.manager.from_table('users') \
            .select('name') \
            .find_records()

        self.assertEqual(len(rows), 1)
