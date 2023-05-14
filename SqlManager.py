import MySQLdb
import re
from enum import Enum
from typing import Union
from typing import Any
import datetime
from functools import singledispatchmethod

class BaseQueryType(Enum):
    SELECT = 1
    INSERT = 2
    UPDATE = 3
    DELETE = 4


class SqlManager:

    def __init__(self, settings: dict) -> None:
        """
        コンストラクタ

        Parameters
        ----------
            settings: dict
                接続情報
        """
        self.__default_setting = {
            'user': settings['user'],
            'passwd': settings['passwd'],
            'host': settings['host'],
            'db': settings['db']
        }
        self.__table = ''
        self.__where_list = []
        self.__where_condition_list = []
        self.__holder_value_list = []
        self.__select = []
        self.__insert_or_update_list = []
        self.__order_by_list = []
        self.__group_by = ''

    def __del__(self) -> None:
        """
        デストラクタ
        """
        del self.__table
        del self.__where_list
        del self.__where_condition_list
        del self.__holder_value_list
        del self.__select
        del self.__insert_or_update_list
        del self.__order_by_list
        del self.__group_by

    def from_table(self, table: str) -> 'SqlManager':
        """
        使用するテーブルを指定する

        Parameters
        ----------
        table : str
            操作するテーブル名

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """

        self.__table = table

        return self

    def where(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, '=')

        return self

    def where_in(self, column: str, value: list) -> 'SqlManager':
        """
        where句

        IN句を利用

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : list
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, 'IN')

    def where_not_in(self, column: str, value: list) -> 'SqlManager':
        """
        where句

        IN句を利用

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : list
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, 'NOT IN')

    def where_gt(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句(>)

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, '>')

    def where_gte(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句(>=)

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, '>=')

    def where_lt(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句(<)

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, '<')

    def where_lte(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句(<=)

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, '<=')

    def where_like(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句(LIKE)

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, 'LIKE')

    def where_is_null(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句(IS NULL)

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, 'IS NULL')

    def where_is_not_null(self, column: str, value: Union[int, str, datetime.date, datetime.datetime]) -> 'SqlManager':
        """
        where句(IS NOT NULL)

        Parameters
        ----------
        column : str
            where句の対象となるカラム名
        value : Union[int, str, datetime.date, datetime.datetime]
            where句で使用する値

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        self._add_wheres(column, value, 'IS NOT NULL')

    def select(self, column: str, as_column: str = None) -> 'SqlManager':
        """
        取得するカラムを指定する

        Parameters
        ----------
        column : str
            取得するカラム名
        as_column : str
            取得するカラム名の別名

        Returns
        -------
            self : SqlManager
                自身のインスタンス            
        """

        if self._is_use_aggregate_functions(column) is True:
            query_select = f"{format(column)}"
        else:
            query_select = f"`{format(column)}`"

        if as_column is not None:
            query_select += f" AS {format(as_column)}"

        self.__select.append(query_select)

        return self

    @singledispatchmethod
    def set(self, column: str, value: Any) -> 'SqlManager':
        """
        １レコードの挿入、更新

        Paramters
        ---------
            column : str
                挿入、更新するカラム
            value : Any
                挿入、更新するカラムのデータ

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        if len(self.__insert_or_update_list) > 0:
            self.__insert_or_update_list[0].update({column: value})
        else:
            self.__insert_or_update_list.append({column: value})

        return self

    @set.register
    def arg_dict_set(self, data: dict):
        """
        １レコードの挿入、更新

        TODO: 戻り値を指定するとエラーになるため指定していない

        Paramters
        ---------
            data : dict
                挿入、更新するカラム

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        if len(self.__insert_or_update_list) > 0:
            self.__insert_or_update_list[0].update(data)
        else:
            self.__insert_or_update_list.append(data)

        return self

    def sets(self, data: Any) -> 'SqlManager':
        """
        複数レコードの挿入、更新

        Paramters
        ---------
            data : Any(List[dict] or dict)
                List[dict]の場合 : [{"id" : 1, "name" : "test",...},...]
                dictの場合 : {"id" : 1, "name" : "test",...}

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        # {カラム : 値}
        if isinstance(data, dict):
            self.__insert_or_update_list.append(data)
        # [{カラム : 値}]
        elif isinstance(data[0], dict):
            self.__insert_or_update_list.extend(data)
        else:
            print("想定していないデータが設定されました。")

        return self

    def order_by_asc(self, columns: list) -> 'SqlManager':
        """
        Order句(昇順)

        Parameters
        ----------
            columns: list
                グループ対象カラム
        
        Returns
        -------
            self: SqlManager
                自身のインスタンス
        """
        self.__order_by_list.append({'order' : 'ASC', 'columns' : columns})

        return self

    def order_by_desc(self, columns: list) -> 'SqlManager':
        """
        Order句(降順)

        Parameters
        ----------
            columns: list
                グループ対象カラム
        
        Returns
        -------
            self: SqlManager
                自身のインスタンス
        """
        self.__order_by_list.append({'order' : 'DESC', 'columns' : columns})

        return self

    def group_by(self, column: Union[str, list]) -> 'SqlManager':
        """
        Group句

        Parameters
        ----------
            column: Union[str, list]
                グルーピング対象のカラム

        Returns
        -------
            self: SqlManager
                自身のインスタンス            
        """
        self.__group_by = "GROUP BY "  + column if type(column) is str else ','.join(column) + ' '

    def update(self) -> None:
        """
        データを更新する
        """
        conn = self._connect()
        cur = conn.cursor()

        query = self._query_build(BaseQueryType.UPDATE)
        if len(self.__holder_value_list) == 0:
            cur.execute(query)
        else:
            cur.execute(query, tuple(self.__holder_value_list))
            self.__holder_value_list = []
        
        conn.commit()

        cur.close
        conn.close

        del cur
        del conn

    def create(self) -> None:
        """
        レコードを作成する

        Returns
        -------
            self : SqlManager
                自身のインスタンス
        """
        conn = self._connect()
        cur = conn.cursor()

        query = self._query_build(BaseQueryType.INSERT)
        if len(self.__holder_value_list) == 0:
            cur.execute(query)
        else:
            cur.execute(query, tuple(self.__holder_value_list))
            self.__holder_value_list = []

        conn.commit()

        cur.close
        conn.close

        del cur
        del conn

        return self

    def count(self) -> int:
        """
        レコード数を取得する

        Returns
        -------
            int(rows[0][0]) : int
                レコード数
        """

        conn = self._connect()
        cur = conn.cursor()

        self.__select.append("COUNT(*)")
        query = self._query_build(BaseQueryType.SELECT)
        if len(self.__holder_value_list) == 0:
            cur.execute(query)
        else:
            cur.execute(query, tuple(self.__holder_value_list))
            self.__holder_value_list = []
        rows = cur.fetchall()

        cur.close
        conn.close

        del cur
        del conn

        return int(rows[0][0])

    def delete(self) -> None:
        """
        レコードを削除する
        """

        conn = self._connect()
        cur = conn.cursor()

        query = self._query_build(BaseQueryType.DELETE)
        if len(self.__holder_value_list) == 0:
            cur.execute(query)
        else:
            cur.execute(query, tuple(self.__holder_value_list))
            self.__holder_value_list = []

        cur.close
        conn.close

        del cur
        del conn
    
    def find_records(self, is_dict_cursor:bool = False) -> Any:
        """
        複数データを取得する

        Paramters
        ---------
            is_dict_cursor : bool
                Dict形式で取得するかどうか(Falseの場合はlist形式)

        Returns
        -------
            is_dict_cursor が Falseの場合 [[1, 2,...],...]

            is_dict_cursor が Trueの場合  [{'key1' : 1, 'key2' : 2, ...},...]
        """

        conn = self._connect()
        cur = conn.cursor(MySQLdb.cursors.DictCursor) if is_dict_cursor else conn.cursor()

        query = self._query_build(BaseQueryType.SELECT)
        if len(self.__holder_value_list) == 0:
            cur.execute(query)
        else:
            cur.execute(query, tuple(self.__holder_value_list))
            self.__holder_value_list = []

        rows = cur.fetchall()

        cur.close
        conn.close

        del cur
        del conn

        return rows

    def _query_where_build(self) -> str:
        """
        where句のクエリを作成する

        Returns
        -------
            str : query
                作成したクエリ            
        """
        query = ""

        if (len(self.__where_list) == 0):
            return query

        wheres = []
        for i, where in enumerate(self.__where_list):

            where_condition = self.__where_condition_list[i]
            for column in where.keys():
                value = where[column]
                condition = where_condition[column]

                if 'IN' in condition:
                    in_wheres = []
                    for datum in value:
                        if type(datum) is int:
                            in_wheres.append(datum)
                        else:
                            #in_wheres.append("\'{}\'".format(datum))
                            in_wheres.append(datum)
                    # joinは文字列配列のみ対応しているため、文字列以外がくる場合は下記のように対応する必要がある。
                    #wheres.append(
                    #    f"`{column}` {condition} (" + ', '.join([str(in_value) for in_value in in_wheres]) + ")")
                        wheres.append(
                        f"`{column}` {condition} (" + ', '.join(["%s"] * len(in_wheres)) + ")")
                    self.__holder_value_list.extend(in_wheres)
                else:
                    # >, >=, <, <=, LIKE, IS NULL, IS NOT NULL
                    if type(value) is int:
                        #wheres.append(f"`{column}` {condition} {value}")
                        wheres.append(f"`{column}` {condition} %s")
                        self.__holder_value_list.append(f"{value}")
                    else:
                        #wheres.append(f"`{column}` {condition} \'{value}\'")
                        wheres.append(f"`{column}` {condition} %s")
                        self.__holder_value_list.append(value)


        query = ' WHERE ' + ' AND '.join(wheres)

        return query

    def _query_insert_build(self) -> str:
        """
        Insert句のクエリを作成する

        Returns
        -------
            str : query
                作成したクエリ            
        """
        query = ""

        insert_or_update_list = self.__insert_or_update_list

        columns = [f"`{miexed}`" for miexed in insert_or_update_list[0]]

        query += f"({','.join(columns)}) VALUES"

        multiple_insert_list = []
        for insert_or_update in insert_or_update_list:
            insert_list = []
            for key in insert_or_update.keys():
                column = key
                value = insert_or_update[column]

                if type(value) is int:
                    insert_list.append(value)
                else:
                    insert_list.append(f"\'{value}\'")
            multiple_insert_list.append(
                "(" + ','.join([str(in_value) for in_value in insert_list]) + ")")
        query += ",".join(multiple_insert_list)

        return query

    def _query_update_build(self) -> str:
        """
        Update句のクエリを作成する

        Returns
        -------
            str : query
                作成したクエリ            
        """
        query = ""

        update_list = []

        # updateで複数行のデータをそれぞれ更新することはできないので、
        # 配列データが格納されているが１つしか存在しない
        insert_or_update = self.__insert_or_update_list[0]

        for key in insert_or_update.keys():
            column = key
            value = insert_or_update[column]

            if type(value) is int:
                update_list.append(f"`{column}` = {value}")
            else:
                update_list.append(f"`{column}` = \"{value}\"")

        query += ",".join(update_list)

        return query

    def _query_order_build(self) -> str:
        """
        Order句のクエリを作成する

        Returns
        -------
            query: str
                order句のクエリ
        """
        if len(self.__order_by_list) == 0:
            return ''
        
        query = ''
        for order_by in self.__order_by_list:
            order = order_by['order']
            columns = order_by['columns']

            query += ' ORDER BY ' + ','.join(columns) + f" {order} "
        
        return query

    def _query_build(self, baseQueryType: BaseQueryType) -> str:
        """
        クエリを組み立てる

        Returns
        -------
            str : query
                作成したクエリ          
        """

        query = ""

        if baseQueryType == BaseQueryType.SELECT:
            if len(self.__select) == 0:
                self.__select.append("*")
            query = "SELECT {} FROM {}".format(
                ",".join(self.__select), self.__table)
            query += self._query_where_build()
            query += self._query_order_build()
            query += self.__group_by

        elif baseQueryType == BaseQueryType.INSERT:
            query = f"INSERT INTO {self.__table}"
            query += self._query_insert_build()
    
        elif baseQueryType == BaseQueryType.DELETE:
            query = f"DELETE FROM {self.__table}"
            query += self._query_where_build()

        elif baseQueryType == BaseQueryType.UPDATE:
            query = f"UPDATE {self.__table} SET "
            query += self._query_update_build()
            query += self._query_where_build()
        else:
            print(
                f"指定したクエリタイプは対応されていません。 base_query_type = {type(self.__table)}")

        self.__table = ''
        self.__where_list = []
        self.__where_condition_list = []
        self.__select = []
        self.__insert_or_update_list = []
        self.__order_by_list = []
        self.__group_by = ''

        return query

    def _connect(self) -> MySQLdb:
        """"
        MySQLに接続する

        Returns
        -------
            MySQLdb : MySQLdb.connect
                MySQLdbインスタンス
        """
        setting = self.__default_setting

        return MySQLdb.connect(
            user=setting['user'],
            passwd=setting['passwd'],
            host=setting['host'],
            db=setting['db']
        )

    def _add_wheres(self, column: str, value: Any, condtion: str) -> None:
        """
        whereリストに値、カラム、条件を追加する

        Parameters
        ---------
            str: columns
                カラム名
            Any: value
                値
            str: condition
                条件
        """
        self.__where_list.append({column: value})
        self.__where_condition_list.append({column: condtion})

    def _is_use_aggregate_functions(self, column: str) -> bool:
        """
        集約関数が利用されているかを判別する

        Paramters
        ---------
            column: str 
                select関数で使用するカラム名

        Returns
        -------
            is_mathc: bool
                利用しているかどうか
        """
        pattern_list = [
            'AVG',
            'BIT_AND',
            'BIT_OR',
            'BIT_XOR',
            'COUNT',
            'GROUP_CONCAT',
            'JSON_ARRAYAGG',
            'JSON_OBJECTAGG',
            'MAX',
            'MIN',
            'STD',
            'STDDEV',
            'STDDEV_POP',
            'STDDEV_POP',
            'SUM',
            'VAR_POP',
            'VAR_SAMP',
            'VARIANCE'
        ]

        is_match = False
        for pattern in pattern_list:
            pattern = f"[a-zA-Z0-9_]?{pattern}\(.*\)[a-zA-Z0-9_]?"
            if re.search(pattern, column, re.IGNORECASE) != None:
                is_match = True
                break

        return is_match
