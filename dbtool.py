import functools
import importlib
import logging
import threading

from dbutils.pooled_db import PooledDB

log = logging.getLogger('dbtool')


class DB:

    def __init__(self, dbtype, dt_row=None, dt_handle_replacer=True, **keys):
        """ Init DB.
        :param dbtype: supports dbtype like sqlite, mysql,...
        :param dt_handle_replacer: handle sql replacer
        :param keys: dbutils.pooled_db.PooledDB args, driver engine args
        """
        creator, replacer, row_factory, handle_replacer = self._resolve_dbtype(dbtype, dt_row, dt_handle_replacer)
        self._datasource = PooledDB(creator, **keys)
        self._dbtype = dbtype
        self._handle_replacer = dt_handle_replacer
        self._replacer = replacer
        self._row_factory = row_factory
        self.__transaction_ctx = _TransactionCtx()

    @staticmethod
    def _resolve_dbtype(dbtype, row_factory, handle_replacer):
        if dbtype == 'sqlite':
            import sqlite3
            creator = sqlite3
            replacer = '?'
            handle_replacer = False
            row_factory = row_factory or DB.__dict_factory
            creator.connect = functools.partial(DB.__sqlite3_connect, connect=creator.connect, row_factory=row_factory)
        elif dbtype == 'mysql':
            import pymysql
            creator = pymysql
            replacer = '%s'
            row_factory = row_factory or creator.cursors.DictCursor
        elif dbtype == 'postgresql':
            import psycopg2
            creator = psycopg2
            replacer = '%s'
            psycopg2_extras = importlib.import_module('psycopg2.extras')
            row_factory = row_factory or psycopg2_extras.RealDictCursor
        elif dbtype == 'sqlserver':
            import pymssql
            creator = pymssql
            row_factory = row_factory or True
            replacer = '%s'
        else:
            raise BaseException('unknown db type')
        return creator, replacer, row_factory, handle_replacer

    def __connection(self):
        """open a connection."""
        if self.__transaction_ctx.conn:
            return self.__transaction_ctx.conn
        conn = self._datasource.connection()
        if self._row_factory:
            if self._dbtype == 'mysql':
                conn.cursor = functools.partial(conn.cursor, self._row_factory)
            elif self._dbtype == 'postgresql':
                conn.cursor = functools.partial(conn.cursor, cursor_factory=self._row_factory)
            elif self._dbtype == 'sqlserver':
                conn.cursor = functools.partial(conn.cursor, as_dict=True)
        self.__transaction_ctx.init_conn_maybe(conn)
        return conn

    def transaction(self, func=None):
        if func is None:
            return self.__transaction_ctx

        def wrapper(*args, **kw):
            with self.__transaction_ctx:
                return func(*args, **kw)

        return wrapper

    def execute(self, sql, args=(), fetchone=False, return_cursor=False, executemany=False, executescript=False):
        """execute sql, like select, insert, update, delete, ... statement."""
        conn, cursor = self.__connection(), None
        if self._handle_replacer:
            sql = self.__handle_replacer(sql)
        try:
            log.debug('sql=[ %s ], args=[ %s ]', sql, args)
            cursor = conn.cursor()
            if executescript:
                if self._dbtype == 'sqlite':
                    cursor.executescript(sql)
                else:
                    cursor.execute(sql, tuple(args))
            elif executemany:
                cursor.executemany(sql, tuple(args))
            else:
                cursor.execute(sql, tuple(args))
            sql_type = DB.__extract_sql_type(sql)
            is_select = not executemany and sql_type == 'SELECT'
            is_insert = not executemany and sql_type == 'INSERT'
            if not is_select and not conn._transaction:
                conn.commit()
            # returns
            if fetchone:
                return cursor.fetchone()
            elif return_cursor:
                cursor._dbutils_connection = conn
                return_cursor = True
                return cursor
            elif is_select:
                return cursor.fetchall()
            elif is_insert:
                return cursor.lastrowid
            else:
                return cursor.rowcount
        finally:
            if cursor and not return_cursor:
                cursor.close()
            if not return_cursor:
                self.__close_connection(conn)

    def execute_fetchone(self, sql, args=()):
        """execute sql, returns one row."""
        return self.execute(sql, args, fetchone=True)

    def execute_count(self, sql, args=()):
        """ execute sql, returns rows counts."""
        count_sql = f"SELECT count(*) total FROM ({sql}) t"
        row = self.execute_fetchone(count_sql, args)
        if type(row) == list or type(row) == tuple:
            return row[0]
        else:
            return row['total']

    def execute_cursor(self, sql, args=()):
        """execute sql, returns cursor."""
        return self.execute(sql, args, return_cursor=True)

    def execute_many(self, sql, args=()):
        """execute sql, like insert, update statement with many args."""
        return self.execute(sql, args, executemany=True)

    def execute_script(self, sql):
        """execute multiples sql, split with semicolon."""
        return self.execute(sql, executescript=True)

    def execute_file(self, file, encoding='utf-8'):
        """execute sql file."""
        with open(file, 'r', encoding=encoding) as f:
            sql = f.read()
            return self.execute_script(sql)

    def close_cursor(self, cursor):
        cursor.close()
        if cursor._dbutils_connection:
            self.__close_connection(cursor._dbutils_connection)

    def __handle_replacer(self, sql):
        """Replace sql replacer ? with driver replacer character."""
        chars = []
        for c in sql:
            if c != '?':
                chars.append(c)
            else:
                chars.append(self._replacer)
        return ''.join(chars)

    @staticmethod
    def __close_connection(conn):
        """close a connection."""
        if conn and not conn._transaction:
            conn.close()

    @staticmethod
    def __extract_sql_type(sql):
        sql_upper = sql.lstrip().upper()
        if sql_upper.startswith('SELECT'):
            return 'SELECT'
        elif sql_upper.startswith('INSERT'):
            return 'INSERT'
        elif sql_upper.startswith('DELETE'):
            return 'DELETE'
        elif sql_upper.startswith('UPDATE'):
            return 'UPDATE'

    def __dict_factory(cursor, row):
        """a row factory to dict."""
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __sqlite3_connect(connect=None, row_factory=None, **keys):
        """wrap sqlite3 connect method for set row_factory"""
        conn = connect(**keys)
        conn.row_factory = row_factory
        return conn

    # ------------------ CRUD ------------------#

    def insert(self, data, table=None):
        """ insert one row.
        :param data: the data of row
        :param table: the db table name
        :return: returns autogenerate id
        """
        k_snippet = ', '.join(data.keys())
        v_snippet = ', '.join([self._replacer] * len(data.keys()))
        sql = f'INSERT INTO {table}({k_snippet}) VALUES({v_snippet})'
        values = data.values()
        return self.execute(sql, values)

    def update(self, data, table=None, id_name='id'):
        """ update one row.
        :param data: the data
        :param table: the db table name
        :param id_name: the primary column name
        :return: returns effective rows counts
        """
        d = DB.__filter_dict(data, excludes=(id_name,))
        id_value = data[id_name]

        set_snippet = ', '.join(list(map(lambda k: k + '=' + self._replacer, d.keys())))
        sql = f'UPDATE {table} SET {set_snippet} WHERE {id_name} = {self._replacer}'
        values = (*d.values(), id_value)
        return self.execute(sql, values)

    def increment(self, data, table=None, id_name='id'):
        """ update rows for increment.
        :param data: the data
        :param table: the db table name
        :param id_name: the primary column name
        :return: returns effective rows counts.
        """
        d = DB.__filter_dict(data, excludes=(id_name,))
        id_value = data[id_name]

        set_snippet = ', '.join(list(map(lambda k: k + '=' + k + '+' + self._replacer, d.keys())))
        sql = f'UPDATE {table} SET {set_snippet} WHERE {id_name} = {self._replacer}'
        values = (*d.values(), id_value)
        return self.execute(sql, values)

    def delete_by_id(self, id_val, table=None, id_name='id'):
        """delete rows by id."""
        sql = f'DELETE FROM {table} where {id_name} = {self._replacer}'
        values = (id_val,)
        return self.execute(sql, values)

    def find_by_id(self, id_val, table=None, id_name='id'):
        """find one row by id"""
        sql = f'SELECT * FROM {table} WHERE {id_name} = {self._replacer}'
        values = (id_val,)
        return self.execute_fetchone(sql, values)

    def find(self, table, **keys):
        """find rows by query."""
        where = self.__build_where_snippet(keys)
        sql = f'SELECT * FROM {table} {where}'
        return self.execute(sql, keys.values())

    def __build_where_snippet(self, keys):
        snippet = ' AND '.join(list(map(lambda k: k + '=' + self._replacer, keys.keys())))
        if snippet:
            snippet = 'WHERE ' + snippet
        return snippet

    def find_one(self, table, **keys):
        """find one row by query."""
        where = self.__build_where_snippet(keys)
        sql = f'SELECT * FROM {table} {where}'
        return self.execute_fetchone(sql, keys.values())

    def find_count(self, table, **keys):
        """count rows by query."""
        where = self.__build_where_snippet(keys)
        sql = f'SELECT count(*) total FROM {table} {where}'
        row = self.execute_fetchone(sql, keys.values())
        if type(row) == list or type(row) == tuple:
            return row[0]
        else:
            return row['total']

    @staticmethod
    def __filter_dict(data, includes=(), excludes=()):
        if includes:
            return {k: v for k, v in data.items() if k in includes}
        elif excludes:
            return {k: v for k, v in data.items() if k not in excludes}
        else:
            return dict(data)


class _TransactionCtx(threading.local):

    def __init__(self):
        self.need_transaction = False
        self.conn = None
        self.transactions = 0

    def __enter__(self):
        self.need_transaction = True
        self.transactions = self.transactions + 1
        return self

    def __exit__(self, exctype, excvalue, traceback):
        self.transactions = self.transactions - 1
        try:
            if self.transactions == 0:
                if exctype is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
        finally:
            if self.transactions == 0:
                self.conn.close()
                self.conn = None

    def init_conn_maybe(self, conn):
        if self.need_transaction and self.conn is None:
            self.conn = conn
            self.conn.begin()
