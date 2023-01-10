import functools
import importlib
import re
import threading
from urllib.parse import urlparse

from dbutils.pooled_db import PooledDB


class DB:

    def __init__(self, url, row_factory=None, handle_placeholder=True, mincached=0, maxconnections=0, blocking=False,
                 **options):
        """ Init DB.
        :param handle_placeholder: handle sql placeholder
        :param mincached: initial number of idle connections in the pool
            (0 means no connections are made at startup)
        :param maxconnections: maximum number of connections generally allowed
            (0 or None means an arbitrary number of connections)
        :param blocking: determines behavior when exceeding the maximum
            (if this is set to true, block and wait until the number of
            connections decreases, otherwise an error will be reported)
        :param options: dbms driver connect parameter, see sqlite3, pymysql, ...
        """
        database_url = _DatabaseUrl(url)
        creator, placeholder, row_factory, handle_placeholder, kwargs = self._resolve_dbms(database_url, row_factory,
                                                                                           handle_placeholder, options)
        self._datasource = PooledDB(creator, mincached=mincached, maxconnections=maxconnections, blocking=blocking,
                                    **kwargs)
        self._dbms = database_url.dbms
        self._handle_placeholder = handle_placeholder
        self._placeholder = placeholder
        self._row_factory = row_factory
        self.__transaction_ctx = _TransactionCtx()

    @staticmethod
    def _resolve_dbms(database_url, row_factory, handle_placeholder, options):
        dbms = database_url.dbms
        connect_parameters = options.copy()
        connect_parameters['host'] = database_url.host
        connect_parameters['port'] = database_url.port
        connect_parameters['user'] = database_url.username
        connect_parameters['password'] = database_url.password
        connect_parameters['database'] = database_url.database
        if dbms == 'sqlite':
            import sqlite3
            creator = sqlite3
            placeholder = '?'
            handle_placeholder = False
            row_factory = row_factory or DB.__dict_factory
            creator.connect = functools.partial(DB.__sqlite3_connect, connect=creator.connect, row_factory=row_factory)
            for key in ['host', 'port', 'user', 'password']:
                del connect_parameters[key]
        elif dbms == 'mysql':
            import pymysql
            creator = pymysql
            placeholder = '%s'
            row_factory = row_factory or creator.cursors.DictCursor
        elif dbms == 'postgresql':
            import psycopg2
            creator = psycopg2
            placeholder = '%s'
            psycopg2_extras = importlib.import_module('psycopg2.extras')
            row_factory = row_factory or psycopg2_extras.RealDictCursor
        elif dbms == 'sqlserver':
            import pymssql
            creator = pymssql
            row_factory = row_factory or True
            placeholder = '%s'
        else:
            raise Exception('unsupported dbms:' + dbms)
        return creator, placeholder, row_factory, handle_placeholder, connect_parameters

    def __connection(self):
        """open a connection."""
        if self.__transaction_ctx.conn:
            return self.__transaction_ctx.conn
        conn = self._datasource.connection()
        if self._row_factory:
            if self._dbms == 'mysql':
                conn.cursor = functools.partial(conn.cursor, self._row_factory)
            elif self._dbms == 'postgresql':
                conn.cursor = functools.partial(conn.cursor, cursor_factory=self._row_factory)
            elif self._dbms == 'sqlserver':
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

    def execute(self, sql, args=()):
        """execute sql, like select, insert, update, delete, ... statement."""
        return self._execute(sql, args)

    def _execute(self, sql, args=(), fetchone=False, return_cursor=False, batch=False, script=False,
                 handel_placeholder=None):
        handel_placeholder = handel_placeholder if handel_placeholder is not None else self._handle_placeholder
        sql = sql if not handel_placeholder else self.__handle_replacer(sql)
        conn, cursor = self.__connection(), None
        try:
            cursor = conn.cursor()
            if script and self._dbms == 'sqlite':
                cursor.executescript(sql)
            elif batch:
                cursor.executemany(sql, tuple(args))
            else:
                cursor.execute(sql, tuple(args))
            statement = _SqlStatement(sql)
            if not statement.is_select and not conn._transaction:
                conn.commit()

            if script:
                return None
            elif return_cursor:
                cursor._dbutils_connection = conn
                return_cursor = True
                return cursor
            elif fetchone:
                return cursor.fetchone()
            elif statement.is_select:
                return cursor.fetchall()
            elif not batch and statement.is_lastrowid:
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
        return self._execute(sql, args, fetchone=True)

    def execute_cursor(self, sql, args=()):
        """execute sql, returns cursor."""
        return self._execute(sql, args, return_cursor=True)

    def execute_batch(self, sql, args=()):
        """execute sql, like insert, update statement with many args."""
        return self._execute(sql, args, batch=True)

    def execute_script(self, sql):
        """execute multiples sql, split with semicolon."""
        return self._execute(sql, script=True)

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
                chars.append(self._placeholder)
        return ''.join(chars)

    @staticmethod
    def __close_connection(conn):
        """close a connection."""
        if conn and not conn._transaction:
            conn.close()

    @staticmethod
    def __dict_factory(cursor, row):
        """a row factory to dict."""
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    @staticmethod
    def __sqlite3_connect(connect=None, row_factory=None, **kwargs):
        """wrap sqlite3 connect method for set row_factory"""
        conn = connect(**kwargs)
        conn.row_factory = row_factory
        return conn

    # ------------------ CRUD ------------------#

    def insert(self, data, table=None):
        """ insert one row.
        :return: returns autogenerate id
        """
        table_name = self.__table_name(type(data), table)
        data_dict = self.__data2dict(data)
        k_snippet = ', '.join(data_dict.keys())
        v_snippet = ', '.join([self._placeholder] * len(data_dict.keys()))
        sql = f'INSERT INTO {table_name}({k_snippet}) VALUES({v_snippet})'
        args = data_dict.values()
        return self._execute(sql, args, handel_placeholder=False)

    def update(self, data, table=None, id_column='id'):
        """ update one row.
        :param data: the data of row, allow types dict or object instant
        :param table: the table name or entity class
        :param id_column: the primary column name
        :return: returns effective rows counts
        """
        table_name = self.__table_name(type(data), table)
        data_dict = self.__data2dict(data)
        d = DB.__filter_dict(data_dict, excludes=(id_column,))
        id_value = data_dict[id_column]

        set_snippet = ', '.join(list(map(lambda k: k + '=' + self._placeholder, d.keys())))
        sql = f'UPDATE {table_name} SET {set_snippet} WHERE {id_column} = {self._placeholder}'
        args = (*d.values(), id_value)
        return self._execute(sql, args, handel_placeholder=False)

    def delete(self, table, filters):
        """delete rows by id.
        :param table: the table name or entity class
        :param filters: the query conditions
        :return: returns effective rows counts
        """
        table_name = self.__table_name(None, table)
        where = self.__build_where_snippet(filters)
        sql = f'DELETE FROM {table_name} {where}'
        args = filters.values()
        return self._execute(sql, args, handel_placeholder=False)

    def find(self, table, filters={}, return_type=None):
        """find rows by query.
        :param table: the table name or entity class
        :param filters: the query conditions
        :param return_type: the return rows type
        """
        table_name = self.__table_name(None, table)
        return_type = self.__return_type(table, return_type)
        where = self.__build_where_snippet(filters)
        sql = f'SELECT * FROM {table_name} {where}'
        args = filters.values()
        rows = self._execute(sql, args, handel_placeholder=False)
        return rows if return_type is None else [self.__create_object(row, return_type) for row in rows]

    def __build_where_snippet(self, keys):
        snippet = ' AND '.join(list(map(lambda k: k + '=' + self._placeholder, keys.keys())))
        if snippet:
            snippet = 'WHERE ' + snippet
        return snippet

    def find_one(self, table, filters, return_type=None):
        """find one row by query.
        :param table: the table name or entity class
        :param filters: the query conditions
        :param return_type: the return rows type
        """
        table_name = self.__table_name(None, table)
        return_type = self.__return_type(table, return_type)
        where = self.__build_where_snippet(filters)
        sql = f'SELECT * FROM {table_name} {where}'
        args = filters.values()
        row = self._execute(sql, args, fetchone=True, handel_placeholder=False)
        return row if return_type is None else self.__create_object(row, return_type)

    def find_count(self, filters={}, table=None):
        """count rows by query.
        :param filters: the query conditions
        :param table: the table name or entity class
        :returns count result of type int
        """
        table_name = self.__table_name(None, table)
        where = self.__build_where_snippet(filters)
        sql = f'SELECT count(*) total FROM {table_name} {where}'
        args = filters.values()
        row = self._execute(sql, args, fetchone=True, handel_placeholder=False)
        if type(row) == list or type(row) == tuple:
            return row[0]
        else:
            return row['total']

    def __table_name(self, data_type, table):
        if type(table) is str:
            return table
        else:
            clazz = table if table else data_type
            # Specific field： TABLE_NAME
            if clazz is not None:
                if hasattr(clazz, 'TABLE_NAME'):
                    val = getattr(clazz, 'TABLE_NAME')
                    if val:
                        return val
            # Class name
            return self.__camel2snake(clazz.__name__)

    def __return_type(self, table, return_type):
        if return_type is not None:
            return return_type if return_type != dict else None
        elif type(table) != str:
            return table
        return None

    def __data2dict(self, data):
        # TODO
        return data if type(data) == dict else vars(data)

    @staticmethod
    def __filter_dict(data, includes=(), excludes=()):
        if includes:
            return {k: v for k, v in data.items() if k in includes}
        elif excludes:
            return {k: v for k, v in data.items() if k not in excludes}
        else:
            return dict(data)

    @staticmethod
    def __create_object(row, return_class):
        if row is None:
            return None
        obj = return_class()
        for item in row.items():
            setattr(obj, item[0], item[1])
        return obj

    __camel_pattern = re.compile(r'(?<!^)(?=[A-Z])')

    @staticmethod
    def __camel2snake(name):
        return DB.__camel_pattern.sub('_', name).lower()


class _TransactionCtx(threading.local):
    """the transaction context"""

    def __init__(self):
        self.transaction = False
        self.conn = None
        self.transactions = 0

    def __enter__(self):
        self.transaction = True
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
        if self.transaction and self.conn is None:
            self.conn = conn
            self.conn.begin()


class _DatabaseUrl:
    """represent database connect url"""

    def __init__(self, url):
        info = urlparse(url)
        self.dbms = info.scheme
        self.host = info.hostname
        self.database = info.path[1:] if len(info.path) > 0 else None
        self.username = info.username
        self.password = info.password
        self.port = info.port


class _SqlStatement:
    """represent sql statement"""

    def __init__(self, sql):
        self.sql = sql.lstrip()
        self.type = None
        self.is_select = self.is_insert = self.is_replace = self.is_delete = self.is_update = self.is_lastrowid = False
        self.__init_type(self.sql)

    def __init_type(self, sql):
        sql = sql[:10].upper()
        if sql.startswith('SELECT'):
            self.type = 'SELECT'
            self.is_select = True
        elif sql.startswith('INSERT'):
            self.type = 'INSERT'
            self.is_insert = True
            self.is_lastrowid = True
        elif sql.startswith('REPLACE'):
            self.type = 'REPLACE'
            self.is_replace = True
            self.is_lastrowid = True
        elif sql.startswith('DELETE'):
            self.type = 'DELETE'
            self.is_delete = True
        elif sql.startswith('UPDATE'):
            self.type = 'UPDATE'
            self.is_update = True


connect = DB
