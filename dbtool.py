import functools
import logging

log = logging.getLogger('dbtool')


class DB:

    def __init__(self, datasource, conn=None, replacer='?', conn_close=True, cursor_type=None):
        self.datasource = datasource
        self.replacer = replacer
        self.conn = conn
        self.conn_close = conn_close
        self.cursor_type = cursor_type

    def connection(self):
        """open a connection."""
        if self.conn:
            return self.conn
        conn = self.datasource.connection()
        if self.cursor_type:
            conn.cursor = functools.partial(conn.cursor, self.cursor_type)
        return conn

    def close_connection(self, conn):
        """close a connection."""
        if conn and self.conn_close:
            conn.close()

    def execute(self, sql, args=(), fetchone=False, return_cursor=False, executemany=False, executescript=False):
        """execute sql, like select, insert, update statement."""
        conn, cursor = self.connection(), None
        try:
            log.debug('sql=[ %s ], args=[ %s ]', sql, args)
            cursor = conn.cursor()
            if executescript:
                if cursor.executescript:
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
            if is_select:
                conn.commit()
            # returns
            if fetchone:
                return cursor.fetchone()
            elif return_cursor:
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
            self.close_connection(conn)

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

    def insert(self, data, table=None):
        """ insert one row.
        :param data: the data of row
        :param table: the db table name
        :return: returns autogenerate id
        """
        k_snippet = ', '.join(data.keys())
        v_snippet = ', '.join([self.replacer] * len(data.keys()))
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

        set_snippet = ', '.join(list(map(lambda k: k + '=' + self.replacer, d.keys())))
        sql = f'UPDATE {table} SET {set_snippet} WHERE {id_name} = ?'
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

        set_snippet = ', '.join(list(map(lambda k: k + '=' + k + '+' + self.replacer, d.keys())))
        sql = f'UPDATE {table} SET {set_snippet} WHERE {id_name} = ?'
        values = (*d.values(), id_value)
        return self.execute(sql, values)

    def delete_by_id(self, id_val, table=None, id_name='id'):
        """delete rows by id."""
        sql = f'DELETE FROM {table} where {id_name} = {self.replacer}'
        values = (id_val,)
        return self.execute(sql, values)

    def find_by_id(self, id_val, table=None, id_name='id'):
        """find one row by id"""
        sql = f'SELECT * FROM {table} WHERE {id_name} = {self.replacer}'
        values = (id_val,)
        return self.execute_fetchone(sql, values)

    def find(self, table, **keys):
        """find rows by query."""
        where = self.__build_where_snippet(keys)
        sql = f'SELECT * FROM {table} {where}'
        return self.execute(sql, keys.values())

    def __build_where_snippet(self, keys):
        snippet = ' AND '.join(list(map(lambda k: k + '=' + self.replacer, keys.keys())))
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
