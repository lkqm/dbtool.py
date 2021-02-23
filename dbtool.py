# 数据库访问工具类
import functools
import logging

log = logging.getLogger('db2ls')


class DB:

    def __init__(self, datasource, conn=None, replacer='?', conn_close=True, cursor_type=None):
        self.datasource = datasource
        self.replacer = replacer
        self.conn = conn
        self.conn_close = conn_close
        self.cursor_type = cursor_type

    # 打开一个连接
    def connection(self):
        if self.conn:
            return self.conn
        conn = self.datasource.connection()
        if self.cursor_type:
            conn.cursor = functools.partial(conn.cursor, self.cursor_type)
        return conn

    # 关闭指定连接
    def close_connection(self, conn):
        if conn and self.conn_close:
            conn.close()

    # 执行查询sql, 返回单行数据.
    def query_one(self, sql, args=()):
        cursor = self.query_cursor(sql, args)
        conn = cursor.connection
        try:
            return cursor.fetchone()
        finally:
            cursor.close()
            self.close_connection(conn)

    # 执行查询sql, 返回所有数据.
    def query_all(self, sql, args=()):
        cursor = self.query_cursor(sql, args)
        conn = cursor.connection
        try:
            return cursor.fetchall()
        finally:
            cursor.close()
            self.close_connection(conn)

    # 执行统计sql, 返回查询条数.
    def query_count(self, sql, args=()):
        count_sql = f"select count(*) from ({sql}) t"
        return self.query_one(count_sql, args)[0]

    # 执行统计sql, 返回游标.
    def query_cursor(self, sql, args=()):
        conn = self.connection()
        cursor = None
        try:
            cursor = conn.cursor()
            log.debug('sql=[ %s ], args=[ %s ]', sql, args)
            cursor.execute(sql, tuple(args))
            conn.commit()
            return cursor
        except Exception as e:
            if cursor:
                cursor.close()
            self.close_connection(conn)
            raise e

    # 执行sql
    def execute(self, sql, args=()):
        conn = self.connection()
        cursor = None
        try:
            cursor = conn.cursor()
            log.debug('sql=[ %s ], args=[ %s ]', sql, args)
            cursor.execute(sql, tuple(args))
            conn.commit()
            if sql.upper().startswith('INSERT INTO'):
                return cursor.lastrowid
            else:
                return cursor.rowcount
        finally:
            if cursor:
                cursor.close()
            self.close_connection(conn)

    # 批量执行sql
    def executemany(self, sql, args=()):
        conn = self.connection()
        cursor = None
        try:
            cursor = conn.cursor()
            log.debug('sql=[ %s ], args=[ %s ]', sql, args)
            cursor.executemany(sql, tuple(args))
            conn.commit()
            return cursor.rowcount
        finally:
            if cursor:
                cursor.close()
            self.close_connection(conn)

    # 执行sql脚本, 脚本中多条以符号;分割
    def executescript(self, sql_script):
        conn = self.connection()
        cursor = None
        try:
            cursor = conn.cursor()
            log.debug('sql=[ %s ]', sql_script)
            cursor.executescript(sql_script)
            conn.commit()
        finally:
            if cursor:
                cursor.close()
            self.close_connection(conn)

    # 执行sql脚本文件
    def executescript_file(self, file):
        with open(file, 'r', encoding='utf-8') as f:
            script = f.read()
        return self.executescript(script)

    # 插入单条数据
    def insert(self, data, table=None):
        k_snippet = ', '.join(data.keys())
        v_snippet = ', '.join([self.replacer] * len(data.keys()))
        sql = f'insert into {table}({k_snippet}) values({v_snippet})'
        values = data.values()
        return self.execute(sql, values)

    # 删除单条数据，依据主键.
    def delete_by_id(self, id_val, table=None, id_name='id'):
        sql = f'delete from {table} where {id_name} = {self.replacer}'
        values = (id_val,)
        return self.execute(sql, values)

    # 修改单条数据.
    def update(self, data, table=None, id_name='id'):
        d = self.__filter_dict(data, excludes=(id_name,))
        id_value = data[id_name]

        set_snippet = ', '.join(list(map(lambda k: k + '=' + self.replacer, d.keys())))
        sql = f'update {table} set {set_snippet} where id = ?'
        values = (*d.values(), id_value)
        return self.execute(sql, values)

    # 修改数据, 以累加方式
    def increment(self, data, table=None, id_name='id'):
        d = self.__filter_dict(data, excludes=(id_name,))
        id_value = data[id_name]

        set_snippet = ', '.join(list(map(lambda k: k + '=' + k + '+' + self.replacer, d.keys())))
        sql = f'update {table} set {set_snippet} where id = ?'
        values = (*d.values(), id_value)
        return self.execute(sql, values)

    # 统计数据条数.
    def count(self, data, table=None):
        where_snippet = ''
        if data:
            where_snippet = 'where ' + ', '.join(list(map(lambda k: k + '=' + self.replacer, data.keys())))
        sql = f'select count(*) from {table} {where_snippet}'
        values = data.values()
        return self.query_count(sql, values)

    # 查询单条数据, 根据主键.
    def find_by_id(self, id_val, table=None, id_name='id'):
        sql = f'select * {table} where {id_name} = {self.replacer}'
        values = (id_val,)
        return self.query_one(sql, values)

    # 查询所有数据
    def find(self, table, **keys):
        where = ' and '.join(list(map(lambda k: k + '=' + self.replacer, keys.keys())))
        if where:
            where = "where " + where
        sql = f'select * from {table} {where}'
        return self.query_all(sql, keys.values())

    # 查询单条数据
    def find_one(self, table, **keys):
        where = ' and '.join(list(map(lambda k: k + '=' + self.replacer, keys.keys())))
        if where:
            where = "where " + where
        sql = f'select * from {table} {where}'
        return self.query_one(sql, keys.values())

    def __filter_dict(self, data, includes=(), excludes=()):
        if includes:
            return {k: v for k, v in data.items() if k in includes}
        elif excludes:
            return {k: v for k, v in data.items() if k not in excludes}
        else:
            return dict(data)
