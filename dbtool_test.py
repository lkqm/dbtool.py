import sqlite3
import unittest

import dbtool


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


conn = sqlite3.connect(':memory:', check_same_thread=False)
conn.row_factory = dict_factory
db = dbtool.DB(None, conn=conn, conn_close=False)
db.executescript("""
    CREATE TABLE user (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT    NOT NULL,
        age  INTEGER NOT NULL
    );
    INSERT INTO user(id, name, age) values(1, 'Mario', 18);
    INSERT INTO user(id, name, age) values(2, 'Kai', 18);
""")


class TestDB(unittest.TestCase):

    def test_query_one(self):
        user = db.query_one('select * from user where id = ?', (1,))
        self.assertIsNotNone(user, "user must not be null")

    def test_query_many(self):
        users = db.query_all('select * from user')
        self.assertEqual(len(users), 2, "数据条数2条")

    def test_query_count(self):
        count = db.query_count('select * from user where id = -1')
        self.assertEqual(count, 0)
        count = db.query_count('select * from user')
        self.assertEqual(count, 2)

    def test_execute(self):
        row_id = db.execute("insert into user(name, age) values(?, ?)", ('Mou', 18))
        self.assertEqual(row_id, 3, 'insert row must be return last autogenerate id')
        rows = db.execute("update user set age = ? where id = ?", (10, row_id))
        self.assertEqual(rows, 1, 'update rows must be return effects rows')

    def test_executemany(self):
        rows = db.executemany("insert into user(name, age) values(?, ?)", [('LK', 18), ('QM', 17)])
        self.assertEqual(rows, 2, 'batch insert must be return effects rows')

    def test_executescript(self):
        db.executescript("""
            create table test(
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT    NOT NULL
            );
            drop table test;
        """)

    def test_insert(self):
        row_id = db.insert({'name': 'M', 'age': 18}, table='user')
        self.assertEqual(row_id, 3, 'insert row must be return last autogenerate id')

    def test_update(self):
        rows = db.insert({'id': 1, 'age': 10}, table='user')
        self.assertEqual(rows, 1)
        user = db.find_one('user', id=1)
        self.assertEqual(user['id'], 1)
        self.assertEqual(user['name'], 'Mario')
        self.assertEqual(user['age'], 10)

    def test_find(self):
        rows = db.find('user')
        self.assertEqual(len(rows), 2)
        rows = db.find('user', age=18)
        self.assertEqual(len(rows), 2)

    def test_find_one(self):
        row = db.find_one('user', id=1)
        self.assertIsNotNone(row)
        self.assertEqual(row.get('id'), 1)
        self.assertEqual(row.get('name'), 'Mario')
        self.assertEqual(row.get('age'), 18)
