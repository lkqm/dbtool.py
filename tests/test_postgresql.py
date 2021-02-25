import unittest

import dbtool

db_config = {
        'host': '127.0.0.1',
        'port': 5432,
        'database': 'postgres',
        'user': None,
        'password': None,
    }
db = dbtool.DB('postgresql', **db_config)


class TestDB(unittest.TestCase):

    def setUp(self):
        db.execute_script("""
            DROP TABLE IF EXISTS test_user;
            CREATE TABLE test_user (
                id   SERIAL PRIMARY KEY,
                name varchar(64)  NOT NULL,
                age  integer NOT NULL
            );
            INSERT INTO test_user(name, age) values('Mario', 18);
            INSERT INTO test_user(name, age) values('Kai', 18);
        """)

    def test_execute_fetchone(self):
        user = db.execute_fetchone('select * from test_user where id = %s', (1,))
        self.assertIsNotNone(user, "user must not be null")

    def test_execute_fetchall(self):
        users = db.execute('select * from test_user')
        self.assertEqual(len(users), 2, "数据条数2条")

    def test_execute_count(self):
        count = db.execute_count('select * from test_user where id = -1')
        self.assertEqual(count, 0)
        count = db.execute_count('select * from test_user')
        self.assertEqual(count, 2)

    def test_execute(self):
        row_id = db.execute("insert into test_user(name, age) values(%s, %s)", ('Mou', 18))
        self.assertEqual(row_id, 3, 'insert row must be return last autogenerate id')
        rows = db.execute("update test_user set age = %s where id = %s", (10, row_id))
        self.assertEqual(rows, 1, 'update rows must be return effects rows')

    def test_execute_many(self):
        rows = db.execute_many("insert into test_user(name, age) values(%s, %s)", [('LK', 18), ('QM', 17)])
        self.assertEqual(rows, 2, 'batch insert must be return effects rows')

    def test_execute_script(self):
        db.execute_script("""
            create table test(
                id   SERIAL PRIMARY KEY,
                name TEXT    NOT NULL
            );
            drop table test;
        """)

    def test_insert(self):
        row_id = db.insert({'name': 'M', 'age': 18}, table='test_user')
        self.assertEqual(row_id, 3, 'insert row must be return last autogenerate id')

    def test_update(self):
        rows = db.update({'id': 1, 'age': 10}, table='test_user')
        self.assertEqual(rows, 1)
        user = db.find_one('test_user', id=1)
        self.assertEqual(user['id'], 1)
        self.assertEqual(user['name'], 'Mario')
        self.assertEqual(user['age'], 10)

    def test_delete_by_id(self):
        rows = db.delete_by_id(2, table='test_user')
        self.assertEqual(rows, 1, 'delete rows should be 1.')
        user = db.find_by_id(2, table='test_user')
        self.assertIsNone(user, 'user id=2 should be deleted.')

    def test_find_by_id(self):
        user = db.find_by_id(1, table='test_user')
        self.assertIsNotNone(user, 'user id=1 should exists.')

    def test_find(self):
        rows = db.find('test_user')
        self.assertEqual(len(rows), 2)
        rows = db.find('test_user', age=18)
        self.assertEqual(len(rows), 2)

    def test_find_one(self):
        row = db.find_one('test_user', id=1)
        self.assertIsNotNone(row)
        self.assertEqual(row.get('id'), 1)
        self.assertEqual(row.get('name'), 'Mario')
        self.assertEqual(row.get('age'), 18)

    def test_find_count(self):
        count = db.find_count('test_user', id=1)
        self.assertEqual(count, 1)
