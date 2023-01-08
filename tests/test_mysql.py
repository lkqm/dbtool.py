import unittest

from pymysql.constants import CLIENT

import dbtool

db = dbtool.connect('mysql://root:123456@127.0.0.1:3306/test', client_flag=CLIENT.MULTI_STATEMENTS)


class TestDB(unittest.TestCase):

    def setUp(self):
        db.execute_script("""
            DROP TABLE IF EXISTS test_user;
            CREATE TABLE test_user (
                id   int PRIMARY KEY AUTO_INCREMENT,
                name varchar(64)    NOT NULL,
                age  int NOT NULL
            );
            INSERT INTO test_user(id, name, age) values(1, 'Mario', 18);
            INSERT INTO test_user(id, name, age) values(2, 'Kai', 18);
        """)

    def test_execute_fetchone(self):
        user = db.execute_fetchone('select * from test_user where id = ?', (1,))
        self.assertIsNotNone(user, "user must not be null")

    def test_execute_fetchall(self):
        users = db.execute('select * from test_user')
        self.assertEqual(len(users), 2, "数据条数2条")

    def test_execute_cursor(self):
        cursor = db.execute_cursor('select * from test_user')
        db.close_cursor(cursor)
        db.execute('select * from test_user')

    def test_execute(self):
        row_id = db.execute("insert into test_user(name, age) values(?, ?)", ('Mou', 18))
        self.assertEqual(row_id, 3, 'insert row must be return last autogenerate id')
        rows = db.execute("update test_user set age = ? where id = ?", (10, row_id))
        self.assertEqual(rows, 1, 'update rows must be return effects rows')

    def test_execute_batch(self):
        rows = db.execute_batch("insert into test_user(name, age) values(?, ?)", [('LK', 18), ('QM', 17)])
        self.assertEqual(rows, 2, 'batch insert must be return effects rows')

    def test_insert(self):
        row_id = db.insert('test_user', {'name': 'M', 'age': 18})
        self.assertEqual(row_id, 3, 'insert row must be return last autogenerate id')

    def test_update(self):
        rows = db.update('test_user', {'id': 1, 'age': 10})
        self.assertEqual(rows, 1)
        user = db.find_one('test_user', id=1)
        self.assertEqual(user['id'], 1)
        self.assertEqual(user['name'], 'Mario')
        self.assertEqual(user['age'], 10)

    def test_delete(self):
        rows = db.delete('test_user', id=2)
        self.assertEqual(rows, 1, 'delete rows should be 1.')
        user = db.find_one('test_user', id=2)
        self.assertIsNone(user, 'user id=2 should be deleted.')

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

    def test_transactional(self):
        # case rollback
        try:
            with db.transaction():
                db.execute("INSERT INTO test_user(id, name, age) values(3, 'Q', 100)")
                row = db.execute_fetchone("select * from test_user where id = 3")
                self.assertIsNotNone(row, 'row is not None in same transactional.')
                raise Exception()
        except:
            ...
        finally:
            row = db.execute_fetchone("select * from test_user where id = 3")
            self.assertIsNone(row, 'row should rollback of id = 3')
        # case commit
        with db.transaction():
            db.execute("INSERT INTO test_user(id, name, age) values(3, 'Q', 100)")
            row = db.execute_fetchone("select * from test_user where id = 3")
            self.assertIsNotNone(row, 'row is not None in same transactional.')

    def test_transactional_nesting(self):
        # case rollback
        try:
            with db.transaction():
                with db.transaction():
                    db.execute("INSERT INTO test_user(id, name, age) values(3, 'Q', 100)")
                    row = db.execute_fetchone("select * from test_user where id = 3")
                    self.assertIsNotNone(row, 'row is not None in same transactional.')
                raise Exception
        except:
            ...
        finally:
            row = db.execute_fetchone("select * from test_user where id = 3")
            self.assertIsNone(row, 'row should rollback of id = 3')
        # case commit
        with db.transaction():
            with db.transaction():
                db.execute("INSERT INTO test_user(id, name, age) values(3, 'Q', 100)")
                row = db.execute_fetchone("select * from test_user where id = 3")
                self.assertIsNotNone(row, 'row is not None in same transactional.')
            row = db.execute_fetchone("select * from test_user where id = 3")
            self.assertIsNotNone(row, 'row is not None in same transactional.')
        row = db.execute_fetchone("select * from test_user where id = 3")
        self.assertIsNotNone(row, 'row is not None in same transactional.')

    def test_transactional_decorator(self):
        # case rollback
        try:
            self.execute_exception()
        except:
            ...
        finally:
            row = db.execute_fetchone("select * from test_user where id = 3")
            self.assertIsNone(row, 'row should rollback of id = 3')
        # case commit
        self.execute_normal()
        row = db.execute_fetchone("select * from test_user where id = 3")
        self.assertIsNotNone(row, 'row is not None in same transactional.')

    @db.transaction
    def execute_normal(self):
        db.execute("INSERT INTO test_user(id, name, age) values(3, 'Q', 100)")
        row = db.execute_fetchone("select * from test_user where id = 3")
        self.assertIsNotNone(row, 'row is not None in same transactional.')

    @db.transaction
    def execute_exception(self):
        db.execute("INSERT INTO test_user(id, name, age) values(3, 'Q', 100)")
        row = db.execute_fetchone("select * from test_user where id = 3")
        self.assertIsNotNone(row, 'row is not None in same transactional.')
        raise Exception()
