import unittest

import dbtool

db = dbtool.connect('sqlite:///:memory:', mincached=1)


class TestDB(unittest.TestCase):

    def setUp(self):
        db.execute_script("""
            DROP TABLE IF EXISTS test_user;
            CREATE TABLE test_user (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT    NOT NULL,
                age  INTEGER NOT NULL
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
        row_id = db.insert({'name': 'M', 'age': 18}, table='test_user')
        self.assertEqual(row_id, 3, 'insert row must be return last autogenerate id')
        user = User(name="kk", age=18)
        row_id = db.insert(user)
        self.assertEqual(row_id, 4, 'insert row must be return last autogenerate id')

    def test_update(self):
        rows = db.update({'id': 1, 'age': 10}, table='test_user')
        self.assertEqual(rows, 1)
        user = db.find_one(User, {'id': 1}, return_type=dict)
        self.assertEqual(user['id'], 1)
        self.assertEqual(user['name'], 'Mario')
        self.assertEqual(user['age'], 10)
        # 对象修改
        user = User(id=2, name="kk", age=100)
        rows = db.update(user)
        self.assertEqual(rows, 1)
        user = db.find_one(User, {'id': 2}, return_type=dict)
        self.assertEqual(user['id'], 2)
        self.assertEqual(user['name'], 'kk')
        self.assertEqual(user['age'], 100)

    def test_delete(self):
        rows = db.delete('test_user', {'id': 1})
        self.assertEqual(rows, 1, 'delete rows should be 1.')
        user = db.find_one(User, {'id': 1})
        self.assertIsNone(user, 'user id=1 should be deleted.')
        # obj
        rows = db.delete(User, {'id': 2})
        self.assertEqual(rows, 1, 'delete rows should be 1.')
        user = db.find_one(User, {'id': 2})
        self.assertIsNone(user, 'user id=2 should be deleted.')

    def test_find(self):
        rows = db.find('test_user')
        self.assertEqual(len(rows), 2)
        rows = db.find('test_user', {'age': 18})
        self.assertEqual(len(rows), 2)
        # 面写对象
        users = db.find(User, {'id': 1})
        self.assertEqual(len(users), 1)
        user = users[0]
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, 'Mario')
        self.assertEqual(user.age, 18)

    def test_find_one(self):
        row = db.find_one('test_user', {'id': 1})
        self.assertIsNotNone(row)
        self.assertEqual(row.get('id'), 1)
        self.assertEqual(row.get('name'), 'Mario')
        self.assertEqual(row.get('age'), 18)
        # 面写对象
        user = db.find_one(User, {'id': 1})
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, 'Mario')
        self.assertEqual(user.age, 18)

    def test_find_count(self):
        count = db.find_count({'id': 1}, table=User)
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


class User:
    TABLE_NAME = 'test_user'

    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.name = kwargs.get('name')
        self.age = kwargs.get('age')
