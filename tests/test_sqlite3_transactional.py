import unittest

import dbtool

db = dbtool.DB('sqlite', database=':memory:')


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
