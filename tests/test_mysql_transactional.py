import unittest

from pymysql.constants import CLIENT

import dbtool

db_config = {
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'test',
    'user': 'root',
    'password': '123456',
    'client_flag': CLIENT.MULTI_STATEMENTS,
}
db = dbtool.DB('mysql', **db_config)


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
