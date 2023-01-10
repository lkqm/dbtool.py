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
