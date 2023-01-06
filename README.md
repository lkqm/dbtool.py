#  dbtool
A lightweight db tools for sql.

```
pip install dbtool
```

```
# sqlite3 ....
db = dbtool.DB('sqlite:///:memory:')
db = dbtool.DB('mysql://root:123456@127.0.0.1:3306/test?client_flag=65536',  mincached=1, maxconnections=20)

# sql
db.execute(sql)
db.execute_fetchone(sql)
db.execute_cursor(sql)
db.execute_batch(sql)
db.execute_script(sql)

# crud
db.insert('user', dict)
db.update('user', dict)
db.delete('user', id=1)
db.find('user', type=0)
db.find_one('user', id=1)
db.find_count('user', type=0)

# transactions
with db.transaction():
    db.execute(sql1)

```

db vs driver

- sqlite - sqlite3
- mysql - pymysql
- postgresql - psycopg2
- sqlserver - pymssql