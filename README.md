#  dbtool
A lightweight db tools for sql.

```
pip install dbtool
```

```
# sqlite3 ....
db = dbtool.connect('sqlite:///:memory:')
db = dbtool.connect('mysql://root:123456@127.0.0.1:3306/test',  mincached=1, maxconnections=20)

# sql
db.execute(sql)
db.execute_fetchone(sql)
db.execute_cursor(sql)
db.execute_batch(sql)
db.execute_script(sql)

# crud
db.insert(user)
db.update(user)
db.delete(User, {'id': 1})
db.find(User, {'status': 1})
db.find_one(User, {'id': 1})
db.find_count(User, {'status': 1})

# transactions
with db.transaction():
    db.execute(sql1)

```

db vs driver

- sqlite - sqlite3
- mysql - pymysql
- postgresql - psycopg2
- sqlserver - pymssql