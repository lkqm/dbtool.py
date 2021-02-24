#  dbtool
A lightweight db tools for sql.

```
pip install dbtool
```

```
# sql
db.execute(sql, fetchone=False, return_cursor=False, executemany=False)
db.execute_fetchone(sql)
db.execute_count(sql)
db.execute_cursort(sql)
db.execute_many(sql)
db.execute_script(sql)
db.execute_file(sql)

# crud
db.insert(dict, table='user')
db.update(dict, table='user', id_name='id')
db.delete_by_id(1, table='user', id_name='id')
db.find_by_id(1, table='user', id_name='id')

db.find('user', type=0)
db.find_one('user', id=1)
db.find_count('user', type=0)

```
