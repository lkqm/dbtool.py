#  dbtool
A lightweight db tools for sql.


```
# sql
db.query_one(sql)
db.query_all(sql)
db.query_count(sql)
db.query_cursor(sql)

db.execute(sql)
db.executemany(sql)
db.executescript(sql)
db.executescript_file(sql)

# crud
db.insert(dict, table='user')
db.update(dict, table='user', id_name='id')
db.delete_by_id(1, table='user', id_name='id')
db.find_by_id(1, table='user', id_name='id')
db.increment()

db.find_one('user', id=1)
db.find('user', type=0)

```
