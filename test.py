from SqlManager import SqlManager

sqlManager = SqlManager({
    'user' : 'root',
    'passwd' : 'r5npr98a',
    'host' : 'localhost',
    'db' : 'money'
})

sqlManager.from_table('test')
print(sqlManager.find_records())