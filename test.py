from Server import Server
from Database import Database
from Table import Table

import time
s = Server(socket='/tmp/mysql.sock')
s = Server(hostname='127.0.0.1', port=3306)
s.debug = 1

print(s)
print(s.status)
print(s.databases)
print(s.databases['test'].definition)
print(s.databases['test'].character_set)
s.databases['test'].character_set = 'utf8mb4'
print(s.databases['test'].character_set)
s.databases['test'].character_set = 'latin1'
print(s.databases['test'].character_set)
print(s.databases['test'].collation)
print(s.global_status)
print(s.global_variables)
print(s.global_variables.keys())
print(s.global_status['uptime'])
print(s.session_variables['sql_mode'])
s.session_variables['sql_mode'] = 'TRADITIONAL'
print(s.session_variables['sql_mode'])
print(s.databases['test'].collation)
print(s.databases['test'].tables)
print(s.databases['test'].tables['test'].definition)
print(s.databases['test'].tables['test'].columns)
print(s.databases['test'].tables['test'].character_set)
print(s.databases['test'].tables['test'].collation)
#print(s.databases['test'].tables['test'].columns['word'].definition)
# print(s.databases['test'].tables['test'].add_column('j', 'int'))
# print(s.databases['test'].tables['test'].columns)
print(s.databases['test'].tables['test'].columns['ID'].name)
s.databases['test'].tables['test'].columns['ID'].name = 'new_ID'
print(s.databases['test'].tables['test'].columns['new_ID'].name)
s.databases['test'].tables['test'].columns['new_ID'].name = 'ID'
print(s.databases['test'].tables['test'].columns['ID'].name)
s.databases['new_db'] = Database('new_db', definition='CHARSET utf8mb4')
s.databases['new_db'].tables['new_table'] = Table('new_table', definition='CREATE TABLE t (i int)')
print(s.databases['test'].tables['test'].columns['ID'].definition)
s.databases['test'].tables['test'].columns['ID'].definition = 'int(11) NOT NULL'
print(s.databases['test'].tables['test'].columns['ID'].definition)
s.databases['test'].tables['test'].columns['ID'].definition = 'int(11) NOT NULL AUTO_INCREMENT'
print(s.databases['test'].tables['test'].columns['ID'].definition)
#del(s.databases['test'].tables['test'].columns['word'])
# print(s.databases['test'].tables['test'].drop_column('k'))
# print(s.databases['test'].tables['test'].columns)
# s.databases['test'].tables['test'].ddl(('ADD column l int', 'ADD INDEX(l)'))
# print(s.databases['test'].tables['test'].columns)
# print(s.databases['test'].tables['test'].drop_column('l'))
# print(s.databases['test'].tables['test'].columns)
print(s.replication.status)
print(s.replication.status['slave_io_running'])
print(s.replication.status['slave_sql_running'])
qps = int(s.global_status['questions'])
time.sleep(1)
print("QPS = " + str(int(s.global_status['questions']) - qps))