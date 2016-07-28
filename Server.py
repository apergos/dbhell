#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import datetime
import time
import Database
import Replication
import socket
import SQLDict
import Resultset

class Server:
    __hostname = None
    __port = None
    __socket = None
    __version = None
    __connection = None
    __ipv4 = None
    __status = None
    __cursor = None
    debug = False
    global_status = None
    global_variables = None
    session_status = None
    session_variables = None

    def __init__(self, hostname = 'localhost', port=None, socket=None, current_db=None):
        self.__hostname = hostname
        if socket is None and port is None:
            self.__port = 3306
        else:
            self.__port = port
        self.__socket = socket
        self.global_status = Server._variable_query_generator(self, scope='GLOBAL', kind='STATUS')
        self.global_variables = Server._variable_query_generator(self, scope='GLOBAL', kind='VARIABLES')
        self.session_status = Server._variable_query_generator(self, scope='SESSION', kind='STATUS')
        self.session_variables = Server._variable_query_generator(self, scope='SESSION', kind='VARIABLES')
        self.replication = Replication.Replication(self)

    @property
    def hostname(self):
        return self.__hostname

    @property
    def port(self):
        return self.__port
     
    @property
    def socket(self):
        return self.__socket

    @property
    def version(self):
        if self.__version is None:
            self.__version = self.global_variables['version']
        return self.__version

    def __repr__(self):
        if self.socket is None:
            return ( self.hostname + ':' + str(self.port) )
        else:
            return ( self.hostname + ':' + self.socket )

    @property
    def connection(self):
        return self.__connection
 
    @staticmethod
    def _variable_query_generator(self, scope='GLOBAL', kind='STATUS'):
        queries = dict()
        lambdas = dict()
        queries['getitem'] = (
            "SELECT VARIABLE_VALUE " +
            "FROM information_schema." + scope + "_" + kind + " " + 
            "WHERE VARIABLE_NAME = '%s'")
        lambdas['getitem'] = lambda result, name: result.rows[0][0]
        queries['len'] = (
            "SELECT count(*) " +
            "FROM information_schema." + scope + "_" + kind)
        lambdas['len'] = lambda result: int(result.rows[0][0])
        queries['items'] = (
            "SELECT lower(VARIABLE_NAME) as k, " +
            "VARIABLE_VALUE as v " +
            "FROM information_schema." + scope + "_" + kind + " " +
            "ORDER BY VARIABLE_NAME" )
        lambdas['items'] = lambda result: dict((row[0], row[1]) for row in result.rows)
        queries['keys'] = (
            "SELECT lower(VARIABLE_NAME) as k " +
            "FROM information_schema." + scope + "_" + kind + " " +
            "ORDER BY VARIABLE_NAME" )
        lambdas['keys'] = lambda result: list(row[0] for row in result.rows)
        queries['values'] = (
            "SELECT VARIABLE_VALUE as v " +
            "FROM information_schema." + scope + "_" + kind + " " +
            "ORDER BY VARIABLE_NAME" )
        lambdas['values'] = lambda result: list(row[0] for row in result.rows)
        
        if kind == 'VARIABLES':
            queries['setitem'] = "SET " + scope + " %s = %s"
        return SQLDict.SQLDict(identifier = (scope + ' ' + kind) , 
            server=self, queries=queries, lambdas=lambdas)


    def cursor(self):
        if self.__connection is None:
            if self.socket is None:
                self.__connection = MySQLdb.connect(host=self.hostname,
                port=self.port,user="root", db="test")
            else:
                self.__connection = MySQLdb.connect(unix_socket=self.socket)
        return self.__connection.cursor()

    @property
    def ipv4(self):
        if self.__ipv4 is None:
            self.__ipv4 = socket.gethostbyname(self.hostname)
        return self.__ipv4

    @property
    def status(self):
        status = dict()
        status['hostname'] = self.hostname
        status['port'] = self.port
        status['socket'] = self.socket
        status['ipv4'] = self.ipv4
        
        try:
            status['uptime'] = str(datetime.timedelta(seconds = int(self.global_status['uptime'])))
            status['up'] = True
            status['version'] = self.version
            replication_status = self.replication.status.items()
            if replication_status:
                status['replication'] = dict([('slave_io_running', replication_status['slave_io_running']),
                                    ('slave_sql_running', replication_status['slave_sql_running']),
                                    ('seconds_behind_master', replication_status['seconds_behind_master']),
                                    ])

            else:
                status['replication'] = None
        except Exception:
            status['up'] = False
            status['uptime'] = None
            status['version'] = None
            status['replication'] = None
        return status

    @property
    def databases(self):
        queries = dict()
        lambdas = dict()
        queries['items'] = "SHOW DATABASES"
        lambdas['items'] = lambda result: dict((x[0], Database.Database(self, x[0])) for x in result.rows)
        queries['getitem'] = "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE schema_name = '%s'"
        lambdas['getitem'] = lambda result, name:  Database.Database(self, result.rows[0][0])
        queries['len'] = "SELECT count(*) FROM information_schema.schemata"
        lambdas['len'] = lambda result: result.rows[0][0]
        return SQLDict.SQLDict(identifier = 'databases', 
            server=self, queries=queries, lambdas=lambdas)
#         query = "SHOW DATABASES"
# 
#         result = self.execute(query)
#         databases = dict()
#         for db in result.rows:
#             db_name = db[0]
#             databases[db_name] = Database.Database(self, db_name)
#         return databases

#     # TODO: Delete def tables(self, database):
#     def tables(self):
#         query = "SHOW TABLES FROM `%s`" % database 
# 
#         cursor = self.cursor()
#         cursor.execute(query)
#         if cursor.rowcount == 0:
#             tables = set()
#         else:
#             tables = set(map(lambda x:x[0], cursor.fetchall()))
#         cursor.close()
# 
#         return tables
# 
#     # TODO: Delete
#     def columns(self, database, table):
#         query = "SHOW COLUMNS FROM `%s`.`%s`" % (database, table)
#  
#         cursor = self.cursor()
#         cursor.execute(query)
#         if cursor.rowcount == 0:
#             columns = set()
#         else:
#             columns = set(map(lambda x:x[0], cursor.fetchall()))
#         cursor.close()
# 
#         return columns

    def definition(self, database, table=None, column=None):
        if table is None and column is None:
            cursor = self.cursor()
            query = "SHOW CREATE DATABASE `%s`" % database
            cursor.execute(query)
            if cursor.rowcount == 0:
                definition = None
            else:
                definition = cursor.fetchone()[1]
            cursor.close()
        else:
            cursor = self.cursor()
            query = "SHOW CREATE TABLE `%s`.`%s`" % (database, table)
            cursor.execute(query)
            if cursor.rowcount == 0:
                definition = None
            else:
                definition = cursor.fetchone()[1]
            cursor.close()

            if column is not None:
                for line in definition.split('\n'):
                    if line.startswith('  `%s` ' % column):
                        line = line.strip()
                        if line.endswith(','): line = line[:-1]
                        return line
                return None

        return definition

    @property
    def processlist(self):
        cursor = self.cursor()
        query = "SHOW FULL PROCESSLIST"
        cursor.execute(query)
        if cursor.rowcount == 0:
            self.__processlist = None
        else:
            self.__processlist = list()
            row = cursor.fetchone()
            while row is not None:
                self.__processlist.append(dict(zip(map(lambda x:x[0].lower(), cursor.description), row)))
                row = cursor.fetchone()
        return self.__processlist

    def log(self, error_level, message):
        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + ": [" + error_level +"] " + message)

    def execute(self, query):
        if self.debug:
            print(query)
        #TODO: Error handling
        cursor = self.cursor()
        cursor.execute(query)
        results = Resultset.Resultset(cursor)
        cursor.close()
        return results
