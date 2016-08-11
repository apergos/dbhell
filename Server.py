#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import datetime
import time
import MySQLdb
import Database
import Replication
import SQLDict
import Resultset

class Host(object):
    def __init__(self, name='localhost', port=None, sock=None):
        self.name = name
        if sock is None and port is None:
            self.port = 3306
        else:
            self.port = port
        self.socket = sock
        self.__ipv4 = None

    def __repr__(self):
        if self.socket is None:
            return self.name + ':' + str(self.port)
        else:
            return self.name + ':' + self.socket

    @property
    def ipv4(self):
        if self.__ipv4 is None:
            self.__ipv4 = socket.gethostbyname(self.name)
        return self.__ipv4

    @property
    def status(self):
        status = dict()
        status['name'] = self.name
        status['port'] = self.port
        status['socket'] = self.socket
        status['ipv4'] = self.ipv4
        return status


class ServerDbInfo(object):
    def __init__(self, conn):
        self.conn = conn
        self.global_status = ServerDbInfo.variable_query_generator(
            conn.server, scope='GLOBAL', kind='STATUS')
        self.global_variables = ServerDbInfo.variable_query_generator(
            conn.server, scope='GLOBAL', kind='VARIABLES')
        self.session_status = ServerDbInfo.variable_query_generator(
            conn.server, scope='SESSION', kind='STATUS')
        self.session_variables = ServerDbInfo.variable_query_generator(
            conn.server, scope='SESSION', kind='VARIABLES')
        self.replication = Replication.Replication(conn.server)

        self.__processlist = None

    @property
    def version(self):
        return self.global_variables['version']

    @staticmethod
    def variable_query_generator(server, scope='GLOBAL', kind='STATUS'):
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
            "ORDER BY VARIABLE_NAME")
        lambdas['items'] = lambda result: dict((row[0], row[1]) for row in result.rows)
        queries['keys'] = (
            "SELECT lower(VARIABLE_NAME) as k " +
            "FROM information_schema." + scope + "_" + kind + " " +
            "ORDER BY VARIABLE_NAME")
        lambdas['keys'] = lambda result: list(row[0] for row in result.rows)
        queries['values'] = (
            "SELECT VARIABLE_VALUE as v " +
            "FROM information_schema." + scope + "_" + kind + " " +
            "ORDER BY VARIABLE_NAME")
        lambdas['values'] = lambda result: list(row[0] for row in result.rows)

        if kind == 'VARIABLES':
            queries['setitem'] = "SET " + scope + " %s = %s"
        return SQLDict.SQLDict(identifier=(scope + ' ' + kind),
                               server=server, queries=queries, lambdas=lambdas)

    @property
    def databases(self):
        queries = dict()
        lambdas = dict()
        queries['items'] = "SHOW DATABASES"
        lambdas['items'] = lambda result: dict((x[0], Database.Database(self.conn, x[0]))
                                               for x in result.rows)
        queries['getitem'] = ("SELECT SCHEMA_NAME FROM information_schema.schemata"
                              "WHERE schema_name = '%s'")
        lambdas['getitem'] = lambda result, name: Database.Database(self.conn, result.rows[0][0])
        queries['len'] = "SELECT count(*) FROM information_schema.schemata"
        lambdas['len'] = lambda result: result.rows[0][0]
        return SQLDict.SQLDict(identifier='databases',
                               server=self, queries=queries, lambdas=lambdas)

    def tables(self, database):
        queries = dict()
        lambdas = dict()
        queries['items'] = "SHOW TABLES FROM `%s`" % database
        lambdas['items'] = lambda result: dict((x[0], Database.Database(self.conn, x[0]))
                                               for x in result.rows)
        return SQLDict.SQLDict(identifier='tables',
                               server=self, queries=queries, lambdas=lambdas)

    def columns(self, database, table):
        queries = dict()
        lambdas = dict()
        queries['items'] = "SHOW COLUMNS FROM `%s`.`%s`" % (database, table)
        lambdas['items'] = lambda result: dict((x[0], Database.Database(self.conn, x[0]))
                                               for x in result.rows)
        return SQLDict.SQLDict(identifier='columns',
                               server=self, queries=queries, lambdas=lambdas)

    def definition(self, database, table=None, column=None):
        if table is None and column is None:
            cursor = self.conn.server.cursor()
            query = "SHOW CREATE DATABASE `%s`" % database
            cursor.execute(query)
            if cursor.rowcount == 0:
                definition = None
            else:
                definition = cursor.fetchone()[1]
            cursor.close()
        else:
            cursor = self.conn.cursor()
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
                        if line.endswith(','):
                            line = line[:-1]
                        return line
                return None

        return definition

    @property
    def processlist(self):
        cursor = self.conn.cursor()
        query = "SHOW FULL PROCESSLIST"
        cursor.execute(query)
        if cursor.rowcount == 0:
            self.__processlist = None
        else:
            self.__processlist = list()
            row = cursor.fetchone()
            while row is not None:
                self.__processlist.append(
                    # dict(zip(map(lambda x: x[0].lower(), cursor.description), row)))
                    dict(zip([field[0].lower() for field in cursor.description], row)))

                row = cursor.fetchone()
        return self.__processlist

    @property
    def status(self):
        status = self.conn.server.host.status

        try:
            status['uptime'] = str(datetime.timedelta(seconds=int(self.global_status['uptime'])))
            status['up'] = True
            # FIXME
            status['version'] = self.version
            # FIXME
            replication_status = self.replication.status.items()
            if replication_status:
                status['replication'] = dict([
                    ('slave_io_running', replication_status['slave_io_running']),
                    ('slave_sql_running', replication_status['slave_sql_running']),
                    ('seconds_behind_master', replication_status['seconds_behind_master']),
                ])

            else:
                status['replication'] = None
        except Exception:
            status['up'] = False
            status['uptime'] = None
            # FIXME
            status['version'] = None
            # FIXME
            status['replication'] = None
        return status


class ServerConn(object):

    def __init__(self, host=None, current_db=None, debug=False):
        self.host = host
        self.current_db = current_db
        self.debug = debug

        self.__connection = None

    @property
    def connection(self):
        return self.__connection

    def cursor(self):
        if self.__connection is None:
            if self.host.socket is None:
                self.__connection = MySQLdb.connect(host=self.host.name,
                                                    port=self.host.port, user="root", db="test")
            else:
                self.__connection = MySQLdb.connect(unix_socket=self.host.socket)
        return self.__connection.cursor()

    def log(self, error_level, message):
        print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') +
              ": [" + error_level +"] " + message)

    def execute(self, query):
        if self.debug:
            print query
        #TODO: Error handling
        cursor = self.cursor()
        cursor.execute(query)
        results = Resultset.Resultset(cursor)
        cursor.close()
        return results
