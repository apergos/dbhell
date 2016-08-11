#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Table
import SQLDict

class Database(object):

    def __init__(self, serverconn, name, definition=None):
        self.__serverconn = serverconn
        self.name = name
        self.__definition = definition

    @property
    def definition(self):
        query = "SHOW CREATE DATABASE `%s`" % self.name
        result = self.__serverconn.execute(query)
        if result.len is None or result.len == 0:
            return None
        else:
            return result.rows[0][1].strip()

    @definition.setter
    def definition(self, value):
        query = "ALTER DATABASE `%s` %s" % (self.name, value,)
        self.__serverconn.execute(query)

    @property
    def character_set(self):
        query = ("SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.schemata "
                 "WHERE schema_name = '%s'" % self.name)
        result = self.__serverconn.execute(query)
        if result.len is None or result.len == 0:
            return None
        else:
            return result.rows[0][0]

    @character_set.setter
    def character_set(self, value):
        query = "ALTER DATABASE `%s` CHARACTER SET '%s'" % (self.name, value)
        self.__serverconn.execute(query)

    @property
    def collation(self):
        query = ("SELECT DEFAULT_COLLATION_NAME FROM information_schema.schemata"
                 " WHERE schema_name = '%s'" % self.name)
        result = self.__serverconn.execute(query)
        if result.len is None or result.len == 0:
            return None
        else:
            return result.rows[0][0]

    @property
    def tables(self):
        queries = dict()
        lambdas = dict()
        queries['items'] = ("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '" +
                            self.name + "' ORDER BY TABLE_NAME")
        lambdas['items'] = lambda result: dict((x[0], Table.Table(self.__serverconn, self, x[0]))
                                               for x in result.rows)
        queries['getitem'] = ("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = '" +
                              self.name + "' and table_name = '%s'")
        lambdas['getitem'] = lambda result, name: Table.Table(self.__serverconn, self, result.rows[0][0])
        queries['len'] = "SELECT count(*) FROM information_schema.schemata"
        lambdas['len'] = lambda result: result.rows[0][0]
        return SQLDict.SQLDict(identifier='tables',
                               server=self.__serverconn, queries=queries, lambdas=lambdas)

    def __repr__(self):
        return self.name
