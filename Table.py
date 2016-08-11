#!/usr/bin/env python
# -*- coding: utf-8 -*-

import Column
import SQLDict

class Table(object):
    def __init__(self, serverconn, database, name, definition=None):
        self.__serverconn = serverconn
        self.__database = database
        self.__name = name
        self.__definition = definition

    def __repr__(self):
        return self.__name

    @property
    def name(self):
        return self.__name

    @property
    def definition(self):
        query = "SHOW CREATE TABLE `%s`.`%s`" % (self.__database, self.__name)
        result = self.__serverconn.execute(query)
        if result.len is None or result.len == 0:
            return None
        else:
            return result.rows[0][1]

    @property
    def columns(self):
        queries = dict()
        lambdas = dict()
        queries['items'] = ("SELECT column_name FROM information_schema.columns WHERE " +
                            "table_schema = '%s' AND table_name = '%s'" %
                            (self.__database.name, self.__name))
        lambdas['items'] = lambda result: dict((x[0], Column.Column(self.__serverconn,
                                                                    self.__database, self, x[0]))
                                               for x in result.rows)
#         queries['getitem'] = ("SELECT TABLE_NAME FROM information_schema.tables " +
#                                "WHERE TABLE_SCHEMA = '" +
#                                self.__name + "' and table_name = '%s'")
#         lambdas['getitem'] = lambda result, name:  Table.Table(self.__serverconn,
#                                                                self, result.rows[0][0])
#         queries['len'] = "SELECT count(*) FROM information_schema.schemata"
#         lambdas['len'] = lambda result: result.rows[0][0]
        queries['delitem'] = ("ALTER TABLE `" + self.__database.name + "`.`" +
                              self.__name + "` DROP COLUMN `%s`")
        return SQLDict.SQLDict(identifier='columns',
                               server=self.__serverconn, queries=queries, lambdas=lambdas)

    @property
    def character_set(self):
        query = ("SELECT CHARACTER_SET_NAME FROM information_schema.collations " +
                 "WHERE COLLATION_NAME = " +
                 "(SELECT TABLE_COLLATION FROM information_schema.tables "+
                 "WHERE table_schema = '%s' and table_name = '%s')" %
                 (self.__database.name, self.__name))
        result = self.__serverconn.execute(query)
        if result.len is None or result.len == 0:
            return None
        else:
            return result.rows[0][0]

    @property
    def collation(self):
        query = ("SELECT TABLE_COLLATION FROM information_schema.tables " +
                 "WHERE table_schema = '%s' and table_name = '%s'" %
                 (self.__database.name, self.__name))
        result = self.__serverconn.execute(query)
        if result.len is None or result.len == 0:
            return None
        else:
            return result.rows[0][0]

#     def ddl(self, change):
#         if type(change) is list or type(change) is tuple:
#             print('it is a list')
#             change = ', '.join(change)
#
#         cursor = self.connection.cursor()
#         query = "ALTER TABLE `%s`.`%s` %s" % (self.database_name, self.table_name, change)
#         print(query)
#         cursor.execute(query)
#         cursor.close()
#         return True
#
#     def add_column(self, column_name, column_definition):
#         return self.ddl("ADD COLUMN `%s` %s" % (column_name, column_definition))
#
#     def drop_column(self, column_name):
#         return self.ddl("DROP COLUMN `%s`" % column_name)
#
#     def rename_column(self, column_name, new_column_name):
#         return self.ddl("CHANGE COLUMN `%s` `%s` %s" % (
#             column_name, new_column_name, self.columns[column_name].definition))
#
#     def change_column(self, column_name, column_definition):
#         return self.ddl("CHANGE COLUMN `%s` `%s` %s" % (
#             column_name, column_name, column_definition))
