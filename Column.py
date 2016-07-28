class Column:
    __server = None
    __database = None
    __table = None
    __name = None
    
    def __init__(self, server, database, table, name):
        self.__server = server
        self.__database = database
        self.__table = table
        self.__name = name

    def __repr__(self):
        return self.__name
    
    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):                
        query = ( "ALTER TABLE `%s`.`%s` CHANGE `%s` `%s` %s" % 
                (self.__database.name, self.__table.name, self.__name, name, self.definition) )
        return self.__server.execute(query)

    @property
    def definition(self):                
        query = "SHOW CREATE TABLE `%s`.`%s`" % (self.__database.name, self.__table.name)
        result = self.__server.execute(query)
        if result.len is None or result.len == 0:
            return None
        else:
            table_definition = result.rows[0][1]
            for line in table_definition.split('\n'):
                column_name_definition = '  `%s` ' % self.__name
                if line.startswith(column_name_definition):
                    line = line[len(column_name_definition):].strip()
                    if line.endswith(','):
                        line = line[:-1]
                    return line
            return None

    @definition.setter
    def definition(self, definition):                
        query = "ALTER TABLE `%s`.`%s` MODIFY `%s` %s" % (self.__database.name, self.__table.name, self.__name, definition)
        return self.__server.execute(query)
