import collections

class SQLDict(collections.MutableMapping):

    __identifier = None
    __server = None
    __queries = None
    __lambdas = None

    def __init__(self, identifier, server, queries, lambdas):
        self.__identifier = identifier
        self.__server = server
        self.__queries = queries
        self.__lambdas = lambdas

    def __repr__(self):
        return str(self.items())

    def __getitem__(self, name):
        if 'getitem' in self.__queries:
            result = self.__server.execute(self.__queries['getitem'] % (name,))
            if result.len is None or result.len == 0:
                raise Exception(name + ' was not found')
            return self.__lambdas['getitem'](result, name)
        else:
            return self.items()[name]

    def __setitem__(self, name, value):
        if not 'setitem' in self.__queries:
            raise Exception(self.__identifier + ' cannot be set')
        if isinstance(value, str):
            new_value = "'" + value + "'"
        else:
            new_value = value
        self.__server.execute(self.__queries['setitem'] % (name, new_value))
        return 0

    def __contains__(self, name):
        if not 'getitem' in self.__queries:
            raise Exception(self.__identifier + ' does not support the contains method')
        return self.__server.execute(self.__queries['getitem'] % (name,)).len > 0
        
    def __getattr__(self, name):
        return None

    def __delitem__(self, name):
        if not ('delitem' in self.__queries):
            raise Exception('You cannot delete items from this set')
        else:
            return self.__server.execute(self.__queries['delitem'] % (name))

    def items(self):
        if self.__queries['items'] is None:
            raise Exception('You cannot get all items from the set')
        result = self.__server.execute(self.__queries['items'])
        if result.len == 0:
            return dict()
        else:
            return self.__lambdas['items'](result)

    def __iter__(self):
        return iter(self.items())

    def keys(self):
        if 'keys' in self.__queries:
            result = self.__server.execute(self.__queries['keys'])
            if result.len is None or result.len == 0:
                return []
            else:
                return self.__lambdas['keys'](result)
        else:
            return self.items().keys()            

    def values(self):
        if 'values' in self.__queries:
            result = self.__server.execute(self.__queries['values'])
            if result.len is None or result.len == 0:
                return []
            else:
                return self.__lambdas['values'](result)
        else:
            return self.items().values()
        
    def __keytransform__(self, key):
        return key

    def __len__(self):
        if 'len' in self.__queries:
            result = self.__server.execute(self.__queries['len'])
            if result.len is None:
                raise Exception('Could not be counted')
            else:
                return self.__lambdas['len'](result)
        else:
            return len(self.keys())
            