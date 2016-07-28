import SQLDict

class Replication:

    __server = None
    
    def __init__(self, server):
        self.__server = server

    @property
    def status(self):
        queries = dict()
        lambdas = dict()
        queries['items'] = "SHOW SLAVE STATUS"
        lambdas['items'] = lambda result: dict(zip((x.lower() for x in result.columns), result.rows[0]))
        return SQLDict.SQLDict(identifier = 'slave_status' , 
            server=self.__server, queries=queries, lambdas=lambdas)

    # stop_slave() -> stops the default connection only
    # stop_slave(connection = 's1') -> stops the s1 connection
    # stop_slave(connection = 'all') -> stop all slave connections
    def stop(self):
        slave_status = self.slave_status
        if slave_status['slave_io_running'] == 'No' and slave_status['slave_sql_running'] == 'No':
            self.__server.log('WARNING', 'Slave was already stopped.')
            return True
        else:
            self.execute('STOP SLAVE')
        return True

    def start(self, until=None):
        slave_status = self.slave_status
        if slave_status['slave_io_running'] == 'Yes' and slave_status['slave_sql_running'] == 'Yes':
            self.__server.log('WARNING', 'Slave was already running.')
            return True
        else:
            self.execute('START SLAVE')
            return True
