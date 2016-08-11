class Resultset(object):

    def __init__(self, cursor):
        self.rows = cursor.fetchall()
        if cursor.description is None:
            self.len = None
            self.columns = None
        else:
            self.len = cursor.rowcount
            self.columns = [i[0] for i in cursor.description]
        self.success = None
