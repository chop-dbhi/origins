from . import base


class Client(base.Client):
    """Client specific for relational database backends that conform to
    the Python DB API.

    The `connect` method must set the `connection` property which is a
    connection to the database.
    """
    def disconnect(self):
        self.connection.close()

    def qn(self, name):
        return '"{}"'.format(name)

    def fetchall(self, *args, **kwargs):
        "Returns all rows."
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchall()

    def fetchone(self, *args, **kwargs):
        "Returns the fist row"
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchone()

    def fetchvalue(self, *args, **kwargs):
        "Returns the first value from the first row."
        row = self.fetchone(*args, **kwargs)
        if row:
            return row[0]


class Database(base.Node):
    def sync(self):
        self.update(self.client.database())
        self._contains(self.client.tables(), Table)

    @property
    def tables(self):
        return self._containers('table')


class Table(base.Node):
    def sync(self):
        self._contains(self.client.columns(self['name']), Column)

    @property
    def columns(self):
        return self._containers('column')


class Column(base.Node):
    pass
