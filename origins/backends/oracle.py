from __future__ import division, print_function, unicode_literals
from ..utils import cached_property
from . import base, _database

import cx_Oracle


DATA_TYPE_NAMES = {
    cx_Oracle.BINARY: 'binary',
    cx_Oracle.BFILE: 'bfile',
    cx_Oracle.BLOB: 'blob',
    cx_Oracle.CLOB: 'clob',
    cx_Oracle.CURSOR: 'cursor',
    cx_Oracle.DATETIME: 'datetime',
    cx_Oracle.FIXED_CHAR: 'fixed_char',
    cx_Oracle.FIXED_UNICODE: 'fixed_unicode',
    cx_Oracle.INTERVAL: 'interval',
    cx_Oracle.LONG_BINARY: 'long_binary',
    cx_Oracle.LONG_STRING: 'long_string',
    cx_Oracle.LONG_UNICODE: 'long_unicode',
    cx_Oracle.NATIVE_FLOAT: 'native_float',
    cx_Oracle.NCLOB: 'nclob',
    cx_Oracle.NUMBER: 'number',
    cx_Oracle.OBJECT: 'object',
    cx_Oracle.STRING: 'string',
    cx_Oracle.TIMESTAMP: 'timestamp',
    cx_Oracle.UNICODE: 'unicode',
}


class Client(_database.Client):
    def __init__(self, database, **kwargs):
        self.database = database
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 1521)

        if self.port:
            self.dsn = cx_Oracle.makedsn(self.host, self.port, database)
        else:
            self.dsn = database

        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        cnx = '{}/{}@{}'.format(user or '', password or '', self.dsn)
        self.connection = cx_Oracle.connect(cnx)

    def qn(self, name):
        return name.upper().replace('%', '%%')

    def columns(self, table_name):
        query = '''
            SELECT * FROM {} WHERE ROWNUM < 2
        '''.format(self.qn(table_name))

        c = self.connection.cursor()
        c.execute(query)

        keys = ('column_name', 'data_type', 'display_size', 'internal_size',
                'precision', 'scale', 'nullable')

        columns = []

        for row in c.description:
            attrs = dict(zip(keys, row))
            attrs['data_type'] = DATA_TYPE_NAMES.get(attrs['data_type'])
            # Lower case table name to make it less burdensome to work with
            attrs['column_name'] = attrs['column_name'].lower()
            columns.append(attrs)

        return columns

    def tables(self):
        query = '''
            SELECT TABLE_NAME FROM USER_TABLES
        '''

        tables = []
        for name in self.fetchall(query):
            tables.append({'table_name': name})
        return tables

    def table_count(self, table_name):
        query = '''
            SELECT COUNT(*) FROM {table}
        '''.format(table=self.qn(table_name))
        return self.fetchvalue(query)

    def column_unique_count(self, table_name, column_name):
        query = '''
            SELECT COUNT(DISTINCT {column}) FROM {table}
        '''.format(column=self.qn(column_name),
                   table=self.qn(table_name))
        return self.fetchvalue(query)

    def column_unique_values(self, table_name, column_name, ordered=True):
        query = '''
            SELECT DISTINCT {column} FROM {table}
        '''.format(column=self.qn(column_name),
                   table=self.qn(table_name))

        if ordered:
            query += ' ORDER BY {column}'.format(column=self.qn(column_name))

        for row in self.fetchall(query):
            yield row[0]


class Database(base.Node):
    branches_property = 'tables'
    label_attribute = 'database_name'

    def synchronize(self):
        self.attrs['database_name'] = self.client.database

    @cached_property
    def tables(self):
        nodes = []
        for attrs in self.client.tables():
            node = Table(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Table(base.Node):
    elements_property = 'columns'
    label_attribute = 'table_name'

    @cached_property
    def columns(self):
        nodes = []
        for attrs in self.client.columns(self['table_name']):
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Column(base.Node):
    label_attribute = 'column_name'


# Export for API
Origin = Database
