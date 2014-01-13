from __future__ import division, print_function, unicode_literals
from . import _database

import cx_Oracle


DATA_TYPE_NAMES = {
    cx_Oracle.BINARY: 'binary',
    cx_Oracle.BFILE: 'bfile',
    cx_Oracle.BLOB: 'blob',
    cx_Oracle.CLOB: 'clob',
    cx_Oracle.CURSOR: 'cursor',
    cx_Oracle.DATETIME: 'datetime',
    cx_Oracle.FIXED_CHAR: 'fixed_char',
    cx_Oracle.INTERVAL: 'interval',
    cx_Oracle.LONG_BINARY: 'long_binary',
    cx_Oracle.LONG_STRING: 'long_string',
    cx_Oracle.NATIVE_FLOAT: 'native_float',
    cx_Oracle.NCLOB: 'nclob',
    cx_Oracle.NUMBER: 'number',
    cx_Oracle.OBJECT: 'object',
    cx_Oracle.STRING: 'string',
    cx_Oracle.TIMESTAMP: 'timestamp',
}

try:
    DATA_TYPE_NAMES[cx_Oracle.UNICODE] = 'unicode'
    DATA_TYPE_NAMES[cx_Oracle.LONG_UNICODE] = 'long_unicode'
    DATA_TYPE_NAMES[cx_Oracle.FIXED_UNICODE] = 'fixed_unicode'
except AttributeError:
    pass


class Client(_database.Client):
    STRING_TYPES = set([
        'fixed_char',
        'fixed_unicode',
        'long_string',
        'long_unicode',
        'string',
        'unicode',
        'nclob',
        'clob',
    ])

    NUMERIC_TYPES = set([
        'number',
        'native_float',
    ])

    TIME_TYPES = set([
        'datetime',
        'timestamp',
    ])

    BOOL_TYPES = set()

    def __init__(self, database, **kwargs):
        self.name = database
        self.schema = kwargs.get('schema')
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
        return '"{}"'.format(name.upper().replace('%', '%%'))

    def database(self):
        return {
            'name': self.name,
            'host': self.host,
            'port': self.port,
        }

    def tables(self):
        if self.schema:
            query = 'SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :1'
            params = [self.schema.upper()]
        else:
            query = 'SELECT TABLE_NAME FROM USER_TABLES'
            params = []

        tables = []
        for name, in self.fetchall(query, params):
            tables.append({'name': name})
        return tables

    def views(self):
        if self.schema:
            query = 'SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = :1'
            params = [self.schema.upper()]
        else:
            query = 'SELECT VIEW_NAME FROM USER_VIEWS'
            params = []

        views = []
        for name, in self.fetchall(query, params):
            views.append({'name': name})
        return views

    def table_columns(self, table_name):
        query = '''
            SELECT * FROM {} WHERE ROWNUM < 2
        '''.format(self.qn(table_name))

        c = self.connection.cursor()
        c.execute(query)

        keys = ('name', 'type', 'display_size', 'internal_size',
                'precision', 'scale', 'nullable')

        columns = []

        for i, row in enumerate(c.description):
            attrs = dict(zip(keys, row))
            attrs['type'] = DATA_TYPE_NAMES.get(attrs['type'])
            attrs['nullable'] = bool(attrs['nullable'])
            attrs['index'] = i
            columns.append(attrs)

        return columns

    def view_columns(self, view_name):
        query = '''
            SELECT * FROM {} WHERE ROWNUM < 2
        '''.format(self.qn(view_name))

        c = self.connection.cursor()
        c.execute(query)

        keys = ('name', 'type', 'display_size', 'internal_size',
                'precision', 'scale', 'nullable')

        columns = []

        for i, row in enumerate(c.description):
            attrs = dict(zip(keys, row))
            attrs['type'] = DATA_TYPE_NAMES.get(attrs['type'])
            attrs['nullable'] = bool(attrs['nullable'])
            attrs['index'] = i
            columns.append(attrs)

        return columns

    def foreign_keys(self, table_name, column_name):
        query = '''
            SELECT
                user_constraints.constraint_name,
                tb.table_name,
                tb.column_name
            FROM
                user_constraints,
                user_cons_columns ca,
                user_cons_columns cb,
                user_tab_cols ta,
                user_tab_cols tb
            WHERE
                ta.table_name = user_constraints.table_name
                AND ta.column_name = ca.column_name
                AND ca.table_name = ta.table_name
                AND user_constraints.constraint_name = ca.constraint_name
                AND user_constraints.r_constraint_name = cb.constraint_name
                AND cb.table_name = tb.table_name
                AND cb.column_name = tb.column_name
                AND ca.position = cb.position
                AND user_constraints.table_name = :1
                AND ta.column_name = :2
        '''

        keys = ('name', 'table', 'column')
        fks = []
        for row in self.fetchall(query, [table_name.upper(),
                                         column_name.upper()]):
            attrs = dict(zip(keys, row))
            fks.append(attrs)
        return fks


class Database(_database.Database):
    def sync(self):
        self.update(self.client.database())
        self.define(self.client.tables(), Table)
        self.define(self.client.views(), View)

    @property
    def views(self):
        return self.definitions('view', sort='name')


class Table(_database.Table):
    def sync(self):
        self.define(self.client.table_columns(self['name']),
                    _database.Column)


class View(_database.Table):
    def sync(self):
        self.define(self.client.view_columns(self['name']),
                    _database.Column)


# Export for API
Origin = Database
