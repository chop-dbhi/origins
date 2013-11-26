from __future__ import division, print_function, unicode_literals
from . import base

import cx_Oracle


class Database(base.Node):
    label_attribute = 'database'
    branches_property = 'tables'

    def branches(self):
        query = 'SELECT lower(TABLE_NAME) FROM USER_TABLES'
        c = self.client.connection.cursor()
        c.execute(query)

        nodes = []
        keys = ['table_name']

        for row in c.fetchall():
            attrs = dict(zip(keys, row))
            node = Table(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        del self['password']
        self['id'] = '{}{}{}'.format(self['database'], base.ID_SEPARATOR,
                                     self['user'])


class Table(base.Node):
    elements_property = 'columns'
    label_attribute = 'table_name'

    def elements(self):
        query = '''
            SELECT * FROM {} WHERE ROWNUM < 2
        '''.format(self.client.qn(self['table_name']))
        c = self.client.connection.cursor()
        c.execute(query)

        nodes = []
        keys = ['column_name', 'data_type', 'display_size', 'internal_size',
                'precision', 'scale', 'nullable']

        for row in c.description:
            attrs = dict(zip(keys, row))
            attrs['data_type'] = self.client.DATA_TYPE_NAMES\
                .get(attrs['data_type'], '')
            attrs['column_name'] = attrs['column_name'].lower()
            attrs['table_name'] = self['table_name']
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        self.attrs['count'] = self.count()

    def count(self):
        query = '''
            SELECT COUNT(*) FROM {}
        '''.format(self.client.qn(self['table_name']))
        return self.client.one(query)[0]


class Column(base.Node):
    label_attribute = 'column_name'

    def count(self):
        query = '''
            SELECT COUNT({column})
            FROM {table}
        '''.format(column=self.client.qn(self['column_name']),
                   table=self.client.qn(self['table_name']))
        return self.client.one(query)[0]

    def unique_count(self):
        query = '''
            SELECT COUNT(DISTINCT {column})
            FROM {table}
        '''.format(column=self.client.qn(self['column_name']),
                   table=self.client.qn(self['table_name']))
        return self.client.one(query)[0]

    def values(self):
        query = '''
            SELECT {column}
            FROM {table}
        '''.format(column=self.client.qn(self['column_name']),
                   table=self.client.qn(self['table_name']))
        return [r[0] for r in self.client.all(query)]

    def unique_values(self):
        query = '''
            SELECT DISTINCT {column}
            FROM {table}
        '''.format(column=self.client.qn(self['column_name']),
                   table=self.client.qn(self['table_name']))
        return [r[0] for r in self.client.all(query)]


# Exported classes for API
Origin = Database


class Client(object):
    """Wrapper for Python DB-API compatible connection objects that removes
    some boilerplate when executing queries.
    """
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

    def __init__(self, database, **kwargs):
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 1521)

        if port:
            dsn = cx_Oracle.makedsn(host, int(port), database)
        else:
            dsn = database

        user = kwargs.get('user', '')
        password = kwargs.get('password', '')

        cnx = '{}/{}@{}'.format(user, password, dsn)
        self.connection = cx_Oracle.connect(cnx)

    def all(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchall()

    def one(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchone()

    def qn(self, name):
        return name.upper().replace('%', '%%')
