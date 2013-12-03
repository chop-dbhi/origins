from __future__ import division, absolute_import
from ..utils import cached_property
from . import base, _database

import MySQLdb


class Client(_database.Client):
    def __init__(self, database, **kwargs):
        self.database = database
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 3306)
        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        self.connection = MySQLdb.connect(db=self.database, host=self.host,
                                          port=self.port, user=user,
                                          passwd=password)

    def version(self):
        return self.fetchvalue('SELECT version()')

    def qn(self, name):
        return '"{}"'.format(name)

    def tables(self):
        query = '''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = {schema}
            ORDER BY table_name
        '''.format(schema=self.qn(self.database))

        keys = ('table_name',)

        tables = []
        for row in self.fetchall(query):
            tables.append(dict(zip(keys, row)))
        return tables

    def columns(self, table_name):
        query = '''
            SELECT column_name,
                ordinal_position,
                is_nullable,
                data_type
            FROM information_schema.columns
            WHERE table_schema = {schema}
                AND table_name = {table}
            ORDER BY table_name, ordinal_position
        '''.format(schema=self.qn(self.database),
                   table=self.qn(table_name))

        keys = ('column_name', 'column_index', 'nullable', 'data_type')

        columns = []
        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            if attrs['nullable'] == 'YES':
                attrs['nullable'] = True
            else:
                attrs['nullable'] = False
            columns.append(attrs)
        return columns

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

    def column_unique_values(self, table_name, column_name):
        query = '''
            SELECT DISTINCT {column} FROM {table}
        '''.format(column=self.qn(column_name),
                   table=self.qn(table_name))

        for row in self.fetchall(query):
            yield row[0]


class Database(base.Node):
    label_attribute = 'database_name'
    branches_property = 'tables'

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


# Exported classes for API
Origin = Database
