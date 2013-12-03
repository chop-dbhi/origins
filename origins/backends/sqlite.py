from __future__ import division, print_function, unicode_literals, \
    absolute_import
from ..utils import cached_property
from . import base, _database

import os
import sqlite3


class Client(_database.Client):
    def __init__(self, path, **kwargs):
        self.path = os.path.abspath(path)
        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        self.connection = sqlite3.connect(self.path)

    def qn(self, name):
        return '"{}"'.format(name)

    def version(self):
        return self.fetchvalue('PRAGMA schema_version')

    def database_size(self):
        page_size = self.fetchvalue('PRAGMA page_size')
        page_count = self.fetchvalue('PRAGMA page_count')
        return page_size * page_count

    def tables(self):
        query = '''
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            ORDER BY name
        '''

        keys = ('table_name',)
        tables = []

        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            tables.append(attrs)

        return tables

    def columns(self, table_name):
        query = 'PRAGMA table_info({table})'.format(table=self.qn(table_name))

        keys = ('column_index', 'column_name', 'data_type', 'nullable')
                #'default_value', 'primary_key')
        columns = []

        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            # Invert boolean
            attrs['nullable'] = not bool(attrs['nullable'])
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
    label_attribute = 'database_name'
    branches_property = 'tables'

    def synchronize(self):
        self.attrs['database_path'] = self.client.path
        self.attrs['database_name'] = os.path.basename(self.client.path)

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
