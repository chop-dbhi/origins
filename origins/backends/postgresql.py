from __future__ import division, unicode_literals, absolute_import
from ..utils import cached_property
from . import base, _database

import psycopg2


class Client(_database.Client):
    def __init__(self, database, **kwargs):
        self.database = database
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 5432)
        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        self.connection = psycopg2.connect(database=self.database,
                                           host=self.host,
                                           port=self.port,
                                           user=user,
                                           password=password)

    def qn(self, name):
        return '"{}"'.format(name)

    def version(self):
        return self.fetchvalue('show server_version')

    def schemas(self):
        query = '''
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname <> 'information_schema'
                AND nspname NOT LIKE 'pg_%'
            ORDER BY nspname
        '''

        keys = ('schema_name',)
        schemas = []

        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            schemas.append(attrs)

        return schemas

    def tables(self, schema_name):
        query = '''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
                AND table_schema = %s
            ORDER BY table_name
        '''

        keys = ('table_name',)
        tables = []

        for row in self.fetchall(query, [schema_name]):
            attrs = dict(zip(keys, row))
            tables.append(attrs)

        return tables

    def columns(self, schema_name, table_name):
        query = '''
            SELECT column_name,
                ordinal_position,
                is_nullable,
                data_type
            FROM information_schema.columns
            WHERE table_schema = %s
                AND table_name = %s
            ORDER BY ordinal_position
        '''

        keys = ('column_name', 'column_index', 'nullable', 'data_type')
        columns = []

        for row in self.fetchall(query, [schema_name, table_name]):
            attrs = dict(zip(keys, row))
            attrs['column_index'] -= 1
            if attrs['nullable'] == 'YES':
                attrs['nullable'] = True
            else:
                attrs['nullable'] = False
            columns.append(attrs)

        return columns

    def table_count(self, schema_name, table_name):
        query = '''
            SELECT COUNT(*) FROM {schema}.{table}
        '''.format(schema=self.qn(schema_name),
                   table=self.qn(table_name))
        return self.one(query)[0]

    def column_unique_count(self, schema_name, table_name, column_name):
        query = '''
            SELECT COUNT(DISTINCT {column}) FROM {schema}.{table}
        '''.format(column=self.qn(column_name),
                   schema=self.qn(schema_name),
                   table=self.qn(table_name))
        return self.one(query)[0]

    def column_unique_values(self, schema_name, table_name, column_name,
                             ordered=True):
        query = '''
            SELECT DISTINCT {column} FROM {schema}.{table}
        '''.format(column=self.qn(column_name),
                   schema=self.qn(schema_name),
                   table=self.qn(table_name))
        if ordered:
            query += ' ORDER BY {column}'.format(column=self.qn(column_name))

        for row in self.all(query):
            yield row[0]


class Database(base.Node):
    label_attribute = 'database_name'
    branches_property = 'schemas'

    def synchronize(self):
        self.attrs['database_name'] = self.client.database

    @cached_property
    def schemas(self):
        nodes = []
        for attrs in self.client.schemas():
            node = Schema(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Schema(base.Node):
    branches_property = 'tables'
    label_attribute = 'schema_name'

    @cached_property
    def tables(self):
        nodes = []
        for attrs in self.client.tables(self['schema_name']):
            node = Table(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Table(base.Node):
    elements_property = 'columns'
    label_attribute = 'table_name'

    @cached_property
    def columns(self):
        nodes = []
        for attrs in self.client.columns(self.source['schema_name'],
                                         self['table_name']):
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Column(base.Node):
    label_attribute = 'column_name'


# Export for API
Origin = Database
