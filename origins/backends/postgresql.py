from __future__ import division, unicode_literals, absolute_import
from . import base, _database

import psycopg2


class Client(_database.Client):
    def __init__(self, database, **kwargs):
        self.name = database
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 5432)
        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        self.connection = psycopg2.connect(database=self.name,
                                           host=self.host,
                                           port=self.port,
                                           user=user,
                                           password=password)

    def version(self):
        return self.fetchvalue('show server_version')

    def database(self):
        return {
            'name': self.name,
            'host': self.host,
            'port': self.port,
            'version': self.version(),
        }

    def schemas(self):
        query = '''
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname <> 'information_schema'
                AND nspname NOT LIKE 'pg_%'
            ORDER BY nspname
        '''

        keys = ('name',)
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

        keys = ('name',)
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

        keys = ('name', 'index', 'nullable', 'type')
        columns = []

        for row in self.fetchall(query, [schema_name, table_name]):
            attrs = dict(zip(keys, row))
            # Postgres column index are 1-based; this changes to 0-based
            attrs['index'] -= 1
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
    def sync(self):
        self.update(self.client.database())
        self._contains(self.client.schemas(), Schema)

    @property
    def schemas(self):
        return self._containers('schema')

    @property
    def tables(self):
        # TODO, should this look up the user's search path?
        default_schema = self.schemas['public']
        return default_schema._containers('table')


class Schema(base.Node):
    def sync(self):
        self._contains(self.client.tables(self['name']), Table)

    @property
    def tables(self):
        return self._containers('table')


class Table(_database.Table):
    def sync(self):
        self._contains(self.client.columns(self.parent['name'], self['name']),
                       _database.Column)


# Export for API
Origin = Database
