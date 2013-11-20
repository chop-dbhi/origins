from __future__ import division, print_function, unicode_literals, \
    absolute_import
from . import base

import psycopg2


class Database(base.Node):
    name_attribute = 'database'
    branches_property = 'schemas'

    def branches(self):
        query = '''
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name <> 'information_schema'
                AND schema_name NOT LIKE 'pg_%'
        '''
        c = self.client.connection.cursor()
        c.execute(query)

        nodes = []
        keys = [col.name for col in c.description]

        for row in c.fetchall():
            attrs = dict(zip(keys, row))
            node = Schema(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        self.attrs['version'] = self.version()
        self.attrs['size'] = self.size()

    def version(self):
        return self.client.one('show server_version')[0]

    def size(self):
        query = "select pg_database_size('{}')".format(self['database'])
        return self.client.one(query)[0]


class Schema(base.Node):
    branches_property = 'tables'
    name_attribute = 'schema_name'

    def branches(self):
        query = '''
            SELECT table_name,
                table_schema
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
                AND table_schema = %s
        '''
        c = self.client.connection.cursor()
        c.execute(query, (self['schema_name'],))

        nodes = []
        keys = [col.name for col in c.description]

        for row in c.fetchall():
            attrs = dict(zip(keys, row))
            node = Table(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes


class Table(base.Node):
    elements_property = 'columns'
    name_attribute = 'table_name'

    def elements(self):
        query = '''
            SELECT table_schema,
                table_name,
                column_name,
                column_default as default,
                is_nullable as nullable,
                data_type as type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                datetime_precision
            FROM information_schema.columns
            WHERE table_schema = %s
                AND table_name = %s
            ORDER BY ordinal_position
        '''
        c = self.client.connection.cursor()
        c.execute(query, (self['table_schema'], self['table_name']))

        nodes = []
        keys = [col.name for col in c.description]

        for row in c.fetchall():
            attrs = dict(zip(keys, row))
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        self.attrs['count'] = self.count()

    def count(self):
        query = '''
            SELECT COUNT(*) FROM "{}"."{}"
        '''.format(self['table_schema'], self['table_name'])
        return self.client.one(query)[0]


class Column(base.Node):
    name_attribute = 'column_name'

    def count(self):
        query = '''
            SELECT COUNT("{schema}"."{table}"."{column}")
            FROM "{schema}"."{table}"
        '''.format(column=self['column_name'],
                   schema=self['table_schema'],
                   table=self['table_name'])
        return self.client.one(query)[0]

    def unique_count(self):
        query = '''
            SELECT COUNT(DISTINCT "{schema}"."{table}"."{column}")
            FROM "{schema}"."{table}"
        '''.format(column=self['column_name'],
                   schema=self['table_schema'],
                   table=self['table_name'])
        return self.client.one(query)[0]

    def values(self):
        query = '''
            SELECT "{schema}"."{table}"."{column}"
            FROM "{schema}"."{table}"
        '''.format(column=self['column_name'],
                   schema=self['table_schema'],
                   table=self['table_name'])
        return [r[0] for r in self.client.all(query)]

    def unique_values(self):
        query = '''
            SELECT DISTINCT "{schema}"."{table}"."{column}"
            FROM "{schema}"."{table}"
        '''.format(column=self['column_name'],
                   schema=self['table_schema'],
                   table=self['table_name'])
        return [r[0] for r in self.client.all(query)]


# Exported classes for API
Origin = Database


class Client(object):
    """Wrapper for Python DB-API compatible connection objects that removes
    some boilerplate when executing queries.
    """
    def __init__(self, **kwargs):
        settings = {
            'host': kwargs.get('host'),
            'port': kwargs.get('port'),
            'database': kwargs.get('database'),
            'user': kwargs.get('user'),
            'password': kwargs.get('password'),
        }
        self.connection = psycopg2.connect(**settings)

    def all(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchall()

    def one(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchone()
