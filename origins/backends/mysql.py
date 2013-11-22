from __future__ import division
from . import base

import MySQLdb


class Column(base.Node):
    label_attribute = 'column_name'

    def count(self):
        query = '''
            SELECT COUNT(`{table}`.`{column}`)
            FROM `{table}`
        '''.format(column=self['column_name'],
                   table=self['table_name'])
        return self.client.one(query)[0]

    def unique_count(self):
        query = '''
            SELECT COUNT(DISTINCT `{table}`.`{column}`)
            FROM `{table}`
        '''.format(column=self['column_name'],
                   table=self['table_name'])
        return self.client.one(query)[0]

    def values(self):
        query = '''
            SELECT `{table}`.`{column}`
            FROM `{table}`
        '''.format(column=self['column_name'],
                   table=self['table_name'])
        return [r[0] for r in self.client.all(query)]

    def unique_values(self):
        query = '''
            SELECT DISTINCT `{table}`.`{column}`
            FROM `{table}`
        '''.format(column=self['column_name'],
                   table=self['table_name'])
        return [r[0] for r in self.client.all(query)]


class Table(base.Node):
    elements_property = 'columns'
    label_attribute = 'table_name'
    element_class = Column

    def elements(self):
        query = '''
            SELECT table_schema,
                table_name,
                column_name,
                column_default as `default`,
                is_nullable as `nullable`,
                data_type as `type`,
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
        keys = [col[0] for col in c.description]

        for row in c.fetchall():
            attrs = dict(zip(keys, row))
            node = self.element_class(attrs=attrs, source=self,
                                      client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        self.attrs['count'] = self.count()

    def count(self):
        query = 'SELECT COUNT(*) FROM `{}`'.format(self['table_name'])
        return self.client.one(query)[0]


class Database(base.Node):
    label_attribute = 'database'
    branches_property = 'tables'
    branch_class = Table

    def branches(self):
        query = '''
            SELECT table_schema,
                table_name,
                table_rows,
                create_time,
                update_time,
                check_time
            FROM information_schema.tables
            WHERE table_schema = %s
        '''
        c = self.client.connection.cursor()
        c.execute(query, (self['database'],))

        nodes = []
        keys = [col[0] for col in c.description]

        for row in c.fetchall():
            attrs = dict(zip(keys, row))
            node = self.branch_class(attrs=attrs, source=self,
                                     client=self.client)
            nodes.append(node)
        return nodes


# Exported classes for API
Origin = Database


class Client(object):
    """Wrapper for Python DB-API compatible connection objects that removes
    some boilerplate when executing queries.
    """
    def __init__(self, database, **kwargs):
        settings = {
            'db': database,
            'host': kwargs.get('host', ''),
            'port': kwargs.get('port', 3306),
            'user': kwargs.get('user', ''),
            'passwd': kwargs.get('password', ''),
        }
        self.connection = MySQLdb.connect(**settings)

    def all(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchall()

    def one(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchone()
