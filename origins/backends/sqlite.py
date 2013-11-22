from __future__ import division, print_function, unicode_literals, \
    absolute_import
import os
from . import base

import sqlite3


class Database(base.Node):
    branches_property = 'tables'

    @property
    def label(self):
        return os.path.basename(self['path'])

    def branches(self):
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        nodes = []

        for row in self.client.all(query):
            attrs = {'table_name': row[0]}
            node = Table(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        self.attrs['version'] = self.version()
        self.attrs['size'] = self.size()

    def version(self):
        return self.client.one('PRAGMA schema_version')[0]

    def size(self):
        page_size = self.client.one('PRAGMA page_size')[0]
        page_count = self.client.one('PRAGMA page_count')[0]
        return page_size * page_count


class Table(base.Node):
    elements_property = 'columns'
    label_attribute = 'table_name'

    def elements(self):
        c = self.client.connection.cursor()
        c.execute('PRAGMA table_info("{}")'.format(self['table_name']))

        nodes = []
        keys = ('position', 'column_name', 'type', 'nullable',
                'default', 'primary_key')

        for row in c.fetchall():
            attrs = dict(zip(keys, row))
            attrs['nullable'] = not attrs['nullable']  # flip negation
            attrs['table_name'] = self['table_name']
            node = Column(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def synchronize(self):
        self.attrs['count'] = self.count()

    def count(self):
        query = 'select count(*) from "{}"'.format(self['table_name'])
        return self.client.one(query)[0]


class Column(base.Node):
    label_attribute = 'column_name'

    def synchronize(self):
        self.attrs['count'] = self.count()
        self.attrs['unique_count'] = self.unique_count()

    def count(self):
        query = 'select count("{}") from "{}"'\
                .format(self['column_name'], self['table_name'])
        return self.client.one(query)[0]

    def unique_count(self):
        query = 'select count(distinct "{}") from "{}"'\
                .format(self['column_name'], self['table_name'])
        return self.client.one(query)[0]

    def values(self):
        query = 'select "{}" from "{}"'\
                .format(self['column_name'], self['table_name'])
        return [r[0] for r in self.client.all(query)]

    def unique_values(self):
        query = 'select distinct "{}" from "{}"'\
                .format(self['column_name'], self['table_name'])
        return [r[0] for r in self.client.all(query)]


# Exported classes for API
Origin = Database


class Client(object):
    """Wrapper for Python DB-API compatible connection objects that removes
    some boilerplate when executing queries.
    """
    def __init__(self, path, **kwargs):
        self.connection = sqlite3.connect(path)

    def all(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchall()

    def one(self, *args, **kwargs):
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchone()
