from __future__ import division, unicode_literals, absolute_import
from . import _database

import os
import sqlite3


class Client(_database.Client):
    def __init__(self, path, **kwargs):
        self.path = os.path.abspath(path)
        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        self.connection = sqlite3.connect(self.path)

    def database_version(self):
        return self.fetchvalue('PRAGMA schema_version')

    def database_size(self):
        page_size = self.fetchvalue('PRAGMA page_size')
        page_count = self.fetchvalue('PRAGMA page_count')
        return page_size * page_count

    def database(self):
        return {
            'name': os.path.basename(self.path),
            'path': self.path,
            'size': self.database_size(),
            'version': self.database_version(),
        }

    def tables(self):
        query = '''
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            ORDER BY name
        '''

        keys = ('name',)
        tables = []

        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            tables.append(attrs)

        return tables

    def columns(self, table_name):
        query = 'PRAGMA table_info({table})'.format(table=self.qn(table_name))

        keys = ('index', 'name', 'type', 'nullable',
                'default_value', 'primary_key')
        columns = []

        for row in self.fetchall(query):
            attrs = dict(zip(keys, row))
            # Invert boolean
            attrs['nullable'] = not bool(attrs['nullable'])
            columns.append(attrs)

        return columns


# Export for API
Origin = _database.Database
