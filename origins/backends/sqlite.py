from __future__ import division, unicode_literals, absolute_import

from collections import defaultdict
from . import _database

import os
import sqlite3


class Client(_database.Client):
    NUMERIC_TYPES = set([
        'BIGINT',
        'DECIMAL',
        'INT',
        'INTEGER',
        'NUMERIC',
        'REAL',
        'SMALLINT',
    ])

    STRING_TYPES = set([
        'CHAR',
        'TEXT',
        'VARCHAR',
        'NVARCHAR',
        'NCHAR',
    ])

    TIME_TYPES = set([
        'DATE',
        'DATETIME',
        'TIME',
        'TIMESTAMP',
    ])

    BOOL_TYPES = set([
        'BOOL',
        'BOOLEAN',
    ])

    def __init__(self, path, **kwargs):
        self.path = os.path.abspath(path)
        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        self.connection = sqlite3.connect(self.path)

    def foreign_keys_supported(self):
        return bool(self.fetchvalue('PRAGMA foreign_keys'))

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
            WHERE type = ?
            ORDER BY name
        '''

        keys = ('name',)
        tables = []

        for row in self.fetchall(query, ['table']):
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

    def _table_foreign_keys(self, table_name):
        return self.fetchall('PRAGMA foreign_key_list({})'
                             .format(self.qn(table_name)))

    def _foreign_key_index(self):
        query = '''
            SELECT name
            FROM sqlite_master
            WHERE type = ?
        '''

        column_fks = defaultdict(lambda: defaultdict(list))

        for target, in self.fetchall(query, ['table']):
            for values in self._table_foreign_keys(target):
                table, _from, to = values[2:5]
                column_fks[target][to].append({
                    'table': table,
                    'column': _from,
                })

        return column_fks

    def foreign_keys(self, table_name, column_name):
        index = self._foreign_key_index()

        return [{
            'name': 'fk_{}_{}'.format(attrs['table'], attrs['column']),
            'table': attrs['table'],
            'column': attrs['column'],
        } for attrs in index[table_name][column_name]]


# Export for API
Origin = _database.Database
