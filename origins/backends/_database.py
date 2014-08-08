from __future__ import unicode_literals, absolute_import

import re
import logging
from . import base

try:
    str = unicode
except NameError:
    pass

logger = logging.getLogger(__name__)


class Client(base.Client):
    """Client specific for relational database backends that conform to
    the Python DB API.

    The `connect` method must set the `connection` property which is a
    connection to the database.
    """
    STRING_TYPES = set()
    NUMERIC_TYPES = set()
    TIME_TYPES = set()
    BOOL_TYPES = set()

    def disconnect(self):
        self.connection.close()

    def qn(self, name):
        return '"{}"'.format(name)

    def fetchall(self, *args, **kwargs):
        "Returns all rows."
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchall()

    def fetchone(self, *args, **kwargs):
        "Returns the fist row"
        c = self.connection.cursor()
        c.execute(*args, **kwargs)
        return c.fetchone()

    def fetchvalue(self, *args, **kwargs):
        "Returns the first value from the first row."
        row = self.fetchone(*args, **kwargs)
        if row:
            return row[0]

    def _parse_dtype(self, dtype):
        match = re.match(r'^([a-z]+)', dtype, re.I)
        if match:
            return match.group()

    def is_string_type(self, dtype):
        "Returns true if the type is a string type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.STRING_TYPES

    def is_time_type(self, dtype):
        "Returns true if the type is a date or time type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.TIME_TYPES

    def is_numeric_type(self, dtype):
        "Returns true if the type is a numeric type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.NUMERIC_TYPES

    def is_bool_type(self, dtype):
        "Returns true if the type is a boolean type."
        dtype = self._parse_dtype(dtype)
        return dtype in self.BOOL_TYPES


class Database(base.Component):
    def sync(self):
        self.update(self.client.database())
        self.define(self.client.tables(), Table)

    @property
    def tables(self):
        return self.definitions('table', sort='name')


class Table(base.Component):
    def sync(self):
        self.define(self.client.columns(self['name']), Column)

    @property
    def columns(self):
        return self.definitions('column', sort='index')


class Column(base.Component):
    def sync(self):
        self._foreign_keys_synced = False

    @property
    def foreign_keys(self):
        if not self._foreign_keys_synced:
            root = self.root
            table_name = self.parent['name']

            for attrs in self.client.foreign_keys(table_name, self['name']):
                # Get referenced node
                node = root.tables[attrs['table']].columns[attrs['column']]

                self.relate(node, 'REFERENCES', {
                    'name': attrs['name'],
                    'type': 'foreignkey',
                })

            self._foreign_keys_synced = True
        return self.rels(type='REFERENCES', outgoing=True)\
            .filter('type', 'foreignkey').nodes()
