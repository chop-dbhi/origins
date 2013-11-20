from __future__ import division, print_function, unicode_literals, \
    absolute_import
from . import base

import pymongo
from bson.code import Code


class Database(base.Node):
    name_attribute = 'database'
    branches_property = 'collections'

    def branches(self):
        names = self.client.db.collection_names()
        nodes = []

        for name in names:
            attrs = {'name': name}
            node = Collection(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def sychronize(self):
        self.attrs.update(self.client.db.command('dbstats'))
        self.attrs['version'] = self.version()

    def version(self):
        return self.client.connection.server_attrs()['version']


class Collection(base.Node):
    elements_property = 'fields'

    _map_fields = Code('''
        function() {
            for (var key in this) {
                emit(key, 1);
            }
        }
    ''')

    _count_fields = Code('''
        function(key, values) {
            var count = 0;
            values.forEach(function() { count++; });
            return count
        }
    ''')

    def elements(self):
        out = self.client.db[self['name']]\
            .inline_map_reduce(self._map_fields, self._count_fields,
                               full_response=True)

        nodes = []

        inputs = out['counts']['input']

        for result in out['results']:
            attrs = {'name': result['_id'],
                     'occurrence': result['value'] / inputs}
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return nodes

    def sychronize(self):
        self.attrs['count'] = self.count()

    def count(self):
        return self.client.db[self['name']].count()


class Field(base.Node):
    def synchronize(self):
        self.attrs['count'] = self.count()
        self.attrs['unique_count'] = self.unique_count()

    def count(self):
        return len(self.values())

    def unique_count(self):
        return len(self.unique_values())

    def values(self):
        cursor = self.client.db[self.source['name']]\
            .find({self['name']: {'$exists': True}},
                  {self['name']: True})
        return [r[self['name']] for r in cursor]

    def unique_values(self):
        return self.client.db[self.source['name']].distinct(self['name'])


# Export for API
Origin = Database


class Client(object):
    def __init__(self, **kwargs):
        settings = {
            'host': kwargs.get('host'),
            'port': kwargs.get('port') or 27017,
        }
        self.connection = pymongo.MongoClient(**settings)
        self.database = kwargs['database']

    @property
    def db(self):
        return self.connection[self.database]
