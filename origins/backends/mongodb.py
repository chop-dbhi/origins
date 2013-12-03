from __future__ import division, unicode_literals, absolute_import
from ..utils import cached_property
from . import base

import pymongo
from bson.code import Code


# Map-reduce functions for counting the number of fields across documents
# in a collection.
map_fields = Code('''
    function() {
        for (var key in this) {
            emit(key, 1);
        }
    }
''')

count_fields = Code('''
    function(key, values) {
        return Array.sum(values);
    }
''')


class Client(base.Client):
    def __init__(self, database, **kwargs):
        self.database = database
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 27017)
        self.connect(user=kwargs.get('user'), password=kwargs.get('password'))

    def connect(self, user=None, password=None):
        self.connection = pymongo.MongoClient(host=self.host, port=self.port)

    def disconnect(self):
        self.connection.close()

    @property
    def db(self):
        return self.connection[self.database]

    def version(self):
        return self.connection.server_info()

    def database_stats(self):
        return self.db.command('dbstats')

    def collections(self):
        return [{
            'collection_name': n,
        } for n in self.db.collection_names()
            if n != 'system.indexes']

    def collection_size(self, collection_name):
        return self.db[collection_name].count()

    def fields(self, collection_name):
        output = self.db[collection_name]\
            .inline_map_reduce(map_fields, count_fields, full_response=True)

        input_count = output['counts']['input']

        fields = []
        for result in output['results']:
            fields.append({
                'field_name': result['_id'],
                'field_occurrence': result['value'] / input_count
            })
        return fields

    def field_values(self, collection_name, field_name):
        cursor = self.db[collection_name].find({
            field_name: {'$exists': True}
        }, {field_name: True})
        return [r[field_name] for r in cursor]

    def field_unique_values(self, collection_name, field_name):
        return self.db[collection_name].distinct(field_name)


class Database(base.Node):
    label_attribute = 'database_name'
    branches_property = 'collections'

    def synchronize(self):
        self.attrs['database_name'] = self.client.database

    @cached_property
    def collections(self):
        nodes = []
        for attrs in self.client.collections():
            node = Collection(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Collection(base.Node):
    label_attribute = 'collection_name'
    elements_property = 'fields'

    @cached_property
    def fields(self):
        nodes = []
        for attrs in self.client.fields(self['collection_name']):
            node = Field(attrs=attrs, source=self, client=self.client)
            nodes.append(node)
        return base.Container(nodes, source=self)


class Field(base.Node):
    label_attribute = 'field_name'


# Export for API
Origin = Database
