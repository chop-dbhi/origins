from __future__ import unicode_literals, absolute_import
from . import base

from solvebio.core.client import SolveClient


class Client(base.Client):
    def __init__(self, api_key=None):
        self.client = SolveClient(api_key)

    def service(self):
        return {'name': 'Solvebio API'}

    def namespaces(self):
        return self.client.get_namespaces()

    def datasets(self, namespace_name):
        namespace = self.client.get_namespace(namespace_name)
        return namespace['datasets']

    def fields(self, namespace_name, dataset_name):
        dataset = self.client.get_dataset(namespace_name, dataset_name)
        return dataset['fields']

    def facets(self, namespace_name, dataset_name, field_name):
        field = self.client.get_dataset_field(namespace_name, dataset_name,
                                              field_name)
        if not field or not field['facets']:
            return {}

        if field['data_type'] == 'string':
            return {'values': field['facets']}

        if field['data_type'] in ('integer', 'double', 'long', 'float'):
            return {
                'min_value': field['facets'][0],
                'max_value': field['facets'][1],
            }


class Service(base.Component):
    def sync(self):
        self.update(self.client.service())
        self.define(self.client.namespaces(), Namespace)

    @property
    def namespaces(self):
        return self.definitions('namespace')


class Namespace(base.Component):
    label_attribute = 'title'

    def sync(self):
        self.define(self.client.datasets(self['name']), Dataset)

    @property
    def datasets(self):
        return self.definitions('dataset')


class Dataset(base.Component):
    def sync(self):
        self.define(self.client.fields(self.parent['name'], self['name']),
                    Field)

    @property
    def fields(self):
        return self.definitions('field')


class Field(base.Component):
    def sync(self):
        namespace = self.parent.parent['name']
        dataset = self.parent['name']
        self.client.facets(namespace, dataset, self['name'])


Origin = Service
