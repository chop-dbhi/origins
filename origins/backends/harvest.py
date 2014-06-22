from __future__ import unicode_literals, absolute_import

from ..utils import cached_property
from . import base

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

import requests


class Client(base.Client):
    def __init__(self, url, token=None):
        self.url = url
        self.token = token

        self._urls = self._get_urls()

    def _get_urls(self):
        return self.send_request(path='/')

    @cached_property
    def _concepts(self):
        url = self._urls['_links']['concepts']['href']
        return self.send_request(url, params={'embed': 1})

    def send_request(self, url=None, path=None, params=None, data=None):
        headers = {
            'Accept': 'application/json',
        }

        if self.token:
            headers['Api-Token'] = self.token

        if not url:
            url = self.url

        if path:
            url = urljoin(url, path.lstrip('/'))

        response = requests.get(url, params=params, data=data, headers=headers)
        response.raise_for_status()
        return response.json()

    def application(self):
        return {
            'url': self.url,
            'version': self._urls['version'],
        }

    def categories(self):
        categories = []
        unique = set()

        for concept in self._concepts:
            category = concept['category']

            if category and category['id'] not in unique:
                categories.append({
                    'id': category['id'],
                    'name': category['name'],
                    'order': category['order'],
                })
                unique.add(category['id'])

        return sorted(categories, key=lambda x: x['order'])

    def concepts(self, category_id):
        concepts = []

        for concept in self._concepts:
            if not category_id and concept['category']:
                continue
            if category_id and not concept['category'] \
                    or concept['category']['id'] != category_id:
                continue

            concept = concept.copy()
            concept.pop('_links')
            concept.pop('fields')
            concept.pop('category')
            concepts.append(concept)

        return sorted(concepts, key=lambda x: x['order'])

    def fields(self, concept_id):
        fields = []

        for concept in self._concepts:
            if concept_id == concept['id']:
                for field in concept['fields']:
                    field = field.copy()
                    field.pop('_links')
                    field.pop('operators')
                    fields.append(field)

                break

        return fields


class Application(base.Component):
    def sync(self):
        self.props.update(self.client.application())

        categories = self.client.categories()
        self.define(categories, Category)

    @property
    def categories(self):
        return self.definitions('category')


class Category(base.Component):
    def sync(self):
        concepts = self.client.concepts(self['id'])
        self.define(concepts, Concept)

    @property
    def concepts(self):
        return self.definitions('concept')


class Concept(base.Component):
    def sync(self):
        fields = self.client.fields(self['id'])
        self.define(fields, Field)

    @property
    def fields(self):
        return self.definitions('field')


class Field(base.Component):
    pass


Origin = Application
