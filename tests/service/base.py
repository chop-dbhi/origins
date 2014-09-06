import os
import json
import unittest
import requests
from urllib.parse import urljoin
from uritemplate import expand
from origins.graph import neo4j


TEST_URL = os.environ.get('ORIGINS_TEST_URL', 'http://localhost:5000')


class ServiceTestCase(unittest.TestCase):
    path = ''

    def setUp(self):
        neo4j.purge()

    def req(self, method, path=None, urivars=None, data=None, params=None,
            headers=None):

        if path is None:
            path = self.path
        elif isinstance(path, dict):
            urivars = path
            path = self.path

        # Expand path with variables if present
        if urivars:
            path = expand(path, urivars)

        method = method.lower()

        if headers is None:
            headers = {}

        # Default data encoding
        if data is not None and not isinstance(data, str):
            data = json.dumps(data)
            headers.setdefault('Content-Type', 'application/json')

        url = urljoin(TEST_URL, path)

        # Call requests method
        func = getattr(requests, method.lower())
        resp = func(url, params=params, data=data, headers=headers)

        if resp.content:
            data = resp.json()
        else:
            data = None

        return resp, data

    def get(self, *args, **kwargs):
        return self.req('get', *args, **kwargs)

    def post(self, *args, **kwargs):
        return self.req('post', *args, **kwargs)

    def put(self, *args, **kwargs):
        return self.req('put', *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.req('delete', *args, **kwargs)

    def patch(self, *args, **kwargs):
        return self.req('patch', *args, **kwargs)
