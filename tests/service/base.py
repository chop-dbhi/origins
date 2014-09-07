import json
import unittest
from uritemplate import expand
from origins.service import app
from origins.graph import neo4j


test_client = app.test_client()


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

        # Call requests method
        func = getattr(test_client, method.lower())
        resp = func(path, query_string=params, data=data, headers=headers)

        if resp.data:
            data = json.loads(resp.data.decode('utf8'))
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
