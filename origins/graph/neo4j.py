from __future__ import unicode_literals, absolute_import

try:
    import requests
except ImportError:
    raise ImportError('The requests library is required to use the '
                      'Neo4j module.')

# Alias str to unicode with unicode_literals imported
try:
    str = unicode
except NameError:
    pass


import json
import logging


logger = logging.getLogger(__name__)


# Default URI to Neo4j REST endpoint
DEFAULT_URI = 'http://localhost:7474/db/data/'

# Endpoint for opening a transaction
TRANSACTION_URI_TMPL = '{}transaction'

# Endpoint for the single transaction
SINGLE_TRANSACTION_URI_TMPL = '{}transaction/commit'

# Supported result formats
RESULT_FORMATS = {'row', 'graph', 'REST'}

# Required headers
HEADERS = {
    'accept': 'application/json; charset=utf-8',
    'content-type': 'application/json',
}


class Neo4jError(Exception):
    def __init__(self, errors, *args):
        if isinstance(errors, list):
            message = []

            for error in errors:
                message.append('{code}: {message}\n{stackTrace}'
                               .format(**error))

            message = '\n'.join(message)
        else:
            message = errors

        super(Neo4jError, self).__init__(message, *args)


def normalize_results(response, keys=True):
    if not response:
        return

    result = []

    # Raw results contained in a list..
    for results in response:
        data = results['data']
        columns = results['columns']

        for row in data:
            row = row['row']

            if keys:
                if row and isinstance(row[0], dict):
                    row = row[0]
                elif isinstance(row, list):
                    row = dict(zip(columns, row))

            result.append(row)

    return result


class Transaction(object):
    def __init__(self, uri=None):
        self.uri = uri or DEFAULT_URI
        self.transaction_uri = TRANSACTION_URI_TMPL.format(self.uri)
        self.commit_uri = None

        # transaction is committed or rolled back
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._closed:
            self.commit()

    def _clean_statements(self, statements, params):
        if not statements:
            return []

        # Statement with params
        if isinstance(statements, dict):
            return [statements]

        if isinstance(statements, (list, tuple)):
            _statements = []

            for x in statements:
                _statements.extend(self._clean_statements(x, params))

            return _statements

        # Bare statement
        return [{'statement': str(statements), 'parameters': params}]

    def _clean_formats(self, formats):
        if isinstance(formats, str):
            formats = [formats]

        invalid = [f for f in formats if f not in RESULT_FORMATS]

        if invalid:
            raise Neo4jError('unkown support format(s): {}'
                             .format(', '.join(invalid)))

        return formats

    def _send(self, url, statements=None, params=None, formats=None):
        if self._closed:
            raise Neo4jError('transaction closed')

        statements = self._clean_statements(statements, params)

        payload = {'statements': statements}

        if formats:
            formats = self._clean_formats(formats)
            payload['resultDataContents'] = formats

        data = json.dumps(payload)
        resp = requests.post(url, data=data, headers=HEADERS)

        resp.raise_for_status()
        resp_data = resp.json()

        if resp_data['errors']:
            # Do not shadow the real error
            try:
                self.rollback()
            except Neo4jError:
                pass
            raise Neo4jError(resp_data['errors'])

        return resp, resp_data

    def send(self, statements, params=None, formats=None, raw=False,
             keys=False):
        resp, data = self._send(self.transaction_uri, statements,
                                params=params, formats=formats)

        if 'location' in resp.headers:
            self.transaction_uri = resp.headers['location']

        if 'commit' in data:
            if not self.commit_uri:
                logger.info('begin: {}'.format(data['commit']))

            self.commit_uri = data['commit']

        if raw:
            return data

        return normalize_results(data['results'], keys=keys)

    def commit(self, statements=None, params=None, formats=None, raw=False,
               keys=False):
        if self.commit_uri:
            uri = self.commit_uri
        else:
            uri = SINGLE_TRANSACTION_URI_TMPL.format(self.uri)

        resp, data = self._send(uri, statements, params=params,
                                formats=formats)

        logger.info('commit: {}'.format(uri))

        self._closed = True

        if raw:
            return data

        return normalize_results(data['results'], keys=keys)

    def rollback(self):
        if not self.commit_uri:
            raise Neo4jError('no pending transaction')

        requests.delete(self.transaction_uri, headers=HEADERS)
        logger.info('rollback: {}'.format(self.transaction_uri))

        self._closed = True


def send(statements, params=None, formats=None, uri=None, raw=False,
         keys=False):

    with Transaction(uri) as tx:
        return tx.commit(statements, params=params, formats=formats,
                         raw=raw, keys=keys)
