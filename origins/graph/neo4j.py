import math
import json
import logging
import requests
import atexit
from origins import config


logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# Default number of statements that will be sent in one request
DEFAULT_BATCH_SIZE = 100

# Endpoint for the REST service
DEFAULT_URI_TMPL = '{scheme}://{host}:{port}/db/data/'

# Endpoint for opening a transaction
TRANSACTION_URI_TMPL = '{}transaction'

# Endpoint for the single transaction
SINGLE_TRANSACTION_URI_TMPL = '{}transaction/commit'

# Required headers
HEADERS = {
    'accept': 'application/json; charset=utf-8',
    'content-type': 'application/json',
    'x-stream': 'true',
}


class Neo4jError(Exception):
    def __init__(self, errors, *args):
        if isinstance(errors, list):
            message = []

            for error in errors:
                error.setdefault('stackTrace', '')
                message.append('{code}: {message}\n{stackTrace}'
                               .format(**error))

            message = '\n'.join(message)
        else:
            message = errors

        super(Neo4jError, self).__init__(message, *args)


def _normalize_results(response):
    "Flattens the results of the Neo4j REST response."
    if not response:
        return

    result = []

    for results in response:
        for row in results['data']:
            result.append(row['row'])

    return result


def _normalize_statements(statements, parameters):
    if not statements:
        return []

    # Statement with parameters
    if isinstance(statements, dict):
        return [statements]

    if isinstance(statements, (list, tuple)):
        _statements = []

        for x in statements:
            _statements.extend(_normalize_statements(x, parameters))

        return _statements

    # Bare statement
    return [{
        'statement': str(statements),
        'parameters': parameters,
    }]


def _merge_response(output, data):
    if output is None:
        return data

    output['results'].extend(data['results'])
    output['errors'].extend(data['errors'])

    if 'transaction' in data:
        output['transaction'] = data['transaction']

    return output


def _transaction_exit(tx):
    if not tx._closed:
        if tx._queue:
            logger.error('transaction has unsent statements')

        if tx.commit_uri:
            logger.error('rolling back uncommitted transaction')
            tx.rollback()


class Client(object):
    def __init__(self, uri=None, host=None, port=None, scheme='http'):
        if not uri:
            uri = DEFAULT_URI_TMPL.format(host=host, port=port, scheme=scheme)

        self.uri = uri

    def transaction(self, batch_size=None, autocommit=False):
        return Transaction(self, batch_size, autocommit)


class Transaction(object):
    def __init__(self, client, batch_size=None, autocommit=False):
        if not batch_size:
            batch_size = DEFAULT_BATCH_SIZE

        self.client = client
        self.transaction_uri = TRANSACTION_URI_TMPL.format(client.uri)
        self.commit_uri = None
        self.batch_size = batch_size
        self.autocommit = autocommit

        # The current queue of statements and number of batches that have
        # been sent by this transaction.
        self._queue = []
        self._batches = 0

        # Transaction is committed or rolled back
        self._closed = False

        # Track the depth of a transaction to prevent it from being committed
        # in sub-context managers.
        self._depth = 0

        # Rollback uncommitted state on exit to prevent blocking subsequent
        # access
        atexit.register(_transaction_exit, self)

    def __enter__(self):
        self._depth += 1

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._depth -= 1

        if not self._closed:
            # Handle error if it occurs
            if exc_type and self.commit_uri:
                self.rollback()

            # Otherwise commit if any statements have been sent or queued
            elif self._depth == 0 and (self._batches > 0 or self._queue):
                self.commit()

    def _close(self):
        if self.autocommit:
            self.transaction_uri = TRANSACTION_URI_TMPL.format(self.client.uri)
            self.commit_uri = None
            self._batches = 0
        else:
            self._closed = True

    def _send_request(self, url, payload):
        "Sends a request to the server."
        # Prevent overhead of serialization
        if logger.level <= logging.DEBUG:
            logger.debug(json.dumps(payload, indent=4))

        data = json.dumps(payload)
        resp = requests.post(url, data=data, headers=HEADERS)

        resp.raise_for_status()
        resp_data = resp.json()

        if resp_data['errors']:
            raise Neo4jError(resp_data['errors'])

        return resp, resp_data,

    def _send(self, statements, parameters, commit=False, defer=False):
        if self._closed:
            raise Neo4jError('transaction closed')

        statements = _normalize_statements(statements, parameters)

        self._queue.extend(statements)

        # Wait to send the requests
        if defer:
            return

        statements = self._queue
        self._queue = []
        resp_data = None

        if self.batch_size:
            batch_size = self.batch_size
        else:
            batch_size = len(statements)

        # Send at least one request
        batches = max(1, int(math.ceil(len(statements) / batch_size)))

        for i in range(batches):
            # Reuse or open a transaction for more than one batch. Otherwise
            # commit for the final batch or send a single request
            if commit and i == batches - 1:
                if self.commit_uri:
                    url = self.commit_uri
                else:
                    url = SINGLE_TRANSACTION_URI_TMPL.format(self.client.uri)

                logger.debug('commiting batch {}/{} to {}'
                             .format(i + 1, batches, url))
            else:
                url = self.transaction_uri

                logger.debug('sending batch {}/{} to {}'
                             .format(i + 1, batches, url))

            start, end = i * self.batch_size, (i + 1) * self.batch_size

            data = {'statements': statements[start:end]}

            resp, _resp_data = self._send_request(url, data)
            resp_data = _merge_response(resp_data, _resp_data)

            # Implicit switch to transaction URL
            if 'location' in resp.headers:
                url = self.transaction_uri = resp.headers['location']

            if 'commit' in resp_data:
                self.commit_uri = resp_data['commit']

            self._batches += 1

        return resp_data

    def send(self, statements, parameters=None, defer=False):
        """Sends statements to an existing transaction or opens a new one.

        This must be followed by `commit` or `rollback` to close the
        transaction, otherwise the transaction will timeout on the server
        and implicitly rolled back.
        """
        if not defer:
            if self.autocommit and self._depth == 0:
                return self.commit(statements, parameters)

        try:
            data = self._send(statements, parameters=parameters, defer=defer)
        except Exception:
            if self.commit_uri:
                self.rollback()
            raise

        if defer:
            return

        if 'commit' in data:
            self.commit_uri = data['commit']

        return _normalize_results(data['results'])

    def commit(self, statements=None, parameters=None):
        "Commits an open transaction or performs a single transaction request."
        try:
            data = self._send(statements, parameters=parameters, commit=True)
        except Exception:
            if self.commit_uri:
                self.rollback()
            raise

        self._close()

        return _normalize_results(data['results'])

    def rollback(self):
        if not self.commit_uri:
            raise Neo4jError('no pending transaction')

        requests.delete(self.transaction_uri, headers=HEADERS)
        logger.debug('rollback: {}'.format(self.transaction_uri))

        self._close()


def purge(*args, **kwargs):
    "Deletes all nodes and relationships."
    tx.send('MATCH (n) '
            'OPTIONAL MATCH (n)-[r]-() '
            'DELETE r, n ',
            *args, **kwargs)


def debug():
    logger.setLevel(logging.DEBUG)


# Initialize the default client
client = Client(**config.options['neo4j'])

# Default transaction with auto-commit enabled
tx = client.transaction(autocommit=True)
