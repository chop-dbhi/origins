from __future__ import unicode_literals, absolute_import

import warnings
from importlib import import_module
from .exceptions import OriginsError

try:
    str = unicode
except NameError:
    pass


class UnknownBackend(OriginsError):
    def __init__(self, backend):
        message = 'unknown backend: {}'.format(backend)
        super(UnknownBackend, self).__init__(message)


class BackendNotSupported(OriginsError):
    def __init__(self, backend):
        message = 'backend not supported: {}'.format(backend)
        super(BackendNotSupported, self).__init__(message)


BACKENDS = {
    'sqlite': 'origins.backends.sqlite',
    'postgresql': 'origins.backends.postgresql',
    'delimited': 'origins.backends.delimited',
    'directory': 'origins.backends.directory',
    'excel': 'origins.backends.excel',
    'mongodb': 'origins.backends.mongodb',
    'mysql': 'origins.backends.mysql',
    'oracle': 'origins.backends.oracle',
    'vcf': 'origins.backends.vcf',
    'redcap-mysql': 'origins.backends.redcap_mysql',
    'redcap-api': 'origins.backends.redcap_api',
    'redcap-csv': 'origins.backends.redcap_csv',
    'noop': 'origins.backends.noop',
}

BACKEND_ALIASES = {
    'redcap': {
        'backend': 'redcap-mysql',
    },
    'csv': {
        'backend': 'delimited',
        'options': {
            'delimiter': b',',
        }
    },
    'tab': {
        'backend': 'delimited',
        'options': {
            'delimiter': b'\t',
        }
    }
}


def register_backend(name, module):
    if name in BACKENDS:
        warnings.warn('Registering over an existing backend.', UserWarning)
    BACKENDS[name] = module


def import_backend(name):
    "Attempts to import a backend by name."
    if name in BACKEND_ALIASES:
        name = BACKEND_ALIASES[name]['backend']

    module = BACKENDS.get(name)

    if not module:
        raise UnknownBackend(name)

    try:
        return import_module(module)
    except ImportError as e:
        raise BackendNotSupported(str(e))


def connect(backend, **options):
    """Connects to the origin of the specified backend.

    `options` are passed to the backend's `Client` which sets up the necessary
    components for the backend to work (database connections, opening file
    handlers, etc.).
    """
    if backend in BACKEND_ALIASES:
        alias = BACKEND_ALIASES[backend]
        backend = alias['backend']
        if 'options' in alias:
            options.update(alias['options'])

    module = import_backend(backend)
    client = module.Client(**options)
    return module.Origin(client=client)
