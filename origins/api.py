from __future__ import unicode_literals, absolute_import
from importlib import import_module
from .exceptions import UnknownBackend, BackendNotSupported


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
    'redcap': 'origins.backends.redcap_mysql',
    'redcap-api': 'origins.backends.redcap_api',
}

BACKEND_ALIASES = {
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
        raise BackendNotSupported(unicode(e))


def connect(backend, **options):
    """Connects to the origin of the specified backend.

    `options` are passed to the backend's `Client` which sets up the necessary
    components for the backend to work (database connections, opening file
    handlers, etc.) It can optionally contain an `attrs` argument that will be
    passed to the `Origin` node on initialization.
    """
    if backend in BACKEND_ALIASES:
        alias = BACKEND_ALIASES[backend]
        backend = alias['backend']
        options.update(alias['options'])

    module = import_backend(backend)

    # Pop out the attributes for the origin
    attrs = options.pop('attrs', None)
    client = module.Client(**options)
    return module.Origin(attrs=attrs, client=client)
