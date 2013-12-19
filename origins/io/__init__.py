from __future__ import unicode_literals, absolute_import
import warnings
from importlib import import_module
from ..exceptions import OriginsError


class UnknownExporter(OriginsError):
    def __init__(self, name):
        message = 'unknown exporter: {}'.format(name)
        super(UnknownExporter, self).__init__(message)


class ExporterNotSupported(OriginsError):
    def __init__(self, name):
        message = 'exporter not supported: {}'.format(name)
        super(ExporterNotSupported, self).__init__(message)


EXPORTERS = {
    'neo4j': 'origins.io.neo4j',
}


def register_exporter(name, module):
    if name in EXPORTERS:
        warnings.warn('Registering over an existing exporter.', UserWarning)
    EXPORTERS[name] = module


def import_exporter(name):
    "Attempts to import a exporter by name."
    module = EXPORTERS.get(name)

    if not module:
        raise UnknownExporter(name)

    try:
        return import_module(module)
    except ImportError as e:
        raise ExporterNotSupported(unicode(e))


def export(name, node, *args, **kwargs):
    """One-off export of a node for the specified exporter.
    The additional arguments are passed to the exporter's `export` method
    for specifying alternate options for the target.
    """
    module = import_exporter(name)

    # module has an export method, prefer this
    if hasattr(module, 'export'):
        return module.export(node, *args, **kwargs)

    exporter = module.Exporter()
    exporter.prepare(node)
    return exporter.export(*args, **kwargs)


def exporter(name):
    "Returns an instance for the specified exporter."
    module = import_exporter(name)
    return module.Exporter()
