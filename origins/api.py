from __future__ import print_function, unicode_literals
from importlib import import_module
from pprint import pformat
from .exceptions import UnknownBackend, BackendNotSupported
from .utils import parse_uri, res, cached_property


BACKENDS = {
    'sqlite': 'origins.backends.sqlite',
}


def register_backend(name, module):
    BACKENDS[name] = module


def import_backend(name):
    "Attempts to import a backend by name."
    module = BACKENDS.get(name)

    if not module:
        raise UnknownBackend(name)

    try:
        return import_module(module)
    except ImportError as e:
        raise BackendNotSupported(unicode(e))


def check_backend(name):
    """Checks if a backend is supported by importing the module.
    Returns none if successful, other the reason for it not being supported
    will be returned.
    """
    try:
        import_backend(name)
    except (UnknownBackend, BackendNotSupported) as e:
        return unicode(e)


def connect(uri, **options):
    "Connects to a backend and returns the origin node."
    if '://' not in uri:
        defaults = {'backend': uri}
    else:
        defaults = parse_uri(uri or '')
        defaults['backend'] = defaults['scheme']

    backend = import_backend(defaults['backend'])

    options.update(defaults)

    client = backend.Client(**options)
    origin = backend.Origin(attrs=options, client=client)

    return Node(origin)


class Node(object):
    def __init__(self, node):
        self._node = node

        if self._node.branches_property:
            setattr(self, self._node.branches_property, self.branches)

        if self._node.elements_property:
            setattr(self, self._node.elements_property, self.elements)

    def __repr__(self):
        return repr(self._node)

    def __unicode__(self):
        return unicode(self._node)

    def __bytes__(self):
        return bytes(self._node)

    def __getitem__(self, key):
        return self._node[key]

    def __contains__(self, key):
        return self._node[key]

    def __iter__(self):
        return iter(self._node)

    @property
    def id(self):
        return res(self._node, 'id')

    @property
    def name(self):
        return res(self._node, 'name')

    @cached_property
    def branches(self):
        nodes = res(self._node, 'branches')
        return Container(nodes, source=self)

    @cached_property
    def elements(self):
        nodes = res(self._node, 'elements')
        return Container(nodes, source=self)


class Container(object):
    "A container provides a dict-like interface that to a set of nodes."
    __slots__ = ('_nodes', 'source')

    def __init__(self, nodes=None, source=None):
        self.source = source
        if nodes is None:
            nodes = ()
        self._nodes = {
            node.id: Node(node) for node in nodes
        }

    def __getitem__(self, key):
        key = '{}.{}'.format(self.source._node.id, key)
        return self._nodes.get(key, None)

    def __contains__(self, key):
        key = '{}.{}'.format(self.source._node.id, key)
        return key in self._nodes

    def __iter__(self):
        for key in self._nodes:
            yield self._nodes[key]

    def __repr__(self):
        prefix = len(self.source.id) + 1  # account for delimiter
        return pformat([n.id[prefix:] for n in self._nodes.values()])

    def __unicode__(self):
        return ', '.join(unicode(n) for n in self)

    def __bytes__(self):
        return ', '.join(bytes(n) for n in self)

    def __len__(self):
        return len(self._nodes)
