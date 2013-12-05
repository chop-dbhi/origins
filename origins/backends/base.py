from __future__ import unicode_literals, absolute_import
import urllib
from pprint import pformat
from collections import OrderedDict
from ..utils import res, cached_property

PATH_SEPERATOR = '/'


def build_uri(scheme, host=None, port=None, path=None):
    """Builds an Origins-based URI which acts as a unique identifier
    for external use.
    """
    if not scheme:
        raise ValueError('cannot have an empty scheme')

    host = host or ''

    if not path:
        path = PATH_SEPERATOR
    elif not path.startswith(PATH_SEPERATOR):
        path = PATH_SEPERATOR + path

    if port:
        netloc = '{}:{}'.format(host, port)
    else:
        netloc = host

    return '{}://{}{}'.format(scheme, netloc, urllib.quote(path))


class Client(object):
    def connect(self, **kwargs):
        pass

    def disconnect(self):
        pass

    def version(self):
        pass

    def scheme(self):
        return self.__module__.split('.')[-1]

    def uri(self, path=None):
        host = res(self, 'host')
        port = res(self, 'port')
        scheme = res(self, 'scheme')
        return build_uri(scheme=scheme, host=host, port=port, path=path)


class Node(object):
    """A node contains attributes and a source (if not the origin).
    It implements a dict-like interface for accessing the attributes of the
    node.

    Each node identifies itself by it's `uri` which is constructed relative to
    the origin and client.
    """
    label_attribute = 'label'
    path_attribute = None

    elements_property = None
    branches_property = None

    def __init__(self, attrs=None, source=None, client=None):
        if attrs is None:
            self.attrs = {}
        else:
            self.attrs = dict(attrs)

        self.source = source
        self.client = client
        self.synchronize()

    def __unicode__(self):
        return unicode(self.label)

    def __bytes__(self):
        return bytes(self.label)

    def __repr__(self):
        return pformat(self.attrs)

    def __getitem__(self, key):
        return self.attrs[key]

    def __setitem__(self, key, value):
        self.attrs[key] = value

    def __delitem__(self, key):
        del self.attrs[key]

    def __contains__(self, key):
        return key in self.attrs

    def __iter__(self):
        return iter(self.attrs)

    @property
    def origin(self):
        "Returns the origin of this node."
        if self.source:
            return self.source.origin
        return self

    @property
    def label(self):
        """Returns the label for this node. The `label_attribute` class
        property can be specified (defaults to 'label') or this can be
        overridden for a custom label.
        """
        return self.attrs.get(self.label_attribute)

    @cached_property
    def uri(self):
        "Returns a unique identifier for this node from the origin."
        return self.client.uri(path=self.path())

    def path(self, root=None):
        "Returns the path to this node relative to `root` or the origin."
        path = self.attrs.get(self.path_attribute or self.label_attribute)

        if root is self:
            return path

        if self.source:
            path = PATH_SEPERATOR.join([self.source.path(), path])

        if root:
            return path[len(root.path()) + len(PATH_SEPERATOR):]
        return path

    def synchronize(self):
        """Synchronize is used update the node's attributes with data from
        the backend such as statistics and new descriptors.
        """

    def serialize(self, label=False, uri=False, source=False):
        """Serializes the node's attributes. If `source` is true, attributes
        from the node's ancestors will be serialized and merged into this
        node's output.
        """
        attrs = self.attrs.copy()

        # Optional attributes
        if label:
            attrs['label'] = self.label
        if uri:
            attrs['uri'] = self.uri
        if source and self.source:
            attrs.update(self.source.serialize(source=True))

        return attrs

    @property
    def isorigin(self):
        return self.origin is self

    @property
    def iselement(self):
        return not self.elements

    @property
    def isbranch(self):
        return not self.isorigin and not self.iselement

    @property
    def elements(self):
        """Generic property for accessing the elements relative to this node.
        By default elements will be aggregated from the branches of this node."
        """
        if self.elements_property:
            return getattr(self, self.elements_property, None)

        branches = self.branches

        if branches:
            nodes = []
            for branch in branches:
                nodes.extend(list(branch.elements))
            return Container(nodes, source=self)

    @property
    def branches(self):
        "Generic property to access the branches of this node."
        if self.branches_property:
            return getattr(self, self.branches_property, None)


class Container(object):
    """Immutable sequence of nodes. Nodes are ordered in the order they
    were extracted from the backend. They can be accessed by index or by key,
    where the key is the path relative `source`.
    """
    __slots__ = ('_nodes',)

    def __init__(self, nodes=None, source=None):
        if nodes is None:
            nodes = ()
        self._nodes = OrderedDict([
            (node.path(source), node) for node in nodes
        ])

    def __getitem__(self, key):
        # Return node by key
        if key in self._nodes:
            return self._nodes[key]
        # Return node by index
        if isinstance(key, int):
            if key < len(self):
                return self._nodes[self._nodes.keys()[key]]
            raise IndexError('collection index out of range')
        raise KeyError(key)

    def __contains__(self, key):
        return key in self._nodes

    def __iter__(self):
        for key in self._nodes:
            yield self._nodes[key]

    def __len__(self):
        return len(self._nodes)

    def __repr__(self):
        return pformat(self._nodes.keys())

    def __unicode__(self):
        return ', '.join(unicode(n) for n in self)

    def __bytes__(self):
        return ', '.join(bytes(n) for n in self)
