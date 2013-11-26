from __future__ import print_function, unicode_literals
from pprint import pformat
from ..exceptions import OriginsError
from ..utils import res


class LazyAttr(object):
    def __repr__(self):
        return '<lazy>'


LAZY_ATTR = LazyAttr()
ID_SEPARATOR = '/'


class Node(object):
    """A node contains attributes and optionally a source. It implements
    a dict-like interface for accessing the attributes of the node.
    """
    id_attribute = 'id'
    label_attribute = 'label'
    lazy_attributes = ()

    branches_property = None
    elements_property = None

    def __init__(self, attrs=None, source=None, client=None):
        if attrs is None:
            attrs = {}
        elif not isinstance(attrs, dict):
            try:
                attrs = dict(attrs)
            except TypeError as e:
                raise TypeError('Invalid type "{}". Attributes must be '
                                'a dict or a sequence of key/value pairs.'
                                .format(type(e)))

        self.attrs = attrs
        self.source = source
        self._client = client

        for attr in self.lazy_attributes:
            self.attrs[attr] = LAZY_ATTR

        self.synchronize()

    def __unicode__(self):
        return unicode(self.label)

    def __bytes__(self):
        return bytes(self.label)

    def __repr__(self):
        return pformat(self.serialize())

    def __getitem__(self, key):
        value = self.attrs.get(key)
        if value is LAZY_ATTR:
            value = res(self, key)
            self.attrs[key] = value
        return value

    def __setitem__(self, key, value):
        self.attrs[key] = value

    def __delitem__(self, key):
        del self.attrs[key]

    def __contains__(self, key):
        return key in self.attrs

    def __iter__(self):
        return iter(self.attrs)

    @property
    def id(self):
        """Returns a unique identifier for this node from the origin.
        By default, the node contains an 'id' attribute, this will be used
        otherwise the id will be generated from the label and ids of all
        ancestors.
        """
        if 'id' in self:
            return self['id']

        label = self.label
        if label is None:
            return ''

        # Prefix with the id of the source
        if self.source:
            path = [self.source.id, label]
        else:
            path = [label]
        return ID_SEPARATOR.join(path)

    def relid(self, root=None):
        "Returns an id relative to the root."
        _id = self.id
        if root is None:
            return _id
        return _id[len(root.id) + len(ID_SEPARATOR):]

    @property
    def label(self):
        """Returns the label for this node. The `label_attribute` class
        property can be specified (defaults to 'label') or this can be
        overridden for custom labels.
        """
        return self.attrs.get(self.label_attribute, '')

    @property
    def client(self):
        """Property for accessing the backend client. Nodes can be initialized
        without a client for programmatically created trees.
        """
        if self._client is None:
            raise OriginsError('Node was initialized without a client')
        return self._client

    def synchronize(self):
        """Synchronizes the node with the backend updating any attributes that
        were derived or computed from the backend. For attributes that are
        expensive, define it as a method and add it as a `lazy_attributes`.
        """

    def serialize(self):
        """Serializes the node's attributes by making a shallow copy. Set deep
        to true to perform a deep copy.
        """
        attrs = {'id': self.id}
        for key in self.attrs:
            attrs[key] = self[key]
        return attrs

    def branches(self):
        "Nodes by default have no branches."
        return []

    def elements(self):
        "Gather up all elements from branches."
        elements = []
        for node in self.branches():
            elements.extend(node.elements())
        return elements
