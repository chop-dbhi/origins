from __future__ import print_function, unicode_literals, absolute_import
from pprint import pformat
from copy import deepcopy


class Node(object):
    """A node contains attributes and optionally a source. It implements
    a dict-like interface for accessing the attributes of the node.
    """
    name_attribute = 'name'
    branches_property = None
    elements_property = None

    def __init__(self, attrs=None, source=None, client=None):
        if attrs is None:
            attrs = {}
        elif not isinstance(attrs, dict):
            try:
                attrs = dict(attrs)
            except TypeError as e:
                raise TypeError('Invalid type "{}". Node attributes must be '
                                'a dict or a sequence of key/value pairs'
                                .format(type(e)))

        self.attrs = attrs
        self.source = source
        self.client = client

        self.synchronize()

    def __getitem__(self, key):
        return self.attrs.get(key, None)

    def __setitem__(self, key, value):
        self.attrs[key] = value

    def __contains__(self, key):
        return key in self.attrs

    def __iter__(self):
        for key in self.attrs:
            yield (key, self.attrs[key])

    def __repr__(self):
        return pformat(self.serialize())

    def __unicode__(self):
        return unicode(self.name)

    def __bytes__(self):
        return bytes(unicode(self))

    @property
    def id(self):
        "Returns a unique identifier for this node relative to the origin."
        path = []
        node = self
        while True:
            path.append(unicode(node))
            if not node.source:
                break
            node = node.source
        return u'.'.join(reversed(path))

    @property
    def name(self):
        "Proxy to the name of this node."
        return self[self.name_attribute]

    def synchronize(self):
        "Implement to synchronize (update) the attributes."

    def serialize(self):
        "Serializes the node's attributes."
        attrs = deepcopy(self.attrs)
        attrs['id'] = self.id
        if self.source:
            attrs['source'] = self.source.id
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
