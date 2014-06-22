from __future__ import unicode_literals

import graphlib
from ..utils import res, build_uri, PATH_SEPERATOR

try:
    str = unicode
except NameError:
    pass


class Client(object):
    """The client is an interface to the underlying backend whether it
    be a database, file, Web API, etc. The intent of the client is to
    abstract away the mechanisms used to interface with the backend and
    provide a simpler access layer.
    """
    def scheme(self):
        return self.__module__.split('.')[-1]

    def uri(self, path=None):
        host = res(self, 'host')
        port = res(self, 'port')
        scheme = res(self, 'scheme')
        return build_uri(scheme=scheme, host=host, port=port, path=path)


class Rel(graphlib.Rel):
    pass


class Component(graphlib.Node):
    """A component contains attributes and a parent (if not the origin).
    It implements a dict-like interface for accessing the attributes of the
    component.
    """
    name_attribute = 'name'
    label_attribute = 'label'
    match_props = ('uri',)
    relclass = Rel

    def __init__(self, *args, **kwargs):
        self.parent = kwargs.pop('parent', None)
        self.client = kwargs.pop('client', None)
        super(Component, self).__init__(*args, **kwargs)
        self.sync()

    def __str__(self):
        return str(self.name)

    def __bytes__(self):
        return bytes(self.name)

    def __repr__(self):
        typename = self.__class__.__name__
        # Strip off 'u' for unicode literals
        label = repr(self.label).lstrip('u')
        return '{}({})'.format(typename, label)

    def define(self, iterable, klass=None, relprops=None):
        """Create and relate a set components that are structurally or
        logically defined under the current component.

        If `klass` is specified, the current component's class will be used.
        """
        if klass is None:
            klass = self.__class__

        if relprops is None:
            relprops = {}

        if 'type' not in relprops:
            relprops['type'] = klass.__name__.lower()

        for props in iterable:
            instance = klass(props, parent=self, client=self.client)
            self.relate(instance, 'DEFINES', relprops)

    def definitions(self, type=None, sort=None):
        """Return components defined under the current component of the
        specified type and optionally sorting them by a property or function.
        This is the complement function to the `define` method.
        """
        rels = self.rels(type='DEFINES', outgoing=True)

        if type:
            rels = rels.filter('type', type)

        nodes = rels.nodes()

        if sort:
            nodes = nodes.sort(sort)

        return nodes

    # Hierarchy-based properties relative to the DEFINES relationship
    @property
    def root(self):
        "Returns the root component."
        if self.parent:
            return self.parent.root
        return self

    @property
    def isroot(self):
        "Returns true if this component is the root."
        return self.root is self

    @property
    def isleaf(self):
        "Returns true if this component is a leaf."
        return len(self.rels(type='DEFINES', outgoing=True)) == 0

    @property
    def relpath(self):
        "Returns the path of relationships from the root to this component."
        path = []
        parent = self.parent
        current = self
        while parent:
            path.append(parent.rels(node=current, type='DEFINES',
                                    outgoing=True)[0])
            current = parent
            parent = current.parent
        path.reverse()
        return graphlib.Rels(path)

    @property
    def path(self):
        "Returns the pathname of this component from the root."
        names = [str(r.start) for r in self.relpath] + [str(self)]
        return PATH_SEPERATOR.join(names)

    @property
    def uri(self):
        """Returns the URI of this component. This is useful as a
        unique identifier.
        """
        return self.client.uri(self.path)

    @property
    def name(self):
        """Returns the name for this component. The `name_attribute` class
        property can be set to an alternate property key.
        """
        return self.props.get(self.name_attribute)

    @property
    def label(self):
        """Returns the label for this component. The `label_attribute` class
        property can be set to an alternate property key. Falls back to
        this component's `name`.
        """
        label = self.props.get(self.label_attribute)
        if label is None or label == '':
            label = self.name
        return label

    def serialize(self, uri=True, name=True, label=True):
        "Serializes component properties."
        props = self.props.copy()
        if uri:
            props['uri'] = self.uri
        if name:
            props['name'] = self.name
        if label:
            props['label'] = self.label
        return props

    def sync(self):
        "Loads and syncs the immediate relationships for to this component."

    def export(self, resource=None, cls=None):
        """Exports this component as a resource and all outgoing
        relationships and components.
        """
        from origins.resources import export

        if not resource:
            resource = {
                'id': self.uri,
                'label': self.label,
            }

        return export(resource, self, incoming=False, cls=cls)
