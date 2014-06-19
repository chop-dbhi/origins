from __future__ import unicode_literals

import graphlib
from ..utils import res, build_uri, id_func_factory, PATH_SEPERATOR

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

    def __eq__(self, other):
        return self.type == other.type and \
            self.start == other.start and \
            self.end == other.end

    def __ne__(self, other):
        return not self.__eq__(other)


class Node(graphlib.Node):
    """A node contains attributes and a parent (if not the origin).
    It implements a dict-like interface for accessing the attributes of the
    node.
    """
    name_attribute = 'name'
    label_attribute = 'label'
    match_props = ('uri',)
    relclass = Rel

    def __init__(self, *args, **kwargs):
        self.parent = kwargs.pop('parent', None)
        self.client = kwargs.pop('client', None)
        super(Node, self).__init__(*args, **kwargs)
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

    def __eq__(self, other):
        return self.__class__.__name__ == other.__class__.__name__ and \
            self.uri == other.uri

    def __ne__(self, other):
        return not self == other

    def define(self, iterable, klass=None, relprops=None):
        """Create and relate a set nodes that are structurally or logically
        defined under the current node. Examples include columns within a
        table or files in a directory.

        If `klass` is specified, the current node's class will be used.
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
        """Return nodes defined under the current node of the specified type
        and optionally sorting them by a property or function. This is the
        complement function to the `define` method.
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
        "Returns the root node."
        if self.parent:
            return self.parent.root
        return self

    @property
    def isroot(self):
        "Returns true if this node is the root."
        return self.root is self

    @property
    def isleaf(self):
        "Returns true if this node is a leaf."
        return len(self.rels(type='DEFINES', outgoing=True)) == 0

    @property
    def relpath(self):
        "Returns the path of relationships from the root to this node."
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
        "Returns the pathname of this node from the root."
        names = [str(r.start) for r in self.relpath] + [str(self)]
        return PATH_SEPERATOR.join(names)

    @property
    def uri(self):
        "Returns the URI of this node. This is useful as a unique identifier."
        return self.client.uri(self.path)

    @property
    def name(self):
        """Returns the name for this node. The `name_attribute` class
        property can be set to an alternate property key.
        """
        return self.props.get(self.name_attribute)

    @property
    def label(self):
        """Returns the label for this node. The `label_attribute` class
        property can be set to an alternate property key. Falls back to
        this node's `name`.
        """
        label = self.props.get(self.label_attribute)
        if label is None or label == '':
            label = self.name
        return label

    def serialize(self, uri=True, name=True, label=True):
        "Serializes node properties."
        props = self.props.copy()
        if uri:
            props['uri'] = self.uri
        if name:
            props['name'] = self.name
        if label:
            props['label'] = self.label
        return props

    def sync(self):
        "Loads and syncs the immediate relationships relative to this node."

    def export(self, id_map=None, resource_id=None, subset=True):
        """Returns a JSON-compatible export of nodes, relationships, and their
        properties. id_map is a dict or function that takes node or rel
        objects and returns ids. If it isn't passed, or when it returns None,
        the default id for the resource is resource.uri, for nodes is
        node.path, and for rels is rel.start.path:rel.type:rel.end.path.
        resource_id is a string to be used as resource_id. If it is None,
        self is assumed the be the resource and id_func (or self.uri by
        default) is used. If the subset flag is True, then incoming 'DEFINES'
        rels are not returned or followed.
        """

        version = 1.0
        components = {}
        relationships = {}

        # Recursion stop condition
        self._exporting = True

        # Sync foreign keys if they exist
        try:
            self.foreign_keys
        except:
            pass

        # Wrap id_map with in a function with defaults
        id_func = id_func_factory(id_map)

        if resource_id:
            self_id = id_func(self)
        else:
            resource_id = self_id = id_func(self, resource=True)

        components[self_id] = {
            'label': self.label,
            'type': self.__class__.__name__,
            'properties': self.props.copy(),
        }

        for rel in self.rels():
            # Implement subset behavior
            if subset and rel.type == 'DEFINES' and self != rel.start:
                continue
            rel_id = id_func(rel)
            relationships[rel_id] = {
                'type': rel.type,
                'start': id_func(rel.start),
                'end': id_func(rel.end),
                'properties': rel.props.copy()
            }

            other = rel.end if rel.start == self else rel.start
            # Restrict recursion to this resource
            if self.root != other.root:
                other_id = id_func(other)
                components[other_id] = {
                    'label': other.label,
                    'type': other.__class__.__name__,
                    'properties': other.props.copy(),
                }

            # Recursion stop condition
            elif not getattr(other, '_exporting', False):
                other_export = other.export(id_map, resource_id=resource_id,
                                            subset=subset)
                components.update(other_export['components'])
                relationships.update(other_export['relationships'])

        self._exporting = False

        return {
            'version': version,
            'resource': resource_id,
            'components': components,
            'relationships': relationships,
        }
