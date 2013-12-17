from __future__ import unicode_literals, absolute_import
import re
import inspect
from collections import defaultdict


class Uid(object):
    "Unique id generator."
    def __init__(self):
        self.i = 0

    def __call__(self):
        i = self.i
        self.i += 1
        return i


uid = Uid()


class Rel(object):
    __slots__ = ('id', 'start', 'end', 'type', 'props')

    def __init__(self, start, end, type, props=None):
        self.id = uid()
        self.start = start
        self.end = end
        self.type = type
        if props is None:
            self.props = {}
        else:
            self.props = dict(props)

    def __repr__(self):
        return '{}({}-{}-{})'.format(self.__class__.__name__, self.start,
                                     self.type, self.end)

    def __getitem__(self, key):
        return self.props[key]

    def __setitem__(self, key, value):
        self.props[key] = value

    def __delitem__(self, key):
        del self.props[key]

    def __contains__(self, key):
        return key in self.props

    def __iter__(self):
        return iter(self.props)

    def update(self, props):
        self.props.update(props)

    def related(self):
        "Returns true if this relationship is currently related."
        return self.start.related(self.end, self.type)

    def unrelate(self):
        "Disassociates this relationship from the start node."
        return self.start.unrelate(self)

    def relate(self):
        "Reassociates this relationship to the start node."
        self.start._add_rel(self)


class Node(object):
    __slots__ = ('id', 'props', '_rels', '_types')

    def __init__(self, props=None):
        self.id = uid()
        if props is None:
            self.props = {}
        else:
            self.props = dict(props)
        # Nested hash of relationships by node id then type. Currently a
        # only a single relationship of the same type can be defined between
        # the same two nodes.
        self._rels = defaultdict(dict)
        # Hash of relationship types with a set of node ids.
        self._types = defaultdict(set)

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self)

    def __getitem__(self, key):
        return self.props[key]

    def __setitem__(self, key, value):
        self.props[key] = value

    def __delitem__(self, key):
        del self.props[key]

    def __contains__(self, key):
        return key in self.props

    def __iter__(self):
        return iter(self.props)

    def update(self, props):
        self.props.update(props)

    def _get_rels_for_type(self, type):
        rels = []
        for id in self._types[type]:
            rels.append(self._rels[id][type])
        return rels

    def _get_rels_for_node(self, id):
        rels = []
        for type in self._rels[id]:
            rels.append(self._rels[id][type])
        return rels

    def _get_rels_for_node_and_type(self, id, type):
        rel = self._rels[id].get(type)
        if rel:
            return [rel]
        return []

    def _get_rels(self):
        rels = []
        for id in self._rels:
            rels.extend(self._rels[id].values())
        return rels

    def _add_rel(self, rel):
        node = rel.end
        self._rels[node.id][rel.type] = rel
        self._types[rel.type].add(node.id)

    def _prune(self):
        "Removes empty entries in _rels and _types."
        for id in self._rels.keys():
            if not self._rels[id]:
                del self._rels[id]

        for type in self._types.keys():
            if not self._types[type]:
                del self._types[type]

    def _del_rel(self, id, type):
        del self._rels[id][type]
        self._types[type].remove(id)

    def _del_rels_for_type(self, type):
        "Deletes all relationships of type."
        count = 0
        for id in self._types[type].keys():
            self._del_rel(id, type)
            count += 1
        return count

    def _del_rels_for_node(self, id):
        "Deletes all relationships for node."
        count = 0
        for type in self._rels[id].keys():
            self._del_rel(id, type)
            count += 1
        return count

    def _del_rels_for_node_and_type(self, id, type):
        "Deletes relationship for node and type."
        if id not in self._rels or type not in self._rels[id]:
            return 0
        self._del_rel(id, type)
        return 1

    def _del_rels(self):
        "Deletes all relationships."
        count = 0
        for id in self._rels.keys():
            for type in self._rels[id].keys():
                self._del_rel(id, type)
                count += 1
        return count

    def relate(self, node, type, props=None):
        "Adds a relationship to node if it does not already exist."
        assert isinstance(node, Node), 'end node must be a Node instance'
        assert isinstance(type, (str, unicode)), 'type must be a string'
        if type in self._types and node.id in self._types[type]:
            rel = self._rels[node.id][type]
            if props:
                rel.update(props)
        else:
            rel = Rel(self, node, type, props)
            self._add_rel(rel)
        return rel

    def unrelate(self, node=None, type=None):
        "Deletes a relationship."
        if node and type:
            count = self._del_rels_for_node_and_type(node.id, type)
        elif node:
            count = self._del_rels_for_node(node.id)
        elif type:
            count = self._del_rels_for_type(type)
        else:
            count = self._del_rels()
        self._prune()
        return count

    def related(self, node, type=None):
        "Returns true if the node is related, optionally by a type."
        related = node.id in self._rels or self.id in node._rels
        if related and type:
            return type in self._rels[node.id] or type in node._rels[self.id]
        return related

    def rels(self, node=None, type=None):
        "Returns relations for the node, optionally filtered by type."
        if node and type:
            rels = self._get_rels_for_node_and_type(node.id, type)
        elif node:
            rels = self._get_rels_for_node(node.id)
        elif type:
            rels = self._get_rels_for_type(type)
        else:
            rels = self._get_rels()
        return Rels(rels)


class Rels(tuple):
    "Tuple of relationships that supports filtering and node extraction."
    def filter(self, key, value=None):
        """Filters the node in this container and returns a new container.
        The most common filtering is by property, so a key and value can be
        supplied as a shorthand, otherwise a filter function must be passed.
        """
        if isinstance(key, (bytes, unicode)):
            def func(node):
                return key in node and node[key] == value
        else:
            func = key
            if not inspect.isfunction(func):
                raise TypeError('filter requires key/value or function')
        return self.__class__(filter(func, self))

    def nodes(self):
        return Nodes(rel.end for rel in self)


class Nodes(tuple):
    """Immutable sequence of nodes which supports dict-like access. Nodes are
    ordered in the order they were provided. They can be accessed by index
    or by key, where the key is the `unicode` representation of the node.
    """
    def __init__(self, *args, **kwargs):
        self._map = {bytes(n): i for i, n in enumerate(self)}

    def __eq__(self, other):
        "Equality based on the the sequence of nodes."
        if len(self) != len(other):
            return False
        for i in xrange(len(self)):
            if self[i] != other[i]:
                return False
        return True

    def __getitem__(self, key):
        "Index and key-based access."
        if isinstance(key, (int, slice)):
            return tuple.__getitem__(self, key)
        if key not in self._map:
            raise KeyError(key)
        return self[self._map[key]]

    def match(self, regexp, flags=re.I):
        "Returns one or more nodes that match the regexp on the key."
        r = re.compile(regexp, flags)
        return Nodes(self[key] for key in self._map if r.match(key))
