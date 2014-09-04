import json
from copy import deepcopy
from hashlib import sha1
from uuid import uuid4
from origins.exceptions import ValidationError
from .packer import unpack
from . import utils


DIFF_ATTRS = {
    'type',
    'label',
    'description',
    'change_dependence',
    'remove_dependence',
}


def _dict_sha1(d):
    "Returns the SHA1 hex of a dictionary."
    if d:
        s = json.dumps(d, sort_keys=True)
        return sha1(s.encode('utf-8')).hexdigest()


class Node(object):
    DEFAULT_TYPE = 'Node'

    def __init__(self, id=None, type=None, label=None, description=None,
                 properties=None, uuid=None, time=None, sha1=None):

        if not type:
            type = self.DEFAULT_TYPE

        if not sha1:
            sha1 = _dict_sha1(properties)

        if not time:
            time = utils.timestamp()

        if not uuid:
            uuid = str(uuid4())

        if not id:
            id = uuid

        self.uuid = uuid
        self.id = id
        self.time = time
        self.type = type
        self.label = label
        self.description = description
        self.properties = properties
        self.sha1 = sha1

    def __str__(self):
        return '{} ({})'.format(self.uuid, self.label or self.id)

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, str(self))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.uuid == other.uuid

    def __ne__(self, other):
        return not self.__eq__(other)

    def _derive_attrs(self):
        return {
            'id': self.id,
            'label': self.label,
            'type': self.type,
            'description': self.description,
        }

    @classmethod
    def parse(cls, attrs):
        return cls(**unpack(attrs))

    @classmethod
    def derive(cls, node, attrs=None):
        if attrs is None:
            attrs = {}

        copy = node._derive_attrs()
        copy.update(attrs)

        # Copy properties
        if node.properties:
            props = deepcopy(node.properties)

            # New properties take precedence
            if attrs.get('properties'):
                props.update(attrs['properties'])

            copy['properties'] = props

        return cls(**copy)

    def to_dict(self):
        attrs = self.__dict__.copy()

        if self.properties:
            attrs['properties'] = deepcopy(self.properties)

        return attrs

    def diff(self, other):
        return diff_attrs(self.to_dict(), other.to_dict(),
                          allowed=DIFF_ATTRS)


class Edge(Node):
    DEFAULT_TYPE = 'related'

    NO_DEPENDENCE = 0
    DIRECTED_DEPENDENCE = 1
    MUTUAL_DEPENDENCE = 2

    DEPENDENCE_TYPES = {
        NO_DEPENDENCE,
        DIRECTED_DEPENDENCE,
        MUTUAL_DEPENDENCE
    }

    def __init__(self, id=None, label=None, description=None, type=None,
                 uuid=None, time=None, sha1=None, properties=None,
                 start=None, end=None, change_dependence=None,
                 remove_dependence=None):

        if change_dependence is None:
            change_dependence = self.DIRECTED_DEPENDENCE
        elif change_dependence not in self.DEPENDENCE_TYPES:
            raise ValidationError('change dependence must be 0 (none), '
                                  '1 (directed), or 2 (mutual)')

        if remove_dependence is None:
            remove_dependence = self.NO_DEPENDENCE
        elif remove_dependence not in self.DEPENDENCE_TYPES:
            raise ValidationError('remove dependence must be 0 (none), '
                                  '1 (directed), or 2 (mutual)')

        super(Edge, self).__init__(
            id=id,
            type=type,
            label=label,
            description=description,
            uuid=uuid,
            time=time,
            sha1=sha1,
            properties=properties
        )

        self.change_dependence = change_dependence
        self.remove_dependence = remove_dependence

        self.start = start
        self.end = end

    @classmethod
    def parse(cls, row):
        if len(row) > 1:
            start = Node.parse(row[1])
            end = Node.parse(row[2])
        else:
            start = None
            end = None

        return cls(start=start, end=end, **unpack(row[0]))

    def _derive_attrs(self):
        return {
            'id': self.id,
            'label': self.label,
            'type': self.type,
            'start': self.start,
            'end': self.end,
            'description': self.description,
            'change_dependence': self.change_dependence,
            'remove_dependence': self.remove_dependence,
        }

    def to_dict(self):
        attrs = super(self.__class__, self).to_dict()

        # These are not attributes, just references to nodes
        attrs.pop('start')
        attrs.pop('end')

        return attrs


def diff_attrs(a, b, allowed=None, encoding='utf-8'):
    """Compare `a` against `b`.

    Keys found in `a` but not in `b` are marked as additions. The key and
    value in `a` is returned.

    Keys found in `b` but not in `a` are marked as removals. The key and
    value in `b` is returned.

    Keys found in both whose values are not *exactly equal*, which involves
    comparing value and type, are marked as changed. The key and a tuple
    of the old value and new value is returned.
    """
    d = {}

    if a is None:
        a = {}

    if b is None:
        b = {}

    for k in a:
        if allowed and k not in allowed:
            continue

        av = a[k]

        # Recurse for dict values
        if isinstance(av, dict):
            _d = diff_attrs(av, b.get(k))

            if _d:
                d[k] = _d

            continue

        # Decode bytes for unicode comparison
        if isinstance(av, bytes):
            av = av.decode(encoding)

        if k in b:
            bv = b[k]

            # Decode bytes for unicode comparison
            if isinstance(bv, bytes):
                bv = bv.decode(encoding)

            if av != bv or type(av) != type(bv):
                d[k] = (bv, av)

        # null values are ignored
        elif av is not None:
            d[k] = (None, av)

    for k in b:
        if allowed and k not in allowed:
            continue

        if k not in a and b[k] is not None:
            d[k] = (b[k], None)

    return d
