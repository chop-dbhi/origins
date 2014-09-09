import re
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
    'direction',
    'dependence',
}

UUID_RE = re.compile(r'^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$')


def is_uuid(s):
    if not isinstance(s, str):
        return False

    return UUID_RE.match(s) is not None


def _dict_sha1(d):
    "Returns the SHA1 hex of a dictionary."
    if d:
        s = json.dumps(d, sort_keys=True)
        return sha1(s.encode('utf-8')).hexdigest()


class Node(object):
    DEFAULT_TYPE = 'Node'
    DEFAULT_MODEL = 'origins:Node'

    def __init__(self, id=None, type=None, label=None, description=None,
                 properties=None, uuid=None, time=None, sha1=None,
                 model=None):

        if not type:
            type = self.DEFAULT_TYPE

        if not model:
            model = self.DEFAULT_MODEL

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
        self.model = model
        self.label = label
        self.description = description
        self.properties = properties
        self.sha1 = sha1

    def __hash__(self):
        return hash(self.uuid)

    def __str__(self):
        if self.label:
            label = self.label
        elif is_uuid(self.id):
            label = self.id[:8]
        else:
            label = self.id

        return '{} ({})'.format(label, self.uuid[:8])

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
            'model': self.model,
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
    DEFAULT_MODEL = 'origins:Edge'

    NO_DEPENDENCE = 'none'
    FORWARD_DEPENDENCE = 'forward'
    INVERSE_DEPENDENCE = 'inverse'
    MUTUAL_DEPENDENCE = 'mutual'

    DEPENDENCE_TYPES = (
        NO_DEPENDENCE,
        FORWARD_DEPENDENCE,
        INVERSE_DEPENDENCE,
        MUTUAL_DEPENDENCE,
    )

    UNDIRECTED = 'undirected'
    DIRECTED = 'directed'
    REVERSE = 'reverse'
    BIDIRECTED = 'bidirected'

    DIRECTION_TYPES = (
        UNDIRECTED,
        DIRECTED,
        REVERSE,
        BIDIRECTED,
    )

    def __init__(self, id=None, label=None, description=None, type=None,
                 uuid=None, time=None, sha1=None, properties=None, model=None,
                 start=None, end=None, dependence=None, direction=None):

        if direction is None:
            direction = self.DIRECTED
        elif direction not in self.DIRECTION_TYPES:
            raise ValidationError('direction not valid. choices are: {}'
                                  .format(', '.join(self.DIRECTION_TYPES)))

        if dependence is None:
            dependence = self.NO_DEPENDENCE
        elif dependence not in self.DEPENDENCE_TYPES:
            raise ValidationError('dependence not valid: choices are: {}'
                                  .format(', '.join(self.DEPENDENCE_TYPES)))

        super(Edge, self).__init__(
            id=id,
            type=type,
            model=model,
            label=label,
            description=description,
            uuid=uuid,
            time=time,
            sha1=sha1,
            properties=properties
        )

        self.direction = direction
        self.dependence = dependence

        # Initialize as nodes assuming these are UUIDs
        if not isinstance(start, Node):
            start = Node(uuid=start)

        if not isinstance(end, Node):
            end = Node(uuid=end)

        self.start = start
        self.end = end

    @classmethod
    def parse(cls, attrs, start=None, end=None):
        if isinstance(start, dict):
            start = Node.parse(start)

        if isinstance(end, dict):
            end = Node.parse(end)

        return cls(start=start, end=end, **unpack(attrs))

    def _derive_attrs(self):
        return {
            'id': self.id,
            'label': self.label,
            'type': self.type,
            'model': self.model,
            'start': self.start,
            'end': self.end,
            'description': self.description,
            'direction': self.direction,
            'dependence': self.dependence,
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
