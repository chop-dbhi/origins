from __future__ import unicode_literals

import re
import time
from . import cypher


try:
    str = unicode
except NameError:
    pass


UUID_RE = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}'
                     r'-[a-f0-9]{4}-[a-f0-9]{12}$', re.I)


# Removes comments, redundant newlines and trims whitespace
def _(s):
    return re.subn(r'\n+', '\n', re.subn(r'//.*', '', s)[0])[0].strip()


def identity(d, check_id=False):
    try:
        return _uuid(d)
    except KeyError:
        if check_id:
            return _id(d)

        raise


def _id(d):
    if isinstance(d, dict):
        if 'origins:id' in d:
            return d['origins:id']

        if 'id' in d and 'properties' in d:
            return d['id']

        raise KeyError('id property not defined')

    return d


def _uuid(d):
    if isinstance(d, dict):
        if 'origins:uuid' in d:
            return d['origins:uuid']

        if 'uuid' in d and 'properties' in d:
            return d['uuid']

        raise KeyError('uuid property not defined')

    return d


def timestamp():
    return int(time.time() * 1000)


def is_uuid(uuid):
    if not isinstance(uuid, str):
        return False
    return UUID_RE.match(uuid) is not None


def denull(a):
    "Removes dict entries whose value is None."
    if not a:
        a = {}

    for k, v in a.items():
        if v is None:
            a.pop(k)

    return a


def pack(a):
    """Takes a dict and merges the nested 'properties' dict into the output.
    Top-level keys are put in the `origins` namespace.
    """
    b = {}
    p = None

    for k, v in a.items():
        if v is None:
            continue

        if k == 'properties':
            p = v
        else:
            if not k.startswith('origins:'):
                k = 'origins:' + k

            b[k] = v

    if p:
        for k, v in p.items():
            if v is not None:
                b[k] = v

    # Do not store the neo4j ID that may be packed
    if 'origins:neo4j' in b:
        b.pop('origins:neo4j')

    return b


def unpack(b, namespace=False):
    """Takes a dict and unpacks non-Origins properties into a nested
    'properties' dict.
    """
    a = {}
    p = {}

    # Includes Neo4j id
    if isinstance(b, (list, tuple)):
        _id, b = b

        if namespace:
            a['origins:neo4j'] = _id
        else:
            a['neo4j'] = _id

    low = len('origins:')

    for k, v in b.items():
        if k.startswith('origins:'):
            if not namespace:
                k = k[low:]

            a[k] = v
        else:
            p[k] = v

    a['properties'] = p

    return a


def diff(a, b, encoding='utf-8'):
    """Compare `a` against `b`.

    Keys found in `a` but not in `b` are marked as additions. The key and
    value in `a` is returned.

    Keys found in `b` but not in `a` are marked as deletions. The key and
    value in `b` is returned.

    Keys found in both whose values are not *exactly equal*, which involves
    comparing value and type, are marked as changed. The key and a tuple
    of the old value and new value is returned.
    """
    additions = {}
    deletions = {}
    changes = {}

    if a is None:
        a = {}

    if b is None:
        b = {}

    for k in a:
        if k.startswith('origins:'):
            continue

        av = a[k]

        if isinstance(av, bytes):
            av = av.decode(encoding)

        if k in b:
            bv = b[k]

            if isinstance(bv, bytes):
                bv = bv.decode(encoding)

            if av != bv or type(av) != type(bv):
                changes[k] = (bv, av)

        # null values are ignored
        elif av is not None:
            additions[k] = av

    for k in b:
        if k.startswith('origins:'):
            continue

        if k not in a and b[k] is not None:
            deletions[k] = b[k]

    output = {}

    if additions:
        output['additions'] = additions

    if deletions:
        output['deletions'] = deletions

    if changes:
        output['changes'] = changes

    return output


def resolve_predicate(predicate):
    if not isinstance(predicate, dict):
        if is_uuid(predicate):
            predicate = {'origins:uuid': predicate}
        else:
            predicate = {'origins:id': predicate}

    placeholder = cypher.map_parameters(predicate.keys(), 'predicate')

    return predicate, placeholder
