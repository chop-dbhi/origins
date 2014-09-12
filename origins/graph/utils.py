import re
import json
import time
from string import Template as T
from hashlib import sha1
from . import cypher


UUID_RE = re.compile(r'^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$')


def is_uuid(s):
    if not isinstance(s, str):
        return False

    return UUID_RE.match(s) is not None


def dict_sha1(d):
    "Returns the SHA1 hex of a dictionary."
    if d:
        s = json.dumps(d, sort_keys=True)
        return sha1(s.encode('utf-8')).hexdigest()


def timestamp():
    return int(time.time() * 1000)


def prep(statement, type=None, model=None, start_model=None, end_model=None,
         **mapping):

    statement = T(statement)

    mapping['type'] = cypher.labels(type)
    mapping['model'] = cypher.labels(model)
    mapping['start_model'] = cypher.labels(start_model)
    mapping['end_model'] = cypher.labels(end_model)

    return statement.safe_substitute(mapping)


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
