import re
import time
from timeit import default_timer


UUID_RE = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}'
                     r'-[a-f0-9]{4}-[a-f0-9]{12}$', re.I)


class Timer(object):
    """Timer as a context manager that can be reused.

        t = Timer()

        with t('prep'):
            ...

        with t('query'):
            ...

        t.results  # {'prep': 0.0184, 'query': 0.213}

    """
    def __init__(self, name='default'):
        self.name = name
        self.results = {'total': 0}

    def __call__(self, name):
        self.name = name
        return self

    def __enter__(self):
        if not self.name:
            raise ValueError('timer must be called with a name')

        self.start = default_timer()
        return self

    def __exit__(self, *args):
        end = default_timer()
        elapsed = (end - self.start) * 1000
        self.results[self.name] = elapsed
        self.results['total'] += elapsed
        self.name = None

    def add_results(self, name, results):
        self.results[name] = results
        self.results['total'] += results['total']


def is_uuid(uuid):
    if not isinstance(uuid, str):
        return False

    return UUID_RE.match(uuid) is not None


def timestamp():
    return int(time.time() * 1000)


def diff(a, b, ignore=None, encoding='utf-8'):
    """Compare `a` against `b`.

    Keys found in `a` but not in `b` are marked as additions. The key and
    value in `a` is returned.

    Keys found in `b` but not in `a` are marked as removals. The key and
    value in `b` is returned.

    Keys found in both whose values are not *exactly equal*, which involves
    comparing value and type, are marked as changed. The key and a tuple
    of the old value and new value is returned.
    """
    if ignore is None:
        ignore = set()

    d = {}

    if a is None:
        a = {}

    if b is None:
        b = {}

    for k in a:
        if k in ignore:
            continue

        av = a[k]

        # Recurse for dict values
        if isinstance(av, dict):
            _d = diff(av, b.get(k))

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
        if k in ignore:
            continue

        if k not in a and b[k] is not None:
            d[k] = (b[k], None)

    return d
