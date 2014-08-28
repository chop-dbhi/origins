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
