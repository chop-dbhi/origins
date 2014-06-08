from __future__ import unicode_literals


def diff(a, b):
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

    for k, v in a.items():
        if k.startswith('origins:'):
            continue

        av = a[k]

        if k in b:
            bv = b[k]

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
