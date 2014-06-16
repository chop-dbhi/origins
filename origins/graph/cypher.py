from __future__ import unicode_literals

try:
    str = unicode
except NameError:
    pass


def value_string(value):
    "Converts a native Python value into an equivalent Cypher value."
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    if value is None:
        return 'null'
    if isinstance(value, (list, tuple)):
        return '[' + ', '.join(value_string(v) for v in value) + ']'
    if isinstance(value, (int, float)):
        return repr(value)

    if isinstance(value, bytes):
        value = value.decode('utf-8')
    elif not isinstance(value, str):
        value = str(value)

    return repr(value).lstrip('u')


def map_string(props):
    "Converts a dict into a valid Cypher map structure."
    if not props:
        return '{}'

    toks = []

    for k, v in props.items():
        if v is None:
            continue

        toks.append('`{}`: {}'.format(k, value_string(v)))

    return '{' + ', '.join(toks) + '}'


def labels_string(labels):
    "Converts and array of labels into the Cypher labels format for a node."
    if not labels:
        return ''

    return ':' + ':'.join(['`{}`'.format(l) for l in labels])
