from __future__ import unicode_literals

try:
    str = unicode
except NameError:
    pass


def cypher_value(value):
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    if value is None:
        return 'null'
    if isinstance(value, (list, tuple)):
        return '[' + ', '.join(cypher_value(v) for v in value) + ']'
    if isinstance(value, (int, float)):
        return repr(value)

    if isinstance(value, bytes):
        value = value.decode('utf-8')
    elif not isinstance(value, str):
        value = str(value)

    return repr(value).lstrip('u')


def cypher_map(props):
    toks = []

    for k in props:
        toks.append('`{}`: {}'.format(k, cypher_value(props[k])))

    return '{' + ', '.join(toks) + '}'
