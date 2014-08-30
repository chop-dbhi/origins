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


def map_parameters(keys, parameter):
    "Produce a valid Cypher parametized map given keys and a parameter name."
    if not keys:
        return '{}'

    toks = []

    for k in keys:
        toks.append('`{k}`: {{ {p} }}.`{k}`'.format(k=k, p=parameter))

    return '{' + ', '.join(toks) + '}'


def set_parameters(properties, var, parameter):
    "Produce a valid Cypher parametized list given keys and a parameter name."
    sets = []
    unsets = []

    for k, v in properties.items():
        if v is None:
            unsets.append('`{v}`.`{k}`'.format(k=k, v=var))
        else:
            sets.append('`{v}`.`{k}` = {{ {p} }}.`{k}`'.format(k=k,
                                                               v=var,
                                                               p=parameter))

    result = ''

    if sets:
        result += 'SET ' + ', '.join(sets)

    if unsets:
        result += 'REMOVE ' + ', '.join(unsets)

    return result


def fuzzy_search(keys, var, parameter, operator='OR'):
    toks = []

    for k in keys:
        toks.append('`{v}`.`{k}` =~ {{ {p} }}.`{k}`'.format(k=k, v=var,
                                                            p=parameter))

    operator = ' ' + operator + ' '

    return operator.join(toks)


def labels_string(labels):
    "Converts and array of labels into the Cypher labels format for a node."
    if not labels:
        return ''

    if isinstance(labels, str):
        labels = [labels]

    return ':' + ':'.join(['`{}`'.format(l) for l in labels])
