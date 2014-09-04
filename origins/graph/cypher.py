def map(keys, parameter):
    "Produce a valid Cypher parametized map given keys and a parameter name."
    if not keys:
        return '{}'

    toks = []

    for k in keys:
        toks.append('`{k}`: {{ {p} }}.`{k}`'.format(k=k, p=parameter))

    return '{' + ', '.join(toks) + '}'


def set(properties, var, parameter):
    """Produces Cypher parametized SET statement for the properties and a
    parameter name. None values will be set to null
    """
    sets = []

    for k, v in properties.items():
        if v is None:
            sets.append('`{v}`.`{k}` = null'.format(k=k, v=var))
        else:
            sets.append('`{v}`.`{k}` = {{ {p} }}.`{k}`'
                        .format(k=k, v=var, p=parameter))

    return 'SET ' + ', '.join(sets)


def search(keys, var, parameter, operator='OR'):
    "Returns a parametized set of regexp-based expressions."
    toks = []

    for k in keys:
        toks.append('`{v}`.`{k}` =~ {{ {p} }}.`{k}`'
                    .format(k=k, v=var, p=parameter))

    # Add spaces around operator
    operator = ' ' + operator + ' '

    return operator.join(toks)


def labels(labels):
    "Converts and array of labels into the Cypher labels format for a node."
    if not labels:
        return ''

    if isinstance(labels, str):
        labels = [labels]

    return ':' + ':'.join(['`{}`'.format(l) for l in labels])
