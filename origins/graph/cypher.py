def map(keys, parameter):
    "Produce a valid Cypher parametized map given keys and a parameter name."
    if not keys:
        return '{}'

    toks = []

    for k in keys:
        toks.append('`{k}`: {{ {p} }}.`{k}`'.format(k=k, p=parameter))

    return '{' + ', '.join(toks) + '}'


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
