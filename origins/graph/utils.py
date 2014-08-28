import re
from ..utils import is_uuid
from .model import Node
from . import cypher


IGNORED_ATTRS = {
    'uuid',
    'neo4j',
    'start',
    'end',
    'resource',
}


# Removes comments, redundant newlines and trims whitespace
def _(s):
    return re.subn(r'\n+', '\n', re.subn(r'//.*', '', s)[0])[0].strip()


def _uuid(d):
    if isinstance(d, dict):
        if 'uuid' in d:
            return d['uuid']

        raise KeyError('uuid key not defined')

    return d


def match_predicate(predicate):
    if isinstance(predicate, Node):
        predicate = {'uuid': predicate}
    elif not isinstance(predicate, dict):
        if is_uuid(predicate):
            predicate = {'uuid': predicate}
        else:
            predicate = {'id': predicate}

    placeholder = cypher.map_parameters(predicate.keys(), 'predicate')

    return predicate, placeholder


def skip_limit(statement, parameters, skip, limit):
    if skip:
        statement += ' SKIP {skip}'
        parameters['skip'] = skip

    if limit:
        statement += ' LIMIT {limit}'
        parameters['limit'] = limit

    return statement, parameters


def merge_attrs(old, new):
    """Merges attributes of new into old.

    Attributes that are explicitly `None` in new will be used to 'unset'
    attributes that have a value in old.
    """
    attrs = {}

    if new is None:
        new = {}

    for k, v in old.items():
        if k == 'properties' and v:
            attrs[k] = merge_attrs(v, new.get(k, {}))
        elif k not in IGNORED_ATTRS and k not in new:
            attrs[k] = v

    for k, v in new.items():
        if k != 'properties' and v is not None:
            attrs[k] = v

    return attrs
