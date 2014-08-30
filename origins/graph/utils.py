import re
from ..utils import is_uuid
from .model import Node
from . import cypher


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
