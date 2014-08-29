from uuid import uuid4
from string import Template as T
from origins import utils, provenance
from origins.graph import neo4j
from ..model import Result, Node, Edge
from ..utils import merge_attrs
from ..packer import pack
from ..cypher import labels_string
from .parsers import parse_node, parse_edge


__all__ = (
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
    'get_outdated',
)


DIFF_IGNORE = {
    'id',
    'uuid',
    'time',
    'start',
    'end',
}


# Returns a single edge with the start and end nodes by it's UUID
GET_EDGE = '''
MATCH (n:`origins:Edge` {`origins:uuid`: { uuid }})
MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e
LIMIT 1
'''

# Returns the latest edge by ID
GET_EDGE_BY_ID = T('''
MATCH (n:`origins:Edge`$labels {`origins:id`: { id }})
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n
MATCH (n)-[:`origins:start`]->(s:`origins:Node`),
      (n)-[:`origins:end`]->(e:`origins:Node`)
RETURN n, s, e
LIMIT 1
''')


# Creates and returns an edge
# The `prov:Entity` label is added to hook into the provenance data model
ADD_EDGE = T('''
MATCH (s:`origins:Node` {`origins:uuid`: { start }}),
      (e:`origins:Node` {`origins:uuid`: { end }})
CREATE (n:`origins:Edge`:`prov:Entity`$labels { attrs }),
       (n)-[:`origins:start`]->(s),
       (n)-[:`origins:end`]->(e)
RETURN n, s, e
''')


# Finds "outdated edges"
# Matches all valid edges that have a invalid end node and finds the latest
# non-invalid revision of that node. The start node, edge and new end node
# is returned.
GET_OUTDATED_EDGES = '''
// Valid edges
MATCH (n:`origins:Edge`)
WHERE NOT (n)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n

// Pointing to an invalid end node
MATCH (n)<-[:`origins:end`]->(e:`origins:Node`)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n, e

// That has a revision
MATCH (e)<-[:`prov:usedEntity`]-(:`prov:Derivation` {`prov:type`: 'prov:Revision'})-[:`prov:generatedEntity`|`prov:usedEntity`*]-(l:`origins:Node` {`origins:id`: e.`origins:id`})
WHERE NOT (l)<-[:`prov:entity`]-(:`prov:Invalidation`)
WITH n, e, l

MATCH (n)-[:`origins:start`]->(s:`origins:Node`)
// Edge, start, end (current), latest
RETURN n, s, e, l
'''  # noqa


def _prepare_attrs(attrs=None):
    if attrs is None:
        attrs = {}
    elif 'uuid' in attrs:
        raise KeyError('UUID cannot be provided')

    attrs['uuid'] = str(uuid4())
    attrs['time'] = utils.timestamp()

    return attrs


def _prepare_statement(statement, labels=None, **mapping):
    mapping['labels'] = labels_string(labels)

    return statement.safe_substitute(mapping)


def get(uuid, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Edge):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with t('prep'):
        query = {
            'statement': GET_EDGE,
            'parameters': {
                'uuid': uuid,
            }
        }

    with t('exec'):
        result = tx.send(query)

        if not result:
            raise ValueError('edge does not exist')

    return Result(
        data=parse_edge(result[0]),
        perf=t.results,
        prov=None,
        time=utils.timestamp(),
    )


def get_by_id(_id, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Edge):
        _id = _id['id']
    elif isinstance(_id, Result):
        _id = _id['data']['id']

    with t('prep'):
        statement = _prepare_statement(GET_EDGE_BY_ID,
                                       labels=labels)

        query = {
            'statement': statement,
            'parameters': {
                'id': _id,
            }
        }

    with t('exec'):
        result = tx.send(query)

        if not result:
            raise ValueError('edge does not exist')

        data = parse_edge(result[0])

    return Result(
        data=data,
        prov=None,
        perf=t.results,
        time=utils.timestamp(),
    )


def add(start, end, attrs=None, labels=None, new=True, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(start, Node):
        start = start['uuid']
    elif isinstance(start, Result):
        start = start['data']['uuid']

    if isinstance(end, Node):
        end = end['uuid']
    elif isinstance(end, Result):
        end = end['data']['uuid']

    with tx as tx:
        with t('prep'):
            attrs = _prepare_attrs(attrs)

            if 'id' not in attrs:
                attrs['id'] = attrs['uuid']

            statement = _prepare_statement(ADD_EDGE, labels=labels)

            parameters = {
                'attrs': pack(attrs),
                'start': start,
                'end': end,
            }

            query = {
                'statement': statement,
                'parameters': parameters,
            }

        with t('exec'):
            result = tx.send(query)[0]

            if not result:
                raise ValueError('start or end node does not exist')

            data = parse_edge(result)

        with t('prov'):
            prov_spec = provenance.add(data['uuid'], new=new)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=data,
            prov=prov,
        )


def set(uuid, attrs=None, new=True, labels=None, force=False, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Edge):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        # Get the entity being updated
        with t('get'):
            prev = get(uuid, tx=tx)

        with t('prep'):
            # Merge the new into the old ones
            attrs = merge_attrs(prev['data'], attrs)

            # Calculate diff
            diff = utils.diff(attrs, prev['data'], ignore=DIFF_IGNORE)

            if not diff and not force:
                return

        # Create a new version of the entity
        rev = add(start=prev['data']['start']['uuid'],
                  end=prev['data']['end']['uuid'],
                  attrs=attrs,
                  new=new,
                  labels=labels,
                  tx=tx)

        t.add_results('add', rev['perf'])

        # Provenance for change
        with t('prov'):
            prov_spec = provenance.change(prev['data']['uuid'],
                                          rev['data']['uuid'])
            prov_spec['wasGeneratedBy'] = rev['prov']['wasGeneratedBy']
            prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'

            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=rev['data'],
            prov=prov,
            diff=diff,
        )


def remove(uuid, reason=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Edge):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            edge = get(uuid, tx=tx)

        # Provenance for remove
        with t('prov'):
            prov_spec = provenance.remove(edge['data']['uuid'], reason=reason)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=edge['data'],
            prov=prov,
        )


def get_outdated(tx=neo4j.tx):
    query = {
        'statement': GET_OUTDATED_EDGES,
    }

    results = []

    for r in tx.send(query):
        results.append({
            'edge': parse_edge(r[:3]),
            'latest': parse_node(r[-1]),
        })

    return results
