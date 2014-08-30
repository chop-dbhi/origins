from string import Template as T
from origins import utils, provenance
from origins.exceptions import DoesNotExist
from ..model import Result, Node, Edge
from ..packer import pack
from ..cypher import labels_string
from .. import neo4j


__all__ = (
    'get',
    'get_by_id',
    'add',
    'set',
    'remove',
    'get_outdated',
)


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

CREATE (s)-[r:`$etype` { props }]->(e)

RETURN n, s, e
''')


DELETE_EDGE = T('''
MATCH (s:`origins:Node` {`origins:uuid`: { start }})-[r:`$etype`]->(e:`origins:Node` {`origins:uuid`: { end }})
DELETE r
''')  # noqa


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


def _prepare_statement(statement, label=None, **mapping):
    mapping['labels'] = labels_string(label)

    return statement.safe_substitute(mapping)


def get(uuid, label=None, tx=neo4j.tx):
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
            raise DoesNotExist('edge does not exist')

    return Result(
        data=Edge.parse(result[0]),
        perf=t.results,
        prov=None,
        time=utils.timestamp(),
    )


def get_by_id(_id, label=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Edge):
        _id = _id['id']
    elif isinstance(_id, Result):
        _id = _id['data']['id']

    with t('prep'):
        statement = _prepare_statement(GET_EDGE_BY_ID,
                                       label=label)

        query = {
            'statement': statement,
            'parameters': {
                'id': _id,
            }
        }

    with t('exec'):
        result = tx.send(query)

        if not result:
            raise DoesNotExist('edge does not exist')

        data = Edge.parse(result[0])

    return Result(
        data=data,
        prov=None,
        perf=t.results,
        time=utils.timestamp(),
    )


def add(start, end, attrs=None, label=None, new=True, tx=neo4j.tx):
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
            edge = Edge.new(attrs)

            statement = _prepare_statement(ADD_EDGE, label=label,
                                           etype=edge.get('type', 'null'))

            parameters = {
                'attrs': pack(edge),
                'start': start,
                'end': end,
                'props': edge.get('properties', {})
            }

            query = {
                'statement': statement,
                'parameters': parameters,
            }

        with t('exec'):
            result = tx.send(query)[0]

            if not result:
                raise ValueError('start or end node does not exist')

            edge = Edge.parse(result)

        with t('prov'):
            prov_spec = provenance.add(edge['uuid'], new=new)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=edge,
            prov=prov,
        )


def set(uuid, attrs=None, new=True, label=None, force=False, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Edge):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            prev = get(uuid, tx=tx)

        with t('prep'):
            # Create new edge merged into the previous attributes
            edge = Edge.new(attrs, merge=prev['data'])

            # Calculate diff
            diff = edge.diff(prev['data'])

            if not diff and not force:
                return

        # Removes the physical edge between the two nodes
        with t('exec'):
            etype = prev['data'].get('type', 'null')
            statement = _prepare_statement(DELETE_EDGE, etype=etype)

            query = {
                'statement': statement,
                'parameters': {
                    'start': prev['data']['start']['uuid'],
                    'end': prev['data']['end']['uuid'],
                }
            }

            tx.send(query)

        # Create a new version of the entity
        rev = add(start=prev['data']['start']['uuid'],
                  end=prev['data']['end']['uuid'],
                  attrs=edge,
                  new=new,
                  label=label,
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

        # Removes the physical edge between the two nodes
        with t('exec'):
            etype = edge['data'].get('type', 'null')
            statement = _prepare_statement(DELETE_EDGE, etype=etype)

            query = {
                'statement': statement,
                'parameters': {
                    'start': edge['data']['start']['uuid'],
                    'end': edge['data']['end']['uuid'],
                }
            }

            tx.send(query)

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
            'edge': Edge.parse(r[:3]),
            'latest': Node.parse(r[-1]),
        })

    return results
