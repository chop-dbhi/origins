from uuid import uuid4
from origins import utils, provenance
from origins.graph import neo4j
from ..model import Result, Node, Edge
from ..utils import merge_attrs
from ..packer import pack, unpack
from ..cypher import labels_string
from .parsers import parse_node, parse_edge
from . import queries

__all__ = (
    'get_node',
    'get_node_by_id',
    'add_node',
    'change_node',
    'remove_node',
    'get_edge',
    'get_edge_by_id',
    'add_edge',
    'change_edge',
    'remove_edge',
)


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


def get_node(uuid, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Node):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with t('prep'):
        query = {
            'statement': queries.GET_NODE,
            'parameters': {
                'uuid': uuid,
            }
        }

    with t('exec'):
        result = tx.send(query)

        if not result:
            raise ValueError('node does not exist')

    return Result(
        data=parse_node(result[0]),
        perf=t.results,
        prov=None,
        time=utils.timestamp(),
    )


def get_node_by_id(_id, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Node):
        _id = _id['id']
    elif isinstance(_id, Result):
        _id = _id['data']['id']

    with t('prep'):
        statement = _prepare_statement(queries.GET_NODE_BY_ID,
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
            raise ValueError('node does not exist')

        data = parse_node(result[0])

    return Result(
        data=data,
        prov=None,
        perf=t.results,
        time=utils.timestamp(),
    )


def add_node(attrs=None, new=True, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    with tx as tx:
        with t('prep'):
            attrs = _prepare_attrs(attrs)

            if 'id' not in attrs:
                attrs['id'] = attrs['uuid']

            statement = _prepare_statement(queries.ADD_NODE, labels=labels)

            parameters = {
                'attrs': pack(attrs),
            }

            query = {
                'statement': statement,
                'parameters': parameters,
            }

        with t('exec'):
            data = parse_node(tx.send(query)[0])

        # Provenance for add
        with t('prov'):
            prov_spec = provenance.add(data['uuid'], new=new)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=data,
            prov=prov,
        )


def _update_edge(attrs, labels, start, end, tx):
    """Updates an edge with new start and end nodes.

    This creates a new revision of the edge.
    """
    t = utils.Timer()

    previous = attrs.pop('uuid')

    with t('add'):
        edge = add_edge(start, end, attrs=attrs, labels=labels, tx=tx)

    with t('prov'):
        prov_spec = provenance.change(previous, edge['data']['uuid'],
                                      reason='origins:NodeChange')
        prov_spec['wasGeneratedBy'] = edge['prov']['wasGeneratedBy']
        prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'
        prov = provenance.load(prov_spec, tx=tx)

    return Result(
        perf=t.results,
        time=utils.timestamp(),
        data=edge['data'],
        prov=prov,
    )


def _update_node_edges(old, new, tx):
    """Gets all directed edges where `old` is the start node and
    changes the edge to `new` (a new revision of old).
    """
    query = {
        'statement': queries.NODE_OUTBOUND_EDGES,
        'parameters': {
            'uuid': old,
        }
    }

    result = tx.send(query)

    results = []

    # TODO, handle incoming edges
    for (edge, edge_labels, end) in result:
        edge = unpack(edge)
        end = unpack(end)['uuid']
        result = _update_edge(edge, edge_labels, new, end, tx)
        results.append(result)

    return results


def change_node(uuid, attrs=None, new=True, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Node):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            prev = get_node(uuid, tx=tx)

        with t('prep'):
            # Merge the new into the old ones
            attrs = merge_attrs(prev['data'], attrs)

        with t('add'):
            # Create a new version of the entity
            rev = add_node(attrs, new=new, labels=labels, tx=tx)
            t.add_results('add', rev['perf'])

        with t('update_edges'):
            # Copy outbound relationships from the previous version
            _update_node_edges(prev['data']['uuid'],
                               rev['data']['uuid'], tx=tx)

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
        )


def remove_node(uuid, reason=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Node):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            node = get_node(uuid, tx=tx)

        # Provenance for remove
        with t('prov'):
            prov_spec = provenance.remove(node['data']['uuid'], reason=reason)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=node['data'],
            prov=prov,
        )


def get_edge(uuid, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Edge):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with t('prep'):
        query = {
            'statement': queries.GET_EDGE,
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


def get_edge_by_id(_id, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Edge):
        _id = _id['id']
    elif isinstance(_id, Result):
        _id = _id['data']['id']

    with t('prep'):
        statement = _prepare_statement(queries.GET_EDGE_BY_ID,
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


def add_edge(start, end, attrs=None, labels=None, new=True, tx=neo4j.tx):
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

            statement = _prepare_statement(queries.ADD_EDGE, labels=labels)

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


def change_edge(uuid, attrs=None, new=True, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Edge):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        # Get the entity being updated
        with t('get'):
            prev = get_edge(uuid, tx=tx)

        with t('prep'):
            # Merge the new into the old ones
            attrs = merge_attrs(prev['data'], attrs)

        # Create a new version of the entity
        rev = add_edge(start=prev['data']['start']['uuid'],
                       end=prev['data']['end']['uuid'],
                       attrs=attrs,
                       new=new,
                       labels=labels,
                       tx=tx)

        t.add_results('add_edge', rev['perf'])

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
        )


def remove_edge(uuid, reason=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(uuid, Edge):
        uuid = uuid['uuid']
    elif isinstance(uuid, Result):
        uuid = uuid['data']['uuid']

    with tx as tx:
        with t('get'):
            edge = get_edge(uuid, tx=tx)

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
