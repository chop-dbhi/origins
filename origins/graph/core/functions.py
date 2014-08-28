from uuid import uuid4
from origins import utils, provenance
from origins.graph import neo4j
from origins.model import Result, Node, Edge
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


NODE_LABEL = 'origins:Node'
EDGE_LABEL = 'origins:Edge'


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


def get_node(_id, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Node):
        _id = _id['neo4j']
    elif isinstance(_id, Result):
        _id = _id['data']['neo4j']

    with t('prep'):
        statement = _prepare_statement(queries.GET_NODE, labels=labels)

        query = {
            'statement': statement,
            'parameters': {
                'node': _id,
            }
        }

    with t('exec'):
        try:
            result = tx.send(query)
        except neo4j.Neo4jError:
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
            prov_spec = provenance.add(data['neo4j'], new=new)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=data,
            prov=prov,
        )


def _update_edge(previous, labels, attrs, start, end, tx):
    """Updates an edge with new start and end nodes.

    This creates a new revision of the edge.
    """
    t = utils.Timer()

    attrs.pop('uuid')

    with t('add'):
        edge = add_edge(start, end, attrs=attrs, labels=labels, tx=tx)

    with t('prov'):
        prov_spec = provenance.change(previous, edge['data']['neo4j'],
                                      reason='origins:NodeChange')
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
            'node': old,
        }
    }

    result = tx.send(query)

    results = []

    for (edge_id, edge_labels, edge, end_id) in result:
        result = _update_edge(edge_id, edge_labels, unpack(edge), new,
                              end_id, tx)
        results.append(result)

    return results


def change_node(_id, attrs=None, new=True, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Node):
        _id = _id['neo4j']
    elif isinstance(_id, Result):
        _id = _id['data']['neo4j']

    with tx as tx:
        with t('get'):
            prev = get_node(_id, tx=tx)

        with t('prep'):
            # Merge the new into the old ones
            attrs = merge_attrs(prev['data'], attrs)

        with t('add'):
            # Create a new version of the entity
            rev = add_node(attrs, new=new, labels=labels, tx=tx)
            t.add_results('add', rev['perf'])

        with t('update_edges'):
            # Copy outbound relationships from the previous version
            _update_node_edges(prev['data']['neo4j'],
                               rev['data']['neo4j'], tx=tx)

        # Provenance for change
        with t('prov'):
            prov_spec = provenance.change(prev['data']['neo4j'],
                                          rev['data']['neo4j'])
            prov_spec['wasGeneratedBy'] = rev['prov']['wasGeneratedBy']
            prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'

            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=rev['data'],
            prov=prov,
        )


def remove_node(_id, reason=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Node):
        _id = _id['neo4j']
    elif isinstance(_id, Result):
        _id = _id['data']['neo4j']

    with tx as tx:
        with t('get'):
            node = get_node(_id, tx=tx)

        # Provenance for remove
        with t('prov'):
            prov_spec = provenance.remove(node['data']['neo4j'], reason=reason)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=node['data'],
            prov=prov,
        )


def get_edge(_id, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Edge):
        _id = _id['neo4j']
    elif isinstance(_id, Result):
        _id = _id['data']['neo4j']

    with t('prep'):
        statement = _prepare_statement(queries.GET_EDGE, labels=labels)

        query = {
            'statement': statement,
            'parameters': {
                'edge': _id,
            }
        }

    with t('exec'):
        try:
            result = tx.send(query)
        except neo4j.Neo4jError:
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
        start = start['neo4j']
    elif isinstance(start, Result):
        start = start['data']['neo4j']

    if isinstance(end, Node):
        end = end['neo4j']
    elif isinstance(end, Result):
        end = end['data']['neo4j']

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
            prov_spec = provenance.add(data['neo4j'], new=new)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=data,
            prov=prov,
        )


def change_edge(_id, attrs=None, new=True, labels=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Edge):
        _id = _id['neo4j']
    elif isinstance(_id, Result):
        _id = _id['data']['neo4j']

    with tx as tx:
        # Get the entity being updated
        with t('get'):
            prev = get_edge(_id, tx=tx)

        with t('prep'):
            # Merge the new into the old ones
            attrs = merge_attrs(prev['data'], attrs)

        # Create a new version of the entity
        rev = add_edge(start=prev['data']['start']['neo4j'],
                       end=prev['data']['end']['neo4j'],
                       attrs=attrs,
                       new=new,
                       labels=labels,
                       tx=tx)

        t.add_results('add_edge', rev['perf'])

        # Provenance for change
        with t('prov'):
            prov_spec = provenance.change(prev['data']['neo4j'],
                                          rev['data']['neo4j'])
            prov_spec['wasGeneratedBy'] = rev['prov']['wasGeneratedBy']
            prov_spec['wasDerivedFrom']['wdf']['prov:generation'] = 'wgb'

            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=rev['data'],
            prov=prov,
        )


def remove_edge(_id, reason=None, tx=neo4j.tx):
    t = utils.Timer()

    if isinstance(_id, Edge):
        _id = _id['neo4j']
    elif isinstance(_id, Result):
        _id = _id['data']['neo4j']

    with tx as tx:
        with t('get'):
            edge = get_edge(_id, tx=tx)

        # Provenance for remove
        with t('prov'):
            prov_spec = provenance.remove(edge['data']['neo4j'], reason=reason)
            prov = provenance.load(prov_spec, tx=tx)

        return Result(
            perf=t.results,
            time=utils.timestamp(),
            data=edge['data'],
            prov=prov,
        )
