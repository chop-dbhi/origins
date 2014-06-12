from __future__ import unicode_literals, absolute_import

import time
import hashlib
from . import neo4j, cypher, utils

try:
    str = unicode
except NameError:
    pass

# Create statement for node with labels and properties
CREATE_RESOURCE_NODE = '''\
CREATE ({labels} {props})
'''

# Create statement for resource relationship
CREATE_RESOURCE_REL = '''\
MATCH (s:`origins:Resource:{sha1}` {start}),
      (e:`origins:Resource:{sha1}` {end})
CREATE (s)-[:`{type}` {props}]->(e)
'''

# Resource graph nodes are labeled with the resource short SHA1 hash.
# IDs are returned for efficiency of write operations.
RESOURCE_NODES = '''\
MATCH (n:`origins:Resource:{sha1}`)
RETURN id(n), n
'''

# Matches all relationships between two resource nodes that is also
# tag with the resource attribute.
RESOURCE_RELS = '''\
MATCH (:`origins:Resource:{sha1}`)-[r]-(:`origins:Resource:{sha1}`)
WHERE r.`origins:resource` = '{sha1}'
RETURN id(r), r
'''

# Deletes the nodes and relationships in a resource graph and returns
# the affected counts.
DELETE_RESOURCE = '''\
MATCH (n:`origins:Resource:{sha1}`)
OPTIONAL MATCH (n)-[r]-()
DELETE r, n
RETURN count(distinct n), count(distinct r)
'''


# Portion of SHA1 used for the node resource label, e.g. Resource:c7e36a
# The label is used to identify all nodes within a resource graph.
SHORT_SHA1_LEN = 6


def _prepare_node_params(oid, node, short_sha1):
    "Prepares string format parameters for an exported node."
    properties = dict(node.get('properties', {}))
    labels = list(node.get('types', []))
    labels.append('origins:Resource:' + short_sha1)

    properties['origins:id'] = oid
    properties['origins:label'] = node.get('label')

    return {
        'labels': cypher.labels_string(labels),
        'props': cypher.map_string(properties),
    }


def _prepare_rel_params(oid, rel, short_sha1):
    "Prepares string format parameters for an exported relationship."
    properties = dict(rel.get('properties', {}))

    properties['origins:id'] = oid
    properties['origins:label'] = rel.get('label')
    properties['origins:resource'] = short_sha1

    start = {'origins:id': rel['start']}
    end = {'origins:id': rel['end']}

    return {
        'start': cypher.map_string(start),
        'end': cypher.map_string(end),
        'props': cypher.map_string(properties),
        'type': rel['type'],
    }


def _get_resource_nodes(short_sha1, tx=None):
    query = RESOURCE_NODES.format(sha1=short_sha1)
    return tx.send(query)


def _get_resource_rels(short_sha1, tx=None):
    query = RESOURCE_RELS.format(sha1=short_sha1)
    return tx.send(query)


def create_resource(data, tx=None):
    "Creates a graph from client exported data."
    if not tx:
        tx = neo4j

    t0 = time.time()

    statements = []

    short_sha1 = hashlib.sha1(data['resource'].encode('utf8'))\
        .hexdigest()[:SHORT_SHA1_LEN]

    for oid, node in data['nodes'].items():
        params = _prepare_node_params(oid, node, short_sha1)
        statements.append(CREATE_RESOURCE_NODE.format(**params))

    for oid, rel in data['relationships'].items():
        params = _prepare_rel_params(oid, rel, short_sha1)
        statements.append(CREATE_RESOURCE_REL
                          .format(sha1=short_sha1, **params))

    tx.send(statements)

    return {
        'time': time.time() - t0,
        'nodes': len(data['nodes']),
        'relationships': len(data['relationships']),
    }


def delete_resource(data, tx=None):
    "Deletes a resource graph given a data export or resource SHA1 hash."
    t0 = time.time()

    if tx is None:
        tx = neo4j

    if isinstance(data, dict):
        short_sha1 = hashlib.sha1(data['resource'].encode('utf8'))\
            .hexdigest()[:SHORT_SHA1_LEN]
    else:
        short_sha1 = data

    query = DELETE_RESOURCE.format(sha1=short_sha1)

    node_count, rel_count = tx.send(query)[0]

    return {
        'time': time.time() - t0,
        'nodes': node_count,
        'relationships': rel_count,
    }


def sync_resource(data, add=True, remove=True, update=True, tx=None):
    """Syncs client exported data with the existing resource graph.

    `add` - Adds new nodes/rels that are not present in the existing graph.
    `remove` - Remove old nodes/rels that are not present in `data`.
    `update` - Merges changes in new nodes/rels into their existing nodes/rels.
    """
    if tx is None:
        tx = neo4j

    t0 = time.time()

    nodes = set()
    rels = set()

    add_nodes = []
    update_nodes = []
    remove_nodes = []

    add_rels = []
    update_rels = []
    remove_rels = []

    statements = []

    short_sha1 = hashlib.sha1(data['resource'].encode('utf8'))\
        .hexdigest()[:SHORT_SHA1_LEN]

    # Existing nodes in the graph
    for nid, old in _get_resource_nodes(short_sha1, tx):
        oid = old['origins:id']

        if oid not in nodes:
            new = data['nodes'].get(oid)

            # No longer exists
            if remove and not new:
                remove_nodes.append(nid)
            elif update:
                diff = utils.diff(new.get('properties'), old)

                # Changes have been made
                if diff:
                    update_nodes.append((nid, oid, new, diff))

            nodes.add(oid)

    # Existing relationships in the graph
    for rid, old in _get_resource_rels(short_sha1, tx):
        oid = old['origins:id']

        if oid not in rels:
            new = data['relationships'].get(oid)

            # No longer exists
            if remove and not new:
                remove_rels.append(nid)
            elif update:
                diff = utils.diff(new.get('properties'), old)

                # Changes have been made
                if diff:
                    update_rels.append((nid, oid, new, diff))

            rels.add(oid)

    # All nodes/rels that have been unseen are marked for creation
    if add:
        for oid, new in data['nodes'].items():
            if oid not in nodes:
                add_nodes.append((oid, new))

        for oid, new in data['relationships'].items():
            if oid not in rels:
                add_rels.append((oid, new))

    # Statements for nodes
    for nid in remove_nodes:
        statements.append('START n=node({}) MATCH (n)-[r]-() '
                          'DELETE r, n'.format(nid))

    for nid, oid, node, diff in update_nodes:
        params = _prepare_node_params(oid, node, short_sha1)
        statements.append('START n=node({nid}) SET n = {props}'
                          .format(nid=nid, **params))

    for oid, node in add_nodes:
        params = _prepare_node_params(oid, node, short_sha1)
        statements.append(CREATE_RESOURCE_NODE.format(**params))

    # Statements for relationships
    for rid in remove_rels:
        statements.append('START r=rel({rid}) DELETE r'.format(rid=rid))

    for rid, oid, rel, diff in update_rels:
        params = _prepare_rel_params(oid, rel, short_sha1)
        statements.append('START r=rel({rid}) SET r = {props}'
                          .format(rid=rid, **params))

    for oid, rel in add_rels:
        params = _prepare_rel_params(oid, rel, short_sha1)
        statements.append(CREATE_RESOURCE_REL
                          .format(sha1=short_sha1, **params))

    tx.send(statements)

    return {
        'time': time.time() - t0,
        'nodes': {
            'added': len(add_nodes),
            'updated': len(update_nodes),
            'removed': len(remove_nodes),
        },
        'relationships': {
            'added': len(add_rels),
            'updated': len(update_rels),
            'removed': len(remove_rels),
        },
    }
