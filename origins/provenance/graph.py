from __future__ import unicode_literals

import time
import logging
import hashlib
from origins.graph import neo4j, utils, SHORT_SHA1_LEN
from .loader import prepare_statements, parse_prov_data


try:
    str = unicode
except NameError:
    pass


logger = logging.getLogger(__name__)


RESOURCE_PROV_NODES = '''\
MATCH (g:`origins:Provenance:{sha1}`)<-[:`prov:generalEntity`]-(:`prov:Specialization`)-[:`prov:specificEntity`]->(n)
OPTIONAL MATCH (n)<-[:`prov:generatedEntity`*]-(:`prov:Derivation`)
RETURN id(g), g, id(n), n
'''  # noqa


# Deletes the nodes and relationships in a resource graph and returns
# the affected counts.
DELETE_PROV_RESOURCE = '''\
MATCH (n:`origins:Provenance:{sha1}`)
OPTIONAL MATCH (n)-[r]-()
DELETE r, n
RETURN count(distinct n), count(distinct r)
'''


def _get_latest_revisions(short_sha1, tx=None):
    query = RESOURCE_PROV_NODES.format(sha1=short_sha1)
    return tx.send(query)


def create_resource(data, tx=None):
    "Creates a provenance graph from client exported data."
    if tx is None:
        tx = neo4j

    t0 = time.time()

    entity = {}
    specialization = {}
    generation = {}
    dependence = {}

    short_sha1 = hashlib.sha1(data['resource'].encode('utf8'))\
        .hexdigest()[:SHORT_SHA1_LEN]

    for oid, node in data['nodes'].items():
        oid_rev = oid + '_rev'
        oid_gen = oid + '_gen'
        oid_spec = oid + '_spec'

        # Permanode
        entity[oid] = {
            'origins:id': oid,
        }

        # Revision
        entity[oid_rev] = {
            'origins:id': oid,
        }

        entity[oid_rev].update(node.get('properties', {}))

        # Generation
        generation[oid_gen] = {
            'prov:entity': oid_rev,
        }

        # Specialization
        specialization[oid_spec] = {
            'prov:generalEntity': oid,
            'prov:specificEntity': oid_rev,
        }

    for oid, rel in data['relationships'].items():
        oid_rev = oid + '_rev'
        oid_gen = oid + '_gen'
        oid_spec = oid + '_spec'
        oid_dep = oid + '_dep'

        # Permanode
        entity[oid] = {
            'origins:id': oid,
            'origins:type': rel['type'],
        }

        # Revision
        entity[oid_rev] = {
            'origins:id': oid,
        }

        entity[oid_rev].update(rel.get('properties', {}))

        generation[oid_gen] = {
            'prov:entity': oid_rev,
        }

        specialization[oid_spec] = {
            'prov:generalEntity': oid,
            'prov:specificEntity': oid_rev,
        }

        # Dependence
        dependence[oid_dep] = {
            'origins:dependent': rel['start'] + '_rev',
            'origins:dependency': rel['end'] + '_rev',
        }

    bundles = parse_prov_data({
        'prov:entity': entity,
        'prov:specializationOf': specialization,
        'prov:wasGeneratedBy': generation,
        'origins:wasDependentOn': dependence,
    })

    statements = prepare_statements(bundles, short_sha1=short_sha1)

    tx.send(statements)

    return {
        'time': time.time() - t0,
        'prov:entity': len(entity),
        'prov:specializationOf': len(specialization),
        'prov:wasGeneratedBy': len(generation),
        'origins:wasDependentOn': len(dependence),
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

    data_entities = {}
    data_entities.update(data.get('nodes', {}))
    data_entities.update(data.get('relationships', {}))

    nodes = set()

    add_nodes = 0
    update_nodes = 0
    remove_nodes = 0

    add_rels = 0
    update_rels = 0
    remove_rels = 0

    entity = {}
    entity_refs = {}
    specialization = {}
    generation = {}
    invalidation = {}
    dependence = {}

    short_sha1 = hashlib.sha1(data['resource'].encode('utf8'))\
        .hexdigest()[:SHORT_SHA1_LEN]

    # ID of general entity and latest revision id and properties
    for gid, gprops, rid, old in _get_latest_revisions(short_sha1, tx):
        oid = gprops['origins:id']

        oid_old = oid + '_old'
        oid_rev = oid + '_rev'
        oid_gen = oid + '_gen'
        oid_inv = oid + '_inv'
        oid_spec = oid + '_spec'
        oid_dep = oid + '_dep'

        entity_refs[oid] = {
            'origins:neo4j': gid,
        }

        # Current revision is the old and new revision
        entity_refs[oid_old] = {
            'origins:neo4j': rid,
        }

        entity_refs[oid_rev] = {
            'origins:neo4j': rid,
        }

        new = data_entities.get(oid)
        isrel = 'start' in new

        # No longer exists
        if remove and not new:
            if isrel:
                remove_rels += 1
            else:
                remove_nodes += 1

            invalidation[oid_inv] = {
                'prov:entity': oid_old
            }
        elif update:
            diff = utils.diff(new.get('properties'), old)

            # Changes have been made
            if diff:
                if isrel:
                    update_rels += 1
                else:
                    update_nodes += 1

                # New revision entity
                entity[oid_rev] = {
                    'origins:id': oid,
                }

                entity[oid_rev].update(new.get('properties', {}))

                # Invalidate previous revision
                invalidation[oid_inv] = {
                    'prov:entity': oid_old
                }

                # Generation of new entity
                generation[oid_gen] = {
                    'prov:entity': oid_rev,
                }

                # Specialization
                specialization[oid_spec] = {
                    'prov:generalEntity': oid,
                    'prov:specificEntity': oid_rev,
                }

                if isrel:
                    # Dependence
                    dependence[oid_dep] = {
                        'origins:dependent': new['start'] + '_rev',
                        'origins:dependency': new['end'] + '_rev',
                    }

        nodes.add(oid)

    # All nodes/rels that have been unseen are marked for creation
    if add:
        for oid, new in data_entities.items():
            isrel = 'start' in new

            if oid not in nodes:
                if isrel:
                    add_rels += 1
                else:
                    add_nodes += 1

                oid_rev = oid + '_rev'
                oid_gen = oid + '_gen'
                oid_spec = oid + '_spec'
                oid_dep = oid + '_dep'

                # Permanode
                entity[oid] = {
                    'origins:id': oid,
                }

                # Revision
                entity[oid_rev] = {
                    'origins:id': oid,
                }

                entity[oid_rev].update(new.get('properties', {}))

                # Generation
                generation[oid_gen] = {
                    'prov:entity': oid_rev,
                }

                # Specialization
                specialization[oid_spec] = {
                    'prov:generalEntity': oid,
                    'prov:specificEntity': oid_rev,
                }

                if isrel:
                    # Dependence
                    dependence[oid_dep] = {
                        'origins:dependent': new['start'] + '_rev',
                        'origins:dependency': new['end'] + '_rev',
                    }

    new_entities = len(entity)
    entity.update(entity_refs)

    bundles = parse_prov_data({
        'prov:entity': entity,
        'prov:specializationOf': specialization,
        'prov:wasGeneratedBy': generation,
        'prov:wasInvalidatedBy': invalidation,
        'origins:wasDependentOn': dependence,
    })

    statements = prepare_statements(bundles, short_sha1=short_sha1)

    tx.send(statements)

    return {
        'time': time.time() - t0,
        'prov:entity': new_entities,
        'prov:specializationOf': len(specialization),
        'prov:wasGeneratedBy': len(generation),
        'prov:wasInvalidatedBy': len(invalidation),
        'origins:wasDependentOn': len(dependence),
        'nodes': {
            'added': add_nodes,
            'updated': update_nodes,
            'removed': remove_nodes,
        },
        'relationships': {
            'added': add_rels,
            'updated': update_rels,
            'removed': remove_rels,
        },
    }


def delete_resource(data, tx=None):
    """Deletes a resource provenance graph given a data export or
    resource SHA1 hash.
    """
    if tx is None:
        tx = neo4j

    t0 = time.time()

    if tx is None:
        tx = neo4j

    if isinstance(data, dict):
        short_sha1 = hashlib.sha1(data['resource'].encode('utf8'))\
            .hexdigest()[:SHORT_SHA1_LEN]
    else:
        short_sha1 = data

    query = DELETE_PROV_RESOURCE.format(sha1=short_sha1)

    node_count, rel_count = tx.send(query)[0]

    return {
        'time': time.time() - t0,
        'nodes': node_count,
        'relationships': rel_count,
    }
