from __future__ import unicode_literals

import time
import logging
import hashlib
from origins.graph import neo4j, SHORT_SHA1_LEN
from .loader import prepare_statements, parse_prov_data


try:
    str = unicode
except NameError:
    pass


logger = logging.getLogger(__name__)


RESOURCE_PROV_NODES = '''\
MATCH (n:`origins:Provenance:{sha1}`)
RETURN id(n), n
'''

RESOURCE_PROV_LATEST_REVISION_NODES = '''\
MATCH (:`origins:Provenance:{sha1}`)<-[:`prov:generalEntity`]-(:`origins:Specialization`)-[:`prov:specificEntity`]->(n)
OPTIONAL MATCH (n)<-[:`prov:generatedEntity`*]-(:`origins:Derivation`)
RETURN id(n), n
'''  # noqa


# Deletes the nodes and relationships in a resource graph and returns
# the affected counts.
DELETE_PROV_RESOURCE = '''\
MATCH (n:`origins:Provenance:{sha1}`)
OPTIONAL MATCH (n)-[r]-()
DELETE r, n
RETURN count(distinct n), count(distinct r)
'''


def create_resource(data, tx=None):
    "Creates a provenance graph from client exported data."
    if tx is None:
        tx = neo4j

    t0 = time.time()

    entity = {}
    specialization = {}
    generation = {}
    dependence = {}

    short_sha1 = hashlib.sha1(data['resource'])\
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
        entity[oid_rev] = dict(node.get('properties', {}))

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
        entity[oid_rev] = dict(rel.get('properties', {}))

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
        short_sha1 = hashlib.sha1(data['resource'])\
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
