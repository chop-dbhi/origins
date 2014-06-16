from __future__ import unicode_literals

from uuid import uuid4
from . import neo4j, utils, queries, resources


try:
    str = unicode
except NameError:
    pass


def sync(data, add=True, remove=True, update=True, tx=neo4j):
    """Syncs a local data export with the Origins graph.

    The export format is as follows:

        {
            'version': 1.0,
            'resource': '...' | {...},
            'components': {...},
            'relationships': {...},
        }

    Where the `components` and `relationships` keys will be used as the
    `origins:id` for the component.

    The `version` should be supplied in order to process correctly, otherwise
    the 'latest' version is assumed.

    If the resource does not exist, it will be created.

    The sync algorithm relies on the `origins:id` to compare local and remote
    states.

    `add` - Adds new components and relationships that are not present in the
            existing graph.
    `remove` - Remove old components/rels that are not present in `data`.
    `update` - Merges changes in new components/rels into their existing
               components/rels.

    """

    resource = data['resource']

    if isinstance(resource, str):
        resource = {'id': resource}

    new = False
    result = resources.byid(resource['id'], tx=tx)

    if not result:
        new = True
        result = resources.create(resource, tx=tx)

    resource = result

    uuid = resource['uuid']

    components = data.get('components', {})
    relationships = data.get('relationships', {})

    statements = []

    output = {
        'resource': resource,
        'components': {
            'added': 0,
            'updated': 0,
            'removed': 0,
        },
        'relationships': {
            'added': 0,
            'updated': 0,
            'removed': 0,
        }
    }

    synced_components = set()
    synced_rels = set()

    if not new:
        for remote in resources.components(uuid, tx=tx):
            if remote['id'] in synced_components:
                continue

            local = components.get(remote['id'])

            # No longer exists
            if not local:
                if remove:
                    output['components']['removed'] += 1

                    statements.append({
                        'statement': queries.DELETE_ID_NODE,
                        'parameters': {
                            'id': remote['neo4j'],
                        },
                    })
            elif update:
                diff = utils.diff(local.get('properties'),
                                  remote.get('properties'))

                # Changes have been made
                if diff:
                    output['components']['updated'] += 1

                    properties = utils.pack(local)
                    properties['origins:id'] = remote['id']
                    properties['origins:uuid'] = remote['uuid']

                    statements.append({
                        'statement': queries.SET_ID_NODE,
                        'parameters': {
                            'id': remote['neo4j'],
                            'properties': properties,
                        },
                    })

            synced_components.add(remote['id'])

        for remote in resources.relationships(uuid, tx=tx):
            if remote['id'] in synced_rels:
                continue

            local = relationships.get(remote['id'])

            # No longer exists
            if not local:
                if remove:
                    output['relationships']['removed'] += 1

                    statements.append({
                        'statement': queries.DELETE_ID_REL,
                        'parameters': {
                            'id': remote['neo4j'],
                        }
                    })
            elif update:
                diff = utils.diff(local.get('properties'),
                                  remote.get('properties'))

                # Changes have been made
                if diff:
                    output['relationships']['updated'] += 1

                    properties = utils.pack(local)

                    properties.pop('origins:start')
                    properties.pop('origins:end')
                    properties.pop('origins:type')

                    properties['origins:id'] = remote['id']
                    properties['origins:uuid'] = remote['uuid']

                    statements.append({
                        'statement': queries.SET_ID_REL,
                        'parameters': {
                            'id': remote['neo4j'],
                            'properties': properties,
                        }
                    })

            synced_rels.add(remote['id'])

    # All components/rels that are new in the local data are added
    if add:
        for _id, component in components.items():
            if _id in synced_components:
                continue

            output['components']['added'] += 1

            properties = utils.pack(component)
            properties['origins:id'] = _id
            properties['origins:uuid'] = str(uuid4())

            statements.append({
                'statement': queries.CREATE_COMPONENT,
                'parameters': {
                    'resource': {
                        'origins:uuid': uuid,
                    },
                    'properties': properties,
                }
            })

        for _id, rel in relationships.items():
            if _id in synced_rels:
                continue

            output['relationships']['added'] += 1

            properties = utils.pack(rel)

            properties.pop('origins:start')
            properties.pop('origins:end')
            properties.pop('origins:type')

            properties['origins:id'] = _id
            properties['origins:uuid'] = str(uuid4())

            statements.append({
                'statement': queries.CREATE_COMPONENT_REL % {
                    'type': rel['type']
                },
                'parameters': {
                    'resource': {
                        'origins:uuid': uuid,
                    },
                    'start': {
                        'id': rel['start'],
                    },
                    'end': {
                        'id': rel['end'],
                    },
                    'properties': properties,
                },
            })

    tx.send(statements)

    return output
