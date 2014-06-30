from __future__ import unicode_literals, absolute_import

from . import neo4j, utils, constraints, components, relationships, resources

try:
    str = unicode
except NameError:
    pass


def sync(data, add=True, remove=True, update=True, tx=None):
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
    constraints.ensure()

    if tx is None:
        tx = neo4j.Transaction()

    with tx as tx:
        resource = data['resource']

        if not isinstance(resource, dict):
            resource = {'origins:id': resource}
        else:
            resource = dict(resource)

        new = False
        result = resources.get(resource['origins:id'], tx=tx)

        if not result:
            new = True
            result = resources.create(resource, tx=tx)

        resource = result

        _components = data.get('components', {})
        _relationships = data.get('relationships', {})

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
        synced_relationships = set()

        component_revisions = {}

        if not new:
            for remote in resources.components(resource, managed=True, tx=tx):
                if remote['id'] in synced_components:
                    continue

                local = _components.get(remote['id'])

                # No longer exists
                if not local:
                    if remove:
                        output['components']['removed'] += 1

                        components.delete(remote, tx=tx)

                elif update:
                    diff = utils.diff(local.get('properties'),
                                      remote.get('properties'))

                    # Changes have been made
                    if diff:
                        output['components']['updated'] += 1

                        properties = utils.pack(local)
                        properties['origins:id'] = remote['id']

                        components.update(remote, properties, tx=tx)

                synced_components.add(remote['id'])
                component_revisions[remote['id']] = remote['uuid']

            for remote in resources.relationships(resource, managed=True,
                                                  tx=tx):
                if remote['id'] in synced_relationships:
                    continue

                local = _relationships.get(remote['id'])

                # No longer exists
                if not local:
                    if remove:
                        output['relationships']['removed'] += 1

                        relationships.delete(remote['uuid'], tx=tx)

                elif update:
                    diff = utils.diff(local.get('properties'),
                                      remote.get('properties'))

                    # Changes have been made
                    if diff:
                        output['relationships']['updated'] += 1

                        properties = utils.pack(local)
                        properties['origins:id'] = remote['id']

                        # Get UUID of the current start and end component
                        # revisions given the provided ID
                        start_id = properties.pop('origins:start')
                        start = component_revisions[start_id]

                        end_id = properties.pop('origins:end')
                        end = component_revisions[end_id]

                        relationships.update(remote, properties, tx=tx)

                synced_relationships.add(remote['id'])

        # All components/rels that are new in the local data are added
        if add:
            for _id, component in _components.items():
                if _id in synced_components:
                    continue

                output['components']['added'] += 1

                properties = utils.pack(component)
                properties['origins:id'] = _id

                remote = components.create(properties, resource=resource,
                                           tx=tx)

                component_revisions[remote['id']] = remote['uuid']

            for _id, rel in _relationships.items():
                if _id in synced_relationships:
                    continue

                output['relationships']['added'] += 1

                properties = utils.pack(rel)

                # Get UUID of the current start and end component
                # revisions given the provided ID
                start_id = properties.pop('origins:start')
                start = component_revisions[start_id]

                end_id = properties.pop('origins:end')
                end = component_revisions[end_id]

                relationships.create(_id,
                                     start=start,
                                     end=end,
                                     properties=properties,
                                     resource=resource,
                                     tx=tx)

        return output
