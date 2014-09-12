from ..exceptions import OriginsError, DoesNotExist
from . import neo4j, Component, Relationship, Resource


class SyncError(OriginsError):
    pass


def sync(data, create=True, add=True, remove=True, update=True, tx=neo4j.tx):
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

    `create` - If true, creates a resource if it does not exists.

    `add` - Adds new components and relationships that are not present in the
            existing graph.

    `remove` - Remove old components/rels that are not present in `data`.

    `update` - Merges changes in new components/rels into their existing
               components/rels.
    """
    with tx as tx:
        resource = data['resource']

        if not resource['id']:
            raise SyncError('cannot sync resource without id')

        # Lookup the resource by ID
        try:
            resource = Resource.get_by_id(resource['id'], tx=tx)
            new = False
        except DoesNotExist:
            if not create:
                raise

            resource = Resource.add(tx=tx, **resource)
            new = True

        _components = data.get('components', {})
        _relationships = data.get('relationships', {})

        output = {
            'resource': resource.to_dict(),
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
            for remote in Resource.managed_components(resource.uuid, tx=tx):
                if remote.id in synced_components:
                    continue

                local = _components.get(remote.id)

                # No longer exists
                if not local:
                    if remove:
                        output['components']['removed'] += 1

                        Component.remove(remote.uuid, tx=tx)

                elif update:
                    rev = Component.set(remote.uuid, tx=tx, **local)

                    if rev:
                        output['components']['updated'] += 1

                synced_components.add(remote.id)
                component_revisions[remote.id] = remote.id

            for remote in Resource.managed_relationships(resource.uuid,
                                                         tx=tx):
                if remote.id in synced_relationships:
                    continue

                local = _relationships.get(remote.id)

                # No longer exists
                if not local:
                    if remove:
                        output['relationships']['removed'] += 1

                        Relationship.remove(remote.uuid, tx=tx)

                elif update:
                    rev = Relationship.set(remote.uuid, tx=tx, **local)

                    # Changes have been made
                    if rev:
                        output['relationships']['updated'] += 1

                synced_relationships.add(remote.id)

        # All components/rels that are new in the local data are added
        if add:
            for _id, local in _components.items():
                if _id in synced_components:
                    continue

                remote = Component.add(id=_id, resource=resource, tx=tx,
                                       **local)

                output['components']['added'] += 1
                component_revisions[remote.id] = remote.uuid

            for _id, local in _relationships.items():
                if _id in synced_relationships:
                    continue

                # Map ID to UUIDs
                local['start'] = component_revisions[local['start']]
                local['end'] = component_revisions[local['end']]

                Relationship.add(id=_id, resource=resource, tx=tx, **local)
                output['relationships']['added'] += 1

        return output
