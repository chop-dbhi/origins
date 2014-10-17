from collections import defaultdict
from ..exceptions import OriginsError, DoesNotExist, ValidationError
from . import neo4j, Component, Relationship, Resource


class SyncError(OriginsError):
    def __init__(self, message=None, *args, **kwargs):
        if not message:
            message = 'Sync failed with validation errors'
        super(OriginsError, self).__init__(message, *args, **kwargs)


def _validate(attrs, allowed_keys, required_keys):
    if not attrs:
        attrs = {}

    required = []
    unknown = []

    for key in required_keys:
        if key not in attrs:
            required.append(key)

    for key in attrs:
        if key not in allowed_keys:
            unknown.append(key)

    errors = {}

    if required:
        errors['missing_attributes'] = required

    if unknown:
        errors['unknown_attributes'] = unknown

    return errors


def _combine_errors(combined, _id, errors):
    if errors:
        for key in errors:
            combined[key][_id] = errors[key]

    return combined


def validate_format(data):
    "Pre-validates the structure of the data."
    combined = defaultdict(dict)

    resource_attrs = {'id', 'label', 'description', 'type',
                      'properties', 'time'}
    component_attrs = {'id', 'label', 'description', 'type',
                       'properties', 'time'}
    relationship_attrs = {'id', 'label', 'description', 'type',
                          'properties', 'time', 'start', 'end',
                          'dependence', 'direction'}

    errors = _validate(data['resource'], resource_attrs, ('id',))

    _combine_errors(combined, 'resource', errors)

    components = data.get('components', {})
    relationships = data.get('relationships', {})

    for cid, attrs in components.items():
        attrs['id'] = cid
        errors = _validate(attrs, component_attrs, ('id',))
        _combine_errors(combined, cid, errors)

    for rid, attrs in relationships.items():
        attrs['id'] = rid
        errors = _validate(attrs, relationship_attrs,
                           ('id', 'start', 'end'))

        if 'start' in attrs and attrs['start'] not in components:
            errors['undefind_start'] = attrs['start']

        if 'end' in attrs and attrs['end'] not in components:
            errors['undefind_end'] = attrs['end']

        _combine_errors(combined, rid, errors)

    if combined:
        raise SyncError(combined)


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
    validate_format(data)

    with tx as tx:
        # Lookup the resource by ID
        try:
            resource = Resource.get_by_id(data['resource']['id'], tx=tx)
            new = False
        except DoesNotExist:
            if not create:
                raise

            resource = Resource.add(tx=tx, validate=False, **data['resource'])
            new = True

        components = data.get('components', {})
        relationships = data.get('relationships', {})

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

        # Map of ID => latest UUID
        component_revisions = {}
        relationship_revisions = {}

        # Locally removed components which are checked against relationships
        # that use them.
        removed_components = set()

        if not new:
            for remote in Resource.managed_components(resource.uuid, tx=tx):
                if remote.id in component_revisions:
                    continue

                local = components.get(remote.id)

                # No longer exists
                if not local:
                    if remove:
                        Component.remove(remote, validate=False, tx=tx)
                        removed_components.add(remote.id)
                        output['components']['removed'] += 1

                elif update:
                    rev = Component.set(remote, validate=False, tx=tx,
                                        **local)

                    if rev:
                        output['components']['updated'] += 1

                component_revisions[remote.id] = remote.id

            for remote in Resource.managed_relationships(resource.uuid,
                                                         tx=tx):
                if remote.id in relationship_revisions:
                    continue

                local = relationships.get(remote.id)
                start = local.get('start')
                end = local.get('end')

                # No longer exists
                if not local:
                    if remove:
                        Relationship.remove(remote, validate=False, tx=tx)

                        output['relationships']['removed'] += 1

                elif update:
                    # TODO should this confirm the UUIDs are the same?
                    rev = Relationship.set(remote, validate=False, tx=tx,
                                           **local)

                    # Changes have been made
                    if rev:
                        output['relationships']['updated'] += 1

                        # If there is a diff, check for a conflict
                        if start in removed_components:
                            raise ValidationError('relationship {} has '
                                                  'changed, but the start '
                                                  'component has been '
                                                  'removed')

                        if end in removed_components:
                            raise ValidationError('relationship {} has '
                                                  'changed, but the end '
                                                  'component has been '
                                                  'removed')

                relationship_revisions[remote.id] = remote.uuid

        # All components/rels that are new in the local data are added
        if add:
            for _id, local in components.items():
                if _id in component_revisions:
                    continue

                local['id'] = _id
                remote = Component.add(resource=resource, tx=tx,
                                       validate=False, **local)

                output['components']['added'] += 1
                component_revisions[remote.id] = remote.uuid

            for _id, local in relationships.items():
                if _id in relationship_revisions:
                    continue

                # Map ID to UUIDs
                local['id'] = _id
                local['start'] = component_revisions[local['start']]
                local['end'] = component_revisions[local['end']]

                remote = Relationship.add(resource=resource, tx=tx,
                                          validate=False, **local)
                output['relationships']['added'] += 1
                relationship_revisions[remote.id] = remote.uuid

        return output
