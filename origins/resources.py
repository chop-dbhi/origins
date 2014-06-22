from __future__ import unicode_literals, absolute_import

import logging
from collections import deque
from .backends.base import Component, Rel

try:
    str = unicode
except NameError:
    pass


EXPORT_FORMAT_VERSION = 1.0

logger = logging.getLogger(__name__)


class ResourceExporter(object):
    """Class for creating resource exports derived from the Origins
    connect API for import by the graph API.

    The output is composed of uniquely defined components and relationships
    that are to be included in the named resource.

    An instance is initialized with a resource id or dict with at least the
    `id` key defined.

    Subclasses can override the `component_*` and `relationship_*` methods
    to change the behavior of how the data is prepared and how IDs are
    generated.
    """
    version = EXPORT_FORMAT_VERSION

    def __init__(self, resource):
        if isinstance(resource, dict):
            if 'id' not in resource:
                raise KeyError('dict-based resource must contain an id')

        self.resource = resource
        self.components = {}
        self.relationships = {}

        self.queue = deque()
        self.exported = set()

    def _export_relationship(self, rel):
        if rel.start.root != rel.end.root:
            logger.warning('cross-resource relationships are not supported')
            return

        self._prepare_relationship(rel)

        key = self.relationship_id(rel)

        if key in self.relationships:
            raise ValueError('relationship id {} is not unique'.format(key))

        data = self.relationship_data(rel)

        data['start'] = self.component_id(rel.start)
        data['end'] = self.component_id(rel.end)
        data.setdefault('type', rel.type)

        self.relationships[key] = data

        self.exported.add(rel)

    def _export_component(self, comp, traverse, type, incoming, outgoing):
        self._prepare_component(comp)

        key = self.component_id(comp)

        if key in self.components:
            raise ValueError('component id {} is not unique'.format(key))

        data = self.component_data(comp)
        self.components[key] = data

        self.exported.add(comp)

        if traverse:
            rels = []

            # Queue relationships to neighbors. The start and end
            # components are guaranteed to be queued first, so there is
            # no need to queue them here.
            for rel in comp.rels(type=type, incoming=incoming,
                                 outgoing=outgoing):
                self._queue(rel.start)
                self._queue(rel.end)
                rels.append(rel)

            # Deferred queuing of rels to ensure breadth-first export
            self._queue(rels)

    def _export(self, item, **kwargs):
        if isinstance(item, Component):
            self._export_component(item, **kwargs)
        else:
            self._export_relationship(item)

    def _queue(self, item):
        if isinstance(item, (tuple, list)):
            for _item in item:
                self._queue(_item)
        elif item not in self.queue and item not in self.exported:
            if isinstance(item, Component):
                self.queue.append(item)
            elif isinstance(item, Rel):
                self._queue(item.start)
                self._queue(item.end)
                self.queue.append(item)
            else:
                raise TypeError('unable to queue objects with type "{}"'
                                .format(type(item)))

    # HACK: This ensures components that use lazy loading have their caches
    # pre-filled. See issue #11 for the discussion on the refactor.
    def _prepare_component(self, comp):
        if hasattr(comp, '_foreign_keys_synced'):
            if not comp._foreign_keys_synced:
                comp.foreign_keys

    def _prepare_relationship(self, rel):
        pass

    def component_data(self, comp):
        """Returns a dict of metadata and embedded properties for a
        component.
        """
        return {
            'label': comp.label,
            'type': comp.__class__.__name__,
            'properties': comp.props.copy(),
        }

    def relationship_data(self, rel):
        """Returns a dict of metadata and embedded properties for a
        relationship. The `start` and `end` component ids are augmented
        automatically.
        """
        return {
            'type': rel.type,
            'properties': rel.props.copy(),
        }

    def component_id(self, comp):
        "Returns a unique identifier for a component."
        return comp.path

    def relationship_id(self, rel):
        "Returns a unique identifier for a relationship."
        start = self.component_id(rel.start)
        end = self.component_id(rel.end)

        return '{}:{}:{}'.format(start, rel.type, end)

    def export(self, item=None, traverse=True, type=None, incoming=True,
               outgoing=True):
        """Prepares a component or relationship (or list of them) for export.

        If traverse is true, component relationships will be traversed and
        exported recursively. The `type`, `incoming` and `outgoing` arguments
        dictate which relationships will be handled.
        """
        if item is not None:
            self._queue(item)

        while self.queue:
            item = self.queue.popleft()
            self._export(item, traverse=traverse, type=type, incoming=incoming,
                         outgoing=outgoing)

        return {
            'version': self.version,
            'resource': self.resource,
            'components': self.components,
            'relationships': self.relationships,
        }


def export(resource, item, cls=None, **kwargs):
    """Convenience function for a one-off export.

    An alternate class can be passed via the `cls` argument.
    """
    if not cls:
        cls = ResourceExporter

    return cls(resource).export(item, **kwargs)
