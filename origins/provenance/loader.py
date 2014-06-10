from __future__ import unicode_literals

import os
import json
import time
import logging
import tempfile
import subprocess
import requests
from uuid import uuid4
from urlparse import urlparse
from origins.graph import cypher, neo4j
from .constants import PROV_BUNDLE, COMPONENTS, ALT_COMPONENTS, NAMESPACES, \
    ORIGINS_ATTR_NEO4J, ORIGINS_ATTR_UUID, RELATION_TYPES, \
    RELATION_ATTRS, ORIGINS_ATTR_TIMESTAMP, EVENT_TYPES, \
    ORIGINS_ATTR_TYPE, PROV_RELATION, PROV_EVENT, PROV_MENTION, \
    PROV_ATTR_BUNDLE, PROV_ATTR_GENERAL_ENTITY, ORIGINS_PROVENANCE


try:
    str = unicode
except NameError:
    pass


logger = logging.getLogger(__name__)


CREATE_NODE = 'CREATE ({labels} {props})'


class InvalidProvenance(Exception):
    pass


def _qualify(props, namespaces=None, default=None):
    "Qualifies keys and string values using the provided namespaces."
    if not namespaces:
        namespaces = NAMESPACES

    copy = {}

    # Convert all keys into qualified names
    for key, value in props.items():
        if isinstance(key, bytes):
            key = key.decode('utf8')

        try:
            key = namespaces.qname(key, default=default)
        except ValueError:
            logger.debug('cannot qualify property key: {}'.format(key))

        # Handle PROV-JSON data typing: http://www.w3.org/Submission/2013/SUBM-prov-json-20130424/#data-typing  # noqa
        # TODO convert to primitive value if supported
        if isinstance(value, dict) and '$' in value:
            value = value['$']

        if isinstance(value, bytes):
            value = value.decode('utf8')

        if isinstance(value, str):
            try:
                value = namespaces.qname(value, default=default)
            except ValueError:
                logger.debug('cannot qualify property value: {}'.format(value))

        copy[key] = value

    return copy


def _parse_component(comp_id, comp_content, namespaces, default):
    # Qualify the component identifier. Since this is part of the
    # PROV-JSON spec, the default namespace is `prov`.
    comp_type = namespaces.qname(comp_id, default='prov')

    # Get the corresponding component type
    if comp_type not in COMPONENTS:
        if comp_type in ALT_COMPONENTS:
            comp_type = ALT_COMPONENTS[comp_type]
        else:
            raise TypeError('unknown component type {}'.format(comp_type))

    descriptions = []

    # Initialize component instance for each type
    for desc_id, desc_attrs in comp_content.items():
        desc_name = namespaces.qname(desc_id, default=default)

        # Bundles do not have attributes of their own
        if comp_type is PROV_BUNDLE:
            qualified_props = {}
        else:
            qualified_props = _qualify(desc_attrs, namespaces, default=default)

        descriptions.append((desc_name, comp_type, qualified_props))

    return descriptions


def _parse_container(prov_data, namespaces, default=None):
    descriptions = []

    # Since they are the most dependent on, queue up first
    for comp_id in ('entity', 'activity', 'agent'):
        if comp_id not in prov_data:
            continue

        comp_content = prov_data.pop(comp_id)
        descriptions.extend(_parse_component(comp_id, comp_content,
                                             namespaces, default))

    # Iterate over component identifiers and initialize using the corresponding
    # component types.
    for comp_id, comp_content in prov_data.items():
        descriptions.extend(_parse_component(comp_id, comp_content,
                                             namespaces, default))

    return descriptions


def _prepare_node_params(comp_type, props, uuid, timestamp, labels):
    # Copy properties. For relation types, ignore relation attributes
    if comp_type in RELATION_ATTRS:
        copy = {}
        attrs = RELATION_ATTRS[comp_type]

        for key in props:
            if key not in attrs:
                copy[key] = props[key]
    else:
        copy = dict(props)

    copy[ORIGINS_ATTR_UUID] = uuid
    copy[ORIGINS_ATTR_TYPE] = comp_type

    if ORIGINS_ATTR_TIMESTAMP not in copy:
        copy[ORIGINS_ATTR_TIMESTAMP] = timestamp

    return {
        'props': cypher.map_string(copy),
        'labels': cypher.labels_string(labels),
    }


def _comp_labels(comp_type=None, short_sha1=None):
    # Generic label for all provenance nodes. This combined with the UUID
    if short_sha1:
        prov_label = str(ORIGINS_PROVENANCE) + ':' + short_sha1
    else:
        prov_label = ORIGINS_PROVENANCE

    labels = [prov_label]

    if comp_type:
        labels.append(comp_type)

        if comp_type in RELATION_TYPES:
            labels.append(PROV_RELATION)

            if comp_type in EVENT_TYPES:
                labels.append(PROV_EVENT)

    return labels


def parse_prov_data(prov_data):
    """Parses data in the PROV-JSON structure for load.

    The output structure is a dict of bundle name, descriptions pairs.
    A bundle name of `None` denotes the document level.
    """
    prefix = prov_data.pop('prefix', {})
    namespaces = NAMESPACES.extend(prefix)
    default = 'default' in prefix and 'default' or None

    bundles = {}

    descriptions = _parse_container(prov_data, namespaces, default=default)

    bundles[None] = descriptions

    for bundle_id, bundle_content in prov_data.get('bundle', {}).items():
        bundle_name = namespaces.qname(bundle_id, default=default)

        prefix = bundle_content.pop('prefix', None)

        if prefix:
            bundle_namespaces = namespaces.extend(prefix)
        else:
            bundle_namespaces = namespaces

        bundles[bundle_name] = _parse_container(bundle_content,
                                                bundle_namespaces,
                                                default=default)

    return bundles


def read_document(url, file_type=None):
    """Reads or fetchs a PROV document.

    This currently relies on the ProvToolbox to convert the input to the
    PROV-JSON format in the file is in PROV-X, PROV-O, or PROV-N.
    """
    # Remote file, attempt to fetch it
    if urlparse(url).scheme:
        if file_type:
            ext = '.' + file_type
        else:
            ext = os.path.splitext(url)[1]

            if not ext:
                raise TypeError('file type not specified and URL does not '
                                'have an extension')

        resp = requests.get(url, stream=True)

        fd, inpath = tempfile.mkstemp(suffix=ext)

        for chunk in resp.iter_content(512, decode_unicode=True):
            os.write(fd, chunk)

        # Flush buffer to file
        os.fsync(fd)
    else:
        inpath = url

    if inpath.endswith('.json'):
        with open(inpath) as f:
            data = json.load(f)
    else:
        fd, outpath = tempfile.mkstemp(suffix='.json')

        try:
            output = subprocess.check_call([
                'provconvert',
                '-infile', inpath,
                '-outfile', outpath,
            ])
        except OSError as e:
            if e.errno == os.errno.ENOENT:
                raise Exception('provconvert not found on path. ProvToolbox '
                                'must be installed to convert from other '
                                'PROV representations.')
        except subprocess.CalledProcessError:
            raise Exception('error converting to PROV-JSON')

        logging.debug(output)

        with os.fdopen(fd) as f:
            data = json.load(f)

    return data


def prepare_statements(bundles, short_sha1=None):
    """Takes parsed provenance bundles and loads it into the graph.

    It performs the following tasks:

    - Creates component nodes if not already identified by a `origins:neo4j`
    id or `origins:uuid`
    - Creates relationships between nodes for relation types
    """
    timestamp = int(time.time() * 1000)

    comp_neo4j = {}
    comp_uuid = {}

    # Set of (bundle, name) pairs for asserting their existence
    comp_names = set()

    statements = []
    relations = []

    for i, (bundle, descriptions) in enumerate(bundles.items()):
        for j, (name, comp_type, props) in enumerate(descriptions):
            # Generic unique reference
            ref = 'n' + str(i + 1 * j)
            pair = (bundle, name)
            comp_names.add(pair)

            # Neo4j or Origins UUID denotes an existing nodes and should not
            # be created.
            if ORIGINS_ATTR_NEO4J in props:
                neo4j_id = props.pop(ORIGINS_ATTR_NEO4J)
                pattern = '{}=node({})'.format(ref, neo4j_id)
                comp_neo4j[pair] = (ref, pattern)
            else:
                if ORIGINS_ATTR_UUID in props:
                    uuid = props[ORIGINS_ATTR_UUID]
                else:
                    # Create UUID for this node for reference
                    uuid = str(uuid4())

                    _labels = _comp_labels(comp_type, short_sha1)
                    params = _prepare_node_params(comp_type, props, uuid,
                                                  timestamp, _labels)

                    statements.append(CREATE_NODE.format(**params))

                _labels = _comp_labels(None, short_sha1)
                uuid_props = {ORIGINS_ATTR_UUID: uuid}
                pattern = '(`{}`{} {})'.format(ref,
                                               cypher.labels_string(_labels),
                                               cypher.map_string(uuid_props))

                comp_uuid[pair] = (ref, pattern)

            if comp_type in RELATION_TYPES:
                relations.append((bundle, name, comp_type, props))

    # Post-process relations to ensure all nodes have been created/referenced
    for bundle, name, comp_type, props in relations:
        # Key for finding the reference and pattern for the start node
        start = (bundle, name)

        for attr, required in RELATION_ATTRS[comp_type].items():
            end_name = props.get(attr)

            if not end_name:
                if not required:
                    continue

                raise InvalidProvenance('relation {} required for component {}'
                                        .format(attr, comp_type))

            # Special handling for mention. The `generalEntity` is located in
            # the bundle specified by the mention attribute `bundle`.
            if comp_type is PROV_MENTION and attr == PROV_ATTR_GENERAL_ENTITY:
                _bundle = props[PROV_ATTR_BUNDLE]
                end = (_bundle, end_name)
            else:
                end = (bundle, end_name)

                if end not in comp_names:
                    end = (None, end_name)

            if end not in comp_names:
                raise InvalidProvenance('relation {} could not be found for '
                                        'component {}'.format(attr, comp_type))

            # Build query

            query = []
            starts = []
            matches = []

            # For start and end nodes, use Neo4j id if available otherwise
            # rely on Origins UUID for matching the node.
            if start in comp_neo4j:
                start_ref, start_pattern = comp_neo4j[start]
                starts.append(start_pattern)
            else:
                start_ref, start_pattern = comp_uuid[start]
                matches.append(start_pattern)

            if end in comp_neo4j:
                end_ref, end_pattern = comp_neo4j[end]
                starts.append(end_pattern)
            else:
                end_ref, end_pattern = comp_uuid[end]
                matches.append(end_pattern)

            if starts:
                query.append('START ' + ','.join(starts))

            if matches:
                query.append('MATCH ' + ','.join(matches))

            query.append('CREATE ({})-[:`{}`]->({})'
                         .format(start_ref, attr, end_ref))

            statements.append(' '.join(query))

    return statements


def load_document(data, file_type=None, tx=None):
    "Loads prov data or from a file."
    if tx is None:
        tx = neo4j

    if isinstance(data, (str, bytes)):
        data = read_document(data, file_type=file_type)

    bundles = parse_prov_data(data)
    statements = prepare_statements(bundles)

    tx.send(statements)
