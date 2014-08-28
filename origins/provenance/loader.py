import os
import json
import logging
import tempfile
import subprocess
import requests
from uuid import uuid4
from urllib.parse import urlparse
from collections import OrderedDict
from origins import utils
from origins.graph import cypher, neo4j
from .identifier import QualifiedName
from .constants import PROV_BUNDLE, COMPONENTS, ALT_COMPONENTS, NAMESPACES, \
    ORIGINS_ATTR_NEO4J, ORIGINS_ATTR_UUID, RELATION_TYPES, \
    RELATION_ATTRS, ORIGINS_ATTR_TIME, EVENT_TYPES, \
    ORIGINS_ATTR_TYPE, PROV_RELATION, PROV_EVENT, PROV_MENTION, \
    PROV_ATTR_BUNDLE, PROV_ATTR_GENERAL_ENTITY


logger = logging.getLogger(__name__)


class InvalidProvenance(Exception):
    pass


def _qualify(attrs, namespaces=None, default=None):
    "Qualifies keys and string values using the provided namespaces."
    if not namespaces:
        namespaces = NAMESPACES

    copy = {}

    # Convert all keys into qualified names
    for key, value in attrs.items():
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
            qualified_attrs = {}
        else:
            qualified_attrs = _qualify(desc_attrs, namespaces, default=default)

        descriptions.append((comp_id, desc_name, comp_type, qualified_attrs))

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


def prepare_props(comp_type, attrs, uuid, time):
    # Copy properties. For relation types, ignore relation attributes
    relations = RELATION_ATTRS.get(comp_type, {})
    props = {}

    for key in attrs:
        # Ignore relation-based attributes since they are defined
        # as edges in the graph
        if key not in relations:
            value = attrs[key]

            if isinstance(value, QualifiedName):
                value = str(value)

            props[str(key)] = value

    props[str(ORIGINS_ATTR_UUID)] = uuid
    props[str(ORIGINS_ATTR_TYPE)] = str(comp_type)

    if ORIGINS_ATTR_TIME not in props:
        props[str(ORIGINS_ATTR_TIME)] = time

    return props


def get_comp_labels(comp_type):
    labels = [comp_type]

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


def prepare_query(start, rel, end, comp_neo4j, comp_uuid):
    # Build query
    query = []
    starts = []
    matches = []
    parameters = {}
    refs = []

    # For start and end nodes, use Neo4j id if available otherwise
    # rely on labels and Origins UUID for matching the node.
    if start in comp_neo4j:
        start_ref, partial, params = comp_neo4j[start]

        starts.append(partial)
        parameters.update(params)
    else:
        start_ref, partial, params = comp_uuid[start]

        matches.append(partial)
        parameters.update(params)

    if end in comp_neo4j:
        end_ref, partial, params = comp_neo4j[end]

        starts.append(partial)
        parameters.update(params)
    else:
        end_ref, partial, params = comp_uuid[end]

        matches.append(partial)
        parameters.update(params)

    refs.append(start_ref)
    refs.append(end_ref)

    if starts:
        query.append('START ' + ','.join(starts))

    if matches:
        query.append('MATCH ' + ','.join(matches))

    query.append('CREATE ({})-[:`{}`]->({})'
                 .format(start_ref, rel, end_ref))

    query.append('RETURN {{{}}}'.format(', '.join('{ref}: id({ref})'
                 .format(ref=r) for r in refs)))

    return {
        'statement': ' '.join(query),
        'parameters': parameters,
    }


def prepare_statements(bundles, time=None):
    """Takes parsed provenance bundles and loads it into the graph.

    It performs the following tasks:

    - Creates nodes if not already identified by a `origins:neo4j`
    id or `origins:uuid`
    - Creates relationships between nodes for relation types
    """
    if not time:
        time = utils.timestamp()

    comp_neo4j = {}
    comp_uuid = {}
    comp_names = OrderedDict()

    queries = []
    relations = []

    for i, (bundle, descriptions) in enumerate(bundles.items()):
        for j, (comp_id, name, comp_type, attrs) in enumerate(descriptions):
            # Unique reference for this comp/name pair
            ref = 'n' + str(i + 1 * j)

            # Keep track of scope of node by giving it a name relative to
            # the bundle
            comp_name = (bundle, name)
            comp_names[comp_name] = ref

            # Neo4j or Origins UUID denotes an existing nodes and should not
            # be created. The partial statement is constructed for a START
            # or MATCH statement downstream
            if ORIGINS_ATTR_NEO4J in attrs:
                neo4j_id = attrs.get(ORIGINS_ATTR_NEO4J)
                partial = '%(ref)s=node({ %(ref)s })' % {'ref': ref}

                comp_neo4j[comp_name] = (ref, partial, {
                    ref: neo4j_id,
                })
            else:
                # Get UUID if present, otherwise add CREATE statement
                if ORIGINS_ATTR_UUID in attrs:
                    uuid = attrs[ORIGINS_ATTR_UUID]
                else:
                    uuid = str(uuid4())

                    props = prepare_props(comp_type, attrs, uuid, time)
                    labels = cypher.labels_string(get_comp_labels(comp_type))

                    statement = 'CREATE (`%(ref)s`%(labels)s { props })' % {
                        'ref': ref,
                        'labels': labels
                    }

                    queries.append({
                        'statement': statement,
                        'parameters': {
                            'props': props,
                        }
                    })

                    # If a relation type, queue for defining edges
                    if comp_type in RELATION_TYPES:
                        relations.append((bundle, name, comp_type, attrs))

                # Construct partial for matching on UUID
                partial = '(%(ref)s%(labels)s {`%(attr)s`: { %(ref)s }})' % {
                    'attr': ORIGINS_ATTR_UUID,
                    'ref': ref,
                    'labels': labels,
                }

                comp_uuid[comp_name] = (ref, partial, {
                    ref: uuid,
                })

            # Description defined in bundle
            if bundle:
                start = (None, bundle)
                end = comp_name
                query = prepare_query(start, 'origins:describes',
                                      end, comp_neo4j, comp_uuid)
                queries.append(query)

    # Post-process relations to ensure all nodes have been created/referenced
    for bundle, name, comp_type, attrs in relations:
        # Key for finding the reference and pattern for the start node
        start = (bundle, name)

        for attr, required in RELATION_ATTRS[comp_type].items():
            end_name = attrs.get(attr)

            if not end_name:
                if not required:
                    continue

                raise InvalidProvenance('relation {} required for component {}'
                                        .format(attr, comp_type))

            # Special handling for mention. The `generalEntity` is located in
            # the bundle specified by the mention attribute `bundle`.
            if comp_type is PROV_MENTION and attr == PROV_ATTR_GENERAL_ENTITY:
                _bundle = attrs[PROV_ATTR_BUNDLE]
                end = (_bundle, end_name)
            else:
                end = (bundle, end_name)

                if end not in comp_names:
                    end = (None, end_name)

            if end not in comp_names:
                raise InvalidProvenance('relation {} could not be found for '
                                        'component {}'.format(attr, comp_type))

            query = prepare_query(start, attr, end, comp_neo4j, comp_uuid)
            queries.append(query)

    return queries, comp_names


def load_document(data, file_type=None, time=None, tx=neo4j.tx):
    "Loads prov data or from a file."
    if isinstance(data, (str, bytes)):
        data = read_document(data, file_type=file_type)

    bundles = parse_prov_data(data)
    statements, comp_names = prepare_statements(bundles, time=time)

    # Creating mapping between ref and neo4j id
    mapping = {}

    for r in tx.send(statements):
        mapping.update(r[0])

    # Rebuild prov document for output
    prov = {}

    for bundle, descriptions in bundles.items():
        for comp_id, name, comp_type, attrs in descriptions:

            ref = comp_names[(bundle, name)]
            neo4j_id = mapping[ref]
            name = name.local

            if bundle:
                prov.setdefault('bundle', {})
                prov['bundle'].setdefault(comp_id, {})

                prov['bundle'][comp_id][name] = {
                    'origins:neo4j': neo4j_id,
                }
            else:
                prov.setdefault(comp_id, {})
                prov[comp_id][name] = {
                    'origins:neo4j': neo4j_id,
                }

    return prov
