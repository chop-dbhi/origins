import logging
from string import Template as T
from uuid import uuid4
from origins.graph import cypher, neo4j, utils
from .constants import PROV_TERMS, PROV_RELATIONS, PROV_TYPES, PROV_EVENTS


logger = logging.getLogger(__name__)


NODE_STATEMENT = T('''
CREATE ($labels { props })
''')


EDGE_STATEMENT = T('''
MATCH (s$slabels {`origins:uuid`: { start }}),
      (e$elabels {`origins:uuid`: { end }})
CREATE (s)-[:`$type`]->(e)
''')


class InvalidProvenance(Exception):
    pass


def _parse_container(prov_data):
    descriptions = []

    for comp_id, comp_content in prov_data.items():
        if comp_id not in PROV_TERMS:
            raise TypeError('unknown component type {}'.format(comp_id))

        for desc_id, desc_attrs in comp_content.items():
            descriptions.append((comp_id, desc_id, desc_attrs))

    return descriptions


def prepare_props(comp_id, attrs, uuid, time):
    # Copy properties. For relation types, ignore relation attributes
    relations = PROV_TYPES[comp_id]
    props = {}

    for key in attrs:
        # Ignore relation-based attributes since they are defined
        # as edges in the graph
        if key not in relations:
            props[key] = attrs[key]

    props['origins:uuid'] = uuid
    props['origins:type'] = PROV_TYPES[comp_id]
    props.setdefault('origins:time', time)

    return props


def get_comp_labels(comp_id):
    labels = [PROV_TYPES[comp_id]]

    if comp_id in PROV_RELATIONS:
        labels.append('prov:Relation')

        if comp_id in PROV_EVENTS:
            labels.append('prov:Event')

    return labels


def parse_prov_data(prov_data):
    """Parses data in the PROV-JSON structure for load.

    The output structure is a dict of bundle name, descriptions pairs.
    A bundle name of `None` denotes the document level.
    """
    bundles = {}

    if 'bundle' in prov_data:
        for bundle_id, bundle_content in prov_data.pop('bundle').items():
            bundles[bundle_id] = _parse_container(bundle_content)

    # Top-level
    bundles[None] = _parse_container(prov_data)

    return bundles


def prepare_query(start, rel, end, comp_uuid):
    start_uuid, start_labels = comp_uuid[start]
    end_uuid, end_labels = comp_uuid[end]

    statement = EDGE_STATEMENT.safe_substitute(start_labels=start_labels,
                                               end_labels=end_labels,
                                               type=type)
    return {
        'statement': statement,
        'parameters': {
            'start': start_uuid,
            'end': end_uuid,
        }
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

    comp_uuid = {}
    comp_names = {}

    queries = []
    relations = []

    for i, (bundle, descriptions) in enumerate(bundles.items()):
        for j, (comp_id, name, attrs) in enumerate(descriptions):
            # Unique reference for this comp/name pair
            ref = 'n' + str(i + 1 * j)

            # Keep track of scope of node by giving it a name relative to
            # the bundle
            comp_name = (bundle, name)
            comp_names[comp_name] = ref

            # An Origins UUID denotes an existing nodes and should not
            # be created. The partial statement is constructed for a START
            # or MATCH statement downstream
            labels = cypher.labels(get_comp_labels(comp_id))

            # Get UUID if present, otherwise add CREATE statement
            if 'origins:uuid' in attrs:
                uuid = attrs['origins:uuid']
            else:
                uuid = str(uuid4())

                props = prepare_props(comp_id, attrs, uuid, time)

                statement = NODE_STATEMENT.safe_substitute(labels=labels)

                queries.append({
                    'statement': statement,
                    'parameters': {
                        'props': props,
                    }
                })

                # If a relation type, queue for defining edges
                if comp_id in PROV_RELATIONS:
                    relations.append((bundle, name, comp_id, attrs))

            comp_uuid[comp_name] = (uuid, labels)

            # Description defined in bundle
            if bundle:
                start = (None, bundle)
                end = comp_name

                start_uuid, start_labels = comp_uuid[start]
                end_uuid, end_labels = comp_uuid[end]

                statement = EDGE_STATEMENT.substitute(slabels=start_labels,
                                                      elabels=end_labels,
                                                      type='origins:describes')
                queries.append({
                    'statement': statement,
                    'parameters': {
                        'start': start_uuid,
                        'end': end_uuid,
                    }
                })

    # Post-process relations to ensure all nodes have been created/referenced
    for bundle, name, comp_id, attrs in relations:
        # Key for finding the reference and pattern for the start node
        start = (bundle, name)

        for attr, required in PROV_RELATIONS[comp_id].items():
            end_name = attrs.get(attr)

            if not end_name:
                if not required:
                    continue

                raise InvalidProvenance('relation {} required for component {}'
                                        .format(attr, comp_id))

            # Special handling for mention. The `generalEntity` is located in
            # the bundle specified by the mention attribute `bundle`.
            if comp_id == 'mentionOf' and attr == 'prov:generalEntity':
                _bundle = attrs['prov:bundle']
                end = (_bundle, end_name)
            else:
                end = (bundle, end_name)

                if end not in comp_names:
                    end = (None, end_name)

            if end not in comp_names:
                raise InvalidProvenance('relation {} could not be found for '
                                        'component {}'.format(attr, comp_id))

            start_uuid, start_labels = comp_uuid[start]
            end_uuid, end_labels = comp_uuid[end]

            statement = EDGE_STATEMENT.substitute(slabels=start_labels,
                                                  elabels=end_labels,
                                                  type=attr)
            queries.append({
                'statement': statement,
                'parameters': {
                    'start': start_uuid,
                    'end': end_uuid,
                }
            })

    return queries, comp_uuid


def load(data, time=None, tx=neo4j.tx):
    "Loads data in the PROV-JSON."
    with tx as tx:
        bundles = parse_prov_data(data)
        statements, mapping = prepare_statements(bundles, time=time)

        tx.send(statements, defer=True)

        # Rebuild prov document for output
        prov = {}

        for bundle, descriptions in bundles.items():
            for comp_id, name, attrs in descriptions:

                uuid, _ = mapping[(bundle, name)]

                if bundle:
                    prov.setdefault('bundle', {})
                    prov['bundle'].setdefault(comp_id, {})

                    prov['bundle'][comp_id][name] = {
                        'origins:uuid': uuid,
                    }
                else:
                    prov.setdefault(comp_id, {})
                    prov[comp_id][name] = {
                        'origins:uuid': uuid,
                    }

        return prov
