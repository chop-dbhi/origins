from __future__ import unicode_literals, absolute_import

import json
import requests

# Default URI
DEFAULT_URI = 'http://localhost:7474/db/data/'

# Endpoint for the single transaction
TRANSACTION_URI_TMPL = '{}transaction/commit'


# Cypher statement template for creating or updating a node based
# on the match properties. The update properties must be a series
# of key = value expressions to ensure other properties are not
# removed.
MERGE_NODE_STMT = '''
MERGE ({ref}:{label} {match_props})
    ON CREATE SET {ref} = {create_props}
    ON MATCH SET {update_props}
'''

# Cypher statement template for creating a relationship
# between nodes.
MERGE_REL_STMT = '''
MERGE ({ref1})-[:{reltype}]->({ref2})
'''

# Cypher statement template for creating a relationship
# between nodes with properties.
MERGE_REL_PROPS_STMT = '''
MERGE ({ref1})-[{ref}:{reltype}]->({ref2})
    ON CREATE SET {ref} = {create_props}
    ON MATCH SET {update_props}
'''


class MergeFactory(object):
    """Produces statements and tracks references to previously matched
    nodes and relationships.

    This enables constructing one large statement rather than doing many
    subsequent lookups which is optimized for exporting hierarchies.
    """
    def __init__(self):
        self.rc = 0
        self.rm = {}

    def getref(self, key):
        if key not in self.rm:
            ref = 'x{}'.format(self.rc)
            self.rc += 1
            self.rm[key] = ref
        return self.rm[key]

    def hasref(self, key):
        return key in self.rm

    def deref(self):
        self.rm = {}
        self.rc = 0

    def _dict_props(self, props):
        "Converts a dict into a valid properties object in Cypher syntax."
        toks = []
        for key, value in props.iteritems():
            if value is None:
                continue
            if isinstance(value, (str, unicode)):
                s = "{}: '{}'"
            else:
                s = '{}: {}'
            toks.append(s.format(key, value))
        return '{{ {} }}'.format(', '.join(toks))

    def _keyword_props(self, ref, props):
        "Converts a dict into an array of valid assignments in Cypher syntax."
        toks = []
        for key, value in props.iteritems():
            if value is None:
                continue
            if isinstance(value, (str, unicode)):
                s = "{}.{} = '{}'"
            else:
                s = '{}.{} = {}'
            toks.append(s.format(ref, key, value))
        return ', '.join(toks)

    def merge_node_stmt(self, node, label):
        props = node.serialize(label=True, uri=True)
        uri = props['uri']

        ref = self.getref(uri)
        match_props = self._dict_props({'uri': uri})
        create_props = self._dict_props(props)
        update_props = self._keyword_props(ref, props)

        return MERGE_NODE_STMT.format(ref=ref, label=label,
                                      match_props=match_props,
                                      create_props=create_props,
                                      update_props=update_props)

    def merge_rel_stmt(self, n1, reltype, n2, props=None):
        ref1 = self.getref(n1.uri)
        ref2 = self.getref(n2.uri)

        if not props:
            return MERGE_REL_STMT.format(ref1=ref1, ref2=ref2, reltype=reltype)

        ref = '_'.join([ref1, reltype, ref2])
        create_props = self._dict_props(props)
        update_props = self._keyword_props(ref, props)
        return MERGE_REL_PROPS_STMT.format(ref=ref, ref1=ref1,
                                           ref2=ref2, reltype=reltype,
                                           create_props=create_props,
                                           update_props=update_props)


def prepare_element(elem, origin, factory, include_sources=True):
    stmts = []
    stmts.append(factory.merge_node_stmt(elem, label='Element'))

    if origin:
        if not factory.hasref(origin.uri):
            stmts.append(factory.merge_node_stmt(origin, label='Origin'))
        stmts.append(factory.merge_rel_stmt(origin, 'ORIGIN', elem))

    # Create the source path by walking up from the element through
    # the sources.
    if include_sources:
        source = elem.source
        child = elem

        while True:
            if not source:
                break

            if not factory.hasref(source.uri):
                stmts.append(factory.merge_node_stmt(source, label='Branch'))
            stmts.append(factory.merge_rel_stmt(source, 'BRANCH', child))

            child = source
            source = child.source

    return stmts


def export(node, uri=DEFAULT_URI, include_sources=True, batch_size=100):
    """Exports a node, it's origin and optionally it's sources.
    `batch_size` is used to prevent constructing too large of a statement
    for the REST API to process (e.g. out of memory errors).
    """
    factory = MergeFactory()
    stmts = []
    size = 0

    if node.iselement:
        elements = [node]
    else:
        elements = node.elements

    origin = node.origin

    for elem in elements:
        stmts.extend(prepare_element(elem, origin, factory, include_sources))
        size += 1

        if batch_size >= size:
            send_request(uri, stmts)
            factory.deref()
            stmts = []
            size = 0

    if size:
        send_request(uri, stmts)


def send_request(uri, stmts):
    "Sends a request to the transaction endpoint."
    url = TRANSACTION_URI_TMPL.format(uri)

    data = json.dumps({
        'statements': [{'statement': ' '.join(stmts)}]
    })

    headers = {
        'accept': 'application/json; charset=utf-8',
        'content-type': 'application/json',
    }

    resp = requests.post(url, data=data, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if data['errors']:
        print(data['errors'])
