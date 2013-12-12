import json
import requests


# Default URI to Neo4j REST endpoint
DEFAULT_URI = 'http://localhost:7474/db/data/'

# Endpoint for the single transaction
TRANSACTION_URI_TMPL = '{}transaction/commit'

# Cypher statement template for creating a node.
CREATE_NODE_STMT = 'CREATE ({r}{labels} {props})'

# Cypher statement template for ensuring a node based.
MERGE_NODE_STMT = 'MERGE ({r}{labels} {props}) {oncreate} {onmatch}'

# Cypher statement template for creating a relationship between nodes.
CREATE_REL_STMT = 'CREATE ({r1})-[{r}:{type} {props}]->({r2})'

# Cypher statement template for ensuring a relationship between nodes.
MERGE_REL_STMT = 'MERGE ({r1})-[{r}:{type}]->({r2}) {oncreate} {onmatch}'


def node_labels(node):
    labels = []
    if node.isleaf:
        labels.append('Element')
    if node.isroot:
        labels.append('Origin')
    return labels


class MergeFactory(object):
    """Produces statements and tracks references to previously matched
    nodes and relationships.

    This enables constructing one large statement rather than doing many
    subsequent lookups which is optimized for bulk imports.
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

    def _labels_stmt(self, labels):
        if not labels:
            return ''
        return ':' + ':'.join(labels)

    def _oncreate_stmt(self, ref, props):
        if not props:
            return ''
        props = self._dict_props(props)
        return 'ON CREATE SET {} = {}'.format(ref, props)

    def _onmatch_stmt(self, ref, props, replace):
        if not props:
            return ''
        if replace:
            props = self._dict_props(props)
            return 'ON MATCH SET {} = {}'.format(ref, props)
        props = self._keyword_props(ref, props)
        return 'ON MATCH SET {}'.format(props)

    def create_node(self, node):
        ref = self.getref(node.id)
        labels = self._labels_stmt(node_labels(node))
        props = self._dict_props(node.serialize())
        return CREATE_NODE_STMT.format(r=ref, labels=labels, props=props)

    def merge_node(self, node, replace=False):
        _props = node.serialize()
        ref = self.getref(node.id)
        labels = self._labels_stmt(node_labels(node))
        props = self._dict_props({'uri': _props['uri']})
        oncreate = self._oncreate_stmt(ref, _props)
        onmatch = self._onmatch_stmt(ref, _props, replace)
        return MERGE_NODE_STMT.format(r=ref, labels=labels, props=props,
                                      oncreate=oncreate, onmatch=onmatch)

    def create_rel(self, rel):
        ref = self.getref(rel.id)
        ref1 = self.getref(rel.start.id)
        ref2 = self.getref(rel.end.id)
        props = self._dict_props(rel.props)
        return CREATE_REL_STMT.format(r=ref, r1=ref1, r2=ref2, props=props)

    def merge_rel(self, rel, replace=False):
        ref = self.getref(rel.id)
        ref1 = self.getref(rel.start.id)
        ref2 = self.getref(rel.end.id)
        oncreate = self._oncreate_stmt(ref, rel.props)
        onmatch = self._onmatch_stmt(ref, rel.props, replace)
        return MERGE_REL_STMT.format(r=ref, r1=ref1, r2=ref2, type=rel.type,
                                     oncreate=oncreate, onmatch=onmatch)


def send_request(statements, uri=None):
    "Sends a request to the transaction endpoint."
    if not uri:
        uri = DEFAULT_URI
    url = TRANSACTION_URI_TMPL.format(uri)

    data = json.dumps({
        'statements': [{'statement': ' '.join(statements)}]
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
