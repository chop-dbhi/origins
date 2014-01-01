from __future__ import unicode_literals, absolute_import

import json
import requests
from requests import exceptions
from origins import logger
from origins.graph import Node, Rel
from . import base

# Default URI to Neo4j REST endpoint
DEFAULT_URI = 'http://localhost:7474/db/data/'

# Endpoint for the single transaction
TRANSACTION_URI_TMPL = '{}transaction/commit'

# Cypher statement template for ensuring a node based.
MERGE_NODE_STMT = 'MERGE ({r}{labels} {props}) {oncreate} {onmatch}'

# Cypher statement template for ensuring a relationship between nodes.
MERGE_REL_STMT = 'MERGE ({r1})-[{r}:{type}]->({r2}) {oncreate} {onmatch}'


def node_labels(node):
    # Default label for all nodes exported via Origins
    labels = ['Origins']
    if node.isleaf:
        labels.append('Element')
    if node.isroot:
        labels.append('Origin')
    return labels


class StatementFactory(object):
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

    def _onmatch_stmt(self, ref, props):
        if not props:
            return ''
        props = self._keyword_props(ref, props)
        return 'ON MATCH SET {}'.format(props)

    def merge_node(self, node):
        _props = node.serialize()
        ref = self.getref(node.id)
        labels = self._labels_stmt(node_labels(node))
        props = self._dict_props({'uri': _props['uri']})
        oncreate = self._oncreate_stmt(ref, _props)
        onmatch = self._onmatch_stmt(ref, _props)
        return MERGE_NODE_STMT.format(r=ref, labels=labels, props=props,
                                      oncreate=oncreate, onmatch=onmatch)

    def merge_rel(self, rel):
        ref = self.getref(rel.id)
        ref1 = self.getref(rel.start.id)
        ref2 = self.getref(rel.end.id)
        oncreate = self._oncreate_stmt(ref, rel.props)
        onmatch = self._onmatch_stmt(ref, rel.props)
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

    try:
        resp = requests.post(url, data=data, headers=headers)
        resp.raise_for_status()
    except exceptions.RequestException:
        logger.exception('error communicating with Neo4j')
        return

    data = resp.json()

    if data['errors']:
        logger.error('There was an error with the request body. '
                     'Please submit a bug report with the following '
                     'content (remember to review and prune out any '
                     'sensitive information).\n\n'
                     + json.dumps(data['errors']))
        return False
    return True


class Exporter(base.Exporter):
    def __init__(self):
        self.statements = []
        self.factory = StatementFactory()
        self.node_count = 0
        self.rel_count = 0

    def _prepare_rel(self, rel):
        if not self.factory.hasref(rel.id):
            if not self.factory.hasref(rel.start.id):
                self.statements.append(self.factory.merge_node(rel.start))
                self.node_count += 1

            if not self.factory.hasref(rel.end.id):
                self.statements.append(self.factory.merge_node(rel.end))
                self.node_count += 1

            self.statements.append(self.factory.merge_rel(rel))
            self.rel_count += 1

    def _prepare_node(self, node, traverse):
        if not self.factory.hasref(node.id):
            for rel in node.relpath:
                self._prepare_rel(rel)

            if traverse:
                for rel in node.rels():
                    self._prepare_node(rel.end, traverse=traverse)
                    self._prepare_rel(rel)

    def prepare(self, node, traverse=True):
        "Prepares a node or relationship for export."
        if isinstance(node, Node):
            self._prepare_node(node, traverse=traverse)
        elif isinstance(node, Rel):
            self._prepare_rel(node)
        elif isinstance(node, (tuple, list)):
            for _node in node:
                self.prepare(_node, traverse=traverse)
        else:
            raise TypeError('unable to prepare objects with type "{}"'
                            .format(type(node)))

    def export(self, uri=None):
        """Exports a node and it's parents up to and including the origin.

        If `traverse` if true, relationships the node has will be recursively
        exported including the end nodes.

        `batch_size` is used to prevent constructing too large of a statement
        for the REST API to process (e.g. out of memory errors).
        """
        if not self.statements:
            logger.warn('nothing to export')
            return

        if send_request(self.statements, uri=uri):
            return {
                'nodes': self.node_count,
                'rels': self.rel_count,
            }


def export(node, traverse=True, uri=None):
    "Convenience function for exporting a node, relationship, or set."
    exporter = Exporter()
    exporter.prepare(node, traverse=traverse)
    return exporter.export(uri=uri)
