"""
A single "origin" index exist for maintaining a unique set of origins.
"""

from __future__ import division, unicode_literals, absolute_import

import json
import requests


MERGE_NODE_STMT = 'MERGE ({ref}:{label} {match_props}) ' \
    'ON CREATE SET {ref} = {create_props} ON MATCH SET {set_props}'

MERGE_REL_STMT = 'MERGE ({ref1})-[:{reltype}]->({ref2})'


class SyncQueryFactory(object):
    def __init__(self):
        # Counter for the number of references in this query
        self.refcount = 0
        # Maps the uri to the reference used in the query
        self.refmap = {}

    def getref(self, key):
        if key not in self.refmap:
            ref = 'x{}'.format(self.refcount)
            self.refcount += 1
            self.refmap[key] = ref
        return self.refmap[key]

    def hasref(self, key):
        return key in self.refmap

    def deref(self):
        self.refmap = {}
        self.refcount = 0

    def _dict_props(self, props):
        toks = []
        for key, value in props.iteritems():
            if value is None:
                value = ''
            if isinstance(value, (str, unicode)):
                s = "{}: '{}'"
            else:
                s = '{}: {}'
            toks.append(s.format(key, value))
        return '{{ {} }}'.format(', '.join(toks))

    def _keyword_props(self, ref, props):
        toks = []
        for key, value in props.iteritems():
            if value is None:
                value = ''
            if isinstance(value, (str, unicode)):
                s = "{}.{} = '{}'"
            else:
                s = '{}.{} = {}'
            toks.append(s.format(ref, key, value))
        return ', '.join(toks)

    def merge_node_statement(self, node, label):
        props = node.serialize(label=True, uri=True)
        uri = props['uri']

        ref = self.getref(uri)
        match_props = self._dict_props({'uri': uri})
        create_props = self._dict_props(props)
        set_props = self._keyword_props(ref, props)

        return MERGE_NODE_STMT.format(ref=ref, label=label,
                                      match_props=match_props,
                                      create_props=create_props,
                                      set_props=set_props)

    def merge_rel_statement(self, n1, reltype, n2):
        ref1 = self.getref(n1.uri)
        ref2 = self.getref(n2.uri)
        return MERGE_REL_STMT.format(ref1=ref1, ref2=ref2, reltype=reltype)


DEFAULT_URI = 'http://localhost:7474/db/data/'
# Endpoint for the single transaction
TRANSACTION_URI_TMPL = '{}transaction/commit'


def export(origin, uri=DEFAULT_URI, include_sources=True, batch_size=100):
    factory = SyncQueryFactory()
    stmts = []
    size = 0

    for elem in origin.elements:
        if not factory.hasref(origin.uri):
            stmts.append(factory.merge_node_statement(origin, label='Origin'))
        stmts.append(factory.merge_node_statement(elem, label='Element'))
        stmts.append(factory.merge_rel_statement(origin, 'ORIGIN', elem))

        # Create the source path by walking up from the element through
        # the sources.
        if include_sources:
            source = elem.source
            child = elem

            while True:
                if not source:
                    break

                if not factory.hasref(source.uri):
                    stmts.append(factory.merge_node_statement(source,
                                                              label='Branch'))
                stmts.append(factory.merge_rel_statement(source,
                                                         'BRANCH', child))

                child = source
                source = child.source

        size += 1

        if batch_size >= size:
            send_request(uri, stmts)
            factory.deref()
            stmts = []
            size = 0

    if size:
        send_request(uri, stmts)


def send_request(uri, stmts):
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
