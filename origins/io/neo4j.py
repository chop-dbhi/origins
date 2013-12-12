from __future__ import unicode_literals, absolute_import

from . import _cypher


def prepare_rel(rel, factory, traverse=True):
    stmts = []

    if not factory.hasref(rel.id):
        if not factory.hasref(rel.start.id):
            stmts.append(factory.merge_node(rel.start))
        if not factory.hasref(rel.end.id):
            stmts.append(factory.merge_node(rel.end))
        if not factory.hasref(rel.id):
            stmts.append(factory.merge_rel(rel))

    return stmts


def prepare_node(node, factory, traverse=True):
    if not factory.hasref(node.id):
        # The relationship from the root.
        for rel in node.relpath:
            yield prepare_rel(rel, factory, traverse=traverse)
        # Other relationships
        for rel in node.rels():
            yield prepare_rel(rel, factory, traverse=traverse)
    yield []


def parse(node, traverse=True):
    statements = []
    factory = _cypher.MergeFactory()

    for stmts in prepare_node(node, factory, traverse=traverse):
        statements.extend(stmts)
    return '\n'.join(statements)


def export(node, uri=None, traverse=True, batch_size=100):
    """Exports a node and it's parents up to and including the origin.

    If `traverse` if true, relationships the node has will be recursively
    exported including the end nodes.

    `batch_size` is used to prevent constructing too large of a statement
    for the REST API to process (e.g. out of memory errors).
    """
    statements = []
    factory = _cypher.MergeFactory()

    for stmts in prepare_node(node, factory, traverse=traverse):
        statements.extend(stmts)
        if batch_size >= len(statements):
            _cypher.send_request(statements, uri=uri)
            factory.deref()
            statements = []

    if statements:
        _cypher.send_request(statements, uri=uri)
