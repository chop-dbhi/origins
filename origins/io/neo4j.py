from __future__ import unicode_literals, absolute_import

from . import _cypher


def prepare_rel(rel, factory):
    stmts = []

    if factory.hasref(rel.id):
        return stmts

    if not factory.hasref(rel.start.id):
        stmts.append(factory.merge_node(rel.start))

    if not factory.hasref(rel.end.id):
        stmts.append(factory.merge_node(rel.end))

    stmts.append(factory.merge_rel(rel))
    return stmts


def prepare_node(node, factory, traverse=True):
    stmts = []

    if factory.hasref(node.id):
        return stmts

    for rel in node.relpath:
        stmts.extend(prepare_rel(rel, factory))

    if traverse:
        for rel in node.rels():
            stmts.extend(prepare_node(rel.end, factory, traverse=traverse))
            stmts.extend(prepare_rel(rel, factory))

    return stmts


def parse(node, traverse=True):
    factory = _cypher.MergeFactory()
    stmts = prepare_node(node, factory, traverse=traverse)
    return '\n'.join(stmts)


def export(node, uri=None, traverse=True, batch_size=100):
    """Exports a node and it's parents up to and including the origin.

    If `traverse` if true, relationships the node has will be recursively
    exported including the end nodes.

    `batch_size` is used to prevent constructing too large of a statement
    for the REST API to process (e.g. out of memory errors).
    """
    factory = _cypher.MergeFactory()

    stmts = prepare_node(node, factory, traverse=traverse)
    if stmts:
        _cypher.send_request(stmts, uri=uri)
