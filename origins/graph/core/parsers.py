from ..model import Node, Edge
from ..packer import unpack


def parse_node(row):
    return Node(unpack(row))


def parse_edge(row):
    edge = Edge(unpack(row[0]))

    # Include start and end nodes
    if len(row) > 1:
        edge['start'] = parse_node(row[1])
        edge['end'] = parse_node(row[2])

    return edge
