from origins.model import Node, Edge
from ..packer import unpack


def parse_node(row):
    return Node(unpack(row))


def parse_edge(row):
    edge = Edge(unpack(row[:2]))
    edge['start'] = parse_node(row[2:4])
    edge['end'] = parse_node(row[4:])

    return edge
