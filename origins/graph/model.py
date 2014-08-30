from uuid import uuid4
from .. import utils
from .packer import unpack


NO_DEPENDENCE = 0
DIRECTED_DEPENDENCE = 1
MUTAL_DEPENDENCE = 2

DIFF_IGNORES = {
    'id',
    'uuid',
    'time',
}

MERGE_IGNORES = {
    'uuid',
    'time',
}


EDGE_DIFF_IGNORES = DIFF_IGNORES | {
    'start',
    'end',
}

EDGE_MERGE_IGNORES = MERGE_IGNORES | {
    'start',
    'end',
}


class Node(dict):
    @classmethod
    def new(cls, attrs=None, merge=None):
        if merge:
            attrs = utils.merge_attrs(merge, attrs, ignore=MERGE_IGNORES)
        elif not attrs:
            attrs = {}

        # TODO what does this time mean exactly?
        attrs['time'] = utils.timestamp()
        attrs['uuid'] = str(uuid4())

        attrs.setdefault('id', attrs['uuid'])
        attrs.setdefault('properties', {})

        return cls(attrs)

    @classmethod
    def parse(cls, row):
        "Parses a row from the graph."
        return cls(unpack(row))

    def diff(self, other):
        return utils.diff(self, other, ignore=DIFF_IGNORES)


class Edge(dict):
    @classmethod
    def new(cls, attrs=None, merge=None):
        if merge:
            attrs = utils.merge_attrs(merge, attrs, ignore=EDGE_MERGE_IGNORES)
        elif not attrs:
            attrs = {}

        # TODO what does this time mean exactly?
        attrs['time'] = utils.timestamp()
        attrs['uuid'] = str(uuid4())

        attrs.setdefault('id', attrs['uuid'])
        attrs.setdefault('properties', {})
        attrs.setdefault('dependence', DIRECTED_DEPENDENCE)

        return cls(attrs)

    @classmethod
    def parse(cls, row):
        edge = cls(unpack(row[0]))

        # Include start and end nodes
        if len(row) > 1:
            edge['start'] = Node.parse(row[1])
            edge['end'] = Node.parse(row[2])

        return edge

    def diff(self, other):
        return utils.diff(self, other, ignore=EDGE_DIFF_IGNORES)


class Result(dict):
    pass
