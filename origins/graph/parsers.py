from __future__ import unicode_literals, absolute_import

from . import utils


def parse_collection(row):
    """Parses a collection query result.

        - id
        - properties
    """
    return utils.unpack(row)


def parse_resource(row):
    """Parses a resource query result.

        - id
        - properties
    """
    return utils.unpack(row)


def parse_component(row):
    """Parses a component query result.

        - component
            - id
            - properties
        - revision
            - id
            - properties
        - valid (bool)
    """
    revision = utils.unpack(row[2:4])

    revision['valid'] = row[-1]

    return revision


def parse_component_relationship(row):
    """Parses a component relationship query result

    """
    result = row[0]
    t1 = row[1]
    r1 = utils.unpack(row[1:3])
    c1 = utils.unpack(row[3:5])
    t2 = row[5]
    r2 = utils.unpack(row[6:8])
    c2 = utils.unpack(row[8:10])

    result[t1] = c1
    c1['resource'] = r1

    result[t2] = c2
    c2['resource'] = r2

    return utils.unpack(result)


def parse_source(row):
    component = parse_component(row[:-1])

    return {
        'depth': row[-1],
        'component': component,
    }


def parse_relationship(row):
    # relationship = utils.unpack(row[:2])
    revision = utils.unpack(row[2:4])
    end = utils.unpack(row[4:6])
    start = utils.unpack(row[6:8])

    revision['valid'] = row[-1]
    revision['start'] = start
    revision['end'] = end

    return revision


def parse_timeline(rows):
    result = []

    _id = None
    event = None
    related = {}

    for row in rows:
        if row[0] != _id:
            if event:
                event.update(related)
                result.append(utils.unpack((_id, event)))

            _id = row[0]
            event = row[1]
            related = {}

        related[row[2]] = utils.unpack(row[3:])

    if event:
        event.update(related)
        result.append(utils.unpack((_id, event)))

    return result
