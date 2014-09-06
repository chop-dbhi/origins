import time
from . import cypher


def timestamp():
    return int(time.time() * 1000)


def prep(statement, model=None, type=None, **mapping):
    mapping['model'] = cypher.labels(model)
    mapping['type'] = cypher.labels(type)

    return statement.safe_substitute(mapping)
