import time
from . import cypher


def timestamp():
    return int(time.time() * 1000)


def prep(statement, label=None, **mapping):
    mapping['labels'] = cypher.labels(label)

    return statement.safe_substitute(mapping)
