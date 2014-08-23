import json
from io import StringIO


def load(f, resource=None):
    """Takes a JSON-encoded file-like object and decodes it into the native
    resource structure.
    """
    data = json.load(f)

    if resource is not None:
        data['resource'] = resource

    return data


def loads(s, resource=None):
    """Takes a JSON-encoded string and decodes it into the native
    resource structure.
    """
    f = StringIO(s)
    return load(f, resource)
