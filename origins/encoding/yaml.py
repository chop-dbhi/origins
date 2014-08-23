from io import StringIO

try:
    import yaml
except ImportError:
    pass


def load(f, resource=None):
    """Takes a YAML-encoded file-like object and decodes it into the native
    resource structure.
    """
    if not yaml:
        raise EnvironmentError('PyYAML not installed')

    data = yaml.load(f)

    if resource is not None:
        data['resource'] = resource

    return data


def loads(s, resource=None):
    """Takes a YAML-encoded string and decodes it into the native
    resource structure.
    """
    f = StringIO(s)
    return load(f, resource)
