import re
from io import StringIO
from .._csv import UnicodeDictReader


# TODO consolidate with origins.graph.utils
UUID_RE = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}'
                     r'-[a-f0-9]{4}-[a-f0-9]{12}$', re.I)


def parse_bool(value):
    if value == '':
        return None

    if value.lower() in ('false', 'f'):
        return False

    if value.lower() in ('true', 't'):
        return True

    raise ValueError('valid values are empty, true, false, t, f, '
                     'not "{}"'.format(value))


def is_uuid(uuid):
    if not isinstance(uuid, str):
        return False
    return UUID_RE.match(uuid) is not None


def validate_component(row, components):
    # Component rows cannot define an relationship attributes
    assert not any([
        row['start'],
        row['end'],
        row['directed'],
        row['dependence'],
    ]), 'Component cannot have any relationship attributes defined'

    # The parent must be a locally defined components if defined
    if row['parent']:
        assert row['parent'] in components


def validate_relationship(row, components):
    # Not locally defined, must be a UUID to a remote component
    if row['start'] not in components:
        if not is_uuid(row['start']):
            raise ValueError('start component not defined '
                             'locally nor is a UUID')

    # Not locally defined, must be a UUID to a remote component
    if row['end'] not in components:
        if not is_uuid(row['end']):
            raise ValueError('end component not defined '
                             'locally nor is a UUID')

    try:
        row['dependence'] = parse_bool(row['dependence'])
    except ValueError as e:
        raise ValueError('dependence: ' + str(e))

    try:
        row['directed'] = parse_bool(row['directed'])
    except ValueError as e:
        raise ValueError('directed: ' + str(e))


# Parses comma-separated key=value pairs
prop_parser = re.compile(r'''(?:([^\s,"']+|"[^"]+"|'[^']+')=([^\s"',]+|"[^"]+"|'[^']+'))+''')  # noqa


def parse_properties(value):
    props = {}

    for key, value in prop_parser.findall(value):
        key = key.strip('\'"')
        value = value.strip('\'"')

        # Try simple type coercion
        if value:
            try:
                value = parse_bool(value)
            except ValueError:
                pass

            try:
                value = float(value)
            except ValueError:
                pass

        props[key] = value

    return props


def load(f, resource):
    """Loads a file-like object in the CSV resource format and converts
    it into the native resource format.
    """
    components = {}
    relationships = {}

    fieldnames = f.readline().strip().split(',')

    reader = UnicodeDictReader(f, fieldnames=fieldnames)

    for row in reader:
        # Use the UUID as the local ID for remote objects
        if row['uuid'] and not row['id']:
            row['id'] = row['uuid']

        # Relationship; the start and end components must be defined
        # before the relationship is defined
        if row['start'] and row['end']:
            validate_relationship(row, components)

            relationships[row['id']] = row
        else:
            validate_component(row, components)

            components[row['id']] = row

        if row['properties']:
            row['properties'] = parse_properties(row['properties'])

        # Remove id since it is already defined in the dicts
        row.pop('id')

    return {
        'version': 1.0,
        'resource': resource,
        'components': components,
        'relationships': relationships,
    }


def loads(s, resource):
    "Takes a string and converts it into the native resource format."
    f = StringIO(s)
    return load(f, resource)
