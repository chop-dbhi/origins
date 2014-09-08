import os
import sys
import json
from copy import deepcopy


# Default configuration options
default_options = {
    'debug': None,

    'host': 'localhost',
    'port': 5000,

    'neo4j': {
        'host': 'localhost',
        'port': 7474,
    },

    'redis': {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
    },
}


def _defaults(a, b):
    "Recursively apply default values from b into a."
    o = {}

    for ak, av in a.items():
        if isinstance(av, dict):
            o[ak] = _defaults(av, b[ak])
        else:
            o[ak] = deepcopy(av)

    # Fill in the remaining defaults
    for bk, bv in b.items():
        # Already handled
        if bk not in o:
            o[bk] = deepcopy(bv)

    return o


def load_options(path):
    "Loads configuration options from path."
    try:
        with open(path, 'rU') as f:
            return make_options(json.load(f))
    except (ValueError, IOError):
        sys.stderr.write('Error loading configuration file {}\n'.format(path))
        raise


def set_options(opts=None):
    global options

    options = make_options(opts)

    return options


def make_options(options=None):
    if not options:
        options = {}

    return _defaults(options, default_options)


# Load config options from environment
if os.environ.get('ORIGINS_CONFIG'):
    options = load_options(os.environ['ORIGINS_CONFIG'])
else:
    options = make_options()
