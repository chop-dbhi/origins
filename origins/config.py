import os
import sys
import json
import logging
import logging.config
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

    'logging': {
        'version': 1,
        'disable_existing_loggers': True,
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
            }
        },
        'loggers': {
            'origins': {
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'origins.events': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': True,
            },
            'origins.graph.neo4j': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': True,
            },
        },
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
            return set_options(json.load(f))
    except (ValueError, IOError):
        sys.stderr.write('Error loading configuration file {}\n'.format(path))
        raise


def set_options(opts=None):
    global options

    options = make_options(opts)

    setup_logging(options)
    return options


def make_options(options=None):
    if not options:
        options = {}

    options = _defaults(options, default_options)
    setup_logging(options)

    return options


def setup_logging(options):
    if 'logging' in options:
        logging.config.dictConfig(options['logging'])
    else:
        logging.basicConfig(level=logging.DEBUG)


def set_loglevel(level):
    if 'handlers' not in options['logging']:
        return

    for opts in options['logging']['handlers'].values():
        opts['level'] = level

    setup_logging(options)


# Load config options from environment
if os.environ.get('ORIGINS_CONFIG'):
    options = load_options(os.environ['ORIGINS_CONFIG'])
else:
    options = make_options()
