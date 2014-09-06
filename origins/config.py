import os
import sys
import json
from copy import deepcopy


# Default configuration options
default_options = {
    'debug': None,
    'events_enabled': None,

    'host': 'localhost',
    'port': 5000,

    'redis_host': 'localhost',
    'redis_port': 6379,
    'redis_db': 0,

    'neo4j_host': 'localhost',
    'neo4j_port': 7474,
}


options = deepcopy(default_options)


config_name = os.environ.get('ORIGINS_CONFIG')


# Update configuration if available
if config_name:
    try:
        with open(config_name) as f:
            try:
                options.update(json.load(f))
            except ValueError:
                print('configuration file must be valid JSON')
                sys.exit(1)
    except IOError:
        print('cannot read configuration file')
        sys.exit(1)


# Override with environment-specific variables
for key in default_options:
    var = 'ORIGINS_' + key.upper()

    if var in os.environ:
        value = os.environ[var]

        try:
            value = int(value)
        except (ValueError, TypeError):
            pass

        options[key] = value
