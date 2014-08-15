from __future__ import unicode_literals, absolute_import

import os
import sys
import json
from copy import deepcopy
from origins.service.resources import app


# Default configuration options
default_config = {
    'debug': None,

    'host': 'localhost',
    'port': 5000,

    'redis_port': 'localhost',
    'redis_host': 6379,
    'redis_db': 0,

    'neo4j_host': 'localhost',
    'neo4j_port': 7474,
}


config = deepcopy(default_config)


# Fragile support for a configuration file
if '-c' in sys.argv:
    try:
        config_name = sys.argv[sys.argv.index('-c') + 1]
    except IndexError:
        print('-c flag requires a path to a configuration file')
        sys.exit(1)
else:
    config_name = os.environ.get('ORIGINS_CONFIG')


# Update configuration if available
if config_name:
    try:
        with open(config_name) as f:
            try:
                config.update(json.load(f))
            except ValueError:
                print('configuration file must be valid JSON')
                sys.exit(1)
    except IOError:
        print('cannot read configuration file')
        sys.exit(1)


# Override with environment-specific variables
for key in default_config:
    var = 'ORIGINS_' + key.upper()

    if var in os.environ:
        value = os.environ[var]

        try:
            value = int(value)
        except (ValueError, TypeError):
            pass

        config[key] = value


app.run(threaded=True,
        debug=config['debug'],
        host=config['host'],
        port=config['port'])
