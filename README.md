# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

Documentation: https://github.com/cbmi/origins/wiki/

## CLI

The `origins` script is located in the `bin/` directed when cloned or installed on your `PATH` otherwise.

```
origins
```

## Config

Origins uses a JSON-encoded configuration file to customize the behavior of the services.

```javascript
{
    "host": "localhost",            // Host/IP address of the service.
    "port": 5000,                   // Port to expose the service on.
    "debug": false,                 // Turn on for debug messages.
    "neo4j": {
        "host": "localhost",        // Host of the Neo4j REST endpoint.
        "port": 7474                // Port of the Neo4j REST endpoint.
    },
    "redis": {
        "host": "localhost",        // Host of the Redis server.
        "port": 6379                // Port of the Redis server.
    }
}
```

## Development

### Service

The REST service:

```
./bin/origins serve --debug [--config /path/to/config.json]
```

### Events

The events daemon that pushes published events to subscribers:

```
./bin/origins events --debug [--config /path/to/config.json]
```

Run the Redis monitor if you're curious:

```
redis-cli monitor
```

## Deployment

Using [Fig](http://fig.sh):

```
fig up
```
