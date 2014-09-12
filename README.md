# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

Documentation: https://github.com/cbmi/origins/wiki/

## Development

### Service

The REST service:

```
./bin/origins serve --debug
```

### Events

The events daemon that pushes published events to subscribers:

```
./bin/origins events --debug
```

Run the Redis monitor if you're curious:

```
redis-cli monitor
```
