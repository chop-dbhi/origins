# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

Documentation: https://github.com/cbmi/origins/wiki/

## Setup

Origins requires [Neo4j](http://www.neo4j.org) and [Redis](http://redis.io/) to be running.

## Usage

Run the built-in server:

```bash
python -m origins.serve
```

### Environment Variables

- `ORIGINS_HOST` - Host to run the built-in server. Defaults to `127.0.0.1:5000`.
- `ORIGINS_DEBUG` - Set to `1` to turn on debug output on errors. If the host is `localhost` or `127.0.0.1`, debugging is on by default.
