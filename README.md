# Origins

[![Build Status](https://travis-ci.org/chop-dbhi/origins.svg?branch=master)](https://travis-ci.org/chop-dbhi/origins) [![Coverage Status](https://coveralls.io/repos/chop-dbhi/origins/badge.svg?branch=master&service=github)](https://coveralls.io/github/chop-dbhi/origins?branch=master) [![GoDoc](https://godoc.org/github.com/chop-dbhi/origins?status.svg)](https://godoc.org/github.com/chop-dbhi/origins)

Origins is an open source bi-temporal database for storing and retrieving facts about the state of things. It supports "time-travel" queries, aggregate views, and change detection.

- This project is in an **alpha** stage of development.
- Interested in working on a temporal database written in Go? Get in touch!
- For more information consult the [documentation](https://origins.readme.io/docs).
- Learn how you can [contribute](https://origins.readme.io/v0.9/docs/contributing).

## Docker

Run in-memory Origins HTTP service.

```
docker run -p 49110:49110 dbhi/origins http
```

Use the BoltDB storage engine with a volume.

```
docker run \
    -p 49110:49110 \
    -v <host-dir>:/data \
    dbhi/origins http \
    --storage=boltdb \
    --path=/data/origins.boltdb
```

## Development

### Environment

**Install Go**

`brew` on OS X.

```
brew install go
```

Otherwise follow [these instructions](http://golang.org/doc/install).

**Install Go dependencies**

```
make install
```
### Testing

Ensure the test dependencies are installed:

```
make test-install
```

Then run:

```
make test
```

### Building

To the build the `origins` command in `./cmd/origins` locally, simply run:

```
make build
```

To build binaries for each platform, ensure the build dependencies are installed:

```
make build-install
```

Then run:

```
make build
```
