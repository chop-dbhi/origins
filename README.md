# Origins

[![Build Status](https://travis-ci.org/chop-dbhi/origins.svg?branch=master)](https://travis-ci.org/chop-dbhi/origins)

Origins is an open source bi-temporal database for storing and retrieving facts. It supports "time-travel" queries, aggregate views, and dependency change detection.

This project is in an **alpha** stage of development.

- [Documentation](https://origins.readme.io/docs)
- Learn how you can [contribute](https://origins.readme.io/v0.9/docs/contributing).

## Development

### Environment

**Install Go**

`brew` on OS X.

```
brew install go
```

Otherwise follow [these instructions](http://golang.org/doc/install).

**Install protocol buffers**

Using `brew` on OS X:

```
brew install protobuf
```

Otherwise follow [these instructions](https://developers.google.com/protocol-buffers/docs/downloads).

**Install dependencies**

```
make install
```

### Protocol Buffers

Google's Protocol Buffers are used for the encoding format of the data written to storage. Code is generated based on PB message files and is version controlled. If the message formats are updated, regenerate the code by calling the `make` target.

```
make proto
```
