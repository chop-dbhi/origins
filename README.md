# Origins

[![Build Status](https://travis-ci.org/chop-dbhi/origins.svg?branch=go)](https://travis-ci.org/chop-dbhi/origins)

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
