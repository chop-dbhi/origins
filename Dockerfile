FROM golang

RUN mkdir -p /go/src/github.com/chop-dbhi/origins
WORKDIR /go/src/github.com/chop-dbhi/origins

COPY . /go/src/github.com/chop-dbhi/origins

RUN make install
RUN make build

EXPOSE 49110

ENTRYPOINT ["/go/bin/origins"]
