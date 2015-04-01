FROM golang:onbuild

RUN make install
RUN make build

EXPOSE 49110

ENTRYPOINT ["/go/bin/origins"]
