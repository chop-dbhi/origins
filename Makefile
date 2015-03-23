all: install

clean:
	go clean ./...

doc:
	godoc -http=:6060

proto:
	protoc --go_out=. fact/*.proto
	protoc --go_out=. storage/*.proto

install:
	go get golang.org/x/tools/cmd/cover
	go get github.com/sirupsen/logrus
	go get github.com/stretchr/testify/assert
	go get github.com/peterbourgon/diskv
	go get github.com/boltdb/bolt
	go get github.com/mattn/go-sqlite3
	go get github.com/golang/protobuf/proto
	go get github.com/golang/protobuf/protoc-gen-go
	go get github.com/spf13/viper
	go get github.com/spf13/cobra

test:
	go test -v -cover ./...

build:
	go build -o bin/origins ./cli

bench:
	go test -v -run=none -bench=. ./...

fmt:
	go vet ./...
	go fmt ./...

lint:
	golint ./...

.PHONY: test proto
