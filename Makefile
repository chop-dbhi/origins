all: install

clean:
	go clean ./...

doc:
	godoc -http=:6060

proto:
	protoc --go_out=. fact/*.proto
	protoc --go_out=. storage/*.proto

install:
	go get github.com/sirupsen/logrus
	go get github.com/boltdb/bolt
	go get github.com/golang/protobuf/proto
	go get github.com/golang/protobuf/protoc-gen-go
	go get github.com/spf13/viper
	go get github.com/spf13/cobra
	go get github.com/julienschmidt/httprouter
	go get github.com/psilva261/timsort
	go get github.com/rs/cors

test-install: install
	go get golang.org/x/tools/cmd/cover
	go get github.com/stretchr/testify/assert
	go get github.com/cespare/prettybench

dev-install: install test-install
	go get github.com/mitchellh/gox

test:
	go test -cover ./...

build:
	mkdir -p bin

	gox -output="bin/origins-{{.OS}}.{{.Arch}}" \
		-os="linux windows darwin" \
		-arch="amd64" \
		./cli > /dev/null

	cp bin/origins-darwin.amd64 $(GOPATH)/bin


bench:
	go test -run=none -bench=. ./... | prettybench

fmt:
	go vet ./...
	go fmt ./...

lint:
	golint ./...

.PHONY: test proto
