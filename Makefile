all: install

clean:
	go clean ./...

doc:
	godoc -http=:6060

proto:
	protoc --go_out=. fact/*.proto
	protoc --go_out=. storage/*.proto
	protoc --go_out=. transactor/*.proto

install:
	go get github.com/tools/godep
	godep restore

test-install: install
	go get golang.org/x/tools/cmd/cover
	go get github.com/stretchr/testify/assert
	go get github.com/cespare/prettybench

dev-install: install test-install
	go get github.com/mitchellh/gox

test:
	go test -cover ./...

build:
	go build -o $(GOPATH)/bin/origins ./cmd/origins

# Build and tag binaries for each OS and architecture.
build-all: build
	mkdir -p bin

	gox -output="bin/origins-{{.OS}}.{{.Arch}}" \
		-os="linux windows darwin" \
		-arch="amd64" \
		./cmd/origins > /dev/null


bench:
	go test -run=none -bench=. ./... | prettybench

fmt:
	go vet ./...
	go fmt ./...

lint:
	golint ./...

.PHONY: test proto
