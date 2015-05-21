all: install

clean:
	go clean ./...

doc:
	godoc -http=:6060

install:
	go get github.com/sirupsen/logrus
	go get github.com/spf13/viper
	go get github.com/spf13/cobra

test-install: install
	go get golang.org/x/tools/cmd/cover
	go get github.com/stretchr/testify/assert
	go get github.com/cespare/prettybench

build-install: install test-install
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

.PHONY: test
