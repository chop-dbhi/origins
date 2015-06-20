all: install

clean:
	go clean ./...

doc:
	godoc -http=:6060

proto:
	protoc --go_out=. dal/*.proto

install:
	go get github.com/sirupsen/logrus
	go get github.com/boltdb/bolt
	go get github.com/golang/protobuf/proto
	go get github.com/golang/protobuf/protoc-gen-go
	go get github.com/spf13/viper
	go get github.com/spf13/cobra
	go get github.com/psilva261/timsort
	go get github.com/satori/go.uuid
	go get github.com/julienschmidt/httprouter
	go get github.com/rs/cors
	go get github.com/jteeuwen/go-bindata/...

test-install: install
	go get golang.org/x/tools/cmd/cover
	go get github.com/stretchr/testify/assert

build-install: install test-install
	go get github.com/mitchellh/gox

# Build the binary assets.
assets:
	go-bindata -o testutil/bindata.go -pkg testutil \
		-ignore \\.sw[a-z] -ignore \\.DS_Store assets/

test:
	go test -cover ./...

bench:
	go test -run=none -bench=. -benchmem ./...

build:
	go build -o $(GOPATH)/bin/origins ./cmd/origins

# Build and tag binaries for each OS and architecture.
build-all: build
	mkdir -p bin

	gox -output="bin/origins-{{.OS}}.{{.Arch}}" \
		-os="linux windows darwin" \
		-arch="amd64" \
		./cmd/origins > /dev/null

fmt:
	go vet ./...
	go fmt ./...

lint:
	golint ./...


.PHONY: test proto assets
