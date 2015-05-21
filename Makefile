all: install

clean:
	go clean ./...

doc:
	godoc -http=:6060

install:
	go get github.com/sirupsen/logrus

test-install: install
	go get golang.org/x/tools/cmd/cover
	go get github.com/stretchr/testify/assert
	go get github.com/cespare/prettybench

test:
	go test -cover ./...

bench:
	go test -run=none -bench=. ./... | prettybench

fmt:
	go vet ./...
	go fmt ./...

lint:
	golint ./...

.PHONY: test
