all: install

clean:
	go clean ./...

doc:
	godoc -http=:6060

install:
	go get github.com/sirupsen/logrus
	go get golang.org/x/tools/cmd/cover

test:
	go test -v -cover ./...

bench:
	go test -v -run=none -bench=. ./...

fmt:
	go vet ./...
	go fmt ./...

lint:
	golint ./...

.PHONY: test
