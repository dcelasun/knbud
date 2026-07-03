.PHONY: build test vet

build:
	go build -o knbud ./cmd/knbud

test:
	go test -race ./...

vet:
	go vet ./...
