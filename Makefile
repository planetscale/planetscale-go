.PHONY: all
all: build test lint

.PHONY: test
test:
	go test ./...

.PHONY: build
build:
	go build ./...

.PHONY: lint
lint:
	@go tool -modfile=tools/go.mod staticcheck ./...
	@go vet ./...

.PHONY: licensed
licensed:
	licensed cache
	licensed status
