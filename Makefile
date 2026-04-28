.PHONY: build test lint fmt vet release-snapshot clean tidy

VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || date +v%Y-%m-%d)
GIT_SHA  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS  := -s -w \
            -X github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version.Version=$(VERSION) \
            -X github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version.GitSHA=$(GIT_SHA)

build:
	CGO_ENABLED=0 go build \
	  -tags=netgo,osusergo \
	  -trimpath \
	  -ldflags="$(LDFLAGS)" \
	  -o bin/acd ./cmd/acd

test:
	go test ./... -race -count=1

vet:
	go vet ./...

fmt:
	gofmt -w .

lint: vet
	@test -z "$$(gofmt -l .)" || (gofmt -d . && echo "gofmt issues above" && exit 1)

tidy:
	go mod tidy

release-snapshot:
	goreleaser release --snapshot --clean

clean:
	rm -rf bin/ dist/
