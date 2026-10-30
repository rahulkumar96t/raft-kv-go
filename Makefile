.PHONY: all build test test-repeat vet fmt cluster cluster-status cluster-stop cluster-clean

all: fmt vet test

build:
	go build -o bin/raftkv ./cmd/raftkv

test:
	go test -race ./...

# Shake out timing-dependent flakes.
test-repeat:
	go test -race -count=5 ./...

vet:
	go vet ./...

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

cluster:
	./scripts/cluster.sh start

cluster-status:
	./scripts/cluster.sh status

cluster-stop:
	./scripts/cluster.sh stop

cluster-clean:
	./scripts/cluster.sh clean
