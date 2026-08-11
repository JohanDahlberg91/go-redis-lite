BINARY := redis-lite
BIN_DIR := bin
PORT ?= 6379

.PHONY: all build run test race bench cover fmt vet lint benchmark-redis clean

all: fmt vet test build

build:
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/server

run:
	go run ./cmd/server -addr :$(PORT)

test:
	go test ./...

# Phase 2 requires the store to be clean under the race detector.
race:
	go test -race ./...

bench:
	go test -bench=. -benchmem -run '^$$' ./pkg/store

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: fmt vet

# Requires a local redis-benchmark binary and a server already running.
benchmark-redis:
	redis-benchmark -p $(PORT) -n 100000 -c 50 -t set,get,incr -q

clean:
	go clean
	rm -rf $(BIN_DIR) coverage.out appendonly.aof
