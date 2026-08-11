# go-redis-lite

A lightweight, in-memory key-value store with support for key expiration (TTL), thread-safe operations, and a TCP interface using the Redis Serialization Protocol (RESP).

## Layout

```
cmd/server/      entry point: flags, AOF recovery, graceful shutdown
pkg/resp/        RESP parser and encoder
pkg/store/       concurrent map, TTL bookkeeping, expiration worker
pkg/aof/         append-only file writer and replay
pkg/server/      TCP listener, connection handling, command table
```

## Running

```sh
make run                     # or: go run ./cmd/server
redis-cli -p 6379 ping
```

Flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `:6379` | listen address |
| `-appendonly` | `true` | persist mutating commands to disk |
| `-appendfilename` | `appendonly.aof` | path of the append-only file |
| `-appendfsync` | `everysec` | `always`, `everysec` or `no` |
| `-expire-interval` | `100ms` | active expiration period |
| `-expire-sample` | `20` | keys sampled per expiration cycle |

## Supported commands

`PING` `ECHO` `SET` (`EX` `PX` `EXAT` `PXAT` `NX` `XX` `KEEPTTL`) `GET` `DEL` `EXISTS`
`EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST` `TTL` `PTTL` `INCR` `DECR`
`INCRBY` `DECRBY` `KEYS` `DBSIZE` `FLUSHALL` `FLUSHDB` `INFO` `CONFIG` `COMMAND` `QUIT`

## How it works

**Expiration** is both passive and active. `GET` drops a key whose deadline has
passed before answering, and a background worker samples 20 keys that carry a
TTL every 100ms, sweeping again immediately while more than a quarter of the
sample turns out to be expired. The worker stops when its context is cancelled.

**Persistence** queues every mutating command on a channel that a single
goroutine drains into a `bufio.Writer`. Commands are normalised before they are
logged — relative expirations become absolute `PXAT`/`PEXPIREAT` timestamps and
increments become the resulting `SET` — so replaying the log rebuilds exactly
the state that was lost, no matter how much later it happens. A command
truncated by a crash is discarded on recovery; everything before it is kept.

## Development

```sh
make test     # go test ./...
make race     # go test -race ./...   (needs a C compiler on PATH)
make bench    # go test -bench=. -benchmem ./pkg/store
make build    # binary in bin/
```

On Windows the race detector needs cgo, and Go only defaults `CGO_ENABLED` to 1
when it finds a C compiler on `PATH`. Install one (for example
`winget install BrechtSanders.WinLibs.POSIX.UCRT`) and open a new shell; force
it with `go env -w CGO_ENABLED=1` only if `go env CGO_ENABLED` still reports 0.

`make benchmark-redis` runs `redis-benchmark -n 100000 -c 50` against a server
you have already started. Pass `PORT=` to match it — note that installing Redis
on Windows registers a service that already holds 6379, so either stop that
service or run both on another port.

### Measured throughput

100,000 requests, 50 clients, on an i7-12700H. The baseline is the Redis
3.0.504 Windows port, which is what `winget install Redis.Redis` provides; it is
a decade old and not representative of a current Redis on Linux, so read the
comparison as a sanity check rather than a scalp.

| | SET | GET | INCR |
| --- | --- | --- | --- |
| go-redis-lite | 42,937 rps | 36,166 rps | 32,457 rps |
| go-redis-lite, `-P 16` | 621,118 rps | 578,035 rps | – |
| Redis 3.0.504 (Windows) | 15,056 rps | 19,216 rps | 18,248 rps |
