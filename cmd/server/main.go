// Command server runs the redis-lite TCP server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/JohanDahlberg91/go-redis-lite/pkg/aof"
	"github.com/JohanDahlberg91/go-redis-lite/pkg/resp"
	"github.com/JohanDahlberg91/go-redis-lite/pkg/server"
	"github.com/JohanDahlberg91/go-redis-lite/pkg/store"
)

func main() {
	addr := flag.String("addr", ":6379", "address to listen on")
	appendOnly := flag.Bool("appendonly", true, "persist mutating commands to an append-only file")
	appendFile := flag.String("appendfilename", "appendonly.aof", "path of the append-only file")
	appendFsync := flag.String("appendfsync", string(aof.PolicyEverySecond), "fsync policy: always, everysec or no")
	expireInterval := flag.Duration("expire-interval", 100*time.Millisecond, "how often the active expiration worker runs")
	expireSample := flag.Int("expire-sample", 20, "keys sampled per active expiration cycle")
	flag.Parse()

	logger := log.New(os.Stderr, "", log.LstdFlags)
	if err := run(*addr, *appendOnly, *appendFile, aof.Policy(*appendFsync), *expireInterval, *expireSample, logger); err != nil {
		logger.Fatalf("fatal: %v", err)
	}
}

func run(addr string, appendOnly bool, appendFile string, policy aof.Policy,
	expireInterval time.Duration, expireSample int, logger *log.Logger) error {

	switch policy {
	case aof.PolicyAlways, aof.PolicyEverySecond, aof.PolicyNo:
	default:
		return fmt.Errorf("unknown appendfsync policy %q", policy)
	}

	st := store.New()

	var persister server.Persister
	if appendOnly {
		if err := replay(st, appendFile, logger); err != nil {
			return err
		}
		commandLog, err := aof.Open(appendFile, policy)
		if err != nil {
			return fmt.Errorf("open aof: %w", err)
		}
		defer func() {
			if err := commandLog.Close(); err != nil {
				logger.Printf("aof: close: %v", err)
			}
		}()
		persister = commandLog
	}

	executor := server.NewExecutor(st, persister, func(err error) {
		logger.Printf("aof: %v", err)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		st.RunExpiryWorker(ctx, expireInterval, expireSample)
	}()

	err := server.New(addr, executor, logger).ListenAndServe(ctx)
	workers.Wait()
	return err
}

// replay rebuilds the store from the append-only file. The executor used here
// has no persister, so recovered commands are not written back to the log.
func replay(st *store.Store, path string, logger *log.Logger) error {
	executor := server.NewExecutor(st, nil, nil)

	start := time.Now()
	applied, err := aof.Replay(path, func(args []string) error {
		if reply := executor.Execute(args); reply.Type == resp.TypeError {
			return fmt.Errorf("replaying %v: %s", args, reply.Str)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("replay aof: %w", err)
	}
	if applied > 0 {
		logger.Printf("aof: replayed %d commands in %s (%d keys)", applied, time.Since(start).Round(time.Millisecond), st.Len())
	}
	return nil
}
