// Package aof implements append-only file persistence. Mutating commands are
// queued on a channel, written to disk by a single background goroutine
// through a bufio.Writer, and replayed command by command on startup.
package aof

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/JohanDahlberg91/go-redis-lite/pkg/resp"
)

// Policy controls how often buffered writes are flushed and fsynced.
type Policy string

const (
	// PolicyEverySecond flushes once per second. This is the default and
	// trades a bounded window of data loss for throughput.
	PolicyEverySecond Policy = "everysec"
	// PolicyAlways fsyncs after every command. Safest and slowest.
	PolicyAlways Policy = "always"
	// PolicyNo leaves flushing to the operating system.
	PolicyNo Policy = "no"
)

const (
	queueSize      = 4096
	writeBufSize   = 64 * 1024
	flushFrequency = time.Second
)

// ErrClosed is returned when appending to a closed log.
var ErrClosed = errors.New("aof: log is closed")

// Log is an append-only command log. It is safe for concurrent use.
type Log struct {
	file   *os.File
	writer *bufio.Writer
	policy Policy

	queue   chan []byte
	drained chan struct{}

	// mu guards closed and serialises Append against Close so nothing is
	// ever sent on a closed channel.
	mu     sync.RWMutex
	closed bool

	errMu   sync.Mutex
	lastErr error
}

// Open opens (creating it if needed) the append-only file at path and starts
// the background writer.
func Open(path string, policy Policy) (*Log, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{
		file:    file,
		writer:  bufio.NewWriterSize(file, writeBufSize),
		policy:  policy,
		queue:   make(chan []byte, queueSize),
		drained: make(chan struct{}),
	}
	go l.run()
	return l, nil
}

// Append queues a command for persistence. It returns as soon as the command
// is on the queue; it only blocks when the queue is full, which applies
// backpressure rather than silently dropping writes.
func (l *Log) Append(args ...string) error {
	if len(args) == 0 {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return ErrClosed
	}
	l.queue <- resp.Command(args...).Marshal()
	return nil
}

// Err reports the first write error the background goroutine hit, if any.
func (l *Log) Err() error {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	return l.lastErr
}

// Close stops the writer, flushes everything still queued and closes the
// file.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.queue)
	l.mu.Unlock()

	<-l.drained // the writer drains the queue and flushes before returning

	err := l.file.Close()
	if writeErr := l.Err(); writeErr != nil {
		return writeErr
	}
	return err
}

// run owns the file: it is the only goroutine that touches the writer, so no
// lock is needed around the buffered writes themselves.
func (l *Log) run() {
	defer close(l.drained)

	ticker := time.NewTicker(flushFrequency)
	defer ticker.Stop()

	for {
		select {
		case record, ok := <-l.queue:
			if !ok {
				l.sync()
				return
			}
			if _, err := l.writer.Write(record); err != nil {
				l.setErr(err)
			}
			if l.policy == PolicyAlways {
				l.sync()
			}
		case <-ticker.C:
			if l.policy == PolicyEverySecond {
				l.sync()
			}
		}
	}
}

func (l *Log) sync() {
	if err := l.writer.Flush(); err != nil {
		l.setErr(err)
		return
	}
	if l.policy != PolicyNo {
		if err := l.file.Sync(); err != nil {
			l.setErr(err)
		}
	}
}

func (l *Log) setErr(err error) {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	if l.lastErr == nil {
		l.lastErr = err
	}
}

// Replay reads the log at path and hands every command to apply in order. A
// missing file is not an error. A truncated final command (the process died
// mid-write) is discarded rather than failing the whole recovery, matching
// how Redis treats a partial tail. It returns the number of commands applied.
func Replay(path string, apply func(args []string) error) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	reader := resp.NewReader(file)
	applied := 0
	for {
		args, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return applied, nil
			}
			return applied, err
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			return applied, err
		}
		applied++
	}
}
