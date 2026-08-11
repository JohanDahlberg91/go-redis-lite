package aof

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")

	log, err := Open(path, PolicyAlways)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	writes := [][]string{
		{"SET", "foo", "bar"},
		{"SET", "n", "1"},
		{"DEL", "foo"},
	}
	for _, args := range writes {
		if err := log.Append(args...); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var replayed [][]string
	count, err := Replay(path, func(args []string) error {
		replayed = append(replayed, args)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if count != len(writes) {
		t.Fatalf("Replay() applied %d commands, want %d", count, len(writes))
	}
	for i, want := range writes {
		if len(replayed[i]) != len(want) {
			t.Fatalf("command %d = %q, want %q", i, replayed[i], want)
		}
		for j := range want {
			if replayed[i][j] != want[j] {
				t.Fatalf("command %d = %q, want %q", i, replayed[i], want)
			}
		}
	}
}

func TestOpenAppendsToExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")

	for round := 0; round < 2; round++ {
		log, err := Open(path, PolicyEverySecond)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		if err := log.Append("SET", "k", strconv.Itoa(round)); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}

	count, err := Replay(path, func([]string) error { return nil })
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("Replay() applied %d commands, want 2 (reopening truncated the log)", count)
	}
}

func TestReplayMissingFile(t *testing.T) {
	count, err := Replay(filepath.Join(t.TempDir(), "nope.aof"), func([]string) error {
		t.Fatal("apply called for a missing file")
		return nil
	})
	if err != nil || count != 0 {
		t.Fatalf("Replay() = %d, %v; want 0, nil", count, err)
	}
}

func TestReplayTruncatedTail(t *testing.T) {
	// A crash mid-write leaves a partial command; recovery must keep every
	// complete command before it and drop the remainder.
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	content := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n" + "*3\r\n$3\r\nSET\r\n$1\r\nb"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	var replayed [][]string
	count, err := Replay(path, func(args []string) error {
		replayed = append(replayed, args)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if count != 1 || len(replayed) != 1 || replayed[0][1] != "a" {
		t.Fatalf("Replay() = %d commands (%q), want just the complete one", count, replayed)
	}
}

func TestAppendAfterCloseIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := Open(path, PolicyNo)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := log.Append("SET", "k", "v"); err != ErrClosed {
		t.Fatalf("Append() after Close() error = %v, want ErrClosed", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

// TestConcurrentAppend exercises the writer under the race detector: many
// producers, one file, nothing lost.
func TestConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := Open(path, PolicyEverySecond)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	const (
		writers   = 8
		perWriter = 250
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := log.Append("SET", "key:"+strconv.Itoa(worker), strconv.Itoa(i)); err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	count, err := Replay(path, func([]string) error { return nil })
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if count != writers*perWriter {
		t.Fatalf("Replay() applied %d commands, want %d", count, writers*perWriter)
	}
}
