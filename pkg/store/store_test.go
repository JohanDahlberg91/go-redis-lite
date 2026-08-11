package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestSetGetDelExists(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("Get() on an empty store reported a hit")
	}

	s.Set("key", "value", 0)
	value, ok := s.Get("key")
	if !ok || value != "value" {
		t.Fatalf("Get() = %v, %v; want \"value\", true", value, ok)
	}

	if got := s.Exists("key", "missing"); got != 1 {
		t.Fatalf("Exists() = %d, want 1", got)
	}
	if got := s.Del("key", "missing"); got != 1 {
		t.Fatalf("Del() = %d, want 1", got)
	}
	if _, ok := s.Get("key"); ok {
		t.Fatal("Get() found a deleted key")
	}
}

func TestSetOverwriteClearsTTL(t *testing.T) {
	s := New()
	s.Set("key", "first", time.Minute)

	s.Set("key", "second", 0)
	if _, _, hasTTL := s.TTL("key"); hasTTL {
		t.Fatal("plain SET did not clear the existing TTL")
	}

	s.Set("key", "third", time.Minute)
	s.SetWithOptions("key", "fourth", SetOptions{KeepTTL: true})
	remaining, exists, hasTTL := s.TTL("key")
	if !exists || !hasTTL || remaining <= 0 {
		t.Fatalf("KEEPTTL lost the expiration: %v, %v, %v", remaining, exists, hasTTL)
	}
}

func TestSetOptionsNXXX(t *testing.T) {
	s := New()

	if !s.SetWithOptions("key", "a", SetOptions{NX: true}) {
		t.Fatal("NX on a missing key should have written")
	}
	if s.SetWithOptions("key", "b", SetOptions{NX: true}) {
		t.Fatal("NX on an existing key should not have written")
	}
	if !s.SetWithOptions("key", "c", SetOptions{XX: true}) {
		t.Fatal("XX on an existing key should have written")
	}
	if s.SetWithOptions("other", "d", SetOptions{XX: true}) {
		t.Fatal("XX on a missing key should not have written")
	}
	if value, _ := s.Get("key"); value != "c" {
		t.Fatalf("Get() = %v, want \"c\"", value)
	}
}

func TestPassiveExpiration(t *testing.T) {
	s := New()
	s.Set("key", "value", 20*time.Millisecond)

	if _, ok := s.Get("key"); !ok {
		t.Fatal("key expired before its TTL elapsed")
	}
	time.Sleep(40 * time.Millisecond)

	if _, ok := s.Get("key"); ok {
		t.Fatal("Get() returned an expired key")
	}
	// The expired key must be gone from the map, not merely hidden.
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
	if got := s.Exists("key"); got != 0 {
		t.Fatalf("Exists() = %d, want 0", got)
	}
}

func TestExpireAndPersist(t *testing.T) {
	s := New()

	if s.ExpireAt("missing", time.Now().Add(time.Minute)) {
		t.Fatal("ExpireAt() on a missing key reported success")
	}

	s.Set("key", "value", 0)
	if !s.ExpireAt("key", time.Now().Add(time.Minute)) {
		t.Fatal("ExpireAt() on an existing key reported failure")
	}
	remaining, _, hasTTL := s.TTL("key")
	if !hasTTL || remaining > time.Minute || remaining < 50*time.Second {
		t.Fatalf("TTL() = %v, want just under a minute", remaining)
	}

	if !s.Persist("key") {
		t.Fatal("Persist() reported no TTL to clear")
	}
	if _, exists, hasTTL := s.TTL("key"); !exists || hasTTL {
		t.Fatal("Persist() did not clear the TTL")
	}
	if s.Persist("key") {
		t.Fatal("Persist() on a key without a TTL should report false")
	}
}

func TestExpireInThePastDeletes(t *testing.T) {
	s := New()
	s.Set("key", "value", 0)

	if !s.ExpireAt("key", time.Now().Add(-time.Second)) {
		t.Fatal("ExpireAt() in the past should report that the key existed")
	}
	if got := s.Exists("key"); got != 0 {
		t.Fatalf("Exists() = %d, want 0", got)
	}
}

func TestTTLReporting(t *testing.T) {
	s := New()
	if _, exists, _ := s.TTL("missing"); exists {
		t.Fatal("TTL() reported a missing key as existing")
	}

	s.Set("key", "value", 0)
	if _, exists, hasTTL := s.TTL("key"); !exists || hasTTL {
		t.Fatal("TTL() reported a TTL on a persistent key")
	}
}

func TestIncr(t *testing.T) {
	s := New()

	got, err := s.Incr("counter", 1)
	if err != nil || got != 1 {
		t.Fatalf("Incr() = %d, %v; want 1, nil", got, err)
	}
	if got, err := s.Incr("counter", 9); err != nil || got != 10 {
		t.Fatalf("Incr() = %d, %v; want 10, nil", got, err)
	}
	if got, err := s.Incr("counter", -12); err != nil || got != -2 {
		t.Fatalf("Incr() = %d, %v; want -2, nil", got, err)
	}

	s.Set("text", "abc", 0)
	if _, err := s.Incr("text", 1); err != ErrNotInteger {
		t.Fatalf("Incr() on a non-numeric value error = %v, want ErrNotInteger", err)
	}
}

func TestIncrKeepsTTL(t *testing.T) {
	s := New()
	s.Set("counter", "1", time.Minute)

	if _, err := s.Incr("counter", 1); err != nil {
		t.Fatalf("Incr() error = %v", err)
	}
	if _, _, hasTTL := s.TTL("counter"); !hasTTL {
		t.Fatal("Incr() dropped the key's TTL")
	}
}

func TestKeysAndFlush(t *testing.T) {
	s := New()
	s.Set("user:1", "a", 0)
	s.Set("user:2", "b", 0)
	s.Set("session:1", "c", 0)
	s.Set("gone", "d", time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	all := s.Keys("*")
	sort.Strings(all)
	want := []string{"session:1", "user:1", "user:2"}
	if fmt.Sprint(all) != fmt.Sprint(want) {
		t.Fatalf("Keys(\"*\") = %v, want %v", all, want)
	}

	matched := s.Keys("user:*")
	sort.Strings(matched)
	if fmt.Sprint(matched) != fmt.Sprint([]string{"user:1", "user:2"}) {
		t.Fatalf("Keys(\"user:*\") = %v", matched)
	}

	s.Flush()
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() after Flush() = %d, want 0", got)
	}
}

func TestActiveExpireCycle(t *testing.T) {
	s := New()
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("volatile:%d", i), i, time.Millisecond)
	}
	s.Set("permanent", "value", 0)
	time.Sleep(20 * time.Millisecond)

	// Sampling only touches keys carrying a TTL, so 100 keys need at most
	// 100 sampled keys to be collected.
	for removed := 0; removed < 100; {
		sampled, expired := s.ActiveExpireCycle(20)
		if sampled == 0 {
			break
		}
		removed += expired
	}

	if got := s.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1 (only the permanent key)", got)
	}
	if _, ok := s.Get("permanent"); !ok {
		t.Fatal("active expiration removed a key without a TTL")
	}
}

func TestRunExpiryWorker(t *testing.T) {
	s := New()
	for i := 0; i < 50; i++ {
		s.Set(fmt.Sprintf("key:%d", i), i, 10*time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunExpiryWorker(ctx, 5*time.Millisecond, 20)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for s.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("expiry worker left %d keys behind", got)
	}

	// The worker must stop when its context is cancelled.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry worker did not stop on context cancellation")
	}
}

// TestConcurrentAccess is the race detector's target: run it with
// `go test -race`.
func TestConcurrentAccess(t *testing.T) {
	s := New()

	const (
		workers = 16
		// A multiple of six so every branch below runs the same number of
		// times, which makes the counter's final value predictable.
		iterations = 600
	)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("key:%d", i%64)
				switch i % 6 {
				case 0:
					s.Set(key, worker, 0)
				case 1:
					s.Set(key, worker, time.Duration(i%10)*time.Millisecond)
				case 2:
					s.Get(key)
				case 3:
					s.Del(key)
				case 4:
					s.Incr("shared-counter", 1)
				case 5:
					s.ExpireAt(key, time.Now().Add(time.Millisecond))
					s.Exists(key)
				}
			}
		}(w)
	}

	// Contend with the readers and writers from a background sweeper too.
	stop := make(chan struct{})
	var sweeper sync.WaitGroup
	sweeper.Add(1)
	go func() {
		defer sweeper.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.ActiveExpireCycle(20)
				s.Len()
			}
		}
	}()

	wg.Wait()
	close(stop)
	sweeper.Wait()

	// Every worker incremented the shared counter once per six iterations.
	value, ok := s.Get("shared-counter")
	if !ok {
		t.Fatal("shared counter is missing")
	}
	want := fmt.Sprint(workers * iterations / 6)
	if value != want {
		t.Fatalf("shared-counter = %v, want %v", value, want)
	}
}
