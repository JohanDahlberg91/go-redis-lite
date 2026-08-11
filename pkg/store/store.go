// Package store implements the concurrent in-memory key-value engine: a map
// guarded by a sync.RWMutex, passive expiration on read and an active
// expiration worker that samples keys carrying a TTL.
package store

import (
	"context"
	"errors"
	"math"
	"path"
	"strconv"
	"sync"
	"time"
)

// ErrNotInteger is returned by Incr when the existing value cannot be parsed
// as a 64 bit integer.
var ErrNotInteger = errors.New("value is not an integer or out of range")

// ErrOverflow is returned by Incr when the result would not fit in an int64.
var ErrOverflow = errors.New("increment or decrement would overflow")

// maxCyclesPerTick bounds how many sampling rounds the active expiration
// worker may run back to back, so a database full of expired keys cannot
// starve client commands of the write lock.
const maxCyclesPerTick = 16

// Store is a thread-safe map of keys to items. The zero value is not usable;
// call New.
type Store struct {
	mu    sync.RWMutex
	items map[string]Item
	// volatile indexes the keys that carry a TTL. Active expiration samples
	// this set instead of walking the whole keyspace.
	volatile map[string]struct{}
}

// New returns an empty store.
func New() *Store {
	return &Store{
		items:    make(map[string]Item),
		volatile: make(map[string]struct{}),
	}
}

// SetOptions carries the optional behaviour of the SET command.
type SetOptions struct {
	// ExpiresAt sets an absolute expiration. The zero value clears any
	// existing TTL unless KeepTTL is set.
	ExpiresAt time.Time
	// KeepTTL retains the TTL already attached to the key.
	KeepTTL bool
	// NX only sets the key when it does not exist yet.
	NX bool
	// XX only sets the key when it already exists.
	XX bool
}

// Set stores value under key. A ttl of zero or less means no expiration.
func (s *Store) Set(key string, value interface{}, ttl time.Duration) {
	var opts SetOptions
	if ttl > 0 {
		opts.ExpiresAt = time.Now().Add(ttl)
	}
	s.SetWithOptions(key, value, opts)
}

// SetWithOptions stores value under key honouring opts. It reports whether
// the write happened; only the NX and XX conditions can prevent it.
func (s *Store) SetWithOptions(key string, value interface{}, opts SetOptions) bool {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.items[key]
	if exists && existing.ExpiredAt(now) {
		s.deleteLocked(key)
		existing, exists = Item{}, false
	}
	if (opts.NX && exists) || (opts.XX && !exists) {
		return false
	}

	item := Item{Value: value, ExpiresAt: opts.ExpiresAt}
	if opts.KeepTTL && exists {
		item.ExpiresAt = existing.ExpiresAt
	}
	s.items[key] = item
	if item.HasTTL() {
		s.volatile[key] = struct{}{}
	} else {
		delete(s.volatile, key)
	}
	return true
}

// Get returns the value stored under key. A key whose TTL has elapsed is
// deleted on the spot (passive expiration) and reported as missing.
func (s *Store) Get(key string) (interface{}, bool) {
	now := time.Now()

	s.mu.RLock()
	item, ok := s.items[key]
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}
	if !item.ExpiredAt(now) {
		return item.Value, true
	}

	// Expired: take the write lock and drop it, re-checking because another
	// goroutine may have replaced the key in the meantime.
	s.mu.Lock()
	if current, ok := s.items[key]; ok && current.ExpiredAt(time.Now()) {
		s.deleteLocked(key)
	}
	s.mu.Unlock()
	return nil, false
}

// Del removes keys and returns how many were actually present.
func (s *Store) Del(keys ...string) int {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, key := range keys {
		item, ok := s.items[key]
		if !ok {
			continue
		}
		s.deleteLocked(key)
		if !item.ExpiredAt(now) {
			removed++
		}
	}
	return removed
}

// Exists returns how many of the given keys are present and unexpired.
// Repeated keys are counted once per occurrence, as Redis does.
func (s *Store) Exists(keys ...string) int {
	now := time.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	found := 0
	for _, key := range keys {
		if item, ok := s.items[key]; ok && !item.ExpiredAt(now) {
			found++
		}
	}
	return found
}

// ExpireAt attaches an absolute expiration to key. A timestamp in the past
// deletes the key. It reports whether the key existed.
func (s *Store) ExpireAt(key string, at time.Time) bool {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return false
	}
	if item.ExpiredAt(now) {
		s.deleteLocked(key)
		return false
	}
	if now.After(at) {
		s.deleteLocked(key)
		return true
	}
	item.ExpiresAt = at
	s.items[key] = item
	s.volatile[key] = struct{}{}
	return true
}

// Persist removes the TTL from key and reports whether one was cleared.
func (s *Store) Persist(key string) bool {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if !ok {
		return false
	}
	if item.ExpiredAt(now) {
		s.deleteLocked(key)
		return false
	}
	if !item.HasTTL() {
		return false
	}
	item.ExpiresAt = time.Time{}
	s.items[key] = item
	delete(s.volatile, key)
	return true
}

// TTL reports the remaining lifetime of key. exists is false for a missing or
// expired key; hasTTL is false for a key that never expires.
func (s *Store) TTL(key string) (remaining time.Duration, exists, hasTTL bool) {
	now := time.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[key]
	if !ok || item.ExpiredAt(now) {
		return 0, false, false
	}
	if !item.HasTTL() {
		return 0, true, false
	}
	return item.ExpiresAt.Sub(now), true, true
}

// Incr adds delta to the integer value stored under key, creating it at zero
// when missing. Any existing TTL is preserved. It returns the new value.
func (s *Store) Incr(key string, delta int64) (int64, error) {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[key]
	if ok && item.ExpiredAt(now) {
		s.deleteLocked(key)
		item, ok = Item{}, false
	}

	var current int64
	if ok {
		text, isString := item.Value.(string)
		if !isString {
			return 0, ErrNotInteger
		}
		parsed, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		current = parsed
	}

	if (delta > 0 && current > math.MaxInt64-delta) ||
		(delta < 0 && current < math.MinInt64-delta) {
		return 0, ErrOverflow
	}

	current += delta
	item.Value = strconv.FormatInt(current, 10)
	s.items[key] = item
	return current, nil
}

// Keys returns the unexpired keys matching a glob-style pattern. "*" matches
// every key.
func (s *Store) Keys(pattern string) []string {
	now := time.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	matches := make([]string, 0, len(s.items))
	for key, item := range s.items {
		if item.ExpiredAt(now) {
			continue
		}
		if pattern != "*" {
			ok, err := path.Match(pattern, key)
			if err != nil || !ok {
				continue
			}
		}
		matches = append(matches, key)
	}
	return matches
}

// Len returns the number of unexpired keys.
func (s *Store) Len() int {
	now := time.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, item := range s.items {
		if !item.ExpiredAt(now) {
			count++
		}
	}
	return count
}

// Flush removes every key.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[string]Item)
	s.volatile = make(map[string]struct{})
}

// ActiveExpireCycle samples up to sampleSize keys carrying a TTL and deletes
// the expired ones. It returns how many keys were sampled and how many were
// removed.
func (s *Store) ActiveExpireCycle(sampleSize int) (sampled, expired int) {
	if sampleSize <= 0 {
		return 0, 0
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Go randomises map iteration order, which gives the random sample the
	// algorithm wants without maintaining a separate index.
	for key := range s.volatile {
		if sampled >= sampleSize {
			break
		}
		sampled++
		if item, ok := s.items[key]; !ok || item.ExpiredAt(now) {
			s.deleteLocked(key)
			expired++
		}
	}
	return sampled, expired
}

// RunExpiryWorker drives active expiration until ctx is cancelled. On every
// tick it samples sampleSize volatile keys and, as long as more than a
// quarter of the sample turned out to be expired, immediately samples again
// (bounded by maxCyclesPerTick) because that ratio suggests many more stale
// keys are waiting.
func (s *Store) RunExpiryWorker(ctx context.Context, interval time.Duration, sampleSize int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := 0; i < maxCyclesPerTick; i++ {
				sampled, expired := s.ActiveExpireCycle(sampleSize)
				if sampled == 0 || expired*4 <= sampled {
					break
				}
			}
		}
	}
}

// deleteLocked removes a key from both maps. The caller must hold the write
// lock.
func (s *Store) deleteLocked(key string) {
	delete(s.items, key)
	delete(s.volatile, key)
}
