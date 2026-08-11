package store

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// The benchmarks compare the store against the two obvious baselines: a plain
// map behind a sync.RWMutex (what the store is, minus expiration bookkeeping)
// and sync.Map. Run them with:
//
//	go test -bench=. -benchmem -run '^$' ./pkg/store

const benchKeyspace = 4096

func benchKey(i int) string { return "key:" + strconv.Itoa(i%benchKeyspace) }

// mutexMap is the naive baseline: map + RWMutex, no TTL support.
type mutexMap struct {
	mu sync.RWMutex
	m  map[string]interface{}
}

func newMutexMap() *mutexMap { return &mutexMap{m: make(map[string]interface{})} }

func (m *mutexMap) set(key string, value interface{}) {
	m.mu.Lock()
	m.m[key] = value
	m.mu.Unlock()
}

func (m *mutexMap) get(key string) (interface{}, bool) {
	m.mu.RLock()
	value, ok := m.m[key]
	m.mu.RUnlock()
	return value, ok
}

func BenchmarkStoreSet(b *testing.B) {
	s := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(benchKey(i), "value", 0)
	}
}

func BenchmarkStoreSetWithTTL(b *testing.B) {
	s := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(benchKey(i), "value", time.Hour)
	}
}

func BenchmarkMutexMapSet(b *testing.B) {
	m := newMutexMap()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.set(benchKey(i), "value")
	}
}

func BenchmarkStoreGet(b *testing.B) {
	s := New()
	for i := 0; i < benchKeyspace; i++ {
		s.Set(benchKey(i), "value", 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(benchKey(i))
	}
}

func BenchmarkMutexMapGet(b *testing.B) {
	m := newMutexMap()
	for i := 0; i < benchKeyspace; i++ {
		m.set(benchKey(i), "value")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.get(benchKey(i))
	}
}

func BenchmarkStoreGetParallel(b *testing.B) {
	s := New()
	for i := 0; i < benchKeyspace; i++ {
		s.Set(benchKey(i), "value", 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(benchKey(i))
			i++
		}
	})
}

func BenchmarkMutexMapGetParallel(b *testing.B) {
	m := newMutexMap()
	for i := 0; i < benchKeyspace; i++ {
		m.set(benchKey(i), "value")
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.get(benchKey(i))
			i++
		}
	})
}

func BenchmarkSyncMapGetParallel(b *testing.B) {
	var m sync.Map
	for i := 0; i < benchKeyspace; i++ {
		m.Store(benchKey(i), "value")
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Load(benchKey(i))
			i++
		}
	})
}

// BenchmarkStoreMixedParallel approximates a real workload: mostly reads with
// a tenth of the operations writing.
func BenchmarkStoreMixedParallel(b *testing.B) {
	s := New()
	for i := 0; i < benchKeyspace; i++ {
		s.Set(benchKey(i), "value", 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				s.Set(benchKey(i), "value", 0)
			} else {
				s.Get(benchKey(i))
			}
			i++
		}
	})
}

func BenchmarkMutexMapMixedParallel(b *testing.B) {
	m := newMutexMap()
	for i := 0; i < benchKeyspace; i++ {
		m.set(benchKey(i), "value")
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				m.set(benchKey(i), "value")
			} else {
				m.get(benchKey(i))
			}
			i++
		}
	})
}

func BenchmarkActiveExpireCycle(b *testing.B) {
	s := New()
	for i := 0; i < benchKeyspace; i++ {
		s.Set(benchKey(i), "value", time.Hour)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ActiveExpireCycle(20)
	}
}
