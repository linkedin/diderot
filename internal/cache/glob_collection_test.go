package internal

import (
	"sync"
	"testing"

	"github.com/puzpuzpuz/xsync/v3"
)

func BenchmarkMaps(b *testing.B) {
	//b.Run("xsync.Map", func(b *testing.B) {
	//	benchmarkMap(b, xsync.NewMap())
	//})
	//b.Run("locked map", func(b *testing.B) {
	//	benchmarkMap(b, &mapWrapper{m: make(map[string]any)})
	//})
	//b.Run("sync.Map", func(b *testing.B) {
	//	benchmarkMap(b, new(syncMapWrapper))
	//})
	const (
		foo = 1
		bar = 2
	)
	b.Run("compute", func(b *testing.B) {
		m := xsync.NewMapOf[int, int]()
		var currentValue any
		for range b.N {
			m.Compute(foo, func(oldValue int, loaded bool) (newValue int, delete bool) {
				if !loaded {
					oldValue = bar
				}

				currentValue = oldValue
				return oldValue, false
			})
		}
		if currentValue != bar {
			b.Fail()
		}
	})
}

type Map interface {
	Load(string) (any, bool)
	Store(string, any)
}

func benchmarkMap(b *testing.B, m Map) {
	m.Store("foo", "bar")
	for range b.N {
		m.Load("foo")
	}
}

type mapWrapper struct {
	lock sync.Mutex
	m    map[string]any
}

func (m *mapWrapper) Load(key string) (any, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	v, ok := m.m[key]
	return v, ok
}

func (m *mapWrapper) Store(key string, v any) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.m[key] = v
}

type syncMapWrapper sync.Map

func (w *syncMapWrapper) Load(k string) (any, bool) {
	return (*sync.Map)(w).Load(k)
}

func (w *syncMapWrapper) Store(k string, v any) {
	(*sync.Map)(w).Store(k, v)
}
