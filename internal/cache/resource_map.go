package internal

import (
	"hash/maphash"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
)

// ResourceMap is a concurrency-safe map. It deliberately does not expose bare Get or Put methods as
// its concurrency model is based on the assumption that access to the backing values must be
// strictly synchronized. Instead, all operations should be executed through the various Compute
// methods.
//
// It uses striped locking as the synchronization mechanism. An array of locks is initialized, and
// the key is hashed then modded to pick the corresponding lock. The compute operations will hold
// the lock for the entire duration of the operation.
//
// The number of locks is picked using a sensible value, in this case the value returned by
// [runtime.GOMAXPROCS], which represents the maximum number of CPUs that will be executing
// simultaneously.
type ResourceMap[K comparable, V any] struct {
	locks   []sync.Mutex
	stripes uint64
	// TODO: This map can likely be replaced with [github.com/puzpuzpuz/xsync.MapOf], which offers a
	//  type-safe functional equivalent of sync.Map. However, until
	//  https://github.com/puzpuzpuz/xsync/issues/160 is resolved, each Compute operation creates too much
	//  garbage.
	resources sync.Map
	size      atomic.Int32
}

var seed = maphash.MakeSeed()

func NewResourceMap[K comparable, V any]() *ResourceMap[K, V] {
	stripes := runtime.GOMAXPROCS(0)

	return &ResourceMap[K, V]{
		stripes: uint64(stripes),
		locks:   make([]sync.Mutex, stripes),
	}
}

func (m *ResourceMap[K, V]) load(key K) (v V, ok bool) {
	vAny, ok := m.resources.Load(key)
	if !ok {
		return v, false
	}
	return vAny.(V), true
}

func (m *ResourceMap[K, V]) withLock(key K, f func()) {
	lock := &m.locks[maphash.Comparable(seed, key)%m.stripes]
	lock.Lock()
	defer lock.Unlock()

	f()
}

// Compute first creates the value for the given key using the given function if no corresponding
// entry exists. Then it executes the given compute function
func (m *ResourceMap[K, V]) Compute(
	key K,
	newValue func(key K) V,
	compute func(value V),
) (v V) {
	m.withLock(key, func() {
		var ok bool
		v, ok = m.load(key)
		if !ok {
			v = newValue(key)
			m.resources.Store(key, v)
			m.size.Add(1)
		}

		compute(v)
	})

	return v
}

// ComputeIfPresent invokes the given function only if a corresponding entry exists in the map for
// the given key.
func (m *ResourceMap[K, V]) ComputeIfPresent(key K, compute func(value V)) {
	m.withLock(key, func() {
		v, ok := m.load(key)
		if !ok {
			return
		}

		compute(v)
	})
}

// ComputeDeletion loads the entry from the map if it still exists, then executes the given condition
// function with the value. If the condition returns true, the entry is deleted from the map,
// otherwise nothing happens. As a "compute" function, the condition is executed synchronously, in
// other words, it is guaranteed that no other "compute" functions are executing on that entry.
func (m *ResourceMap[K, V]) ComputeDeletion(key K, condition func(value V) (deleteEntry bool)) {
	m.withLock(key, func() {
		v, ok := m.load(key)
		if !ok {
			return
		}

		if condition(v) {
			m.resources.Delete(key)
			m.size.Add(-1)
		}
	})
}

// Keys returns an [iter.Seq] that will iterate over all keys in this map.
func (m *ResourceMap[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.resources.Range(func(k, v any) bool {
			return yield(k.(K))
		})
	}
}

// Size returns the current number of entries in the map.
func (m *ResourceMap[K, V]) Size() int {
	return int(m.size.Load())
}
