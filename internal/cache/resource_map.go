package internal

import (
	"iter"

	"github.com/puzpuzpuz/xsync/v4"
)

// ResourceMap is a concurrency-safe map. It deliberately does not expose bare Get or Put methods as
// its concurrency model is based on the assumption that access to the backing values must be
// strictly synchronized. Instead, all operations should be executed through the various Compute
// methods.
type ResourceMap[K comparable, V any] xsync.Map[K, V]

func NewResourceMap[K comparable, V any]() *ResourceMap[K, V] {
	return (*ResourceMap[K, V])(xsync.NewMap[K, V]())
}

// Compute first creates the value for the given key using the given function if no corresponding
// entry exists, then it executes the given compute function. It returns the value itself, and a
// boolean indicating whether the value was created.
func (m *ResourceMap[K, V]) Compute(
	key K,
	newValue func(key K) V,
	compute func(value V),
) (v V, created bool) {
	v, _ = (*xsync.Map[K, V])(m).Compute(key, func(v V, loaded bool) (_ V, op xsync.ComputeOp) {
		if !loaded {
			v = newValue(key)
			op = xsync.UpdateOp
			created = true
		}
		compute(v)
		return v, op
	})
	return v, created
}

// ComputeIfPresent invokes the given function only if a corresponding entry exists in the map for
// the given key.
func (m *ResourceMap[K, V]) ComputeIfPresent(key K, f func(value V)) (wasPresent bool) {
	(*xsync.Map[K, V])(m).Compute(key, func(oldValue V, loaded bool) (_ V, op xsync.ComputeOp) {
		if !loaded {
			return oldValue, op
		}

		f(oldValue)
		wasPresent = true
		return oldValue, op
	})
	return wasPresent
}

// ComputeDeletion loads the entry from the map if it still exists, then executes the given condition
// function with the value. If the condition returns true, the entry is deleted from the map,
// otherwise nothing happens. As a "compute" function, the condition is executed synchronously, in
// other words, it is guaranteed that no other "compute" functions are executing on that entry.
func (m *ResourceMap[K, V]) ComputeDeletion(key K, condition func(value V) (deleteEntry bool)) (deleted bool) {
	(*xsync.Map[K, V])(m).Compute(key, func(oldValue V, loaded bool) (_ V, op xsync.ComputeOp) {
		if !loaded {
			return oldValue, op
		}

		if condition(oldValue) {
			op = xsync.DeleteOp
			deleted = true
		}
		return oldValue, op
	})

	return deleted
}

// Range returns an [iter.Seq2] that will iterate over all entries in this map.
func (m *ResourceMap[K, V]) Range() iter.Seq2[K, V] {
	return (*xsync.Map[K, V])(m).Range
}

// Size returns the current number of entries in the map.
func (m *ResourceMap[K, V]) Size() int {
	return (*xsync.Map[K, V])(m).Size()
}
