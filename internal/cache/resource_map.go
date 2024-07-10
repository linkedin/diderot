package internal

import (
	"sync"
)

// The resourceMapEntry is the value type in a ResourceMap. Access to an entry is guarded by lock and isDeleted.
//
// The reason ResourceMap does not directly insert the value type into the backing sync.Map is because of the
// following scenario:
// Routine 1 wants to delete an entry because it is not needed anymore, while routine 2 wants to execute an operation on
// the entry. Routine 2 loads the value from the map and runs its operation. In the meantime, routine 1 simply deletes
// the entry from the map, completely voiding routine 2's work. To prevent this, the additional resourceMapEntry layer
// provides a sync.RWMutex and a flag indicating the entry has been deleted from the map (isDeleted). All operations
// acquire the read lock and check isDeleted, bailing if the entry has been deleted between when it was loaded from the
// map and the read lock was acquired. Conversely, the write lock on the entry must be held during deletion, and
// isDelete must be set once the entry has been deleted and while still holding the write lock. This way, only one
// routine can successfully delete the entry from the map, but it must wait for all other in-flight read operations to
// do so (see ResourceMap.DeleteIf for additional info).
type resourceMapEntry[T any] struct {
	// The write lock must be held while inserting or removing the entry from the resources map. The read lock must be
	// held while interacting with value. Before interacting with the value, isDeleted must be checked.
	lock sync.RWMutex
	// isDeleted must always be checked before accessing value. If true, this entry should be entirely discarded.
	// Guarded by lock.
	isDeleted bool
	// the actual value
	value T
}

// ResourceMap is a typed extension of a sync.Map which allows for fine-grained control over the creations and deletions
// of entries. It is meant to imitate Java's compute/computeIfPresent/computeIfAbsent methods on ConcurrentHashMap as
// sync.Map does not natively provide these constructs. It deliberately does not expose bare Get or Put methods as its
// concurrency model is based on the assumption that access to the backing values must be strictly synchronized.
// Instead, all operations should be executed through the various compute methods.
type ResourceMap[K comparable, T any] struct {
	syncMap sync.Map
}

// ComputeIfPresent executes the given compute function if the entry is present in the map. There can be multiple
// executions of ComputeIfPresent in flight at the same time for the same entry.
func (m *ResourceMap[K, T]) ComputeIfPresent(key K, compute func(key K, value T)) bool {
	eAny, _ := m.syncMap.Load(key)
	e, ok := eAny.(*resourceMapEntry[T])
	if !ok {
		// The entry didn't exist in the map, do nothing.
		return false
	}

	e.lock.RLock()
	defer e.lock.RUnlock()
	if e.isDeleted {
		// In between loading the entry and acquiring the read lock, the entry was deleted, do nothing.
		return false
	}

	compute(key, e.value)
	return true
}

// Compute ensures that an entry for the given name exists in the map before executing the given compute function. If
// the entry was already in the map, it has the same semantics as ComputeIfPresent. Otherwise, it uses the given
// newValue constructor to create a new value before executing the given compute method, and no ComputeIfPresent will
// run until both the newValue constructor and the compute function complete. This is to prevent
// subsequent ComputeIfPresent operations from reading a partially initialized value.
func (m *ResourceMap[K, T]) Compute(
	key K,
	newValue func(key K) T,
	compute func(key K, value T),
) {
	// Do an initial check to see if the entry is already present in the map, and if so, apply the function.
	if m.ComputeIfPresent(key, compute) {
		return
	}

	// Otherwise, attempt to create the entry
	e := new(resourceMapEntry[T])
	// Inserting the entry while already holding the write lock means another routine that loads this entry from the
	// map will be forced to wait until it is fully initialized before reading its value.
	e.lock.Lock()
	defer e.lock.Unlock()

	// This loop guarantees the creation of the entry by repeatedly attempting to insert the value in the map,
	// guaranteeing that the entry is eventually inserted.
	for {
		// Attempt to insert the new resourceMapEntry in the map
		eAny, didNotStore := m.syncMap.LoadOrStore(key, e)
		if !didNotStore {
			// Entry successfully inserted in the map, the rest of the function can initialize it safely as it is
			// currently holding the write lock.
			break
		}

		// Unhappy path: between the original ComputeIfPresent check and now, another routine created the entry.
		e := eAny.(*resourceMapEntry[T])
		e.lock.RLock()
		if !e.isDeleted {
			// The entry was already present in the map, execute the compute function safely since the read lock is
			// currently held.
			compute(key, e.value)
			// Nothing left to be done, exit the function
			e.lock.RUnlock()
			return
		}

		// Very unhappy path: the entry was deleted between this invocation reading it from the map and
		// acquiring the read lock! Attempt to insert the new entry again by continuing the loop. Note that this
		// condition is the reason this is in a loop in the first place. It should also be noted that it is
		// extremely unlikely for this occur, and in most instances this branch will never be reached.
		e.lock.RUnlock()
	}

	// Initialize the value while holding the write lock, otherwise a call to ComputeIfPresent could read e.value as
	// nil.
	e.value = newValue(key)
	// Technically, at this point, the write lock does not need to be held anymore, only the read lock since e.value
	// is initialized. It is however impossible to downgrade the lock write to read without actually releasing it. In
	// between releasing the write lock and acquiring the read lock, it is possible for the entry to be deleted! Hence,
	// the write lock is kept.
	compute(key, e.value)
}

// DeleteIf loads the entry from the map if it still exists, then executes the given condition function with the value.
// If the condition returns true, the entry is deleted from the map, otherwise nothing happens. It is guaranteed that
// the condition function will only be executed once any in-flight ComputeIfPresent operations for that entry complete.
// Conversely, once the in-flight operations complete, no new ComputeIfPresent operations will be started for that entry
// until the condition has been checked. If the entry was deleted, any ComputeIfPresent operations queued for that entry
// while the condition was being checked will be abandoned. No two executions of DeleteIf can execute in parallel for
// the same entry.
func (m *ResourceMap[K, T]) DeleteIf(key K, condition func(key K, value T) bool) {
	eAny, ok := m.syncMap.Load(key)
	if !ok {
		return
	}

	e := eAny.(*resourceMapEntry[T])
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.isDeleted {
		return
	}
	if condition(key, e.value) {
		m.syncMap.Delete(key)
	}
}

// Keys iterates through all the keys in the map. It does not expose the actual value as all
// operations on the values should be executed through Compute, ComputeIfPresent and DeleteIf.
func (m *ResourceMap[K, T]) Keys(f func(key K) bool) {
	m.syncMap.Range(func(key, value any) bool {
		return f(key.(K))
	})
}
