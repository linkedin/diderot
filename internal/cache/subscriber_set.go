package internal

import (
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkedin/diderot/ads"
	"google.golang.org/protobuf/proto"
)

// SubscriberSetVersion is a monotonically increasing counter that tracks how many times subscribers
// have been added to a given SubscriberSet. This means a subscriber can check whether they are in a
// SubscriberSet by storing the version returned by SubscriberSet.Subscribe and comparing it against
// the version returned by SubscriberSet.Iterator.
type SubscriberSetVersion uint64

// SubscriberSet is a concurrency-safe data structure that stores a set of unique subscribers. It is
// specifically designed to support wildcard and glob subscriptions such that they can be shared by
// multiple watchableValues instead of requiring each WatchableValue to store each subscriber. After
// subscribing to a given value, the SubscriptionHandler is supposed to be notified of the current
// value immediately, which usually simply means reading WatchableValue.currentValue and notifying
// the handler. However, it is possible that the notification loop for the WatchableValue is already
// running, and it could result in a double notification. To avoid this, this data structure
// introduces a notion of versioning. This way, the notification loop can record which version it is
// about to iterate over (in WatchableValue.lastSeenSubscriberSetVersions) such that subscribers can
// determine whether the loop will notify them and avoid the double notification. This is done by
// recording the version returned by SubscriberSet.Subscribe and checking whether it's equal to or
// smaller than the version in WatchableValue.lastSeenSubscriberSetVersions.
//
// The implementation uses a sync.Map to store and iterate over the subscribers. In this case it's
// impossible to use a normal map since the subscriber set will be iterated over frequently. However,
// sync.Map provides no guarantees about what happens if the map is modified while another goroutine
// is iterating over the entries. Specifically, if an entry is added during the iteration, the
// iterator may or may not actually yield the new entry, which means the iterator may yield an entry
// that was added _after_ Iterator was invoked, violating the Iterator contract that it will only
// yield entries that were added before. To get around this, the returned iterator simply records the
// version at which it was initially created, and drops entries that have a greater version, making
// it always consistent.
type SubscriberSet[T proto.Message] struct {
	// Protects entry creation in the set.
	lock sync.Mutex
	// Maps SubscriptionHandler instances to the subscriber instance containing the metadata.
	subscribers sync.Map // Real type: map[SubscriptionHandler[T]]*subscriber
	// The current subscriber set version.
	version SubscriberSetVersion
	// Stores the current number of subscribers.
	size atomic.Int64
}

type subscriber struct {
	subscribedAt time.Time
	id           SubscriberSetVersion
}

// IsSubscribed checks whether the given handler is subscribed to this set.
func (m *SubscriberSet[T]) IsSubscribed(handler ads.SubscriptionHandler[T]) bool {
	if m == nil {
		return false
	}

	_, ok := m.subscribers.Load(handler)
	return ok
}

// Subscribe registers the given SubscriptionHandler as a subscriber and returns the time and version
// at which the subscription was processed. The returned version can be compared against the version
// returned by Iterator to check whether the given handler is present in the iterator.
func (m *SubscriberSet[T]) Subscribe(handler ads.SubscriptionHandler[T]) (subscribedAt time.Time, id SubscriberSetVersion) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.version++
	s := &subscriber{
		subscribedAt: timeProvider(),
		id:           m.version,
	}
	_, loaded := m.subscribers.Swap(handler, s)
	if !loaded {
		m.size.Add(1)
	}

	return s.subscribedAt, s.id
}

// Unsubscribe removes the given handler from the set, and returns whether the set is now empty as a
// result of this unsubscription.
func (m *SubscriberSet[T]) Unsubscribe(handler ads.SubscriptionHandler[T]) (empty bool) {
	_, loaded := m.subscribers.LoadAndDelete(handler)
	if !loaded {
		return m.size.Load() == 0
	}

	return m.size.Add(-1) == 0
}

// Size returns the number of subscribers in the set. For convenience, returns 0 if the receiver is
// nil.
func (m *SubscriberSet[T]) Size() int {
	if m == nil {
		return 0
	}
	return int(m.size.Load())
}

type SubscriberSetIterator[T proto.Message] iter.Seq2[ads.SubscriptionHandler[T], time.Time]

// Iterator returns an iterator over the SubscriberSet. The returned associated version can be used
// by subscribers to check whether they are present in the iterator. For convenience, returns an
// empty iterator and invalid version if the receiver is nil.
func (m *SubscriberSet[T]) Iterator() (SubscriberSetIterator[T], SubscriberSetVersion) {
	if m == nil {
		return func(yield func(ads.SubscriptionHandler[T], time.Time) bool) {}, 0
	}

	m.lock.Lock()
	version := m.version
	m.lock.Unlock()

	return func(yield func(ads.SubscriptionHandler[T], time.Time) bool) {
		m.subscribers.Range(func(key, value any) bool {
			s := value.(*subscriber)
			if s.id > version {
				return true
			}
			return yield(key.(ads.SubscriptionHandler[T]), s.subscribedAt)
		})
	}, version
}
