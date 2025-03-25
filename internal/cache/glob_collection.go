package internal

import (
	"sync"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	"google.golang.org/protobuf/proto"
)

// A globCollection is used to track all the resources in the collection.
type globCollection[T proto.Message] struct {
	// The URL that corresponds to this collection, represented as the raw string rather than a
	// GlobCollectionURL to avoid repeated redundant calls to GlobCollectionURL.String.
	url string

	// The current subscribers to this collection.
	subscribers SubscriberSet[T]
	// Protects values and nonNilValueNames.
	lock sync.RWMutex
	// The set of values in the collection, used by new subscribers to subscribe to all values.
	values utils.Set[*WatchableValue[T]]
	// The set of all non-nil resource names in this collection. Used to track whether a collection is
	// empty. Note that a collection can be empty even if values is non-empty since values that are
	// explicitly subscribed to are kept in the collection/cache to track the subscription in case the
	// value returns.
	nonNilValueNames utils.Set[string]
}

func newGlobCollection[T proto.Message](url string) *globCollection[T] {
	return &globCollection[T]{
		url:              url,
		values:           make(utils.Set[*WatchableValue[T]]),
		nonNilValueNames: make(utils.Set[string]),
	}
}

func (g *globCollection[T]) hasNoValuesOrSubscribersNoLock() bool {
	return len(g.values) == 0 && g.subscribers.Size() == 0
}

// hasNoValuesOrSubscribers returns true if the collection is empty and has no subscribers.
func (g *globCollection[T]) hasNoValuesOrSubscribers() bool {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.hasNoValuesOrSubscribersNoLock()
}

// resourceSet notifies the collection that the given resource has been created.
func (g *globCollection[T]) resourceSet(name string) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.nonNilValueNames.Add(name)
}

// resourceCleared notifies the collection that the given resource has been cleared. If there are no
// remaining non-nil values in the collection (or no values at all), the subscribers are all notified
// that the collection has been deleted.
func (g *globCollection[T]) resourceCleared(name string) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.nonNilValueNames.Remove(name)

	if len(g.nonNilValueNames) > 0 {
		return
	}

	deletedAt := time.Now()

	for handler, subscribedAt := range g.subscribers.Iterator() {
		handler.Notify(g.url, nil, ads.SubscriptionMetadata{
			SubscribedAt:      subscribedAt,
			ModifiedAt:        deletedAt,
			CachedAt:          deletedAt,
			GlobCollectionURL: g.url,
		})
	}
}
