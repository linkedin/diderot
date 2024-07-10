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

	// Protects subscribers and values.
	subscribersAndValuesLock sync.RWMutex
	// The current subscribers to this collection.
	subscribers SubscriberSet[T]
	// The set of values in the collection, used by new subscribers to subscribe to all values.
	values utils.Set[*WatchableValue[T]]

	// Protects nonNilValueNames
	nonNilValueNamesLock sync.Mutex
	// The set of all non-nil resource names in this collection. Used to track whether a collection is
	// empty. Note that a collection can be empty even if values is non-empty since values that are
	// explicitly subscribed to are kept in the collection/cache to track the subscription in case the
	// value returns.
	nonNilValueNames utils.Set[string]
}

func (g *globCollection[T]) hasNoValuesOrSubscribersNoLock() bool {
	return len(g.values) == 0 && g.subscribers.Size() == 0
}

// hasNoValuesOrSubscribers returns true if the collection is empty and has no subscribers.
func (g *globCollection[T]) hasNoValuesOrSubscribers() bool {
	g.subscribersAndValuesLock.RLock()
	defer g.subscribersAndValuesLock.RUnlock()

	return g.hasNoValuesOrSubscribersNoLock()
}

// isSubscribed checks if the given handler is already subscribed to the collection.
func (g *globCollection[T]) isSubscribed(handler ads.SubscriptionHandler[T]) bool {
	g.subscribersAndValuesLock.Lock()
	defer g.subscribersAndValuesLock.Unlock()

	return g.subscribers.IsSubscribed(handler)
}

// subscribe adds the given handler as a subscriber to the collection, and iterates through all the
// values in the collection, notifying the handler for each value. If the collection is empty, the
// handler will be notified that the resource is deleted.
func (g *globCollection[T]) subscribe(handler ads.SubscriptionHandler[T]) {
	g.subscribersAndValuesLock.Lock()
	defer g.subscribersAndValuesLock.Unlock()

	subscribedAt, version := g.subscribers.Subscribe(handler)

	if len(g.nonNilValueNames) == 0 {
		handler.Notify(g.url, nil, ads.SubscriptionMetadata{
			SubscribedAt: subscribedAt,
			ModifiedAt:   time.Time{},
			CachedAt:     time.Time{},
		})
	} else {
		for v := range g.values {
			v.NotifyHandlerAfterSubscription(handler, GlobSubscription, subscribedAt, version)
		}
	}
}

// unsubscribe unsubscribes the given handler from the collection. Returns true if the collection has
// no subscribers and is empty.
func (g *globCollection[T]) unsubscribe(handler ads.SubscriptionHandler[T]) bool {
	g.subscribersAndValuesLock.Lock()
	defer g.subscribersAndValuesLock.Unlock()

	g.subscribers.Unsubscribe(handler)
	return g.hasNoValuesOrSubscribersNoLock()
}

// resourceSet notifies the collection that the given resource has been created.
func (g *globCollection[T]) resourceSet(name string) {
	g.nonNilValueNamesLock.Lock()
	defer g.nonNilValueNamesLock.Unlock()

	g.nonNilValueNames.Add(name)
}

// resourceCleared notifies the collection that the given resource has been cleared. If there are no
// remaining non-nil values in the collection (or no values at all), the subscribers are all notified
// that the collection has been deleted.
func (g *globCollection[T]) resourceCleared(name string) {
	g.nonNilValueNamesLock.Lock()
	defer g.nonNilValueNamesLock.Unlock()

	g.nonNilValueNames.Remove(name)

	if len(g.nonNilValueNames) == 0 {
		g.subscribersAndValuesLock.Lock()
		defer g.subscribersAndValuesLock.Unlock()

		deletedAt := time.Now()

		subscribers, _ := g.subscribers.Iterator()
		for handler, subscribedAt := range subscribers {
			handler.Notify(g.url, nil, ads.SubscriptionMetadata{
				SubscribedAt:      subscribedAt,
				ModifiedAt:        deletedAt,
				CachedAt:          deletedAt,
				GlobCollectionURL: g.url,
			})
		}
	}
}
