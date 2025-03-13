package internal

import (
	"log/slog"
	"sync"
	"time"

	"github.com/linkedin/diderot/ads"
	"google.golang.org/protobuf/proto"
)

// GlobCollectionsMap used to map individual GlobCollectionURL to their corresponding globCollection.
// This uses a ResourceMap under the hood because it has similar semantics to cache entries:
//  1. A globCollection is created lazily, either when an entry for that collection is created, or a
//     subscription to that collection is made.
//  2. A globCollection is only deleted once all subscribers have unsubscribed and the collection is
//     empty. Crucially, a collection can be empty but will remain in the cache as long as some
//     subscribers remain subscribed.
type GlobCollectionsMap[T proto.Message] struct {
	collections ResourceMap[ads.GlobCollectionURL, *globCollection[T]]
}

// createOrModifyCollection gets or creates the globCollection for the given GlobCollectionURL, and
// executes the given function on it.
func (gcm *GlobCollectionsMap[T]) createOrModifyCollection(
	gcURL ads.GlobCollectionURL,
	f func(gcURL ads.GlobCollectionURL, collection *globCollection[T]),
) {
	gcm.collections.Compute(
		gcURL,
		func(gcURL ads.GlobCollectionURL) *globCollection[T] {
			gc := newGlobCollection[T](gcURL.String())
			slog.Debug("Created collection", "url", gcURL)
			return gc
		},
		f,
	)
}

// PutValueInCollection creates the glob collection if it was not already created, and puts the given
// value in it.
func (gcm *GlobCollectionsMap[T]) PutValueInCollection(gcURL ads.GlobCollectionURL, value *WatchableValue[T]) {
	gcm.createOrModifyCollection(gcURL, func(gcURL ads.GlobCollectionURL, collection *globCollection[T]) {
		collection.lock.Lock()
		defer collection.lock.Unlock()

		value.globCollection = collection
		collection.values.Add(value)
		value.SubscriberSets[GlobSubscription] = &collection.subscribers
	})
}

// RemoveValueFromCollection removes the given value from the collection. If the collection becomes
// empty as a result, it is removed from the map.
func (gcm *GlobCollectionsMap[T]) RemoveValueFromCollection(gcURL ads.GlobCollectionURL, value *WatchableValue[T]) {
	var isEmpty bool
	gcm.collections.ComputeIfPresent(gcURL, func(gcURL ads.GlobCollectionURL, collection *globCollection[T]) {
		collection.lock.Lock()
		defer collection.lock.Unlock()

		collection.values.Remove(value)

		isEmpty = collection.hasNoValuesOrSubscribersNoLock()
	})
	if isEmpty {
		gcm.deleteCollectionIfEmpty(gcURL)
	}
}

// Subscribe creates or gets the corresponding collection for the given URL using
// createOrModifyCollection. It adds the given handler as a subscriber to the collection, then
// iterates through all the values in the collection, notifying the handler for each value. If the
// collection is empty, the handler will be notified that the resource is deleted. See the
// documentation on [WatchableValue.NotifyHandlerAfterSubscription] for more insight on the returned
// [sync.WaitGroup] slice.
func (gcm *GlobCollectionsMap[T]) Subscribe(
	gcURL ads.GlobCollectionURL, handler ads.SubscriptionHandler[T],
) (wgs []*sync.WaitGroup) {
	gcm.createOrModifyCollection(gcURL, func(_ ads.GlobCollectionURL, collection *globCollection[T]) {
		subscribedAt, version := collection.subscribers.Subscribe(handler)

		collection.lock.RLock()
		defer collection.lock.RUnlock()

		if len(collection.nonNilValueNames) == 0 {
			handler.Notify(collection.url, nil, ads.SubscriptionMetadata{
				SubscribedAt:      subscribedAt,
				ModifiedAt:        time.Time{},
				CachedAt:          time.Time{},
				GlobCollectionURL: collection.url,
			})
		} else {
			for v := range collection.values {
				wg := v.NotifyHandlerAfterSubscription(handler, GlobSubscription, subscribedAt, version)
				if wg != nil {
					wgs = append(wgs, wg)
				}
			}
		}
	})
	return wgs
}

// Unsubscribe invokes globCollection.unsubscribe on the collection for the given URL, if it exists.
// If, as a result, the collection becomes empty, it invokes deleteCollectionIfEmpty.
func (gcm *GlobCollectionsMap[T]) Unsubscribe(gcURL ads.GlobCollectionURL, handler ads.SubscriptionHandler[T]) {
	var isEmpty bool
	gcm.collections.ComputeIfPresent(gcURL, func(_ ads.GlobCollectionURL, collection *globCollection[T]) {
		collection.lock.RLock()
		defer collection.lock.RUnlock()

		collection.subscribers.Unsubscribe(handler)
		isEmpty = collection.hasNoValuesOrSubscribersNoLock()
	})
	if isEmpty {
		gcm.deleteCollectionIfEmpty(gcURL)
	}
}

// deleteCollectionIfEmpty attempts to completely remove the collection from the map, if and only if
// there are no more subscribers and the collection is empty.
func (gcm *GlobCollectionsMap[T]) deleteCollectionIfEmpty(gcURL ads.GlobCollectionURL) {
	gcm.collections.DeleteIf(gcURL, func(_ ads.GlobCollectionURL, collection *globCollection[T]) bool {
		empty := collection.hasNoValuesOrSubscribers()
		if empty {
			slog.Debug("Deleting collection", "url", gcURL)
		}
		return empty
	})
}

// IsSubscribed checks if the given handler is subscribed to the collection.
func (gcm *GlobCollectionsMap[T]) IsSubscribed(gcURL ads.GlobCollectionURL, handler ads.SubscriptionHandler[T]) (subscribed bool) {
	gcm.collections.ComputeIfPresent(gcURL, func(_ ads.GlobCollectionURL, collection *globCollection[T]) {
		// Locking is not required here, as SubscriberSet is safe for concurrent access.
		subscribed = collection.subscribers.IsSubscribed(handler)
	})
	return subscribed
}
