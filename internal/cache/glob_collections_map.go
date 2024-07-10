package internal

import (
	"log/slog"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
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
			gc := &globCollection[T]{
				url:              gcURL.String(),
				values:           make(utils.Set[*WatchableValue[T]]),
				nonNilValueNames: make(utils.Set[string]),
			}
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
		collection.subscribersAndValuesLock.Lock()
		defer collection.subscribersAndValuesLock.Unlock()

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
		collection.subscribersAndValuesLock.Lock()
		defer collection.subscribersAndValuesLock.Unlock()

		collection.values.Remove(value)

		isEmpty = collection.hasNoValuesOrSubscribersNoLock()
	})
	if isEmpty {
		gcm.deleteCollectionIfEmpty(gcURL)
	}
}

// Subscribe creates or gets the corresponding collection for the given URL using
// createOrModifyCollection, then invokes globCollection.subscribe with the given handler.
func (gcm *GlobCollectionsMap[T]) Subscribe(gcURL ads.GlobCollectionURL, handler ads.SubscriptionHandler[T]) {
	gcm.createOrModifyCollection(gcURL, func(_ ads.GlobCollectionURL, collection *globCollection[T]) {
		collection.subscribe(handler)
	})
}

// Unsubscribe invokes globCollection.unsubscribe on the collection for the given URL, if it exists.
// If, as a result, the collection becomes empty, it invokes deleteCollectionIfEmpty.
func (gcm *GlobCollectionsMap[T]) Unsubscribe(gcURL ads.GlobCollectionURL, handler ads.SubscriptionHandler[T]) {
	var isEmpty bool
	gcm.collections.ComputeIfPresent(gcURL, func(_ ads.GlobCollectionURL, collection *globCollection[T]) {
		isEmpty = collection.unsubscribe(handler)
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
		subscribed = collection.isSubscribed(handler)
	})
	return subscribed
}
