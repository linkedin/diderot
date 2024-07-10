package internal

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkedin/diderot/ads"
	"google.golang.org/protobuf/proto"
)

type Priority int

type valueWithMetadata[T proto.Message] struct {
	resource          *ads.Resource[T]
	modifiedAt        time.Time
	cachedAt          time.Time
	idx               Priority
	globCollectionURL string
}

func (v *valueWithMetadata[T]) subscriptionMetadata(subscribedAt time.Time) ads.SubscriptionMetadata {
	return ads.SubscriptionMetadata{
		SubscribedAt:      subscribedAt,
		ModifiedAt:        v.modifiedAt,
		CachedAt:          v.cachedAt,
		Priority:          int(v.idx),
		GlobCollectionURL: v.globCollectionURL,
	}
}

type WatchableValue[T proto.Message] struct {
	name           string
	globCollection *globCollection[T]

	// lock protects all fields of this struct except name and globCollection since they are not modified
	// after initialization. currentValue is not strictly protected by lock since it can be read without
	// holding lock, but in practice lock is always held when updating currentValue.
	lock sync.Mutex
	// valuesFromDifferentPrioritySources is represented as a slice to represent the backing prioritized
	// values. A lower index means a higher priority.
	valuesFromDifferentPrioritySources []*ads.Resource[T]
	// currentIndex is always a valid index into valuesFromDifferentPrioritySources and represents the
	// currently highest priority (lowest index) non-nil value in valuesFromDifferentPrioritySources. If
	// valuesFromDifferentPrioritySources[currentIndex] is nil, the value has been cleared at all
	// priorities. See NewPrioritizedCache for additional details on how priority.
	currentIndex Priority
	// modifiedAt contains the timestamp provided by the most recent call to set or clear.
	modifiedAt time.Time
	// cachedAt contains the timestamp of the most recent call to set or clear.
	cachedAt time.Time
	// loopStatus stores the current state of the loop, see startNotificationLoop for additional
	// details.
	loopStatus loopStatus
	// loopWg is incremented every time the loop starts, and decremented whenever it completes.
	loopWg sync.WaitGroup
	// SubscriberSets is holds all the async.SubscriberSet instances relevant to this WatchableValue.
	SubscriberSets [subscriptionTypes]*SubscriberSet[T]
	// lastSeenSubscriberSetVersions stores the SubscriberSetVersion of the most-recently iterated
	// SubscriberSet. When subscribing to this WatchableValue, subscriber goroutines should check this
	// value
	lastSeenSubscriberSetVersions [subscriptionTypes]SubscriberSetVersion

	// currentValue always contains the value of valuesFromDifferentPrioritySources[currentIndex] so that
	// the current value can be read without holding valueLock.
	currentValue atomic.Pointer[ads.Resource[T]]
}

func NewValue[T proto.Message](
	name string,
	prioritySlots int,
) *WatchableValue[T] {
	return &WatchableValue[T]{
		name:                               name,
		valuesFromDifferentPrioritySources: make([]*ads.Resource[T], prioritySlots),
		currentIndex:                       Priority(prioritySlots - 1),
		SubscriberSets:                     [subscriptionTypes]*SubscriberSet[T]{new(SubscriberSet[T])},
	}
}

func (v *WatchableValue[T]) IsSubscribed(handler ads.SubscriptionHandler[T]) bool {
	return v.SubscriberSets[ExplicitSubscription].IsSubscribed(handler)
}

func (v *WatchableValue[T]) Subscribe(handler ads.SubscriptionHandler[T]) {
	subscribedAt, version := v.SubscriberSets[ExplicitSubscription].Subscribe(handler)
	v.NotifyHandlerAfterSubscription(handler, ExplicitSubscription, subscribedAt, version)
}

func (v *WatchableValue[T]) Unsubscribe(handler ads.SubscriptionHandler[T]) (empty bool) {
	return v.SubscriberSets[ExplicitSubscription].Unsubscribe(handler)
}

func (v *WatchableValue[T]) Read() *ads.Resource[T] {
	return v.currentValue.Load()
}

func (v *WatchableValue[T]) readWithMetadataNoLock() valueWithMetadata[T] {
	var gcURL string
	if v.globCollection != nil {
		gcURL = v.globCollection.url
	}
	return valueWithMetadata[T]{
		resource:          v.valuesFromDifferentPrioritySources[v.currentIndex],
		modifiedAt:        v.modifiedAt,
		cachedAt:          v.cachedAt,
		idx:               v.currentIndex,
		globCollectionURL: gcURL,
	}
}

func (v *WatchableValue[T]) Clear(p Priority, clearedAt time.Time) (isFullClear bool) {
	v.lock.Lock()
	defer v.lock.Unlock()

	defer func() {
		isFullClear = v.valuesFromDifferentPrioritySources[v.currentIndex] == nil
	}()

	// Nothing to be done since the value is already nil
	if v.valuesFromDifferentPrioritySources[p] == nil {
		return
	}

	v.valuesFromDifferentPrioritySources[p] = nil

	// If any value other than the current value is being cleared, no need to notify the subscribers. The
	// invariants maintained by clear and set guarantee that v.currentIndex points to the highest
	// priority value. In other words, there exists no index i such that i < v.currentIndex where
	// v.valuesFromDifferentPrioritySources[i] isn't nil.
	if p != v.currentIndex {
		return
	}

	// If the current value is being cleared, then subscribers need to be notified of the next highest priority non-nil
	// value. If no non-nil values remain, the subscribers need to be notified of the deletion.
	for ; int(v.currentIndex) != len(v.valuesFromDifferentPrioritySources); v.currentIndex++ {
		if v.valuesFromDifferentPrioritySources[v.currentIndex] != nil {
			break
		}
	}

	if int(v.currentIndex) == len(v.valuesFromDifferentPrioritySources) {
		// Didn't find any non-nil values, leave v.currentIndex at the lowest priority. The invariant maintained
		// here is that v.currentIndex should always be a valid index into v.valuesFromDifferentPrioritySources.
		v.currentIndex = Priority(len(v.valuesFromDifferentPrioritySources) - 1)
	}

	v.notify(clearedAt)
	return
}

// Set updates resource to the given version and value and notifies all subscribers of the new value. It is invalid to
// invoke this method with a nil resource.
func (v *WatchableValue[T]) Set(p Priority, r *ads.Resource[T], modifiedAt time.Time) {
	v.lock.Lock()
	defer v.lock.Unlock()

	v.valuesFromDifferentPrioritySources[p] = r
	// Ignore updates of a lower priority than the current value
	if p > v.currentIndex {
		return
	}

	v.currentIndex = p

	v.notify(modifiedAt)
}

// SetTimeProvider can be used to provide an alternative time provider, which is important when
// benchmarking the cache.
func SetTimeProvider(now func() time.Time) {
	timeProvider = now
}

var timeProvider = time.Now

func (v *WatchableValue[T]) notify(modifiedAt time.Time) {
	v.cachedAt = timeProvider()
	v.modifiedAt = modifiedAt
	v.currentValue.Store(v.valuesFromDifferentPrioritySources[v.currentIndex])

	v.startNotificationLoop()
}

type loopStatus byte

const (
	// notRunning means the loop has either never run, or has completed running through one cycle after
	// the value has received an update. Subscribers should check the corresponding
	// WatchableValue.lastSeenSubscriberSetVersions to see if they have already been notified.
	notRunning = loopStatus(iota)
	// initialized means the goroutine has been started but has not yet loaded the subscriber map and updated
	// WatchableValue.lastSeenSubscriberSetVersions.
	initialized
	// running means the goroutine has started and has loaded the subscriber map.
	running
)

// NotifyHandlerAfterSubscription should be invoked by subscribers after subscribing to the
// corresponding SubscriberSet. This function is guaranteed to only return once the handler has been
// notified of the current value, since the xDS protocol spec explicitly states that an explicit
// subscription to an entry must always be respected by sending the current value:
// https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#subscribing-to-resources
//
//	A resource_names_subscribe field may contain resource names that the server believes the client is
//	already subscribed to, and furthermore has the most recent versions of. However, the server must
//	still provide those resources in the response; due to implementation details hidden from the
//	server, the client may have "forgotten" those resources despite apparently remaining subscribed.
func (v *WatchableValue[T]) NotifyHandlerAfterSubscription(
	handler ads.SubscriptionHandler[T],
	subType subscriptionType,
	subscribedAt time.Time,
	version SubscriberSetVersion,
) {
	v.lock.Lock()
	value := v.readWithMetadataNoLock()

	switch {
	// If a WatchableValue is initially nil (i.e. it's being preserved in the map to keep track of
	// explicit subscriptions), avoid notifying implicit subscribers, as the resource doesn't actually
	// exist.
	case subType.isImplicit() && value.resource == nil:
		v.lock.Unlock()
	// If the loop isn't currently running and the subscriber map version is equal or greater than the
	// subscriber version, the loop has already completed and has already notified the subscriber, so
	// exit immediately.
	case v.loopStatus != running && v.lastSeenSubscriberSetVersions[subType] >= version:
		v.lock.Unlock()
	// Since lastSeenSubscriberSetVersions is updated by the notification loop goroutine while holding the lock,
	// it can be used to track whether the loop goroutine has already picked up the new
	// SubscriptionHandler and will notify it as part of its ongoing execution. In this case, the
	// subscriber goroutine simply waits for the loop to complete to guarantee that the handler has been
	// notified. This is as opposed to notifying the handler directly even though the loop is running,
	// potentially resulting in a double notification.
	case v.loopStatus == initialized || (v.loopStatus == running && v.lastSeenSubscriberSetVersions[subType] >= version):
		v.lock.Unlock()
		v.loopWg.Wait()
	default:
		handler.Notify(v.name, value.resource, value.subscriptionMetadata(subscribedAt))
		v.lock.Unlock()
	}
}

// startNotificationLoop spawns a goroutine that will notify all the subscribers to this entry of the
// current value. If the current loopStatus is not notRunning (i.e. the goroutine from a previous
// invocation is still running), immediately returns and does nothing. Must be invoked while holding
// lock. If the value is updated while the subscribers are being notified, it will bail on updating
// the rest of the subscribers and start from the top again. This way the routine can be reused by
// back-to-back updates instead of creating a new one every time. The loopStatus will be updated to
// reflect the current status of the goroutine, i.e. it will initialized while the goroutine is being
// spun up, then running when the goroutine has loaded the SubscriberSets.
func (v *WatchableValue[T]) startNotificationLoop() {
	if v.loopStatus != notRunning {
		return
	}

	v.loopStatus = initialized
	v.loopWg.Add(1)

	go func() {
		defer v.loopWg.Done()
		for {
			v.lock.Lock()
			value := v.readWithMetadataNoLock()

			var subscriberIterators [subscriptionTypes]SubscriberSetIterator[T]
			for i := range subscriptionTypes {
				subscriberIterators[i], v.lastSeenSubscriberSetVersions[i] = v.SubscriberSets[i].Iterator()
			}

			v.loopStatus = running
			v.lock.Unlock()

			if v.globCollection != nil && value.resource != nil {
				// To ensure proper ordering of notifications for subscribers, it's important to notify the
				// collection of created resources _before_ looping through the subscribers, and to notify it of
				// deleted resources _after_.
				v.globCollection.resourceSet(v.name)
			}

			if !v.notifySubscribers(value, subscriberIterators) {
				// If notifySubscribers returns false, the value changed during the loop. Immediately reload the
				// value and try again, reusing the goroutine.
				continue
			}

			if v.globCollection != nil && value.resource == nil {
				v.globCollection.resourceCleared(v.name)
			}

			v.lock.Lock()
			done := v.valuesFromDifferentPrioritySources[v.currentIndex] == value.resource
			if done {
				// At this point, the most recent value was successfully pushed to all subscribers since it has not
				// changed from when it was initially read at the top of the loop. Since the lock is currently held,
				// setting loopRunning to false will signal to the next invocation of startNotificationLoop that the
				// loop routine is not running.
				v.loopStatus = notRunning
			}
			// Otherwise, if done isn't true then the value changed in between notifying the subscribers and
			// grabbing the lock. In this case the loop will restart, reusing the goroutine.
			v.lock.Unlock()

			if done {
				return
			}
		}
	}()
}

// notifySubscribers is invoked by WatchableValue.startNotificationLoop with the desired update. It
// returns true if all the subscribers were notified, or false if the loop exited early because the
// value changed during the iteration.
func (v *WatchableValue[T]) notifySubscribers(
	value valueWithMetadata[T],
	iterators [subscriptionTypes]SubscriberSetIterator[T],
) bool {
	for handler, subscribedAt := range v.iterateSubscribers(iterators) {
		// If the value has changed while looping over the subscribers, stop the loop and try again with the
		// latest value. This avoids doing duplicate/wasted work since subscribers only care about the latest
		// version of the value and don't mind missing intermediate values.
		if v.Read() != value.resource {
			return false
		}

		handler.Notify(v.name, value.resource, value.subscriptionMetadata(subscribedAt))
	}
	return true
}

// iterateSubscribers returns a SubscriberSetIterator that iterates over all the unique subscribers
// to this value. This means if a subscriber is subscribed to this value both with a wildcard and an
// explicit subscription, the returned sequence will only yield the handler once.
//
// Author's note: there is a known race condition in this function if a subscriber is present in more
// than one iterator. The loop iterates through the given iterators in order, and looks back at
// previous iterators to check whether a given SubscriptionHandler has already been notified with
// SubscriberSet.IsSubscribed. Suppose the following sequence:
//  1. A SubscriptionHandler "foo" is present both as an explicit and wildcard subscriber to this
//     value.
//  2. iterateSubscribers completes the iteration over the explicit subscribers, yielding foo once.
//  3. foo unsubscribes from the explicit SubscriberSet as iterateSubscribers begins iterating over the
//     wildcard SubscriberSet. iterateSubscribers encounters foo in the wildcard SubscriberSet and checks
//     whether foo is present in the explicit SubscriberSet (which it has already iterated over), sees
//     that foo is not explicitly subscribed, and yields foo again since it believes it is the first time
//     foo is encountered.
//
// Such a scenario can lead to foo being notified of the same update twice, which is undesirable but
// acceptable. Note however that the sequence of subscriptions "subscribe explicit" -> "subscribe
// wildcard" -> "unsubscribe explicit" does not actually exist in the standard Envoy flow, meaning
// this edge case is extremely unlikely. This is because iterateSubscribers always iterates through
// the subscriber sets in the same order: ExplicitSubscription -> GlobSubscription ->
// WildcardSubscription. The [known flow] of "subscribe wildcard" -> "subscribe explicit" ->
// "unsubscribe wildcard" is therefore completely unaffected by this and behaves as expected where
// foo is only yielded once.
//
// Possible alternatives to this iteration approach were considered:
//
// # Reverse checking order
//
// Instead of checking whether a SubscriptionHandler was already yielded by looking at *previous*
// SubscriberSets, iterateSubscribers could look at the *upcoming* iterators and determine whether a
// handler will eventually be yielded by a subsequent iterator, and skip yielding the handler from
// this iterator. While this addresses the edge case above, it symmetrically opens a significantly
// worse edge case in the reverse scenario:
//  1. A SubscriptionHandler "foo" is present both as an explicit and wildcard subscriber to this
//     value.
//  2. iterateSubscribers completes the iteration over the explicit subscribers, skipping foo since it
//     sees that it is currently present in the wildcard SubscriberSet.
//  3. foo unsubscribes from the wildcard SubscriberSet as iterateSubscribers begins iterating over the
//     wildcard SubscriberSet. foo is never encountered again, meaning it will never be notified of the
//     resource update.
//
// This edge case means updates will altogether be dropped rather than delivered twice, which is
// unacceptable. Not only that, unlike the current iteration approach, the [known flow] is likely to
// trigger this edge, making this approach completely non-viable.
//
// # Running set
//
// The obvious way to iterate through the union of subscribers would be to simply use a set to keep
// track of all the SubscriptionHandlers that have already been yielded. This however defeats the
// very point of the SubscriberSet data structure in the first place, which *avoids* storing state
// for each subscriber. It would create memory pressure as each invocation of iterateSubscribers
// would create a new map which would get promptly discarded once the invocation exits. The memory
// pressure could be addressed by leveraging a sync.Pool to allow reusing these sets, but it would
// only introduce complexity and overhead in an already complex loop. As such, this was ultimately
// deemed not necessary as the current solution will only duplicate notifications
// unknown/non-standard subscription flows.
//
// [known flow]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return
func (v *WatchableValue[T]) iterateSubscribers(
	iterators [subscriptionTypes]SubscriberSetIterator[T],
) SubscriberSetIterator[T] {
	return func(yield func(ads.SubscriptionHandler[T], time.Time) bool) {
	subscriberLoop:
		for i, subscribers := range iterators {
			for handler, subscribedAt := range subscribers {
				// If this handler was already yielded once from a previous iterator, skip it.
				for j := i - 1; j >= 0; j-- {
					if v.SubscriberSets[j].IsSubscribed(handler) {
						continue subscriberLoop
					}
				}

				if !yield(handler, subscribedAt) {
					break
				}
			}
		}
	}
}
