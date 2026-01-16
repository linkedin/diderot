package diderot

import (
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/linkedin/diderot/ads"
	internal "github.com/linkedin/diderot/internal/cache"
	"google.golang.org/protobuf/proto"
)

// Cache is the primary type provided by this package. It provides an efficient storage mechanism for
// ads.RawResource objects, and the means to subscribe to them via the SubscriptionHandler interface.
// For example, it can be used to store the set of "envoy.config.listener.v3.Listener" available to
// clients.
type Cache[T proto.Message] interface {
	RawCache
	// Set stores the given resource in the cache. If the resource name corresponds to a resource URN, it
	// will also be stored in the corresponding glob collection (see [TP1 proposal] for additional
	// details on the format). See Subscribe for more details on how the resources added by this method
	// can be subscribed to. Invoking Set whenever possible is preferred to RawCache.SetRaw, since it can
	// return an error if the given resource's type does not match the expected type while Set validates
	// at compile time that the given value matches the desired type. A zero [time.Time] can be used to
	// represent that the time at which the resource was created or modified is unknown (or ignored).
	//
	// WARNING: It is imperative that the Resource and the underlying [proto.Message] not be modified
	// after insertion! This resource will be read by subscribers to the cache and callers of Get, and
	// modifying the resource may at best result in incorrect reads for consumers and at worst panics if
	// the consumer is reading a map as it's being modified. When in doubt, callers should pass in a deep
	// copy of the resource. Note that the cache takes no responsibility in enforcing this since cloning
	// every resource as it is inserted in the cache may incur unexpected and avoidable costs.
	//
	// [TP1 proposal]: https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#uri-based-xds-resource-names
	Set(name, version string, t T, modifiedAt time.Time) *ads.Resource[T]
	// SetResource is the more verbose equivalent of Set which supports the additional fields in [ads.Resource].
	SetResource(r *ads.Resource[T], modifiedAt time.Time)
	// Get fetches the entry, or nil if it's not present and/or has been deleted.
	Get(name string) *ads.Resource[T]
	// IsSubscribedTo checks whether the given handler is subscribed to the given named entry.
	IsSubscribedTo(name string, handler ads.SubscriptionHandler[T]) bool
	// Subscribe registers the handler as a subscriber of the given named resource. The handler is always
	// immediately called with the current values of the entries selected by this call, even if it was
	// already subscribed.
	//
	// If the name is ads.WildcardSubscription, the handler is registered as a wildcard subscriber. This
	// means the handler will be subscribed to all existing entries, and be automatically subscribed to
	// any new entries until a corresponding call to Unsubscribe is made.
	//
	// If the name is a glob collection URL, the handler will be subscribed to all entries in the
	// collection, along with being automatically subscribed to any new entries. If the collection is
	// empty, the handler will receive a deletion notification for the entire collection. This behavior
	// is defined in the [TP1 proposal]:
	//	If no resources are present in the glob collection, the server should reply with a
	//	DeltaDiscoveryResponse in which the glob collection URL is specified in removed_resources.
	// The subscription will be preserved even if the glob collection is empty (or becomes empty) until a
	// corresponding call to Unsubscribe is made.
	//
	// Otherwise, the handler will be subscribed to the resource specified by the given name and receive
	// notifications any time the resource changes. If a resource by that name does not exist, the
	// handler will immediately receive a deletion notification, but will not be unsubscribed until a
	// corresponding call to Unsubscribe is made. See the [spec on deletions] for more details.
	//
	// Note that there are therefore three ways to subscribe to a given resource:
	//  1. The simplest way is to explicitly subscribe to a resource, via its name. Such a subscription is
	//     it can only be cancelled with a corresponding call to Unsubscribe. It will not, for example, be
	//     cancelled by unsubscribing from the wildcard. This is by design, as it allows clients to discover
	//     resources by emitting a wildcard subscription, finding which resources they are interested in,
	//     explicitly subscribing to those then removing the implicit subscriptions to other resources by
	//     unsubscribing from the wildcard. This is outlined in the [sample xDS flows].
	//  2. If the resource's name is a URN, a subscription to the matching glob collection URL will
	//     subscribe the given handler to the resource. Similar to the explicit subscription listed in 1.,
	//     unsubscribing from the wildcard will not cancel a glob collection to a resource, only a
	//     corresponding unsubscription to the collection will cancel it.
	//  3. A wildcard subscription will also implicitly create a subscription to the resource.
	//     subscribe
	//
	// Note that while the xDS docs are clear on what the behavior should be when a subscription is
	// "upgraded" from a wildcard subscription to an explicit subscription, they are not clear as to what
	// happens when a subscription is "downgraded". For example, if a client subscribes to a resource "A"
	// then subscribes to the wildcard, should an unsubscription from the wildcard cancel the
	// subscription to "A"? Similarly, the docs are unclear as to what should happen if a client
	// subscribes to the wildcard, then subscribes to resource "A", then unsubscribes from "A". Should
	// the original implicit subscription to "A" via the wildcard be honored? To address both of these,
	// the cache will preserve all subscriptions that target a specific resource. This means a client that
	// subscribed to a resource both via a wildcard and an explicit subscription (regardless of order) will
	// only be unsubscribed from that resource once it has both explicitly unsubscribed from the resource and
	// unsubscribed from the wildcard (regardless of order).
	//
	// It is unsafe for multiple goroutines to invoke Subscribe and/or Unsubscribe with the same
	// SubscriptionHandler, and will result undefined behavior.
	//
	// [TP1 proposal]: https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#glob
	// [sample xDS flows]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return
	// [spec on deletions]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#id2
	Subscribe(name string, handler ads.SubscriptionHandler[T])
	// Unsubscribe removes the given handler from the named entry's list of subscribers.
	//
	// If the given name is ads.WildcardSubscription, the handler is unsubscribed from all entries it did
	// not explicitly subscribe to (see definition of explicit subscription in Subscribe).
	//
	// If the given name is a glob collection URL, it is unsubscribed from the collection, unsubscribing
	// it from all matching entries.
	//
	// Noop if the resource does not exist or the handler was not subscribed to it.
	Unsubscribe(name string, handler ads.SubscriptionHandler[T])
}

// RawCache is a subset of the [Cache] interface and provides a number of methods to interact with
// the [Cache] without needing to know the underlying resource type at compile time. All RawCache
// implementations *must* also implement [Cache] for the underlying resource type.
type RawCache interface {
	// Type returns the corresponding [Type] for this cache.
	Type() Type
	// EntryNames returns an [iter.Seq] that will iterate over all the current entry names in the cache.
	EntryNames() iter.Seq[string]
	// GetRaw is the untyped equivalent of Cache.Get. There are uses for this method, but the preferred
	// way is to use Cache.Get because this function incurs the cost of marshaling the resource. Returns
	// an error if the resource cannot be marshaled.
	GetRaw(name string) (*ads.RawResource, error)
	// SetRaw is the untyped equivalent of Cache.Set. There are uses for this method, but the preferred
	// way is to use Cache.Set since it offers a typed API instead of the untyped ads.RawResource parameter.
	// Subscribers will be notified of the new version of this resource. See Cache.Set for additional
	// details on how the resources are stored. Returns an error if the given resource's type URL does
	// not match the expected type URL, or the resource cannot be unmarshaled.
	SetRaw(r *ads.RawResource, modifiedAt time.Time) error
	// Clear clears the entry (if present) and notifies all subscribers that the entry has been deleted.
	// A zero [time.Time] can be used to represent that the time at which the resource was cleared is
	// unknown (or ignored). For example, when watching a directory, the filesystem does not keep track
	// of when the file was deleted.
	Clear(name string, clearedAt time.Time)
	// EstimateSubscriptionSize estimates the number of resources targeted by the given list of
	// subscriptions. This is only an estimation since the resource count is dynamic, and repeated
	// invocations of this function with the same parameters may not yield the same results.
	EstimateSubscriptionSize(resourceNamesSubscribe []string) int
}

// NewCache returns a simple Cache with only 1 priority (see NewPrioritizedCache).
func NewCache[T proto.Message]() Cache[T] {
	return NewPrioritizedCache[T](1)[0]
}

// NewPrioritizedCache creates a series of Cache accessors that all point to the same underlying
// cache, but have different "priorities". The Cache object that appears first in the returned slice
// has the highest priority, with every subsequent Cache having correspondingly lower priority. If
// the same resource is provided by two Caches, the resource defined by the Cache with the highest
// priority will be provided to subscribers and returned by Cache.GetResource. Conversely, if a Cache
// with a high priority clears a resource, the underlying cache will fall back to lower priority
// definitions if present. A resource is only fully cleared if it is cleared at all priority levels.
//
// Concretely, this feature is intended to be used when a resource definition can come from multiple
// sources. For example, if resource definitions are being migrated from one source to another, it
// would be sane to always use the new source if it is present, otherwise fall back to the old
// source. This would be as opposed to simply picking whichever source defined the resource most
// recently, as it would mean the resource definition is nondeterministic.
func NewPrioritizedCache[T proto.Message](prioritySlots int) []Cache[T] {
	c := newCache[T](prioritySlots)
	caches := make([]Cache[T], prioritySlots)
	for i := range caches {
		caches[i] = newCacheWithPriority[T](c, internal.Priority(i))
	}

	return caches
}

func newCache[T proto.Message](prioritySlots int) *cache[T] {
	ref := TypeOf[T]()
	return &cache[T]{
		typeReference:   ref,
		prioritySlots:   prioritySlots,
		resources:       internal.NewResourceMap[string, *internal.WatchableValue[T]](),
		globCollections: internal.NewGlobCollectionsMap[T](),
	}
}

// A cache implements a data structure that allows storing and subscribing to xDS objects. It's expected that there will
// be tens of thousands of cache readers each subscribed to hundreds of resources, making the cache particularly
// read-heavy. Under such load, it is preferable to have more work occur on each write to alleviate the work that needs
// to be done on each read since one write can, at worst, be multiplied into hundreds of thousands of reads. As such,
// the cache is based on a subscription model (via cache.Subscribe) that minimizes reader overhead. Instead of reading
// from the backing map every time, subscribers subscribe directly to updates on the backing value. Writers call
// cache.Set to notify all active subscribers.
type cache[T proto.Message] struct {
	// This is the type of each resource in this cache. Set and SetResource guarantee that all insertions
	// in this cache satisfy this invariant.
	typeReference TypeReference[T]
	// This resourceMap maps the resource's name to its corresponding WatchableValue.
	resources *internal.ResourceMap[string, *internal.WatchableValue[T]]
	// The number of slots watchableValue instances should be created with (see NewPrioritizedCache for
	// details on the cache priority).
	prioritySlots int
	// The set of wildcard subscribers that should be automatically subscribed to any new entries.
	wildcardSubscribers internal.SubscriberSet[T]
	// This secondary data structure is updated any time a resource that belongs to a glob collection is
	// added or removed from the map. Resources belong to glob collections if their name is a xdstp URN
	// (see ExtractGlobCollectionURLFromResourceURN).
	globCollections *internal.GlobCollectionsMap[T]
}

func (c *cache[T]) Type() Type {
	return c.typeReference
}

func (c *cache[T]) IsSubscribedTo(name string, handler ads.SubscriptionHandler[T]) (subscribed bool) {
	if c.wildcardSubscribers.IsSubscribed(handler) {
		return true
	}

	if gcURL, err := ads.ParseGlobCollectionURL[T](name); err == nil {
		return c.globCollections.IsSubscribed(gcURL, handler)
	}

	c.resources.ComputeIfPresent(name, func(value *internal.WatchableValue[T]) {
		subscribed = value.IsSubscribed(handler)
	})

	return subscribed
}

func (c *cache[T]) Subscribe(name string, handler ads.SubscriptionHandler[T]) {
	// More details about this can be found in the documentation of WatchableValue. But the short version
	// of it is that because Subscribe needs to not return until all notifications are delivered, and
	// subscribing to a WatchableValue returns a WaitGroup if the loop goroutine will deliver the
	// notification instead of this goroutine. To respect the Subscribe contract, wait for all the
	// returned WaitGroups. Crucially however, because the various Compute methods on the resource map
	// hold locks, waiting on the WaitGroups must be done *outside* of the lambdas passed to those
	// methods. Hence, to avoid accidentally blocking anything, wait for the WaitGroups in a deferred
	// statement.
	var waitGroups []*sync.WaitGroup
	appendWg := func(wg *sync.WaitGroup) {
		// Only add non-nil WaitGroups to the slice to avoid nil-pointer panics, but primarily to avoid
		// allocating a large slice of mostly nil WaitGroups since it is rare for WatchableValue.Subscribe to
		// actually return a non-nil WaitGroup.
		if wg != nil {
			waitGroups = append(waitGroups, wg)
		}
	}
	defer func() {
		for _, wg := range waitGroups {
			wg.Wait()
		}
	}()

	if name == ads.WildcardSubscription {
		subscribedAt, version := c.wildcardSubscribers.Subscribe(handler)

		for name := range c.EntryNames() {
			// Cannot call c.Subscribe here because it always creates a backing watchableValue if it does not
			// already exist. For wildcard subscriptions, if the entry doesn't exist (or in this case has been
			// deleted), a subscription isn't necessary. If the entry reappears, it will be automatically
			// subscribed to.
			c.resources.ComputeIfPresent(name, func(value *internal.WatchableValue[T]) {
				appendWg(value.NotifyHandlerAfterSubscription(
					handler,
					internal.WildcardSubscription,
					subscribedAt,
					version,
				))
			})
		}
	} else if gcURL, err := ads.ParseGlobCollectionURL[T](name); err == nil {
		waitGroups = c.globCollections.Subscribe(gcURL, handler)
	} else {
		c.createOrModifyEntry(name, func(v *internal.WatchableValue[T]) {
			appendWg(v.Subscribe(handler))
		})
	}
}

func (c *cache[T]) EstimateSubscriptionSize(resourceNamesSubscribe []string) (resources int) {
	for _, name := range resourceNamesSubscribe {
		if name == ads.WildcardSubscription {
			// If the subscription includes a wildcard, it will target all resources, so immediately return the
			// size of the cache.
			return c.resources.Size()
		} else if gcURL, err := ads.ParseGlobCollectionURL[T](name); err == nil {
			resources += c.globCollections.Size(gcURL)
		} else {
			resources++
		}
	}
	return resources
}

// parseGlobCollectionURN checks if the given name is a valid glob collection URN. Note: by
// definition, a URN is not a URL! Therefore, if the name ends with /*, this function will return an
// error. This should be used when setting or clearing individual members of a glob collection, as it
// is meaningless to "set" an entire glob collection. Similarly, clearing an entire glob collection
// by calling [RawCache.Clear] with the corresponding glob collection URL is not supported, and is
// effectively a noop.
func parseGlobCollectionURN[T proto.Message](name string) (ads.GlobCollectionURL, error) {
	gcURL, resource, err := ads.ParseGlobCollectionURN[T](name)
	if err != nil || resource == ads.WildcardSubscription {
		return ads.GlobCollectionURL{}, ads.ErrInvalidGlobCollectionURI
	}
	return gcURL, nil
}

// createOrModifyEntry executes the given function on the value of that name after ensuring that it exists in the map.
func (c *cache[T]) createOrModifyEntry(name string, f func(v *internal.WatchableValue[T])) {
	c.resources.Compute(
		name,
		func(name string) *internal.WatchableValue[T] {
			v := internal.NewValue[T](name, c.prioritySlots)
			v.SubscriberSets[internal.WildcardSubscription] = &c.wildcardSubscribers

			if gcURL, err := parseGlobCollectionURN[T](name); err == nil {
				c.globCollections.PutValueInCollection(gcURL, v)
			}

			return v
		},
		f,
	)
}

// deleteAfterOpIfNilAndNotSubscribed applies the given op to the entry in the map (if it exists)
// while holding the lock on the entry (this is done via [internal.ResourceMap.DeleteIf], which
// prevents other operations on that entry). If, as a result of the op, the entry is now nil, has no
// subscribers, and the backing [internal.WatchableValue]'s notification loop is not running, it is
// deleted. If it is not nil or has an explicit subscriber, it is not deleted, and if the
// notification loop is running, the deletion is queued to be executed by the goroutine currently
// executing the notification loop.
func (c *cache[T]) deleteAfterOpIfNilAndNotSubscribed(name string, op func(v *internal.WatchableValue[T])) {
	c.resources.ComputeDeletion(name, func(v *internal.WatchableValue[T]) bool {
		if op != nil {
			op(v)
		}

		subs := v.SubscriberSets[internal.ExplicitSubscription]
		if !subs.IsEmpty() {
			return false
		}

		// At this point, because the lock is held, no new *explicit* subscribers will be added. However, a
		// notification loop can still be running due to glob or wildcard subscribers. In this case, the
		// deletion should only be attempted once the notification loop stops. DeleteNowOrQueueDeletion
		// checks whether the value is empty and the notification loop is not running and returns true if the
		// entry can be deleted. Otherwise, it schedules re-invoking this function after the notification
		// loop ends. Deleting the entry without checking whether the notification loop is running can result
		// in multiple WatchableValues created for the same resource, and therefore multiple, competing
		// notification loops which can result in non-deterministic behavior.
		if !v.DeleteNowOrQueueDeletion(func(name string) { c.deleteAfterOpIfNilAndNotSubscribed(name, nil) }) {
			return false
		}

		if gcURL, err := parseGlobCollectionURN[T](name); err == nil {
			c.globCollections.RemoveValueFromCollection(gcURL, v)
		}
		return true
	})
}

// unsubscribe implements actually unsubscribing the given handler from the value of that name (if it exists). If
// onlyIfWildcard is true, the handler will only be unsubscribed if its subscription is denoted as a wildcard
// subscription in the backing watchableValue (see Cache.DisableWildcardSubscription for more details on why this
// exists)
func (c *cache[T]) unsubscribe(name string, handler ads.SubscriptionHandler[T]) {
	c.deleteAfterOpIfNilAndNotSubscribed(name, func(v *internal.WatchableValue[T]) {
		v.Unsubscribe(handler)
	})
}

func (c *cache[T]) Unsubscribe(name string, handler ads.SubscriptionHandler[T]) {
	if name == ads.WildcardSubscription {
		c.wildcardSubscribers.Unsubscribe(handler)
	} else if gcURL, err := ads.ParseGlobCollectionURL[T](name); err == nil {
		c.globCollections.Unsubscribe(gcURL, handler)
	} else {
		c.unsubscribe(name, handler)
	}
}

func (c *cache[T]) Get(name string) (r *ads.Resource[T]) {
	c.resources.ComputeIfPresent(name, func(value *internal.WatchableValue[T]) {
		r = value.Read()
	})
	return r
}

func (c *cache[T]) GetRaw(name string) (*ads.RawResource, error) {
	r := c.Get(name)
	if r == nil {
		return nil, nil
	}
	return r.Marshal()
}

func (c *cache[T]) EntryNames() iter.Seq[string] {
	return func(yield func(string) bool) {
		c.resources.Range()(func(k string, v *internal.WatchableValue[T]) bool {
			if v.Read() == nil {
				return true
			}
			return yield(k)
		})
	}
}

// AllEntryNames unlike its cousin EntryNames returns the names of the entries that are present in
// the cache even if they are nil. It is used exclusively in testing, and is effectively private, as
// it is not exposed by the [Cache] interface. It cannot however be literally private (i.e. lowercase
// allEntryNames) because the tests are executed from a different package.
func (c *cache[T]) AllEntryNames() iter.Seq[string] {
	return func(yield func(string) bool) {
		c.resources.Range()(func(k string, _ *internal.WatchableValue[T]) bool {
			return yield(k)
		})
	}
}

var _ Cache[proto.Message] = (*cacheWithPriority[proto.Message])(nil)

func newCacheWithPriority[T proto.Message](c *cache[T], p internal.Priority) *cacheWithPriority[T] {
	return &cacheWithPriority[T]{cache: c, p: p}
}

// cacheWithPriority holds a reference to an underlying cache along with a specific priority index.
// It is the only implementation of Cache. Whenever the SetEntry, SetResource or ClearEntry methods
// are invoked, it invokes the respective watchableValue.set or watchableValue.clear methods with the
// priority index. This way, each source gets its own Cache reference that has a built-in priority
// index, instead of being required to explicitly specify the index, which is error-prone and could
// lead to unexpected behavior.
type cacheWithPriority[T proto.Message] struct {
	*cache[T]
	p internal.Priority
}

func (c *cacheWithPriority[T]) Clear(name string, clearedAt time.Time) {
	c.deleteAfterOpIfNilAndNotSubscribed(name, func(v *internal.WatchableValue[T]) {
		v.Clear(c.p, clearedAt)
	})
}

func (c *cacheWithPriority[T]) Set(name, version string, t T, modifiedAt time.Time) *ads.Resource[T] {
	r := &ads.Resource[T]{
		Name:     name,
		Version:  version,
		Resource: t,
	}
	c.SetResource(r, modifiedAt)
	return r
}

func (c *cacheWithPriority[T]) SetResource(r *ads.Resource[T], modifiedAt time.Time) {
	c.createOrModifyEntry(r.Name, func(v *internal.WatchableValue[T]) {
		v.Set(c.p, r, modifiedAt)
	})
}

func (c *cacheWithPriority[T]) SetRaw(raw *ads.RawResource, modifiedAt time.Time) error {
	// Ensure that the given resource's type URL is correct.
	if u := raw.GetResource().GetTypeUrl(); u != c.typeReference.URL() {
		return fmt.Errorf("diderot: invalid type URL, expected %q got %q", c.typeReference, u)
	}

	r, err := ads.UnmarshalRawResource[T](raw)
	if err != nil {
		return err
	}

	c.SetResource(r, modifiedAt)

	return nil
}
