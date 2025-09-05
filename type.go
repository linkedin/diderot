package diderot

import (
	"fmt"
	"iter"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	"google.golang.org/protobuf/proto"
)

// TypeOf returns a TypeReference that corresponds to the type parameter.
func TypeOf[T proto.Message]() TypeReference[T] {
	return typeReference[T](utils.GetTypeURL[T]())
}

// typeReference is the only implementation of the Type and, by extension, the TypeReference
// interface. It is not exposed publicly to ensure that all instances are generated through TypeOf,
// which uses reflection on the type parameter to determine the type URL. This is to avoid potential
// runtime complications due to invalid type URL strings.
type typeReference[T proto.Message] string

// TypeReference is a superset of the Type interface which captures the actual runtime type.
type TypeReference[T proto.Message] interface {
	Type
}

// Type is a type reference for a type that can be cached. Only accessible through TypeOf.
type Type interface {
	// URL returns the type URL for this Type.
	URL() string
	// TrimmedURL returns the type URL for this Type without the leading "types.googleapis.com/" prefix.
	// This string is useful when constructing xdstp URLs.
	TrimmedURL() string
	// NewCache is the untyped equivalent of this package's NewCache. The returned RawCache still retains
	// the runtime type information and can be safely cast to the corresponding Cache type with
	// [UnwrapRawCache].
	NewCache() RawCache
	// NewPrioritizedCache is the untyped equivalent of this package's NewPrioritizedCache. The returned
	// RawCache instances can be safely cast to the corresponding Cache type with [UnwrapRawCache].
	NewPrioritizedCache(prioritySlots int) []RawCache

	watch(c *ADSClient, name string, watcher Watcher[proto.Message])
}

func (t typeReference[T]) URL() string {
	return string(t)
}

func (t typeReference[T]) TrimmedURL() string {
	return utils.TrimTypeURL(t.URL())
}

func (t typeReference[T]) NewCache() RawCache {
	return rawCache[T]{NewCache[T]()}
}

func (t typeReference[T]) NewPrioritizedCache(prioritySlots int) []RawCache {
	caches := NewPrioritizedCache[T](prioritySlots)
	out := make([]RawCache, len(caches))
	for i, c := range caches {
		out[i] = rawCache[T]{c}
	}
	return out
}

type wrappedWatcher[T proto.Message] struct {
	Watcher[proto.Message]
}

func (r wrappedWatcher[T]) Notify(resources iter.Seq2[string, *ads.Resource[T]]) error {
	return r.Watcher.Notify(func(yield func(string, *ads.Resource[proto.Message]) bool) {
		for name, resource := range resources {
			if !yield(name, &ads.Resource[proto.Message]{
				Name:         resource.Name,
				Version:      resource.Version,
				Resource:     resource.Resource,
				Ttl:          resource.Ttl,
				CacheControl: resource.CacheControl,
				Metadata:     resource.Metadata,
			}) {
				return
			}
		}
	})
}

func (t typeReference[T]) watch(c *ADSClient, name string, watcher Watcher[proto.Message]) {
	Watch[T](c, name, wrappedWatcher[T]{watcher})
}

// A RawCache is an alternate API for a [Cache]. It allows untyped operations against the underlying
// cache and offers the same set of operations as the typed interface. This is useful when the hard
// type [T] is not known at runtime. Can only be created via [ToRawCache].
type RawCache interface {
	// Type returns the corresponding [Type] for this cache.
	Type() Type
	// EntryNames returns an [iter.Seq] that will iterate over all the current entry names in the cache.
	EntryNames() iter.Seq[string]
	// EstimateSubscriptionSize estimates the number of resources targeted by the given list of
	// subscriptions. This is only an estimation since the resource count is dynamic, and repeated
	// invocations of this function with the same parameters may not yield the same results.
	EstimateSubscriptionSize(resourceNamesSubscribe []string) int
	// Subscribe registers the handler as a subscriber of the given named resource by invoking the
	// underlying generic API [diderot.Cache.Subscribe].
	Subscribe(name string, handler ads.RawSubscriptionHandler)
	// Unsubscribe unregisters the handler as a subscriber of the given named resource by invoking the
	// underlying generic API [diderot.Cache.Unsubscribe].
	Unsubscribe(name string, handler ads.RawSubscriptionHandler)
	// IsSubscribedTo checks whether the given handler is subscribed to the given named resource by invoking
	// the underlying generic API [diderot.Cache.IsSubscribedTo].
	IsSubscribedTo(name string, handler ads.RawSubscriptionHandler) bool
	// Get is the untyped equivalent of [Cache.Get]. There are uses for this method, but the preferred
	// way is to use [Cache.Get] because this function incurs the cost of marshaling the resource.
	// Returns an error if the resource cannot be marshaled.
	Get(name string) (*ads.RawResource, error)
	// Set is the untyped equivalent of [Cache.Set]. There are uses for this method, but the preferred
	// way is to use [Cache.Set] since it offers a typed API instead of the untyped [ads.RawResource]
	// parameter which can return an error. Subscribers will be notified of the new version of this
	// resource. See [Cache.Set] for additional details on how the resources are stored. Returns an error
	// if the given resource's type URL does not match the expected type URL, or the resource cannot be
	// unmarshaled.
	Set(r *ads.RawResource, modifiedAt time.Time) error
	// Clear clears the entry (if present) and notifies all subscribers that the entry has been deleted.
	// A zero [time.Time] can be used to represent that the time at which the resource was cleared is
	// unknown (or ignored). For example, when watching a directory, the filesystem does not keep track
	// of when the file was deleted.
	Clear(name string, clearedAt time.Time)

	private()
}

type rawCache[T proto.Message] struct {
	Cache[T]
}

func ToRawCache[T proto.Message](c Cache[T]) RawCache {
	return rawCache[T]{c}
}

type wrappedHandler[T proto.Message] struct {
	ads.RawSubscriptionHandler
}

func (w wrappedHandler[T]) Notify(name string, r *ads.Resource[T], metadata ads.SubscriptionMetadata) {
	var raw *ads.RawResource
	if r != nil {
		var err error
		raw, err = r.Marshal()
		if err != nil {
			w.RawSubscriptionHandler.ResourceMarshalError(name, r.Resource, err)
			return
		}
	}
	w.RawSubscriptionHandler.Notify(name, raw, metadata)
}

// toGenericHandler wraps the given RawSubscriptionHandler into a typed SubscriptionHandler. Multiple
// invocations of this function with the same RawSubscriptionHandler always return a semantically
// equivalent value, meaning it's possible to do the following, without needing to explicitly store
// and reuse the returned SubscriptionHandler:
//
//	var c Cache[*ads.Endpoint]
//	var rawHandler RawSubscriptionHandler
//	c.Subscribe("foo", toGenericHandler[*ads.Endpoint](rawHandler))
//	c.Unsubscribe("foo", toGenericHandler[*ads.Endpoint](rawHandler))
func (c rawCache[T]) toGenericHandler(raw ads.RawSubscriptionHandler) ads.SubscriptionHandler[T] {
	return wrappedHandler[T]{raw}
}

func (c rawCache[T]) Subscribe(name string, handler ads.RawSubscriptionHandler) {
	c.Cache.Subscribe(name, c.toGenericHandler(handler))
}

func (c rawCache[T]) Unsubscribe(name string, handler ads.RawSubscriptionHandler) {
	c.Cache.Unsubscribe(name, c.toGenericHandler(handler))
}

func (c rawCache[T]) IsSubscribedTo(name string, handler ads.RawSubscriptionHandler) bool {
	return c.Cache.IsSubscribedTo(name, c.toGenericHandler(handler))
}

func (c rawCache[T]) Get(name string) (*ads.RawResource, error) {
	r := c.Cache.Get(name)
	if r == nil {
		return nil, nil
	}
	return r.Marshal()
}

func (c rawCache[T]) Set(raw *ads.RawResource, modifiedAt time.Time) error {
	// Ensure that the given resource's type URL is correct.
	if u := raw.GetResource().GetTypeUrl(); u != c.Cache.Type().URL() {
		return fmt.Errorf("diderot: invalid type URL, expected %q got %q", c.Cache.Type(), u)
	}

	r, err := ads.UnmarshalRawResource[T](raw)
	if err != nil {
		return err
	}

	c.Cache.SetResource(r, modifiedAt)

	return nil
}

func (c rawCache[T]) private() {}

// UnwrapRawCache returns the underlying [Cache] for the given [RawCache] if its type is [T],
// otherwise it returns nil, false.
func UnwrapRawCache[T proto.Message](raw RawCache) (Cache[T], bool) {
	c, ok := raw.(rawCache[T])
	if !ok {
		return nil, false
	}
	return c.Cache, true
}

// MustUnwrapRawCache is the equivalent of [UnwrapCache], except that it panics if the given
// [RawCache]'s type is not [T].
func MustUnwrapRawCache[T proto.Message](raw RawCache) Cache[T] {
	c, ok := UnwrapRawCache[T](raw)
	if !ok {
		panic("RawCache was for type " + raw.Type().URL() + " instead of " + TypeOf[T]().URL())
	}
	return c
}
