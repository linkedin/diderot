package diderot

import (
	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	"google.golang.org/protobuf/proto"
)

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
	// NewCache is the untyped equivalent of this package's NewCache. The returned RawCache still
	// retains the runtime type information and can be safely cast to the corresponding Cache type.
	NewCache() RawCache
	// NewPrioritizedCache is the untyped equivalent of this package's NewPrioritizedCache. The returned
	// RawCache instances can be safely cast to the corresponding Cache type.
	NewPrioritizedCache(prioritySlots int) []RawCache

	isSubscribedTo(c RawCache, name string, handler ads.RawSubscriptionHandler) bool
	subscribe(c RawCache, name string, handler ads.RawSubscriptionHandler)
	unsubscribe(c RawCache, name string, handler ads.RawSubscriptionHandler)
}

func (t typeReference[T]) URL() string {
	return string(t)
}

func (t typeReference[T]) TrimmedURL() string {
	return utils.TrimTypeURL(t.URL())
}

func (t typeReference[T]) NewCache() RawCache {
	return NewCache[T]()
}

func (t typeReference[T]) NewPrioritizedCache(prioritySlots int) []RawCache {
	caches := NewPrioritizedCache[T](prioritySlots)
	out := make([]RawCache, len(caches))
	for i, c := range caches {
		out[i] = c
	}
	return out
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
//	c.Subscribe("foo", ToGenericHandler[*ads.Endpoint](rawHandler))
//	c.Unsubscribe("foo", ToGenericHandler[*ads.Endpoint](rawHandler))
func (t typeReference[T]) toGenericHandler(raw ads.RawSubscriptionHandler) ads.SubscriptionHandler[T] {
	return wrappedHandler[T]{raw}
}

func (t typeReference[T]) isSubscribedTo(c RawCache, name string, handler ads.RawSubscriptionHandler) bool {
	return c.(Cache[T]).IsSubscribedTo(name, t.toGenericHandler(handler))
}

func (t typeReference[T]) subscribe(c RawCache, name string, handler ads.RawSubscriptionHandler) {
	c.(Cache[T]).Subscribe(name, t.toGenericHandler(handler))
}

func (t typeReference[T]) unsubscribe(c RawCache, name string, handler ads.RawSubscriptionHandler) {
	c.(Cache[T]).Unsubscribe(name, t.toGenericHandler(handler))
}

// TypeOf returns a TypeReference that corresponds to the type parameter.
func TypeOf[T proto.Message]() TypeReference[T] {
	return typeReference[T](utils.GetTypeURL[T]())
}

// IsSubscribedTo checks whether the given handler is subscribed to the given named resource by invoking
// the underlying generic API [diderot.Cache.IsSubscribedTo].
func IsSubscribedTo(c RawCache, name string, handler ads.RawSubscriptionHandler) bool {
	return c.Type().isSubscribedTo(c, name, handler)
}

// Subscribe registers the handler as a subscriber of the given named resource by invoking the
// underlying generic API [diderot.Cache.Subscribe].
func Subscribe(c RawCache, name string, handler ads.RawSubscriptionHandler) {
	c.Type().subscribe(c, name, handler)
}

// Unsubscribe unregisters the handler as a subscriber of the given named resource by invoking the
// underlying generic API [diderot.Cache.Unsubscribe].
func Unsubscribe(c RawCache, name string, handler ads.RawSubscriptionHandler) {
	c.Type().unsubscribe(c, name, handler)
}
