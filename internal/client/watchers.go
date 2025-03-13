package internal

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"sync"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	"google.golang.org/protobuf/proto"
)

// Watcher is a copy of the interface of the same name in the root package, to avoid import cycles.
type Watcher[T proto.Message] interface {
	Notify(resources iter.Seq2[string, *ads.Resource[T]]) error
}

// RawResourceHandler is a non-generic interface implemented by [ResourceHandler]. Used by the
// non-generic [github.com/linkedin/diderot.ADSClient].
type RawResourceHandler interface {
	// AllSubscriptions returns a sequence of all the subscriptions in this client.
	AllSubscriptions() iter.Seq[string]
	// HandleResponses should be called whenever responses are received. Accepts a slice of responses,
	// since the client may support chunking. The given boolean parameter indicates whether this is the
	// set of responses received for a stream. This is used to determine whether any resources were
	// deleted while the client was disconnected.
	HandleResponses(isFirst bool, responses []*ads.DeltaDiscoveryResponse) error
}

// Ensure that [ResourceHandler] implements the [RawResourceHandler] interface.
var _ RawResourceHandler = (*ResourceHandler[proto.Message])(nil)

// ResourceHandler implements the core logic of managing notifications for watchers.
type ResourceHandler[T proto.Message] struct {
	lock sync.Mutex

	// All the resources currently known by this client.
	resources map[string]*ads.Resource[T]

	// Maps resource name to watchers of the resource.
	subscriptions map[string]*subscription[T]
	// Maps glob collection URL to watchers of the collection.
	globSubscriptions map[ads.GlobCollectionURL]*globSubscription[T]
	// Contains the set of wildcard watchers, if present.
	wildcardSubscription *subscription[T]
}

// resolve returns an [iter.Seq2] that resolves the resources for the given sequence of resource
// names. The [ads.Resource] will be nil if no such resource is known.
func (h *ResourceHandler[T]) resolve(in iter.Seq[string]) iter.Seq2[string, *ads.Resource[T]] {
	return func(yield func(string, *ads.Resource[T]) bool) {
		for name := range in {
			if !yield(name, h.resources[name]) {
				return
			}
		}
	}
}

func (h *ResourceHandler[T]) resolveSingle(name string) iter.Seq2[string, *ads.Resource[T]] {
	return func(yield func(string, *ads.Resource[T]) bool) {
		yield(name, h.resources[name])
	}
}

// setResource updates the map of known resources. Returns a boolean indicating whether the resource
// was actually changed.
func (h *ResourceHandler[T]) setResource(name string, resource *ads.Resource[T]) (updated bool) {
	var previous *ads.Resource[T]
	previous, ok := h.resources[name]
	if !ok && resource == nil {
		// Ignore deletions for unknown resources
		return false
	}
	if ok && resource.Equals(previous) {
		// Ignore updates identical to the most recently seen resource.
		return false
	}

	if resource == nil {
		delete(h.resources, name)
	} else {
		h.resources[name] = resource
	}
	return true
}

func NewResourceHandler[T proto.Message]() *ResourceHandler[T] {
	return &ResourceHandler[T]{
		resources:         make(map[string]*ads.Resource[T]),
		subscriptions:     make(map[string]*subscription[T]),
		globSubscriptions: make(map[ads.GlobCollectionURL]*globSubscription[T]),
	}
}

// AddWatcher registers the given [Watcher] against the given resource name. The watcher will be
// notified whenever the resource is created, updated or deleted. The returned boolean indicates
// whether the watcher was a new registration, or was already previously registered. If a value for
// the given resource is already known, the watcher is immediately notified.
func (h *ResourceHandler[T]) AddWatcher(name string, w Watcher[T]) bool {
	h.lock.Lock()
	defer h.lock.Unlock()

	// contains the set of watchers to update
	var watchers utils.Set[Watcher[T]]
	// set if a value for the resource is already known.
	var resources iter.Seq2[string, *ads.Resource[T]]

	if name == ads.WildcardSubscription {
		if h.wildcardSubscription == nil {
			h.wildcardSubscription = newSubscription[T]()
		}
		watchers = h.wildcardSubscription.watchers
		if h.wildcardSubscription.initialized {
			resources = maps.All(h.resources)
		}
	} else if gcURL, err := ads.ParseGlobCollectionURL[T](name); err == nil {
		globSub, ok := h.globSubscriptions[gcURL]
		if !ok {
			globSub = &globSubscription[T]{
				subscription: *newSubscription[T](),
				entries:      make(utils.Set[string]),
			}
			h.globSubscriptions[gcURL] = globSub
		}
		watchers = globSub.watchers
		if globSub.initialized {
			resources = h.resolve(globSub.entries.Values())
		}
	} else {
		sub, ok := h.subscriptions[name]
		if !ok {
			sub = newSubscription[T]()
			h.subscriptions[name] = sub
		}
		// In the event that there is already data from another subscription for this specific resource,
		// immediately satisfy the watcher.
		_, sub.initialized = h.resources[name]
		watchers = sub.watchers
		if sub.initialized {
			resources = h.resolveSingle(name)
		}
	}

	if resources != nil {
		_ = w.Notify(resources)
	}

	return watchers.Add(w)
}

func (h *ResourceHandler[T]) AllSubscriptions() iter.Seq[string] {
	return func(yield func(string) bool) {
		h.lock.Lock()
		defer h.lock.Unlock()

		for k := range h.subscriptions {
			if !yield(k) {
				return
			}
		}
		for k := range h.globSubscriptions {
			if !yield(k.String()) {
				return
			}
		}
		if h.wildcardSubscription != nil {
			yield(ads.WildcardSubscription)
		}
	}
}

func (h *ResourceHandler[T]) HandleResponses(isFirst bool, responses []*ads.DeltaDiscoveryResponse) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	var errs []error
	addErr := func(err error) {
		errs = append(errs, err)
	}
	notifyWatchers := func(sub *subscription[T], seq iter.Seq2[string, *ads.Resource[T]]) {
		for w := range sub.watchers {
			err := w.Notify(seq)
			if err != nil {
				addErr(err)
			}
		}
	}

	totalAddedResources := 0
	totalDeletedResources := 0
	for _, response := range responses {
		totalAddedResources += len(response.Resources)
		totalDeletedResources += len(response.RemovedResources)
	}

	if totalAddedResources+totalDeletedResources == 0 {
		return fmt.Errorf("empty response")
	}

	// Contains the set of resource names that wildcard watchers should be notified of. Only set if any
	// wildcard watchers are registered.
	var wildcardUpdates utils.Set[string]
	if h.wildcardSubscription != nil {
		wildcardUpdates = make(utils.Set[string], totalAddedResources+totalDeletedResources)
	}
	// Contains the set of resource names received. Only set if this is the first set of responses for
	// the stream, as it is used to determine whether any resources were deleted while the client was
	// disconnected. For example, suppose resources foo and bar are present on the ADS server. If a
	// wildcard watcher is registered, it will initially receive updates for those two resources. Then
	// the client disconnects, reconnects and resubmits its wildcard subscription. If bar was deleted
	// during the disconnect, the server will only send back an update for foo, but never an explicit
	// deletion for bar. This set is therefore used to compare against h.resources, i.e. the set
	// known/previously received resources to see if wildcard and glob collection watchers need to be
	// notified of any deletions.
	var receivedResources utils.Set[string]
	if isFirst {
		receivedResources = make(utils.Set[string], totalAddedResources)
	}

	globUpdates := make(map[*globSubscription[T]]utils.Set[string])

	for name, r := range iterateResources(responses) {
		sub := h.subscriptions[name]

		var globSub *globSubscription[T]
		gcURL, gcResourceName, err := ads.ParseGlobCollectionURN[T](name)
		if err == nil {
			globSub = h.globSubscriptions[gcURL]
		}

		if sub == nil && globSub == nil && h.wildcardSubscription == nil {
			addErr(fmt.Errorf("not subscribed to resource %q", name))
			continue
		}

		var resource *ads.Resource[T]
		if r != nil {
			resource, err = ads.UnmarshalRawResource[T](r)
			if err != nil {
				addErr(err)
				continue
			}
			if isFirst {
				receivedResources.Add(name)
			}
		}

		updated := h.setResource(name, resource)

		if sub != nil && (!sub.initialized || updated) {
			sub.initialized = true
			notifyWatchers(sub, h.resolve(func(yield func(string) bool) { yield(name) }))
		}

		if globSub != nil {
			updates := GetNestedMap(globUpdates, globSub)
			if resource == nil && gcResourceName == ads.WildcardSubscription {
				maps.Copy(updates, globSub.entries)
				clear(globSub.entries)
				continue
			} else if !globSub.initialized || updated {
				updates.Add(name)
				if resource != nil {
					globSub.entries.Add(name)
				} else {
					globSub.entries.Remove(name)
				}
			}
		}

		if h.wildcardSubscription != nil && (!h.wildcardSubscription.initialized || updated) {
			wildcardUpdates.Add(name)
		}
	}

	if isFirst {
		for name := range h.resources {
			if _, ok := receivedResources[name]; !ok {
				delete(h.resources, name)
				if h.wildcardSubscription != nil {
					wildcardUpdates.Add(name)
				}
			}
		}
	}

	if h.wildcardSubscription != nil {
		h.wildcardSubscription.initialized = true

		if len(wildcardUpdates) > 0 {
			notifyWatchers(h.wildcardSubscription, h.resolve(wildcardUpdates.Values()))
		}
	}

	for globSub, updates := range globUpdates {
		globSub.initialized = true
		if len(updates) > 0 {
			notifyWatchers(&globSub.subscription, h.resolve(updates.Values()))
		}
	}

	return errors.Join(errs...)
}

// iterateResources returns an [iter.Seq2] that iterates over all the resources in the given
// response. If the [ads.RawResource] is nil, the resource is being deleted.
func iterateResources(responses []*ads.DeltaDiscoveryResponse) iter.Seq2[string, *ads.RawResource] {
	return func(yield func(string, *ads.RawResource) bool) {
		for _, res := range responses {
			for _, r := range res.Resources {
				if !yield(r.Name, r) {
					return
				}
			}
			for _, name := range res.RemovedResources {
				if !yield(name, nil) {
					return
				}
			}
		}
	}
}

func newSubscription[T proto.Message]() *subscription[T] {
	return &subscription[T]{
		watchers: make(utils.Set[Watcher[T]]),
	}
}

type subscription[T proto.Message] struct {
	initialized bool
	watchers    utils.Set[Watcher[T]]
}

type globSubscription[T proto.Message] struct {
	subscription[T]
	entries utils.Set[string]
}

// GetNestedMap is a utility function for nested maps. It will create the map at the given key if it
// does not already exist, then returns the corresponding map.
func GetNestedMap[K1, K2 comparable, V any, M ~map[K2]V](m map[K1]M, k K1) M {
	v, ok := m[k]
	if !ok {
		v = make(M)
		m[k] = v
	}
	return v
}
