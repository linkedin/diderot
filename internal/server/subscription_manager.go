package internal

import (
	"context"
	"slices"
	"sync"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	"google.golang.org/protobuf/proto"
)

// ResourceLocator is a copy of the interface in the root package, to avoid import cycles.
type ResourceLocator interface {
	Subscribe(
		streamCtx context.Context,
		typeURL, resourceName string,
		handler ads.RawSubscriptionHandler,
	) (unsubscribe func())
}

type SubscriptionManager[REQ proto.Message] interface {
	// ProcessSubscriptions handles subscribing/unsubscribing from the resources provided in the given
	// xDS request. This function will always invoke BatchSubscriptionHandler.StartNotificationBatch
	// before it starts processing the subscriptions and always complete with
	// BatchSubscriptionHandler.EndNotificationBatch. Since the cache implementation always notifies the
	// SubscriptionHandler with the current value of the subscribed resource,
	// BatchSubscriptionHandler.EndNotificationBatch will be invoked after the handler has been notified
	// of all the resources requested.
	ProcessSubscriptions(REQ)
	// IsSubscribedTo checks whether the client has subscribed to the given resource name.
	IsSubscribedTo(name string) bool
	// UnsubscribeAll cleans up any active subscriptions and disables the wildcard subscription if enabled.
	UnsubscribeAll()
}

// subscriptionManagerCore keeps track of incoming subscription and unsubscription requests, and
// executes the corresponding actions against the underlying cache. It is meant to be embedded in
// deltaSubscriptionManager and sotWSubscriptionManager to deduplicate the subscription tracking
// logic.
type subscriptionManagerCore struct {
	ctx           context.Context
	locator       ResourceLocator
	typeURL       string
	handler       BatchSubscriptionHandler
	sizeEstimator SendBufferSizeEstimator

	lock          sync.Mutex
	subscriptions map[string]func()
}

func newSubscriptionManagerCore(
	ctx context.Context,
	locator ResourceLocator,
	typeURL string,
	handler BatchSubscriptionHandler,
	sizeEstimator SendBufferSizeEstimator,
) *subscriptionManagerCore {
	c := &subscriptionManagerCore{
		ctx:           ctx,
		locator:       locator,
		typeURL:       typeURL,
		handler:       handler,
		sizeEstimator: sizeEstimator,
		subscriptions: make(map[string]func()),
	}
	// Ensure all the subscriptions managed by this subscription manager are cleaned up, otherwise they
	// will dangle forever in the cache and prevent the backing SubscriptionHandler from being collected
	// as well.
	context.AfterFunc(ctx, func() {
		c.UnsubscribeAll()
	})
	return c
}

type deltaSubscriptionManager struct {
	*subscriptionManagerCore
	firstCallReceived bool
}

// NewDeltaSubscriptionManager creates a new SubscriptionManager specifically designed to handle the
// Delta xDS protocol's subscription semantics.
func NewDeltaSubscriptionManager(
	ctx context.Context,
	locator ResourceLocator,
	typeURL string,
	handler BatchSubscriptionHandler,
	sizeEstimator SendBufferSizeEstimator,
) SubscriptionManager[*ads.DeltaDiscoveryRequest] {
	return &deltaSubscriptionManager{
		subscriptionManagerCore: newSubscriptionManagerCore(ctx, locator, typeURL, handler, sizeEstimator),
	}
}

type sotWSubscriptionManager struct {
	*subscriptionManagerCore
	receivedExplicitSubscriptions bool
}

// NewSotWSubscriptionManager creates a new SubscriptionManager specifically designed to handle the
// State-of-the-World xDS protocol's subscription semantics.
func NewSotWSubscriptionManager(
	ctx context.Context,
	locator ResourceLocator,
	typeURL string,
	handler BatchSubscriptionHandler,
	sizeEstimator SendBufferSizeEstimator,
) SubscriptionManager[*ads.SotWDiscoveryRequest] {
	return &sotWSubscriptionManager{
		subscriptionManagerCore: newSubscriptionManagerCore(ctx, locator, typeURL, handler, sizeEstimator),
	}
}

// ProcessSubscriptions processes the subscriptions for a delta stream. It manages the implicit
// wildcard subscription outlined in [the spec]. The server should default to the wildcard
// subscription if the client's first request does not provide any resource names to explicitly
// subscribe to. The client must then explicit unsubscribe from the wildcard. Subsequent requests
// that do not provide any explicit resource names will not alter the current subscription state.
//
// [the spec]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol.html#how-the-client-specifies-what-resources-to-return
func (m *deltaSubscriptionManager) ProcessSubscriptions(req *ads.DeltaDiscoveryRequest) {
	subscribe, estimatedSize := m.cleanSubscriptionsAndEstimateSize(
		req.ResourceNamesSubscribe, req.InitialResourceVersions,
	)

	m.handler.StartNotificationBatch(req.InitialResourceVersions, estimatedSize)
	defer m.handler.EndNotificationBatch()

	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.firstCallReceived {
		m.firstCallReceived = true
		if len(subscribe) == 0 {
			subscribe = []string{ads.WildcardSubscription}
		}
	}

	for _, name := range subscribe {
		m.subscribe(name)
	}

	for _, name := range req.ResourceNamesUnsubscribe {
		m.unsubscribe(name)
	}
}

// ProcessSubscriptions processes the subscriptions for a state of the world stream. It manages the
// implicit wildcard subscription outlined in [the spec]. The server should default to the wildcard
// subscription if the client has not sent any resource names to explicitly subscribe to. After the
// first request that provides explicit resource names, the implicit wildcard subscription should
// disappear.
//
// [the spec]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol.html#how-the-client-specifies-what-resources-to-return
func (m *sotWSubscriptionManager) ProcessSubscriptions(req *ads.SotWDiscoveryRequest) {
	subscribe, estimatedSize := m.cleanSubscriptionsAndEstimateSize(req.ResourceNames, nil)

	// sotWSubscriptionManager does not support initial resource versions, so we pass nil.
	m.handler.StartNotificationBatch(nil, estimatedSize)
	defer m.handler.EndNotificationBatch()

	m.lock.Lock()
	defer m.lock.Unlock()

	m.receivedExplicitSubscriptions = m.receivedExplicitSubscriptions || len(subscribe) != 0
	if !m.receivedExplicitSubscriptions {
		subscribe = []string{ads.WildcardSubscription}
	}

	intersection := utils.Set[string]{}
	for _, name := range subscribe {
		if _, ok := m.subscriptions[name]; ok {
			intersection.Add(name)
		}
	}

	for name := range m.subscriptions {
		if !intersection.Contains(name) {
			m.unsubscribe(name)
		}
	}

	for _, name := range subscribe {
		if !intersection.Contains(name) {
			m.subscribe(name)
		}
	}
}

func (c *subscriptionManagerCore) IsSubscribedTo(name string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	_, nameOk := c.subscriptions[name]
	_, wildcardOk := c.subscriptions[ads.WildcardSubscription]
	return nameOk || wildcardOk
}

func (c *subscriptionManagerCore) UnsubscribeAll() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for name := range c.subscriptions {
		c.unsubscribe(name)
	}
}

func (c *subscriptionManagerCore) subscribe(name string) {
	c.unsubscribe(name)
	c.subscriptions[name] = c.locator.Subscribe(c.ctx, c.typeURL, name, c.handler)
}

func (c *subscriptionManagerCore) unsubscribe(name string) {
	if unsub, ok := c.subscriptions[name]; ok {
		unsub()
		delete(c.subscriptions, name)
	}
}

// cleanSubscriptionsAndEstimateSize clones the given slice and removes duplicate elements by sorting
// it. This ensures that the server does not process the same subscription twice for the same
// request. It then estimates the size of send buffer to pass to
// [BatchSubscriptionHandler.StartNotificationBatch] if a [SendBufferSizeEstimator] was provided.
func (c *subscriptionManagerCore) cleanSubscriptionsAndEstimateSize(
	resourceNamesSubscribe []string,
	initialResourceVersions map[string]string,
) (cleaned []string, size int) {
	cleaned = slices.Clone(resourceNamesSubscribe)
	slices.Sort(cleaned)
	cleaned = slices.Compact(cleaned)
	if len(initialResourceVersions) == 0 && c.sizeEstimator != nil {
		size = c.sizeEstimator.EstimateSubscriptionSize(c.ctx, c.typeURL, cleaned)
	}
	return cleaned, size
}
