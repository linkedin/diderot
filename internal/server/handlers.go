package internal

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	serverstats "github.com/linkedin/diderot/stats/server"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

// SendBufferSizeEstimator is a copy of the interface in the root package, to avoid import cycles.
type SendBufferSizeEstimator interface {
	EstimateSubscriptionSize(streamCtx context.Context, typeURL string, resourceNamesSubscribe []string) int
}

// BatchSubscriptionHandler is an extension of the SubscriptionHandler interface in the root package
// which allows a handler to be notified that a batch of calls to Notify is about to be received
// (StartNotificationBatch). The batch of notifications should not be sent to the client until all
// notifications for that batch have been received (EndNotificationBatch). Start and End will never
// be invoked out of order, i.e. there will never be a call to EndNotificationBatch without a call to
// StartNotificationBatch immediately preceding it. However, SubscriptionHandler.Notify can be
// invoked at any point.
type BatchSubscriptionHandler interface {
	StartNotificationBatch(map[string]string, int)
	ads.RawSubscriptionHandler
	EndNotificationBatch()
}

// sendBuffer is an alias for the map type used by the handler to accumulate pending resource updates
// before sending them to the client.
type sendBuffer map[string]serverstats.SentResource

func newHandler(
	ctx context.Context,
	typeURL string,
	granularLimiter handlerLimiter,
	globalLimiter handlerLimiter,
	statsHandler serverstats.Handler,
	ignoreDeletes bool,
	send func(entries sendBuffer) error,
) *handler {
	h := &handler{
		typeURL:                       typeURL,
		granularLimiter:               granularLimiter,
		globalLimiter:                 globalLimiter,
		statsHandler:                  statsHandler,
		ctx:                           ctx,
		ignoreDeletes:                 ignoreDeletes,
		send:                          send,
		immediateNotificationReceived: newNotifyOnceChan(),
		notificationReceived:          newNotifyOnceChan(),
	}
	go h.loop()
	return h
}

func newNotifyOnceChan() notifyOnceChan {
	return make(chan struct{}, 1)
}

// notifyOnceChan is a resettable chan that only receives a notification once. It is exclusively
// meant to be used by handler. All methods should be invoked while holding the corresponding
// handler.lock.
type notifyOnceChan chan struct{}

// notify notifies the channel using a non-blocking send
func (ch notifyOnceChan) notify() {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// reset ensures the channel has no pending notifications in case they were never read (this can
// happen if a notification comes in after the granular rate limit clears but before the
// corresponding handler.lock is acquired).
func (ch notifyOnceChan) reset() {
	select {
	// clear the channel if it has a pending notification. This is required since
	// immediateNotificationReceived can be notified _after_ the granular limit clears. If it isn't
	// cleared during the reset, the loop will read from it and incorrectly detect an immediate
	// notification.
	case <-ch:
	// otherwise return immediately if the channel is empty
	default:
	}
}

var sendBufferPool = sync.Pool{New: func() any { return make(sendBuffer) }}

// handler implements the BatchSubscriptionHandler interface using a backing map to aggregate updates
// as they come in, and flushing them out, according to when the limiter permits it.
type handler struct {
	typeURL         string
	granularLimiter handlerLimiter
	globalLimiter   handlerLimiter
	statsHandler    serverstats.Handler
	lock            sync.Mutex
	ctx             context.Context
	ignoreDeletes   bool
	send            func(entries sendBuffer) error

	entries sendBuffer

	// The following notifyOnceChan instances are the signaling mechanism between loop and Notify. Calls
	// to Notify will first invoke notifyOnceChan.notify on immediateNotificationReceived based on the
	// contents of the subscription metadata, then call notify on notificationReceived. loop waits on the
	// channel that backs notificationReceived to be signaled and once the first notification is
	// received, waits for the global rate limit to clear. This allows updates to keep accumulating. It
	// then checks whether immediateNotificationReceived has been signaled, and if so skips the granular
	// rate limiter. Otherwise, it either waits for the granular rate limit to clear, or
	// immediateNotificationReceived to be signaled, whichever comes first. Only then does it invoke
	// swapEntries which resets notificationReceived, immediateNotificationReceived and entries to a
	// state where they can receive more notifications while, in the background, it invokes send with all
	// accumulated entries up to this point. Once send completes, it returns to waiting on
	// notificationReceived. All operations involving these channels will exit early if ctx is cancelled,
	// terminating the loop.
	immediateNotificationReceived notifyOnceChan
	notificationReceived          notifyOnceChan

	// If batchStarted is true, Notify will not notify notificationReceived. This allows the batch to
	// complete before the response is sent, minimizing the number of responses.
	batchStarted bool

	// initialResourceVersions is a map of resource names to their initial versions.
	// this informs the server of the versions of the resources the xDS client knows of.
	initialResourceVersions map[string]*initialResourceVersion
}

type initialResourceVersion struct {
	// initial version of the resource, which the xDS client has seen.
	version string
	// received flag indicates if the resource has been received from the server and skipped from the response
	// being sent. we are maintaining this flag to differentiate between the resource which is deleted on cache and
	// the resource which is not updated since client has last seen it.
	received bool
}

// swapEntries grabs the lock then swaps the entries map to a nil map. It resets notificationReceived
// and immediateNotificationReceived, and returns original entries map that was swapped.
func (h *handler) swapEntries() sendBuffer {
	h.lock.Lock()
	defer h.lock.Unlock()
	entries := h.entries
	h.entries = nil
	h.notificationReceived.reset()
	h.immediateNotificationReceived.reset()
	return entries
}

func (h *handler) loop() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.notificationReceived:
			// Always wait for the global rate limiter to clear
			if waitForGlobalLimiter(h.ctx, h.globalLimiter, h.statsHandler) != nil {
				return
			}
			// Wait for the granular rate limiter
			if h.waitForGranularLimiterOrShortCircuit() != nil {
				return
			}
		}

		entries := h.swapEntries()

		var start time.Time
		if h.statsHandler != nil {
			start = time.Now()
		}

		err := h.send(entries)

		if h.statsHandler != nil {
			h.statsHandler.HandleServerEvent(h.ctx, &serverstats.ResponseSent{
				TypeURL:   h.typeURL,
				Resources: entries,
				Duration:  time.Since(start),
			})
		}

		// Return the used map to the pool after clearing it.
		clear(entries)
		sendBufferPool.Put(entries)

		if err != nil {
			return
		}
	}
}

func waitForGlobalLimiter(
	ctx context.Context,
	globalLimiter handlerLimiter,
	statsHandler serverstats.Handler,
) error {
	if statsHandler != nil {
		start := time.Now()
		defer func() {
			statsHandler.HandleServerEvent(ctx, &serverstats.TimeInGlobalRateLimiter{Duration: time.Since(start)})
		}()
	}

	reservation, cancel := globalLimiter.reserve()
	defer cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-reservation:
		return nil
	}
}

// waitForGranularLimiterOrShortCircuit will acquire a reservation from granularLimiter and wait on
// it, but will short circuit the reservation if an immediate notification is received (or if ctx is
// canceled).
func (h *handler) waitForGranularLimiterOrShortCircuit() error {
	reservation, cancel := h.granularLimiter.reserve()
	defer cancel()

	for {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case <-h.immediateNotificationReceived:
			// If an immediate notification is received, immediately return instead of waiting for the granular
			// limit. Without this, a bootstrapping client may be forced to wait for the initial versions of the
			// resources it is interested in. The purpose of the rate limiter is to avoid overwhelming the
			// client, however if the client is healthy enough to request new resources then those resources
			// should be sent without delay. Do note, however, that the responses will still always be rate
			// limited by the global limiter.
			return nil
		case <-reservation:
			// Otherwise, wait for the granular rate limit to clear.
			return nil
		}
	}
}

func (h *handler) Notify(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if metadata.CachedAt.IsZero() && h.statsHandler != nil {
		h.statsHandler.HandleServerEvent(h.ctx, &serverstats.UnknownResourceRequested{
			TypeURL:      h.typeURL,
			ResourceName: name,
		})
	}

	if r == nil && h.ignoreDeletes {
		return
	}

	if h.entries == nil {
		h.entries = sendBufferPool.Get().(sendBuffer)
	}

	if h.handleMatchFromIRV(name, r) {
		return
	}

	h.entries[name] = serverstats.SentResource{
		Resource: r,
		Metadata: metadata,
		QueuedAt: time.Now(),
	}

	if r != nil && metadata.GlobCollectionURL != "" {
		// When a glob collection is empty, it is signaled to the client with a corresponding deletion of
		// that collection's name. For example, if a collection Foo/* becomes empty (or the client subscribed
		// to a collection that does not exist), it will receive a deletion notification for Foo/*. There is
		// an edge case in the following scenario: suppose a collection currently has some resource Foo/A in
		// it. Upon subscribing, the handler will be notified that the resource exists. Foo/A is then
		// removed, so the handler receives a notification that Foo/A is removed, and because Foo/* is empty
		// it also receives a corresponding notification. But, soon after, resource Foo/B is created,
		// reviving Foo/* and the handler receives the corresponding notification for Foo/B. At this point,
		// if the response were to be sent as-is, it would contain both the creation of Foo/B and the
		// deletion of Foo/*. Depending on the order in which the client processes the response's contents,
		// it may ignore Foo/B altogether. To avoid this, always clear out the deletion of Foo/* when a
		// notification for the creation of an entry within Foo/* is received.
		delete(h.entries, metadata.GlobCollectionURL)
	}

	if !h.batchStarted {
		h.notificationReceived.notify()
	}
}

func (h *handler) ResourceMarshalError(name string, resource proto.Message, err error) {
	if h.statsHandler != nil {
		h.statsHandler.HandleServerEvent(h.ctx, &serverstats.ResourceMarshalError{
			ResourceName: name,
			Resource:     resource,
			Err:          err,
		})
	}
}

func (h *handler) StartNotificationBatch(initialResourceVersions map[string]string, estimatedSize int) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if len(initialResourceVersions) > 0 {
		h.initialResourceVersions = make(map[string]*initialResourceVersion, len(initialResourceVersions))

		// setting the initial version of resources to filter out unchanged resources.
		for name, version := range initialResourceVersions {
			h.initialResourceVersions[name] = &initialResourceVersion{version: version}
		}
	} else if estimatedSize > 0 {
		// Only preallocate the send buffer if the initialResourceVersions is empty. Otherwise, it's very
		// likely that the buffer will be underutilized and waste resources.
		prevBuf := h.entries
		h.entries = make(sendBuffer, estimatedSize+len(prevBuf))

		if len(prevBuf) > 0 {
			// Is possible that some notifications were already pending, so ensure those are not lost before
			// returning the  to the pool.
			maps.Copy(h.entries, prevBuf)
			clear(prevBuf)
			sendBufferPool.Put(prevBuf)
		}
	}
	h.batchStarted = true
}

func (h *handler) EndNotificationBatch() {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.handleDeletionsFromIRV()
	h.batchStarted = false
	// Resetting the initial resource versions to nil, we need to make sure to handle IRV
	// for each incoming request independently
	h.initialResourceVersions = nil
	if len(h.entries) > 0 {
		h.immediateNotificationReceived.notify()
		h.notificationReceived.notify()
	}
}

// handleDeletionsFromIRV processes resources that are known to the client but are no longer present on the server.
// This indicates that the resource has been deleted and the client is unaware of this (e.g. in a re-connect scenario)
// The method update the entries to nil for such resources.
func (h *handler) handleDeletionsFromIRV() {
	for name, irv := range h.initialResourceVersions {
		if _, ok := h.entries[name]; !ok && !irv.received {
			slog.Debug("Resource no longer exists on the server but is still present on the client. "+
				"Explicitly marking the resource for deletion.", "resourceName", name)

			// in some corner case, when last resource is deleted. and there is no subscribed resource present in cache,
			// entries might be nil, so we need to allocate a new map.
			if h.entries == nil {
				h.entries = sendBufferPool.Get().(sendBuffer)
			}
			h.entries[name] = serverstats.SentResource{}
		}
	}
}

// handleMatchFromIRV checks if the given resource matches the initial resource versions (IRV).
func (h *handler) handleMatchFromIRV(name string, r *ads.RawResource) bool {
	if res, ok := h.initialResourceVersions[name]; ok {
		if r != nil && res.version == r.Version {
			slog.Debug(
				"Resource version matches with IRV from client, skipping this resource",
				"resourceName", name,
				"version", res.version,
			)
			res.received = true

			if h.statsHandler != nil {
				h.statsHandler.HandleServerEvent(h.ctx, &serverstats.IRVMatchedResource{
					ResourceName: name,
					Resource:     r,
				})
			}
			return true
		}
	}
	return false
}

func NewSotWHandler(
	ctx context.Context,
	granularLimiter *rate.Limiter,
	globalLimiter *rate.Limiter,
	statsHandler serverstats.Handler,
	typeURL string,
	send func(res *ads.SotWDiscoveryResponse) error,
) BatchSubscriptionHandler {
	return newSotWHandler(
		ctx,
		(*rateLimiterWrapper)(granularLimiter),
		(*rateLimiterWrapper)(globalLimiter),
		statsHandler,
		typeURL,
		send,
	)
}

func newSotWHandler(
	ctx context.Context,
	granularLimiter handlerLimiter,
	globalLimiter handlerLimiter,
	statsHandler serverstats.Handler,
	typeURL string,
	send func(res *ads.SotWDiscoveryResponse) error,
) *handler {
	isPseudoDeltaSotW := utils.IsPseudoDeltaSotW(typeURL)
	var looper func(resources sendBuffer) error
	if isPseudoDeltaSotW {
		looper = func(entries sendBuffer) error {
			versions := map[string]string{}

			for name, e := range entries {
				versions[name] = e.Resource.Version
			}

			res := &ads.SotWDiscoveryResponse{
				TypeUrl: typeURL,
				Nonce:   utils.NewNonce(0),
			}
			for _, e := range entries {
				res.Resources = append(res.Resources, e.Resource.Resource)
			}
			res.VersionInfo = utils.MapToProto(versions)
			return send(res)
		}
	} else {
		allResources := sendBuffer{}
		versions := map[string]string{}

		looper = func(resources sendBuffer) error {
			for name, r := range resources {
				if r.Resource != nil {
					allResources[name] = r
					versions[name] = r.Resource.Version
				} else {
					delete(allResources, name)
					delete(versions, name)
				}
			}

			res := &ads.SotWDiscoveryResponse{
				TypeUrl: typeURL,
				Nonce:   utils.NewNonce(0),
			}
			for _, r := range allResources {
				res.Resources = append(res.Resources, r.Resource.Resource)
			}
			res.VersionInfo = utils.MapToProto(versions)
			return send(res)
		}
	}

	return newHandler(
		ctx,
		typeURL,
		granularLimiter,
		globalLimiter,
		statsHandler,
		isPseudoDeltaSotW,
		looper,
	)
}
