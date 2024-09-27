package internal

import (
	"context"
	"sync"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	serverstats "github.com/linkedin/diderot/stats/server"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

// BatchSubscriptionHandler is an extension of the SubscriptionHandler interface in the root package
// which allows a handler to be notified that a batch of calls to Notify is about to be received
// (StartNotificationBatch). The batch of notifications should not be sent to the client until all
// notifications for that batch have been received (EndNotificationBatch). Start and End will never
// be invoked out of order, i.e. there will never be a call to EndNotificationBatch without a call to
// StartNotificationBatch immediately preceding it. However, SubscriptionHandler.Notify can be
// invoked at any point.
type BatchSubscriptionHandler interface {
	StartNotificationBatch()
	ads.RawSubscriptionHandler
	EndNotificationBatch()
}

type entry struct {
	Resource *ads.RawResource
	metadata ads.SubscriptionMetadata
}

func newHandler(
	ctx context.Context,
	granularLimiter handlerLimiter,
	globalLimiter handlerLimiter,
	statsHandler serverstats.Handler,
	ignoreDeletes bool,
	send func(entries map[string]entry) error,
) *handler {
	h := &handler{
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

var entryMapPool = sync.Pool{New: func() any {
	return make(map[string]entry)
}}

// handler implements the BatchSubscriptionHandler interface using a backing map to aggregate updates
// as they come in, and flushing them out, according to when the limiter permits it.
type handler struct {
	granularLimiter handlerLimiter
	globalLimiter   handlerLimiter
	statsHandler    serverstats.Handler
	lock            sync.Mutex
	ctx             context.Context
	ignoreDeletes   bool
	send            func(entries map[string]entry) error

	entries map[string]entry

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
}

// swapEntries grabs the lock then swaps the entries map to a nil map. It resets notificationReceived
// and immediateNotificationReceived, and returns original entries map that was swapped.
func (h *handler) swapEntries() map[string]entry {
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
		if err := h.send(entries); err != nil {
			return
		}

		// Return the used map to the pool after clearing it.
		clear(entries)
		entryMapPool.Put(entries)
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

	if h.statsHandler != nil {
		h.statsHandler.HandleServerEvent(h.ctx, &serverstats.ResourceQueued{
			ResourceName:   name,
			Resource:       r,
			Metadata:       metadata,
			ResourceExists: !metadata.CachedAt.IsZero(),
		})
	}

	if r == nil && h.ignoreDeletes {
		return
	}

	if h.entries == nil {
		h.entries = entryMapPool.Get().(map[string]entry)
	}

	h.entries[name] = entry{
		Resource: r,
		metadata: metadata,
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

func (h *handler) StartNotificationBatch() {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.batchStarted = true
}

func (h *handler) EndNotificationBatch() {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.batchStarted = false

	if len(h.entries) > 0 {
		h.immediateNotificationReceived.notify()
		h.notificationReceived.notify()
	}
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
	typeUrl string,
	send func(res *ads.SotWDiscoveryResponse) error,
) *handler {
	isPseudoDeltaSotW := utils.IsPseudoDeltaSotW(typeUrl)
	var looper func(resources map[string]entry) error
	if isPseudoDeltaSotW {
		looper = func(entries map[string]entry) error {
			versions := map[string]string{}

			for name, e := range entries {
				versions[name] = e.Resource.Version
			}

			res := &ads.SotWDiscoveryResponse{
				TypeUrl: typeUrl,
				Nonce:   utils.NewNonce(0),
			}
			for _, e := range entries {
				res.Resources = append(res.Resources, e.Resource.Resource)
			}
			res.VersionInfo = utils.MapToProto(versions)
			return send(res)
		}
	} else {
		allResources := map[string]entry{}
		versions := map[string]string{}

		looper = func(resources map[string]entry) error {
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
				TypeUrl: typeUrl,
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
		granularLimiter,
		globalLimiter,
		statsHandler,
		isPseudoDeltaSotW,
		looper,
	)
}
