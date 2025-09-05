package diderot

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sync"
	"time"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/linkedin/diderot/ads"
	internal "github.com/linkedin/diderot/internal/client"
	"github.com/linkedin/diderot/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ADSClientOption func(*options)

const (
	defaultInitialReconnectBackoff   = 100 * time.Millisecond
	defaultMaxReconnectBackoff       = 2 * time.Minute
	defaultResponseChunkingSupported = true
)

// NewADSClient creates a new [*ADSClient] with the given options. To stop the client, close the
// backing [grpc.ClientConn].
func NewADSClient(conn grpc.ClientConnInterface, node *ads.Node, opts ...ADSClientOption) *ADSClient {
	c := &ADSClient{
		conn:            conn,
		node:            node,
		newSubscription: make(chan struct{}, 1),
		handlers:        make(map[string]internal.RawResourceHandler),
		options: options{
			initialReconnectBackoff:   defaultInitialReconnectBackoff,
			maxReconnectBackoff:       defaultMaxReconnectBackoff,
			responseChunkingSupported: defaultResponseChunkingSupported,
		},
	}

	for _, opt := range opts {
		opt(&c.options)
	}

	go c.loop()

	return c
}

type options struct {
	initialReconnectBackoff   time.Duration
	maxReconnectBackoff       time.Duration
	responseChunkingSupported bool
}

// WithReconnectBackoff provides backoff configuration when reconnecting to the xDS backend after a
// connection failure. The default settings are 100ms and 2m for the initial and max backoff
// respectively.
func WithReconnectBackoff(initialBackoff, maxBackoff time.Duration) ADSClientOption {
	return func(o *options) {
		o.initialReconnectBackoff = initialBackoff
		o.maxReconnectBackoff = maxBackoff
	}
}

// WithResponseChunkingSupported changes whether response chunking should be supported (see
// [ads.ParseRemainingChunksFromNonce] for additional details). This feature is only provided by the
// [ADSServer] implemented in this package. This enabled by default.
func WithResponseChunkingSupported(supported bool) ADSClientOption {
	return func(o *options) {
		o.responseChunkingSupported = supported
	}
}

// An ADSClient is a client that implements the xDS protocol, and can therefore be used to talk to
// any xDS backend. Use the [Watch], [WatchGlob] and [WatchWildcard] to subscribe to resources.
type ADSClient struct {
	options
	node *ads.Node
	conn grpc.ClientConnInterface

	newSubscription chan struct{}

	lock     sync.Mutex
	handlers map[string]internal.RawResourceHandler
}

// A Watcher is used to receive updates from the xDS backend using an [ADSClient]. It is passed into
// the various [Watch] methods in this package. Note that it is imperative that implementations be
// hashable as it will be stored as the key to a map (unhashable types include slices and functions).
type Watcher[T proto.Message] interface {
	// Notify is invoked whenever a response is processed. The given sequence will iterate over all the
	// resources in the response, with a nil resource indicating a deletion. Implementations should
	// return an error if any resource is invalid, and this error will be propagated as a NACK to the xDS
	// backend.
	Notify(resources iter.Seq2[string, *ads.Resource[T]]) error
}

// Watch registers the given watcher in the given client, triggering a subscription (if necessary)
// for the given resource name such that the [Watcher] will be notified whenever the resource is
// updated. If a resource is already known (for example from a previous existing subscription), the
// watcher will be immediately notified. Glob or wildcard subscriptions are supported, and
// [Watcher.Notify] will be invoked with a sequence that iterates over all the updated resources.
func Watch[T proto.Message](c *ADSClient, name string, watcher Watcher[T]) {
	if getResourceHandler[T](c).AddWatcher(name, watcher) {
		c.notifyNewSubscription()
	}
}

// Watch is the equivalent of the top-level [Watch] function, except that it can be used to watch
// resources without knowing the hard type [T] at runtime. Useful when writing type-agnostic code.
func (c *ADSClient) Watch(t Type, name string, watcher Watcher[proto.Message]) {
	t.watch(c, name, watcher)
}

// getResourceHandler gets or initializes the [internal.ResourceHandler] for the specified type in
// the given client.
func getResourceHandler[T proto.Message](c *ADSClient) *internal.ResourceHandler[T] {
	c.lock.Lock()
	defer c.lock.Unlock()

	typeURL := utils.GetTypeURL[T]()
	if hAny, ok := c.handlers[typeURL]; !ok {
		h := internal.NewResourceHandler[T]()
		c.handlers[typeURL] = h
		return h
	} else {
		return hAny.(*internal.ResourceHandler[T])
	}
}

func (c *ADSClient) getResourceHandler(typeURL string) (internal.RawResourceHandler, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	h, ok := c.handlers[typeURL]
	return h, ok
}

// notifyNewSubscription signals to the subscription loop that a new subscription was added.
func (c *ADSClient) notifyNewSubscription() {
	select {
	case c.newSubscription <- struct{}{}:
	default:
	}
}

// This is a type alias for the set of resources the client is subscribed to. The key is the typeURL
// and the value is the set of resource names subscribed to within that type.
type subscriptionSet map[string]utils.Set[string]

// getPendingSubscriptions iterates over all the subscriptions returned by invoking
// [internal.ResourceHandler.AllSubscriptions] on all registered resource handlers, and compares it
// against the given set of already registered subscriptions. If any are missing, they are added to
// the returned subscription set after being added to the given set. This means that repeated
// invocations of this method will return an empty set if no new subscriptions are added in between.
func (c *ADSClient) getPendingSubscriptions(registeredSubscriptions subscriptionSet) subscriptionSet {
	c.lock.Lock()
	defer c.lock.Unlock()

	pendingSubscriptions := make(subscriptionSet)
	add := func(typeURL string, name string) {
		registered := internal.GetNestedMap(registeredSubscriptions, typeURL)
		if !registered.Contains(name) {
			registered.Add(name)
			internal.GetNestedMap(pendingSubscriptions, typeURL).Add(name)
		}
	}

	for t, handler := range c.handlers {
		for k := range handler.AllSubscriptions() {
			add(t, k)
		}
	}

	return pendingSubscriptions
}

// loop simply calls newStream and subscriptionLoop forever, until the underlying gRPC connection is
// closed.
func (c *ADSClient) loop() {
	for {
		// See documentation on subscriptionLoop. It returns when the stream ends, so a fresh stream needs to
		// be created every time.
		stream, responses, err := c.newStream()
		if err != nil {
			return
		}

		err = c.subscriptionLoop(stream, responses)
		slog.WarnContext(stream.Context(), "Restarting ADS stream", "err", err)
	}
}

// subscriptionLoop is the critical logic loop for the client. It polls the given responses channel,
// notifying watchers when new responses come in. Each slice returned by the responses channel is
// expected to contain responses that are all for the same typeURL. In most cases, the slice will
// only have one response in it, but if response chunking is supported, the slice will have all the
// response chunks in it. It also waits for any new subscriptions to be registered, and sends them to
// the server. This returns whenever the stream ends.
func (c *ADSClient) subscriptionLoop(stream deltaClient, responsesCh <-chan []*ads.DeltaDiscoveryResponse) error {
	registeredSubscriptions := make(subscriptionSet)

	sendPendingSubscriptions := func() error {
		pending := c.getPendingSubscriptions(registeredSubscriptions)
		if len(pending) == 0 {
			return nil
		}

		slog.InfoContext(stream.Context(), "Subscribing to resources", "subscriptions", pending)
		for t, subs := range pending {
			err := stream.Send(&ads.DeltaDiscoveryRequest{
				Node:                   c.node,
				TypeUrl:                t,
				ResourceNamesSubscribe: slices.Collect(subs.Values()),
			})
			if err != nil {
				return err
			}
		}
		return nil
	}

	isFirst := true
	for {
		err := sendPendingSubscriptions()
		if err != nil {
			return err
		}

		select {
		case <-c.newSubscription:
			err := sendPendingSubscriptions()
			if err != nil {
				return err
			}
		case responses := <-responsesCh:
			h, ok := c.getResourceHandler(responses[0].TypeUrl)
			if !ok {
				for _, res := range responses {
					err := c.sendACKOrNACK(
						stream,
						res,
						fmt.Errorf("received response with unknown type: %q", res.TypeUrl),
					)
					if err != nil {
						slog.WarnContext(stream.Context(), "ADS stream closed", "err", err)
						return err
					}
				}
				continue
			}

			// Always ACK all but the last response. Errors will only be reported back to the server once all
			// chunks are processed.
			for _, res := range responses[:len(responses)-1] {
				err := c.sendACKOrNACK(stream, res, nil)
				if err != nil {
					return err
				}
			}

			handlerErr := h.HandleResponses(isFirst, responses)
			isFirst = false
			if err = c.sendACKOrNACK(stream, responses[len(responses)-1], handlerErr); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// sendACKOrNACK will send an ACK or NACK (depending on the given error) for the given response.
func (c *ADSClient) sendACKOrNACK(stream deltaClient, res *ads.DeltaDiscoveryResponse, err error) error {
	req := &ads.DeltaDiscoveryRequest{
		Node:          c.node,
		TypeUrl:       res.TypeUrl,
		ResponseNonce: res.Nonce,
	}
	if err != nil {
		req.ErrorDetail = status.New(codes.InvalidArgument, err.Error()).Proto()
		slog.WarnContext(stream.Context(), "NACKing response", "res", res, "err", err)
	} else {
		slog.DebugContext(stream.Context(), "ACKing response", "res", res)
	}
	return stream.Send(req)
}

// newStream acquires a fresh stream from getDeltaClient and kicks off a goroutine that will read all
// responses from the stream, writing them to the returned channel. The goroutine will exit when the
// stream ends.
func (c *ADSClient) newStream() (deltaClient, <-chan []*ads.DeltaDiscoveryResponse, error) {
	stream, err := c.getDeltaClient()
	if err != nil {
		return nil, nil, err
	}

	responses := make(chan []*ads.DeltaDiscoveryResponse)
	go func() {
		chunkedResponses := make(map[string][]*ads.DeltaDiscoveryResponse)

		for {
			res, err := stream.Recv()
			if err != nil {
				slog.WarnContext(stream.Context(), "ADS stream closed", "err", err)
				return
			}

			slog.Debug("Response received", "res", res)

			var resSlice []*ads.DeltaDiscoveryResponse

			if c.responseChunkingSupported {
				resSlice = chunkedResponses[res.TypeUrl]
				resSlice = append(resSlice, res)
				chunkedResponses[res.TypeUrl] = resSlice
				if remainingChunks, _ := ads.ParseRemainingChunksFromNonce(res.Nonce); remainingChunks != 0 {
					continue
				} else {
					delete(chunkedResponses, res.TypeUrl)
				}
			} else {
				resSlice = []*ads.DeltaDiscoveryResponse{res}
			}

			select {
			case responses <- resSlice:
			case <-stream.Context().Done():
				slog.WarnContext(stream.Context(), "ADS stream closed", "err", stream.Context().Err())
				return
			}
		}
	}()

	return stream, responses, nil
}

type deltaClient interface {
	Send(*ads.DeltaDiscoveryRequest) error
	Recv() (*ads.DeltaDiscoveryResponse, error)
	Context() context.Context
}

// getDeltaClient attempts to reconnect to the ADS Server until it either successfully establishes a
// stream, or the underlying gRPC connection is explicitly closed, signaling a shutdown.
func (c *ADSClient) getDeltaClient() (deltaClient, error) {
	backoff := c.initialReconnectBackoff
	for {
		delta, err := discoveryv3.NewAggregatedDiscoveryServiceClient(c.conn).
			DeltaAggregatedResources(context.Background())
		if err != nil {
			// This only occurs if c.conn was closed since context.Background() is used to create the stream.
			if st := status.Convert(err); st.Code() == codes.Canceled {
				return nil, err
			}

			slog.Warn("Failed to create Delta stream, retrying", "backoff", backoff, "err", err)
			time.Sleep(backoff)
			backoff = min(backoff*2, c.maxReconnectBackoff)
			continue
		}
		return delta, nil
	}
}
