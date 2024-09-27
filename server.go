package diderot

import (
	"context"
	"log/slog"
	"sync"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/linkedin/diderot/ads"
	internal "github.com/linkedin/diderot/internal/server"
	"github.com/linkedin/diderot/internal/utils"
	serverstats "github.com/linkedin/diderot/stats/server"
	"golang.org/x/time/rate"
	grpcStatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

var _ ads.Server = (*ADSServer)(nil)

// An ADSServer is an implementation of the xDS protocol. It implements the tricky parts of an xDS
// control plane such as managing subscriptions, parsing the incoming [ads.SotWDiscoveryRequest] and
// [ads.DeltaDiscoveryRequest], etc. The actual business logic of locating the resources is injected
// via the given ResoureLocator.
type ADSServer struct {
	discovery.UnimplementedAggregatedDiscoveryServiceServer

	locator ResourceLocator

	requestLimiter       *rate.Limiter
	globalLimiter        *rate.Limiter
	statsHandler         serverstats.Handler
	maxDeltaResponseSize int
	controlPlane         *corev3.ControlPlane

	granularLimitLock sync.Mutex
	granularLimit     rate.Limit
	granularLimiters  utils.Set[*rate.Limiter]
}

// NewADSServer creates a new [*ADSServer] with the given options.
func NewADSServer(locator ResourceLocator, options ...ADSServerOption) *ADSServer {
	s := &ADSServer{
		locator: locator,

		requestLimiter:   rate.NewLimiter(rate.Inf, 1),
		globalLimiter:    rate.NewLimiter(rate.Inf, 1),
		granularLimit:    rate.Inf,
		granularLimiters: utils.NewSet[*rate.Limiter](),
	}
	for _, opt := range options {
		opt.apply(s)
	}

	return s
}

// ADSServerOption configures how the ADS Server is initialized.
type ADSServerOption interface {
	apply(s *ADSServer)
}

type serverOption func(s *ADSServer)

func (f serverOption) apply(s *ADSServer) {
	f(s)
}

// defaultLimit interprets the given limit according to the documentation outlined in the various
// WithLimit options, i.e. if the given limit is negative, 0 or rate.Inf, it returns rate.Inf which,
// if given to a rate.Limiter disables the rate limiting.
func defaultLimit(limit rate.Limit) rate.Limit {
	if limit <= 0 {
		return rate.Inf
	}
	return limit
}

// WithRequestRateLimit sets the rate limiting parameters for client requests. When a client's
// request is being limited, it will block all other requests for that client until the rate limiting
// expires. If not specified, 0 or rate.Inf is provided, this feature is disabled.
func WithRequestRateLimit(limit rate.Limit) ADSServerOption {
	return serverOption(func(s *ADSServer) {
		s.SetRequestRateLimit(limit)
	})
}

// WithGlobalResponseRateLimit enforces a maximum rate at which the server will respond to clients.
// This prevents clients from being overloaded with responses and throttles the resource consumption
// on the server. If not specified, 0 or rate.Inf is provided, this feature is disabled.
func WithGlobalResponseRateLimit(globalLimit rate.Limit) ADSServerOption {
	return serverOption(func(s *ADSServer) {
		s.SetGlobalResponseRateLimit(globalLimit)
	})
}

// WithGranularResponseRateLimit is an additional layer of rate limiting to the one provided by
// WithGlobalResponseRateLimit. If specified, it will be applied to each resource type requested by
// each client. For example, a client can receive updates to its LDS, RDS, CDS and EDS subscriptions
// at a rate of 10 responses per second per type, for a potential maximum rate of 40 responses per
// second since it is subscribed to 4 individual types. When determining how long a response should
// be stalled however, the server computes the wait time required to satisfy both limits and picks
// the largest one. This means this granular limit cannot override the global limit. If not
// specified, this feature is disabled.
func WithGranularResponseRateLimit(granularLimit rate.Limit) ADSServerOption {
	return serverOption(func(s *ADSServer) {
		s.SetGranularResponseRateLimit(granularLimit)
	})
}

// WithServerStatsHandler registers a stats handler for the server. The given handler will be invoked
// whenever a corresponding event happens. See the [stats] package for more details.
func WithServerStatsHandler(statsHandler serverstats.Handler) ADSServerOption {
	return serverOption(func(s *ADSServer) {
		s.statsHandler = statsHandler
	})
}

// WithMaxDeltaResponseSize limits the size of responses sent by the server when the Delta variant of the xDS protocol
// is being used. As it builds the response from the set of resource updates it wants to send, the server will check
// how large the serialized message will be, stopping before it reaches the threshold. It then sends the chunk it
// has built up until this point before restarting the process over until the desired set of updates is sent. Note that
// this cannot be implemented for SotW protocols due to the nature of the protocol itself.
// The configuration is ignored if 0 and is disabled by default.
func WithMaxDeltaResponseSize(maxResponseSize int) ADSServerOption {
	return serverOption(func(s *ADSServer) {
		s.maxDeltaResponseSize = maxResponseSize
	})
}

// WithControlPlane causes the server to include the given corev3.ControlPlane instance in each response.
func WithControlPlane(controlPlane *corev3.ControlPlane) ADSServerOption {
	return serverOption(func(s *ADSServer) {
		s.controlPlane = controlPlane
	})
}

// SetRequestRateLimit updates the incoming request rate limit. If the given limit is 0, negative or
// [rate.Inf], it disables the rate limiting.
func (s *ADSServer) SetRequestRateLimit(newLimit rate.Limit) {
	s.requestLimiter.SetLimit(defaultLimit(newLimit))
}

// GetRequestRateLimit returns the current incoming request rate limit.
func (s *ADSServer) GetRequestRateLimit() rate.Limit {
	return s.requestLimiter.Limit()
}

// SetGlobalResponseRateLimit updates the global response rate limit. If the given limit is 0,
// negative or [rate.Inf], it disables the rate limiting.
func (s *ADSServer) SetGlobalResponseRateLimit(newLimit rate.Limit) {
	s.globalLimiter.SetLimit(defaultLimit(newLimit))
}

// GetGlobalResponseRateLimit returns the current global response rate limit.
func (s *ADSServer) GetGlobalResponseRateLimit() rate.Limit {
	return s.globalLimiter.Limit()
}

// SetGranularResponseRateLimit updates the granular response rate limit. If the given limit is 0,
// negative or [rate.Inf], it disables the rate limiting.
func (s *ADSServer) SetGranularResponseRateLimit(newLimit rate.Limit) {
	s.granularLimitLock.Lock()
	defer s.granularLimitLock.Unlock()
	s.granularLimit = defaultLimit(newLimit)
	for l := range s.granularLimiters {
		l.SetLimit(s.granularLimit)
	}
}

func (s *ADSServer) newGranularRateLimiter() *rate.Limiter {
	s.granularLimitLock.Lock()
	defer s.granularLimitLock.Unlock()

	l := rate.NewLimiter(s.granularLimit, 1)
	s.granularLimiters.Add(l)
	return l
}

// GetGranularResponseRateLimit returns the current granular response rate limit.
func (s *ADSServer) GetGranularResponseRateLimit() rate.Limit {
	return s.granularLimit
}

// StreamAggregatedResources is the implementation of the state-of-the-world variant of the ADS protocol.
func (s *ADSServer) StreamAggregatedResources(stream ads.SotWStream) (err error) {
	h := &streamHandler[*ads.SotWDiscoveryRequest, *ads.SotWDiscoveryResponse]{
		server:     s,
		stream:     stream,
		streamType: ads.SotWStreamType,
		newHandler: func(
			ctx context.Context,
			granularLimiter *rate.Limiter,
			statsHandler serverstats.Handler,
			typeUrl string,
			send func(*ads.SotWDiscoveryResponse) error,
		) internal.BatchSubscriptionHandler {
			return internal.NewSotWHandler(
				ctx,
				granularLimiter,
				s.globalLimiter,
				statsHandler,
				typeUrl,
				send,
			)
		},
		newManager: internal.NewSotWSubscriptionManager,
		noSuchTypeResponse: func(req *ads.SotWDiscoveryRequest) *ads.SotWDiscoveryResponse {
			return &ads.SotWDiscoveryResponse{
				Resources: nil,
				TypeUrl:   req.TypeUrl,
				Nonce:     utils.NewNonce(0),
			}
		},
		setControlPlane: func(res *ads.SotWDiscoveryResponse, controlPlane *corev3.ControlPlane) {
			res.ControlPlane = controlPlane
		},
	}

	return h.loop()
}

// DeltaAggregatedResources is the implementation of the delta/incremental variant of the ADS
// protocol.
func (s *ADSServer) DeltaAggregatedResources(stream ads.DeltaStream) (err error) {
	h := &streamHandler[*ads.DeltaDiscoveryRequest, *ads.DeltaDiscoveryResponse]{
		server:     s,
		stream:     stream,
		streamType: ads.DeltaStreamType,
		// TODO: respect the initial_resource_versions map instead of sending everything every time
		newHandler: func(
			ctx context.Context,
			responseLimiter *rate.Limiter,
			statsHandler serverstats.Handler,
			typeUrl string,
			send func(*ads.DeltaDiscoveryResponse) error,
		) internal.BatchSubscriptionHandler {
			return internal.NewDeltaHandler(
				ctx,
				responseLimiter,
				s.globalLimiter,
				statsHandler,
				s.maxDeltaResponseSize,
				typeUrl,
				send,
			)
		},
		newManager: internal.NewDeltaSubscriptionManager,
		noSuchTypeResponse: func(req *ads.DeltaDiscoveryRequest) *ads.DeltaDiscoveryResponse {
			return &ads.DeltaDiscoveryResponse{
				TypeUrl:          req.GetTypeUrl(),
				RemovedResources: req.GetResourceNamesSubscribe(),
				Nonce:            utils.NewNonce(0),
				ControlPlane:     s.controlPlane,
			}
		},
		setControlPlane: func(res *ads.DeltaDiscoveryResponse, controlPlane *corev3.ControlPlane) {
			res.ControlPlane = controlPlane
		},
	}

	return h.loop()
}

type adsDiscoveryRequest interface {
	proto.Message
	GetTypeUrl() string
	GetResponseNonce() string
	GetErrorDetail() *grpcStatus.Status
	GetNode() *ads.Node
}

type adsStream[REQ adsDiscoveryRequest, RES proto.Message] interface {
	Context() context.Context
	Recv() (REQ, error)
	Send(RES) error
}

// streamHandler captures the various elements required to handle an ADS stream.
type streamHandler[REQ adsDiscoveryRequest, RES proto.Message] struct {
	sendLock sync.Mutex

	server     *ADSServer
	stream     adsStream[REQ, RES]
	streamCtx  context.Context
	streamType ads.StreamType
	newHandler func(
		ctx context.Context,
		granularLimiter *rate.Limiter,
		statsHandler serverstats.Handler,
		typeUrl string,
		send func(RES) error,
	) internal.BatchSubscriptionHandler
	newManager func(
		ctx context.Context,
		locator internal.ResourceLocator,
		typeURL string,
		handler internal.BatchSubscriptionHandler,
	) internal.SubscriptionManager[REQ]
	noSuchTypeResponse     func(req REQ) RES
	setControlPlane        func(res RES, controlPlane *corev3.ControlPlane)
	aggregateSubscriptions map[string]internal.SubscriptionManager[REQ]
}

// send invokes Send on the stream with the given response, returning an error if Send returns an error. Crucially,
// Send can only be invoked by one goroutine at a time, so this function protects the invocation of Send with sendLock.
func (h *streamHandler[REQ, RES]) send(res RES) (err error) {
	if h.server.statsHandler != nil {
		start := time.Now()
		defer func() {
			h.server.statsHandler.HandleServerEvent(h.streamCtx, &serverstats.ResponseSent{
				Res:      res,
				Duration: time.Since(start),
			})
		}()
	}

	h.sendLock.Lock()
	defer h.sendLock.Unlock()
	h.setControlPlane(res, h.server.controlPlane)
	slog.DebugContext(h.streamCtx, "Sending", "msg", res)
	return h.stream.Send(res)
}

func (h *streamHandler[REQ, RES]) recv() (REQ, error) {
	// TODO: Introduce a timeout on receiving the first request. In order to keep a stream alive, gRPC needs to send
	//  keepalives etc. If a client never sends the first request to identify itself etc it should eventually be kicked
	//  since it is wasting resources.
	return h.stream.Recv()
}

// getSubscriptionManager returns a [internal.SubscriptionManager] for the given type url. If the
// type is not supported (checked via the ResourceLocator), this function returns nil, false. This
// indicates that the given type is unknown by the system and the request should be ignored.
// Subsequent calls to this function with the same type url always return the same subscription
// manager.
func (h *streamHandler[REQ, RES]) getSubscriptionManager(
	typeURL string,
) (internal.SubscriptionManager[REQ], bool) {
	// Manager was already created, return immediately.
	if manager, ok := h.aggregateSubscriptions[typeURL]; ok {
		return manager, true
	}

	if !h.server.locator.IsTypeSupported(h.streamCtx, typeURL) {
		return nil, false
	}

	manager := h.newManager(
		h.streamCtx,
		h.server.locator,
		typeURL,
		h.newHandler(
			h.streamCtx,
			h.server.newGranularRateLimiter(),
			h.server.statsHandler,
			typeURL,
			h.send,
		),
	)

	h.aggregateSubscriptions[typeURL] = manager
	return manager, true
}

func (h *streamHandler[REQ, RES]) loop() error {
	for {
		req, err := h.recv()
		if err != nil {
			return err
		}

		// initialize the stream context with the node on the first request
		if h.streamCtx == nil {
			h.streamCtx = context.WithValue(h.stream.Context(), nodeContextKey{}, req.GetNode())
		}

		err = h.handleRequest(req)
		if err != nil {
			return err
		}
	}
}

func (h *streamHandler[REQ, RES]) handleRequest(req REQ) (err error) {
	slog.DebugContext(h.streamCtx, "Received request", "req", req)

	var stat *serverstats.RequestReceived
	if h.server.statsHandler != nil {
		start := time.Now()
		stat = &serverstats.RequestReceived{Req: req}
		defer func() {
			stat.Duration = time.Since(start)
			h.server.statsHandler.HandleServerEvent(h.streamCtx, stat)
		}()
	}

	err = h.server.requestLimiter.Wait(h.streamCtx)
	if err != nil {
		return err
	}

	if h.aggregateSubscriptions == nil {
		h.aggregateSubscriptions = make(map[string]internal.SubscriptionManager[REQ])
	}

	typeURL := req.GetTypeUrl()
	manager, ok := h.getSubscriptionManager(typeURL)
	if !ok {
		slog.WarnContext(h.streamCtx, "Ignoring unknown requested type", "typeURL", typeURL, "req", req)
		if stat != nil {
			stat.IsRequestedTypeUnknown = true
		}
		return h.send(h.noSuchTypeResponse(req))
	}

	switch {
	case req.GetErrorDetail() != nil:
		slog.WarnContext(h.streamCtx, "Got client NACK", "req", req)
		if stat != nil {
			stat.IsNACK = true
		}
	case req.GetResponseNonce() != "":
		slog.DebugContext(h.streamCtx, "ACKED", "req", req)
		if stat != nil {
			stat.IsACK = true
		}
	}

	manager.ProcessSubscriptions(req)

	return nil

}

// The ResourceLocator abstracts away the business logic used to locate resources and subscribe to
// them. For example, while Subscribe is trivially implemented with a [Cache] which only serves
// static predetermined resources, it could be implemented to instead generate a resource definition
// on the fly, based on the client's attributes. Alternatively, some attribute in the client's
// [ads.Node] may show that the client does not support IPv6 and should instead be shown IPv4
// addresses in the [ads.Endpoint] response.
//
// Many users of this library may also choose to implement a
// [google.golang.org/grpc.StreamServerInterceptor] to populate additional values in the stream's
// context, which can be used to better identify the client. However, for convenience, the [ads.Node]
// provided in the request will always be provided in the stream context, and can be accessed with
// [NodeFromContext].
type ResourceLocator interface {
	// IsTypeSupported is used to check whether the given client supports the requested type.
	IsTypeSupported(streamCtx context.Context, typeURL string) bool
	// Subscribe subscribes the given handler to the desired resource. The returned function should
	// execute the unsubscription to the resource. It is guaranteed that the desired type has been
	// checked via IsTypeSupported, and that therefore it is supported.
	Subscribe(
		streamCtx context.Context,
		typeURL, resourceName string,
		handler ads.RawSubscriptionHandler,
	) (unsubscribe func())
	// Resubscribe will be called whenever a client resubscribes to a given resource. The xDS protocol
	// dictates that re-subscribing to a resource should cause the server to re-send the resource. Note
	// that implementations of this interface that leverage a [Cache] already support this behavior
	// out-of-the-box.
	Resubscribe(
		streamCtx context.Context,
		typeURL, resourceName string,
		handler ads.RawSubscriptionHandler,
	)
}

type nodeContextKey struct{}

// NodeFromContext returns the [ads.Node] in the given context, if it exists. Note that the
// [ADSServer] will always provide the Node in the context when invoking methods on the
// [ResourceLocator].
func NodeFromContext(streamCtx context.Context) (*ads.Node, bool) {
	node, ok := streamCtx.Value(nodeContextKey{}).(*ads.Node)
	return node, ok
}
