/*
Package ads provides a set of utilities and definitions around the Aggregated Discovery Service xDS
protocol (ADS), such as convenient type aliases, constants and core definitions.
*/
package ads

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tls "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtime "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	types "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Alias to xDS types, for convenience and brevity.
type (
	// Server is the core interface that needs to be implemented by an xDS control plane. The
	// "Aggregated" service (i.e. ADS, the name of this package) service is type agnostic, the desired
	// type is specified in the request. This avoids the need for clients to open multiple streams when
	// requesting different types, along with not needing new service definitions such as
	// [github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3.EndpointDiscoveryServiceServer].
	Server = discovery.AggregatedDiscoveryServiceServer
	// Node is an alias for the client information included in both Delta and SotW requests [core.Node].
	Node = core.Node

	// SotWClient is an alias for the state-of-the-world client type
	// [discovery.AggregatedDiscoveryService_StreamAggregatedResourcesClient].
	SotWClient = discovery.AggregatedDiscoveryService_StreamAggregatedResourcesClient
	// SotWStream is an alias for the state-of-the-world stream type for the server
	// [discovery.AggregatedDiscoveryService_StreamAggregatedResourcesServer].
	SotWStream = discovery.AggregatedDiscoveryService_StreamAggregatedResourcesServer
	// SotWDiscoveryRequest is an alias for the state-of-the-world request type
	// [discovery.DiscoveryRequest].
	SotWDiscoveryRequest = discovery.DiscoveryRequest
	// SotWDiscoveryResponse is an alias for the state-of-the-world response type
	// [discovery.DiscoveryResponse].
	SotWDiscoveryResponse = discovery.DiscoveryResponse

	// DeltaClient is an alias for the delta client type
	// [discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesClient].
	DeltaClient = discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesClient
	// DeltaStream is an alias for the delta (also known as incremental) stream type for the server
	// [discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer].
	DeltaStream = discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer
	// DeltaDiscoveryRequest is an alias for the delta request type [discovery.DeltaDiscoveryRequest].
	DeltaDiscoveryRequest = discovery.DeltaDiscoveryRequest
	// DeltaDiscoveryResponse is an alias for the delta response type [discovery.DeltaDiscoveryResponse].
	DeltaDiscoveryResponse = discovery.DeltaDiscoveryResponse

	// RawResource is a type alias for the core ADS type [discovery.Resource]. It is "raw" only in
	// contrast to the [*Resource] type defined in this package, which preserves the underlying
	// resource's type as a generic parameter.
	RawResource = discovery.Resource
)

// NewResource is a convenience method for creating a new [*Resource].
func NewResource[T proto.Message](name, version string, t T) *Resource[T] {
	return &Resource[T]{
		Name:     name,
		Version:  version,
		Resource: t,
	}
}

// Resource is the typed equivalent of [RawResource] in that it preserves the underlying resource's
// type at compile time. It defines the same fields as [RawResource] (except for unsupported fields
// such as [ads.RawResource.Aliases]), and can be trivially serialized to a [RawResource] with
// Marshal. It is undefined behavior to modify a [Resource] after creation.
type Resource[T proto.Message] struct {
	Name         string
	Version      string
	Resource     T
	Ttl          *durationpb.Duration
	CacheControl *discovery.Resource_CacheControl
	Metadata     *core.Metadata

	marshalOnce sync.Once
	marshaled   *RawResource
	marshalErr  error
}

// Marshal returns the serialized version of this Resource. Note that this result is cached, and can
// be called repeatedly and from multiple goroutines.
func (r *Resource[T]) Marshal() (*RawResource, error) {
	r.marshalOnce.Do(func() {
		var out *anypb.Any
		out, r.marshalErr = anypb.New(r.Resource)
		if r.marshalErr != nil {
			// This shouldn't really ever happen, especially when serializing to Any
			slog.Error(
				"Failed to serialize proto",
				"msg", r.Resource,
				"type", string(r.Resource.ProtoReflect().Descriptor().FullName()),
				"err", r.marshalErr,
			)
			return
		}

		r.marshaled = &RawResource{
			Name:         r.Name,
			Version:      r.Version,
			Resource:     out,
			Ttl:          r.Ttl,
			CacheControl: r.CacheControl,
			Metadata:     r.Metadata,
		}
	})
	return r.marshaled, r.marshalErr
}

// TypeURL returns the underlying resource's type URL.
func (r *Resource[T]) TypeURL() string {
	var t T
	return types.APITypePrefix + string(t.ProtoReflect().Descriptor().FullName())
}

// UnmarshalRawResource unmarshals the given RawResource and returns a Resource of the corresponding
// type. Resource.Marshal on the returned Resource will return the given RawResource instead of
// re-serializing the resource.
func UnmarshalRawResource[T proto.Message](raw *RawResource) (*Resource[T], error) {
	m, err := raw.Resource.UnmarshalNew()
	if err != nil {
		return nil, err
	}

	r := &Resource[T]{
		Name:         raw.Name,
		Version:      raw.Version,
		Resource:     m.(T),
		Ttl:          raw.Ttl,
		CacheControl: raw.CacheControl,
		Metadata:     raw.Metadata,
	}
	// Set marshaled using marshalOnce, otherwise the once will not be set and subsequent calls to
	// Marshal will serialize the resource, overwriting the field.
	r.marshalOnce.Do(func() {
		r.marshaled = raw
	})

	return r, nil
}

const (
	// WildcardSubscription is a special resource name that triggers a subscription to all resources of a
	// given type.
	WildcardSubscription = "*"
	// XDSTPScheme is the prefix for which all resource URNs (as defined in the [TP1 proposal]) start.
	//
	// [TP1 proposal]: https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#uri-based-xds-resource-names
	XDSTPScheme = "xdstp://"
)

// A SubscriptionHandler will receive notifications for the cache entries it has subscribed to using
// RawCache.Subscribe. Note that it is imperative that implementations be hashable as it will be
// stored as the key to a map (unhashable types include slices and functions).
type SubscriptionHandler[T proto.Message] interface {
	// Notify is invoked when the given entry is modified. A deletion is denoted with a nil resource. The given time
	// parameters provides the time at which the client subscribed to the resource and the time at which the
	// modification happened respectively. Note that if an entry is modified repeatedly at a high rate, Notify will not
	// be invoked for all intermediate versions, though it will always *eventually* be invoked with the final version.
	Notify(name string, r *Resource[T], metadata SubscriptionMetadata)
}

// RawSubscriptionHandler is the untyped equivalent of SubscriptionHandler.
type RawSubscriptionHandler interface {
	// Notify is the untyped equivalent of SubscriptionHandler.Notify.
	Notify(name string, r *RawResource, metadata SubscriptionMetadata)
	// ResourceMarshalError is invoked whenever a resource cannot be marshaled. This should be extremely
	// rare and requires immediate attention. When a resource cannot be marshaled, the notification will
	// be dropped and Notify will not be invoked.
	ResourceMarshalError(name string, resource proto.Message, err error)
}

// SubscriptionMetadata contains metadata about the subscription that triggered the Notify call on
// the [RawSubscriptionHandler] or [SubscriptionHandler].
type SubscriptionMetadata struct {
	// The time at which the resource was subscribed to
	SubscribedAt time.Time
	// The time at which the resource was modified (can be UnknownModifiedTime if the modification time is unknown)
	ModifiedAt time.Time
	// The time at which the update to the resource was received by the cache (i.e. when [Cache.Set] was
	// called, not strictly when the server actually received the update). If this is metadata is for a
	// subscription to a resource that does not yet exist, will be UnknownModifiedTime.
	CachedAt time.Time
	// The current priority index of the value. Will be 0 unless the backing cache was created with
	// [NewPrioritizedCache], [NewPrioritizedAggregateCache] or
	// [NewPrioritizedAggregateCachesByClientTypes]. If this metadata is for a subscription to a resource
	// that has been deleted (or does not yet exist), Priority will be the last valid index priority
	// index (because a resource is only considered deleted once it has been deleted from all cache
	// sources). For example, if the cache was created like this:
	//	NewPrioritizedCache(10)
	// Then the last valid index is 9, since the slice of cache objects returned is of length 10.
	Priority int
	// The glob collection this resource belongs to, empty if it does not belong to any collections.
	GlobCollectionURL string
}

// These aliases mirror the constants declared in [github.com/envoyproxy/go-control-plane/pkg/resource/v3]
type (
	Endpoint        = endpoint.ClusterLoadAssignment
	LbEndpoint      = endpoint.LbEndpoint
	Cluster         = cluster.Cluster
	Route           = route.RouteConfiguration
	ScopedRoute     = route.ScopedRouteConfiguration
	VirtualHost     = route.VirtualHost
	Listener        = listener.Listener
	Secret          = tls.Secret
	ExtensionConfig = core.TypedExtensionConfig
	Runtime         = runtime.Runtime
)

// StreamType is an enum representing the different possible ADS stream types, SotW and Delta.
type StreamType int

const (
	// UnknownStreamType is the 0-value, unknown stream type.
	UnknownStreamType StreamType = iota
	// DeltaStreamType is the delta/incremental variant of the ADS protocol.
	DeltaStreamType
	// SotWStreamType is the state-of-the-world variant of the ADS protocol.
	SotWStreamType
)

var streamTypeStrings = [...]string{"UNKNOWN", "Delta", "SotW"}

func (t StreamType) String() string {
	return streamTypeStrings[t]
}

// StreamTypes is an array containing the valid [StreamType] values.
var StreamTypes = [...]StreamType{UnknownStreamType, DeltaStreamType, SotWStreamType}

// LookupStreamTypeByRPCMethod checks whether the given RPC method string (usually acquired from
// [google.golang.org/grpc.StreamServerInfo.FullMethod] in the context of a server stream
// interceptor) is either [SotWStreamType] or [DeltaStreamType]. Returns ([UnknownStreamType], false)
// if it is neither.
func LookupStreamTypeByRPCMethod(rpcMethod string) (StreamType, bool) {
	switch rpcMethod {
	case "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources":
		return SotWStreamType, true
	case "/envoy.service.discovery.v3.AggregatedDiscoveryService/DeltaAggregatedResources":
		return DeltaStreamType, true
	default:
		return UnknownStreamType, false
	}
}

var (
	invalidNonceEncodingErr = errors.New("nonce isn't in hex encoding")
	invalidNonceLengthErr   = errors.New("decoded nonce did not have expected length")
)

// ParseRemainingChunksFromNonce checks whether the Diderot server implementation chunked the delta
// responses because not all resources could fit in the same response without going over the default
// max gRPC message size of 4MB. A nonce from Diderot always starts with the 64-bit nanosecond
// timestamp of when the response was generated on the server. Then the number of remaining chunks as
// a 32-bit integer. The sequence of integers is binary encoded with [binary.BigEndian] then hex
// encoded. If the given nonce does not match the expected format, this function simply returns 0
// along with an error describing why it does not match. If the error isn't nil, it means the nonce
// was not created by a Diderot server implementation, and therefore does not contain the expected
// information.
func ParseRemainingChunksFromNonce(nonce string) (remainingChunks int, err error) {
	decoded, err := hex.DecodeString(nonce)
	if err != nil {
		return 0, invalidNonceEncodingErr
	}

	if len(decoded) != 12 {
		return 0, invalidNonceLengthErr
	}

	return int(binary.BigEndian.Uint32(decoded[8:12])), nil
}
