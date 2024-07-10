package serverstats

import (
	"context"
	"time"

	"github.com/linkedin/diderot/ads"
	"google.golang.org/protobuf/proto"
)

// Handler will be invoked with an event of the corresponding type when said event occurs.
type Handler interface {
	HandleServerEvent(context.Context, Event)
}

// Event contains information about a specific event that happened in the server.
type Event interface {
	isServerEvent()
}

// RequestReceived contains the stats of a request received by the server.
type RequestReceived struct {
	// The received request, either [ads.SotWDiscoveryRequest] or [ads.DeltaDiscoveryRequest].
	Req proto.Message
	// True if the client requested a type that is not supported, determined by the ResourceLocator.
	IsRequestedTypeUnknown bool
	// Whether the request is an ACK
	IsACK bool
	// Whether the request is a NACK. Note that this is an important stat that requires immediate human
	// intervention.
	IsNACK bool
	// The given duration represents the time it took to handle the request, i.e. validating it and
	// processing its subscriptions if necessary. It does not include the time for any of the
	// resources to be sent in a response.
	Duration time.Duration
}

func (s *RequestReceived) isServerEvent() {}

// ResponseSent contains the stats of a response sent by the server.
type ResponseSent struct {
	// The response sent, either [ads.SotWDiscoveryResponse] or [ads.DeltaDiscoveryResponse].
	Res proto.Message
	// How long the Send operation took. This includes any time added by flow-control.
	Duration time.Duration
}

func (s *ResponseSent) isServerEvent() {}

// TimeInGlobalRateLimiter contains the stats of the time spent in the global rate limiter.
type TimeInGlobalRateLimiter struct {
	// How long the server waited for the global rate limiter to clear.
	Duration time.Duration
}

func (s *TimeInGlobalRateLimiter) isServerEvent() {}

// ResourceMarshalError contains the stats for a resource that could not be marshaled. This
// should be extremely rare and requires immediate attention.
type ResourceMarshalError struct {
	// The name of the resource that could not be marshaled.
	ResourceName string
	// The resource that could not be marshaled.
	Resource proto.Message
	// The marshaling error.
	Err error
}

func (s *ResourceMarshalError) isServerEvent() {}

// ResourceOverMaxSize contains the stats for a critical error that signals a resource will
// never be received by clients that are subscribed to it. It likely requires immediate human
// intervention.
type ResourceOverMaxSize struct {
	// The resource that could not be sent.
	Resource *ads.RawResource
	// The encoded resource size.
	ResourceSize int
	// The maximum resource size (usually 4MB, gRPC's default max message size).
	MaxResourceSize int
}

func (s *ResourceOverMaxSize) isServerEvent() {}

// ResourceQueued contains the stats for a resource entering the send queue.
type ResourceQueued struct {
	// The name of the resource
	ResourceName string
	// The resource itself, nil if the resource is being deleted.
	Resource *ads.RawResource
	// The metadata for the resource and subscription.
	Metadata ads.SubscriptionMetadata
	// Indicates whether the resource existed at all and is being deleted, or whether the client
	// subscribed to a resource that never existed. This should be rare, and can be indicative of a
	// client-side bug.
	ResourceExists bool
}

func (s *ResourceQueued) isServerEvent() {}
