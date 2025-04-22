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

// QueuedResource contains all the metadata about a resource about to be sent by the server. Will be
// the 0-value for any resource that was provided via the initial_resource_versions field which was
// not explicitly subscribed to and did not exist.
type QueuedResource struct {
	// The resource itself, nil if the resource is being deleted.
	Resource *ads.RawResource
	// The metadata for the resource and subscription.
	Metadata ads.SubscriptionMetadata
}

// ResourcesQueued contains the stats for resources about to be sent.
type ResourcesQueued struct {
	// Iterates over all the resources about to be sent.
	Resources map[string]QueuedResource
}

func (s *ResourcesQueued) isServerEvent() {}

// IRVMatchedResource represents stats for resources that are not sent by the server
// because their version matches the `initial_resource_versions` provided in the client request.
type IRVMatchedResource struct {
	// The name of the resource
	ResourceName string
	// The resource itself, nil if the resource is being deleted.
	Resource *ads.RawResource
}

func (s *IRVMatchedResource) isServerEvent() {}

// UnknownResourceRequested indicates whether a resource that was subscribed never existed. This
// should be rare, and can be indicative of a client-side bug.
type UnknownResourceRequested struct {
	// The resource's type.
	TypeURL string
	// The resource's name.
	ResourceName string
}

func (s *UnknownResourceRequested) isServerEvent() {}
