package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/linkedin/diderot"
	"github.com/linkedin/diderot/ads"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8080))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	// populate your ops
	grpcServer := grpc.NewServer(opts...)

	// Use a very simple ResourceLocator that only supports a limited set of types (namely LDS -> RDS -> CDS -> EDS).
	locator := NewSimpleResourceLocator(ListenerType, RouteType, ClusterType, EndpointType)

	go PopulateCaches(locator)

	hostname, _ := os.Hostname()

	adsServer := diderot.NewADSServer(locator,
		// Send max 10k responses per second.
		diderot.WithGlobalResponseRateLimit(10_000),
		// Send max one response per type per client every 500ms, to not overload clients.
		diderot.WithGranularResponseRateLimit(2),
		// Process max 1k requests per second.
		diderot.WithRequestRateLimit(1000),
		diderot.WithControlPlane(&corev3.ControlPlane{Identifier: hostname}),
	)
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, adsServer)

	grpcServer.Serve(lis)
}

var (
	ListenerType = diderot.TypeOf[*ads.Listener]()
	RouteType    = diderot.TypeOf[*ads.Route]()
	ClusterType  = diderot.TypeOf[*ads.Cluster]()
	EndpointType = diderot.TypeOf[*ads.Endpoint]()
)

// SimpleResourceLocator is a bare-bones [diderot.ResourceLocator] that provides the bare minimum
// functionality.
type SimpleResourceLocator map[string]diderot.RawCache

func (sl SimpleResourceLocator) IsTypeSupported(streamCtx context.Context, typeURL string) bool {
	_, ok := sl[typeURL]
	return ok
}

func (sl SimpleResourceLocator) Subscribe(
	streamCtx context.Context,
	typeURL, resourceName string,
	handler ads.RawSubscriptionHandler,
) (unsubscribe func()) {
	c := sl[typeURL]
	diderot.Subscribe(c, resourceName, handler)
	return func() {
		diderot.Unsubscribe(c, resourceName, handler)
	}
}

func (sl SimpleResourceLocator) Resubscribe(
	streamCtx context.Context,
	typeURL, resourceName string,
	handler ads.RawSubscriptionHandler,
) {
	diderot.Subscribe(sl[typeURL], resourceName, handler)
}

// getCache extracts a typed [diderot.Cache] from the given [SimpleResourceLocator].
func getCache[T proto.Message](sl SimpleResourceLocator) diderot.Cache[T] {
	return sl[diderot.TypeOf[T]().URL()].(diderot.Cache[T])
}

func (sl SimpleResourceLocator) GetListenerCache() diderot.Cache[*ads.Listener] {
	return getCache[*ads.Listener](sl)
}

func (sl SimpleResourceLocator) GetRouteCache() diderot.Cache[*ads.Route] {
	return getCache[*ads.Route](sl)
}

func (sl SimpleResourceLocator) GetClusterCache() diderot.Cache[*ads.Cluster] {
	return getCache[*ads.Cluster](sl)
}

func (sl SimpleResourceLocator) GetEndpointCache() diderot.Cache[*ads.Endpoint] {
	return getCache[*ads.Endpoint](sl)
}

func NewSimpleResourceLocator(types ...diderot.Type) SimpleResourceLocator {
	sl := make(SimpleResourceLocator)
	for _, t := range types {
		sl[t.URL()] = t.NewCache()
	}
	return sl
}

func PopulateCaches(locator SimpleResourceLocator) {
	// this is where the business logic of populating the caches should happen. For example, you can read
	// the resource definitions from disk, listen to ZK, etc...
}
